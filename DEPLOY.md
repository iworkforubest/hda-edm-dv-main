# DEPLOY.md — Instructions for Claude Code

> ## STALE RUNBOOK -- DO NOT RUN THE COMMANDS BELOW AS WRITTEN
>
> **This runbook is EU-first and has NOT been updated for the usnc_tds retarget.**
> `weu_tds` is **not used** by this project; `usnc_tds` is the development
> environment. Every `-t weu_tds` / `--profile hfig-weu-tds` command in the
> phases below is stale and must not be run as written. Running one lands
> deployed artefacts in the wrong regional lake -- exactly the failure
> `checks/preflight_target.py` exists to catch.
>
> The phase sequencing itself (Phase 0d, 3, 5, 7 and the promotion order in
> Phase 7) is EU-ordered and is **pending a separate rewrite** -- this is not a
> simple find-and-replace of `weu_tds` to `usnc_tds`.
>
> **Before running anything in this file**, read
> `docs/superpowers/specs/2026-08-24-usnc-tds-retarget-design.md` for the
> current target model, and substitute the correct target/profile for every
> deploy command below.

**Accelerator v0.2.0.** Read `CHANGELOG.md` for what is still open before you start.

You are deploying the HFIG Data Vault accelerator to a Databricks workspace. Read this
file completely before running anything.

## STOP — check you have the whole project

This file is one of **35 files in 7 directories**. If you are looking at it in a folder
alongside a handful of loose `.py`, `.sql` and `.yml` files, someone downloaded the files
individually instead of extracting the archive, and **nothing here will work**: the
bundle uses relative paths (`resources/vault_job.yml` refers to `../checks/`), so a flat
folder cannot be deployed regardless of what it contains.

```bash
unzip hfig-dv-accelerator-v0.2.zip
cd hfig-dv-accelerator-v0.2
uv run python verify_repo.py          # first thing it prints is a layout check
```

Expect `[0] layout: the project tree is intact`. If instead you get
`LAYOUT CHECK FAILED`, it lists exactly what is missing — fix that before reading on.

This is a **production data platform handling worker and payroll data**. Several steps
are irreversible. The runbook is written so that the irreversible ones come last and
each is preceded by a gate that can stop you.

---

## Rules for this deployment

1. **Never run `--full-refresh`.** Not on a pipeline, not on a dbt model, not "just to
   clean up". It rebuilds insert-only history from whatever is in staging today. The
   loss is silent, total and unrecoverable. If a pipeline seems to need it, stop and
   ask the human.
2. **Never `ALTER`, `UPDATE`, `DELETE` or `MERGE` a vault table.** If a load produced
   wrong rows, the fix is a new row or a quarantine entry, never an edit.
3. **Deploy to `weu_tds` first.** Do not touch `weu` (production) until every gate in
   Phase 5 is green and a human has authorised it.
4. **Do not deploy the other six workspaces.** `uks`, `uks_tds`, `usnc`, `usnc_tds`,
   `aue`, `aue_tds` are declared and deliberately out of scope for this session. Fan-out
   requires the conformance gate (Phase 7) and a human decision.
5. **Stop and report rather than work around a failing gate.** Every gate in here
   exists because the failure it catches is expensive or silent. A gate that fails is
   information, not an obstacle. Do not add `--force`, do not comment out a check, do
   not relax an expectation to make a run go green.
6. **If a Databricks API behaves differently from what this repo assumes**, record the
   difference, fix the code, and re-run the verification. Several API shapes are marked
   VERIFY in Phase 3 precisely because they were written without a live workspace.

---

## Phase 0 — Prerequisites

Confirm each of these before proceeding. Report anything missing rather than guessing.

```bash
databricks --version          # CLI v0.230.0 or later (bundle + aitools support)
python3 --version             # 3.11+ (pyproject requires-python, and what CI tests)
git status                    # clean tree; you will be committing fixes
```

Ask the human for, and confirm you have:

| Needed | Why |
|---|---|
| Confirmation that the legacy "Datalake" workspace is UC-attached | if not, `weu` is the wrong target |
| An authenticated CLI profile for that workspace | `databricks auth login` or `.databrickscfg` |
| Unity Catalog metastore admin, or someone who is | Phase 6 creates functions and grants |
| The catalog names to use | bundle defaults are `hfig_<code>_tds` / `hfig_<code>` — confirm, do not assume |
| Whether Bronze tables already exist | the metadata references placeholder `hfig_eu.bronze.*` names (placeholders) |
| Account-level group names | `apply_masks.sql` uses placeholders (`hfig_worker_pii_reader` etc.) |
| Confirmation this is registered via WISE intake | see Phase 8 |

If Bronze tables do not exist yet, **stop**. This accelerator loads Silver from Bronze;
it does not create the Bronze feeds. Report which tables are missing.

### 0a — The topology, and discovering the URLs

**8 workspaces: 4 lakes x 2 environments.** TDS is a separate workspace, not a catalog.

| Lake | Code | Env | Workspace | Workspace id | EEA |
|---|---|---|---|---|---|
| EU | `weu` | PROD | **"Datalake"** (legacy name) | 4750792675027163 | yes |
| EU | `weu_tds` | TDS | `db-weu-datalakehouse-tds` | 7405615198748199 | yes |
| UK | `uks` | PROD | `db-uks-datalakehouse` | 585699997116621 | adequacy |
| UK | `uks_tds` | TDS | `db-uks-datalakehouse-tds` | 718050136221554 | adequacy |
| US | `usnc` | PROD | `db-usnc-datalakehouse` | 1543126086586625 | **no** |
| US | `usnc_tds` | TDS | `db-usnc-datalakehouse-tds` | 2593897084138079 | **no** |
| APAC | `aue` | PROD | `db-aue-datalakehouse` | 270953068323345 | **no** |
| APAC | `aue_tds` | TDS | `db-aue-datalakehouse-tds` | 2093214595152673 | **no** |

**All 8 exist.** Note the EU prod alias: the workspace is named **"Datalake"**, not
`db-weu-datalakehouse`. It predates this estate. The target is still `weu` for
consistency with the other three lakes, and preflight matches on **host, not name**, so
the alias is safe — it is recorded in `databricks.yml` so nobody "corrects" it.

**Only 4 metastores, not 8.** A Unity Catalog account has one metastore per region, so
PROD and TDS in a region *share* it. `hfig_weu` and `hfig_weu_tds` are catalogs in the
same West Europe metastore, visible from both workspaces by default. Separate catalogs
are **not** isolation — see Phase 0d.

The URLs are already committed as defaults in `databricks.yml`. A workspace URL is an
identifier, not a credential; committing it makes the target-to-lake mapping reviewable
in a pull request instead of typed on a command line. Credentials come from CLI profiles
and never appear in the repo.

Bundle target names mirror the workspace names: `weu_tds`, `weu`, `uks_tds`, `uks`,
`usnc_tds`, `usnc`, `aue_tds`, `aue`, plus a `dev` target for individual work.

**In this session you deploy to `weu_tds` only.** Everything else is later and needs a
human decision.

### 0b — Set up one profile per workspace, then prove it

Authenticate a profile per workspace, named as `checks/conformance_check.py` expects:
`hfig-weu-tds`, `hfig-uks-tds`, `hfig-usnc-tds`, `hfig-aue-tds`, and the prod equivalents.

```bash
databricks auth login --host https://adb-7405615198748199.19.azuredatabricks.net \
  --profile hfig-weu-tds
databricks auth profiles                     # confirm what exists
```

**Before every single deploy, run preflight.** With 8 near-identical workspaces this is
the realistic failure mode — not a bug in the model, but artefacts landing in the wrong
lake because a profile was left pointing elsewhere.

```bash
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
```

**Expected:** `PREFLIGHT PASSED: authenticated workspace matches the target.` It prints
both workspace ids on failure so the mismatch is unambiguous. If it fails, fix the
profile or the `--target` — **never the bundle**.

### 0d — The legacy EU prod workspace, and metastore isolation

Two things specific to this estate. Both are checks, not assumptions.

**1. "Datalake" is a pre-existing workspace.** It may hold catalogs, schemas, jobs and
legacy Hive tables this bundle did not create. Before deploying to `weu`:

```bash
uv run python checks/preflight_target.py --target weu --profile hfig-weu

# what is already there?
databricks catalogs list                     # existing catalogs -- is hfig_weu free?
databricks schemas list hive_metastore       # legacy Hive tables still in use?
databricks jobs list                         # existing jobs -- will root_path collide?
databricks metastores current                # is this workspace UC-attached at all?
```

**If `metastores current` returns nothing, stop.** An older workspace may not be
attached to a Unity Catalog metastore, in which case none of the governance in this
repo can be applied and the deploy target is wrong. Report it.

**If `hfig_weu` already exists and was not created by this bundle**, stop and report.
Do not adopt someone else's catalog — pick a name that is unambiguously this estate's.

**2. PROD and TDS share the West Europe metastore.** This is the finding that matters
most on this page. Confirm it, because the whole isolation design depends on it:

```bash
# run in BOTH the weu and weu_tds workspaces -- expect the SAME metastore id
databricks metastores current --profile hfig-weu
databricks metastores current --profile hfig-weu-tds
```

If the ids match, then without catalog binding **production worker and payroll data is
queryable from the TDS workspace** by anyone holding the catalog grant — including from
a notebook in a staging pipeline. `governance/apply_masks.sql` now sets isolation mode
as its first statement, but **the binding itself is an API call, not SQL**:

```bash
databricks workspace-bindings update-bindings catalog hfig_weu \
  --json '{"add":[{"workspace_id":4750792675027163,"binding_type":"BINDING_TYPE_READ_WRITE"}]}'
```

Then verify from the TDS workspace that `hfig_weu` is **no longer visible**:

```bash
databricks catalogs list --profile hfig-weu-tds    # hfig_weu must be ABSENT
```

Report that result explicitly. "Isolation configured" without that negative check is
an assumption, not a control.

### 0c — Discover what exists inside the workspace

```bash

# does the catalog the bundle assumes already exist?
databricks catalogs list                     # is there a hfig_weu / hfig_weu_tds?
databricks schemas list <catalog>            # does bronze / silver_vault exist?

# the Bronze tables the metadata references
databricks tables list <catalog> bronze

# a serverless SQL warehouse for the conformance gate later
databricks warehouses list

# existing pipelines, in case Bronze ingestion is already built
databricks pipelines list-pipelines
```

Then reconcile what you found against the bundle and **report any mismatch rather than
creating things to fit**:

| Bundle assumes | Confirm |
|---|---|
| catalog `hfig_weu_tds` | do these exist, or is there an established naming convention (e.g. derived from `datalakehouse-tds`)? If the latter, change the `catalog` variable in the target, not the metadata. |
| schemas `bronze`, `silver_vault`, `gold`, `governance` | exist or need creating |
| Bronze tables `hfig_eu.bronze.striive_*` in `metadata/entities/*.yml` | **these are placeholder names.** Update every `bronze_table:` to the real fully-qualified table. |
| `-tds` = TEST-DEV-STAGING, confirmed | already reflected: `weu_tds` and `weu` are separate targets with separate hosts |

**The metadata files currently reference `hfig_eu.bronze.*` (placeholders), which will not exist.**
Updating those `bronze_table:` values to real tables is a prerequisite, not a detail —
a flow reading a table that is not there fails the pipeline at definition time, and takes
every other flow down with it. Do it before Phase 4 and re-run `uv run python verify_repo.py`.

**Or declare the binding inactive, which is the supported answer when the source genuinely
is not in this lake** (parent decision D5). The `active_sources` variable on a target lists
the bindings that actually load there; anything omitted still gets its streaming table —
so the inventory, and cross-region conformance, stay identical — but no append flow, and
therefore nothing to fail. An entry is a bare source name (`GP_US`) or `entity/SOURCE`
(`journal_line/UKG_US`) for a source name that is real on some entities and a placeholder
on others. An empty value means every binding is active. A name matching no binding is
refused at pipeline start rather than ignored. See `databricks.yml`'s `active_sources`
variable and `usnc_tds`, which declares exactly the bindings present in
`01_usnc_bronze_dev`.

**Before you deploy a SECOND lake, read section 2b of
`docs/superpowers/specs/2026-08-24-subproject3a-gp-journals-design.md`.** An inactive
table is created from its ghost flow alone — correct name, eight columns (hash key plus
the seven system columns; DEF-25 added cdc_op) — so D5's table
*inventory* guarantee holds while its column *layout* guarantee does not, and
`conformance_check.py` compares `information_schema.columns`. That is a recorded design
decision to take, not a gate failure to debug. Nothing fails today: conformance needs two
lakes and only `usnc_tds` exists.

Write the discovered values into `databricks.yml` variable defaults, or supply them at
deploy time:

```bash
export BUNDLE_VAR_weu_tds_host="https://adb-<id>.<n>.azuredatabricks.net"
databricks bundle validate -t weu_tds
```

---

## Phase 1 — Verify the repository offline

Nothing here needs a workspace. Run it first: if it fails, a deploy would fail later
and less clearly.

```bash
uv run python verify_repo.py            # expect: VERIFICATION PASSED
uv run python tests/test_accelerator.py # expect: ALL CHECKS PASSED
uv run python checks/apply_governance.py --catalog hfig_weu_tds --dry-run
```

**Expected:** all three exit 0. The third prints 18 rendered statements with no
unresolved `${...}` placeholders.

**If `verify_repo.py` fails:** fix the finding, do not skip it. It checks cross-file
integrity (bundle variables declared, referenced files exist, task graph coherent, no
`full_refresh: true` anywhere, no hash expression built outside `hashing.py`).

---

## Phase 2 — Reconcile the rulebook against the workbook

**Do not skip this. It is the only step that cannot be corrected after data lands.**

The hash rulebook in `src/accelerator/hashing.py` is the platform default inferred from
the spike documents, not an authority. Three families of constants must be confirmed
against `TECH_COLUMNS_STANDARD` in the EDM sample-data workbook:

1. `DELIMITER = "||"`, `NULL_TOKEN = "^^"`
2. Column names in `src/accelerator/naming.py` — the ERD says `ingested_at` and
   `record_hash`; the spike says `LOAD_DATETIME`/`ldts` and `HASHDIFF`. **These cannot
   all be right.**
3. Mask group names in `governance/apply_masks.sql`

`ALGORITHM = sha2_256` and `HASHDIFF_UPPERCASE = False` are ratified and guarded — do
not change them without a human decision and a `RULEBOOK_VERSION` bump.

**Ask the human to confirm 1–3.** If the workbook differs, change the constants, then:

```bash
# regenerate the golden vectors deliberately, because digests will change
uv run python - <<'EOF'
import sys, json; sys.path.insert(0, "src")
from accelerator import hashing as h
v = json.loads(open("tests/golden_hash_vectors.json").read())
v["rulebook"] = {"version": h.RULEBOOK_VERSION, "algorithm": h.ALGORITHM,
                 "delimiter": h.DELIMITER, "null_token": h.NULL_TOKEN,
                 "key_uppercase": h.KEY_UPPERCASE, "trim": h.TRIM,
                 "binary_output": h.BINARY_OUTPUT,
                 "hashdiff_uppercase": h.HASHDIFF_UPPERCASE}
for c in v["keys"]:
    c["pre_hash_string"] = h.reference_payload_key(c["values"], source_scope=c["source_scope"])
    c["sha256_hex"] = h.reference_key(c["values"], source_scope=c["source_scope"])
for c in v["hashdiffs"]:
    c["pre_hash_string"] = h.reference_payload_hashdiff(c["values"])
    c["sha256_hex"] = h.reference_hashdiff(c["values"])
open("tests/golden_hash_vectors.json", "w").write(json.dumps(v, indent=2))
print("vectors regenerated")
EOF
uv run python verify_repo.py
git add -A && git commit -m "reconcile hash rulebook against TECH_COLUMNS_STANDARD"
```

Report the pre-hash strings and digests to the human so they can be checked against the
workbook's `record_hash` convention independently. **"Close" is not "equal"** — the
control plane compares hashes across the boundary and carries no translation layer.

---

## Phase 3 — Verify the API assumptions (VERIFY list)

This repo was written without a live workspace. These specific API shapes need
confirming, and each has a documented fallback. Test them in a scratch pipeline before
the real deploy.

| # | Assumption | Where | If it fails |
|---|---|---|---|
| 1 | `dp.create_streaming_table(..., cluster_by=[...])` accepts `cluster_by` | `factory.py::_emit_target` | Drop the argument; apply `ALTER TABLE ... CLUSTER BY` in `apply_masks.sql` |
| 2 | `@dp.append_flow(..., once=True)` gives append-once semantics | `factory.py::_emit_ghost` | Seed ghost rows from a separate one-off job task instead |
| 3 | `@dp.expect_all_or_drop` stacks under `@dp.append_flow` in that order | `factory.py::_register_source_flows` | Move expectations into the query as a filter, and keep the quarantine flow as the record of rejects |
| 4 | `current_pipeline_update_id()` exists as a SQL function | `factory.py::_system_columns` | Pass a batch id via pipeline `configuration` and read it with `spark.conf.get` |
| 5 | `event_log: {catalog, schema, name}` is valid on a pipeline resource | `resources/vault_pipeline.yml` | Remove the block; read the event log via the pipeline UI/API |
| 6 | `information_schema.column_masks` exists and has these columns | `checks/mask_survival_check.py` | Substitute `DESCRIBE TABLE EXTENDED` parsing |
| 7 | `spark_python_task` + `environment_key` works on serverless jobs | `resources/vault_job.yml` | Convert the gate tasks to notebook tasks |
| 8 | `dp.create_streaming_table(..., schema="col MASK fn")` accepts mask clauses | `factory.py::_emit_target` | Declare the table in SQL inside the pipeline. **Do not** fall back to `ALTER TABLE ... SET MASK` — it does not survive a pipeline update — and do not ship the table unmasked |
| 9 | `ALTER CATALOG ... SET ISOLATION MODE ISOLATED` syntax | `governance/apply_masks.sql` | Set isolation in Catalog Explorer and record that it was done |

**Method:** create a throwaway pipeline in `weu_tds` with one metadata entity, run it,
and observe. Fix `factory.py` for whatever diverges, re-run `verify_repo.py`, commit
each fix separately with a message naming the assumption that failed.

Report the results of all seven before moving on.

---

## Phase 4 — Deploy to `weu_tds`

```bash
uv run python checks/preflight_target.py --target weu_tds --profile hfig-weu-tds
databricks bundle validate -t weu_tds
```

Preflight first, every time. `bundle validate` does not check that you are pointed at
the lake you intend.

**Expect preflight to FAIL here, and read why before you work around it.** `weu_tds`
declares no `run_as`, so this deploy's loads will run as *you*, not as a service
principal — and mask functions evaluate with the run-as identity's rights during a
refresh. `governance.mask_money` and `mask_money_double` admit
`scope_unmask_currency_values`; if the account you are about to load as is not in it,
every masked amount reads NULL and the first materialized view over a masked column
writes those NULLs into the **insert-only** silver vault. That write cannot be undone.

The four targets with no `run_as` are `weu_tds`, `uks_tds`, `aue_tds` and `dev`.
Full reasoning in [6b](#6b--the-refresh-as-owner-trap-get-this-wrong-and-you-corrupt-the-vault),
which you are reading two phases early on purpose.

Once you have confirmed that identity is in the group — by explicit request to the
platform team, not by assumption — re-run the preflight command above with your identity
appended, and nothing else changed:

```bash
    --unmask-identity you@example.com
```

(The flag is shown on its own deliberately: `verify_repo.py` ratchets the number of
executable command lines in this runbook still pointing at `weu_tds`, an abandoned lake,
and a worked example here would raise a count that is only ever meant to fall.)

`is_account_group_member` returns `false` identically for a non-member and for a group
that does not exist, so preflight cannot confirm this for you. It refuses by default and
records your claim instead.

**Expected:** no errors. If it reports undeclared variables, they need adding to the
`variables:` block in `databricks.yml` — do not pass them ad hoc with `--var` as a
workaround, because the other three regions need them declared too.

```bash
databricks bundle deploy -t weu_tds
```

Then confirm what was created before running anything:

```bash
databricks bundle summary -t weu_tds
```

**Expected:** one pipeline (`silver_vault`) and one job
(`vault_load`) with 8 tasks, `assert_hash_parity` first.

---

## Phase 5 — Run the gates, in order

**THIS PHASE ASSUMES A TARGET THAT DECLARES `active_sources`.** The commands below name
`usnc_tds`, which does. They previously named `weu_tds`, which does **not** — and an empty
`active_sources` means EVERY declared binding is active, including the placeholder ones
whose Bronze tables do not exist. Run this phase against such a target and the pipeline
fails at definition time on the first placeholder flow, before any gate below is reached.
That is the failure `active_sources` exists to prevent; see the variable's description in
`databricks.yml` and section 2b of
`docs/superpowers/specs/2026-08-24-subproject3a-gp-journals-design.md`.

### 5a — Hash parity (gate zero)

```bash
databricks bundle run vault_load -t usnc_tds --only assert_hash_parity
```

**Expected:** `HASH PARITY GATE PASSED: 7 keys and 7 hashdiffs match the reference`.

**If it fails:** stop entirely. A digest mismatch means this region's Spark produces
different keys from the reference implementation, so keys computed here could never
join to keys computed in another lake. The output names the failing case and prints the
exact pre-hash string — report both. Do not proceed.

The most important single line in that output is the schema-evolution assertion:
`appending a null column does NOT change the hashdiff`. If that fails, adding a column
to any satellite would reinsert every row in the estate.

### 5b — First load

```bash
databricks bundle run vault_load -t usnc_tds
```

Watch for: the pipeline graph should show, per entity, one streaming table plus one
append flow **per source**. `hub_job_request` has three sources, so expect three flows
into it. If you see one flow with a UNION, the factory is not doing what it should.

### 5c — Append-only gate

Runs automatically as a task. To run alone:

```bash
databricks bundle run vault_load -t usnc_tds --only assert_append_only
```

**Expected:** `APPEND-ONLY GATE PASSED: zero mutating operations across all vault tables`.

**If it fails:** report the exact table and version. A mutating operation in the vault
means something is using `AUTO CDC` / `APPLY CHANGES` where it must not, or a table was
recreated. Do not clear history to make it pass.

### 5d — Loop-1 reconciliation

**PREREQUISITE — `ctl_approval_manifest` must already exist, and this repo does not
create it.** `checks/loop1_reconciliation.py` reads
`<catalog>.governance.ctl_approval_manifest` (columns `manifest_id`, `approved_count`)
as the control every comparison is made against, and it is the *first* thing the gate
reads: if the table is absent the gate fails immediately and by design, and no amount of
active-source awareness changes that. It is a platform-owned `ctl_` table under the ARB
boundary rule — the generator consumes it and never creates it — so **confirm it exists
and is populated for the manifests you are about to load before running the job.** If the
estate does not yet produce one, that is a conversation with the platform team, not a
change to this repo.

**Known defect, not fixed in this round:** the schema is hardcoded as `.governance.` in
`loop1_reconciliation.py` rather than taken from `${var.governance_schema}`, so a target
whose governance schema is named anything else will not find the table even when it
exists. Pass `--manifest-table` explicitly in that case.

**Expected:** `landed + (quarantined - superseded) = approved for every manifest`,
across the tables named by `--entity` (the job names the three journal tables). That is
the literal string the gate prints — it changed when `supersede_quarantine` was added, so
a runbook follower looking for the older `landed + quarantined = approved` will not find
it. Tables with no active source binding are listed under `NOT EVALUATED` — that is a
stated absence, not a pass.

`superseded` is the count of rejects a later run legitimately re-accepted, recorded in
`control.ctl_quarantine_superseded` by the `supersede_quarantine` task that runs before
this one. It is the only adjustment in this gate that can make it WEAKER, so the gate also
reports any manifest superseding MORE rejects than it quarantined as its own failure —
`superseded record(s) against … quarantined row(s)` — rather than absorbing it into the
arithmetic.

**If it fails:** this is real information, not a bug in the check. Rows are going
missing between the manifest and the vault. Report the variance per manifest.

### 5e — Idempotency (do this explicitly, it is not automatic)

Re-run the same load with no new source data (same target as 5b):

```bash
databricks bundle run vault_load -t usnc_tds
```

**Expected:** zero new rows in every satellite, and `assert_append_only` still passes
its uniqueness check. Confirm with:

```sql
SELECT 'sat' AS t, COUNT(*) FROM hfig_weu_tds.silver_vault.sat_job_request_details_striive
UNION ALL
SELECT 'nhl', COUNT(*) FROM hfig_weu_tds.silver_vault.nhl_timesheet_line;
```

Run it before and after. The counts must be identical.

**Pay particular attention to `nhl_timesheet_line`.** NHLs have no hashdiff, so re-run
safety comes only from the dedup guard on the transaction key. If the NHL count grew,
you have found the highest-priority bug in the repo — report it immediately and do not
proceed to production.

### 5f — A green run is not the same as four gates that asserted something

**Do this every run. It takes one command and it is the only thing standing between a
deliberately partial lake and a job that looks fully gated while proving very little.**

Four post-load gates are active-set aware: `reconcile_loop1`,
`assert_journal_integrity`, `assert_mask_survival` and
`assert_aggregate_reconciliation`. Each of them skips what has no active source binding
in this lake and **exits 0 while announcing it**, because a declared dormancy is a
correct outcome for a lake that deliberately loads only some sources — see the
`active_sources` variable in `databricks.yml`. Databricks has no "green with a warning"
task state, so **a dormant gate and a fully-asserting gate look identical on the run
page.**

Every gate therefore ends with one machine-readable line:

```
GATE SUMMARY :: <gate> :: status=<PASSED|NOT_EVALUATED|FAILED> asserted=<n> not_evaluated=<m>
```

`status` is the gate's own verdict and is NOT the exit code: `NOT_EVALUATED` exits 0 and
must never be read as `PASSED`. After a green run, pull the four lines out of the task
outputs and read them:

```bash
databricks jobs get-run <run-id> --output json \
  | jq -r '.tasks[].task_key' \
  | while read t; do
      databricks jobs get-run-output --run-id <task-run-id-for-$t> --output json \
        | jq -r '.logs' | grep '^GATE SUMMARY' ;
    done
```

(Or simply open each of the four tasks and look at the last line — the point is that the
figure is there and is the last thing printed.)

**What to expect on the first `usnc_tds` load**, with the three journal tables active and
the payroll sources deferred (3a §3.1):

| Gate | Expected | Why |
|---|---|---|
| `loop1_reconciliation` | `status=PASSED asserted=3` | the job names the three journal tables |
| `journal_integrity` | `status=PASSED` with `not_evaluated≥4` | no control total in GP; no line ordinal in UKG |
| `mask_survival` | `status=PASSED asserted=6` | the six masked amount columns on the three journal tables |
| `aggregate_reconciliation` | **`status=NOT_EVALUATED asserted=0`** | the payroll pair is deferred: `nhl_payroll_detail` and the classification `csat` have no active binding |

**An `asserted=0` you did not expect is a finding**, even on a green run: it means a gate
proved nothing about something you thought was loading. Check the `NOT EVALUATED` list
printed above the summary line, then check `active_sources` for that target.

**ACTIVATE `payroll_detail/UKG_US` AND `payroll_line_classification/BUSINESS_VAULT` IN THE
SAME CHANGE.** The aggregate gate needs *three* tables loading — the GL journal, the
payroll register beneath it, and the Business Vault classification that maps payroll codes
to ledger accounts — and it skips the pair if any one of them is inactive. Activate the
register without the classification (or the other way round) and you get the worst
available outcome: the GL journal and the payroll register both load, both look complete,
and the control that proves the money in one equals the money in the other reports
`status=NOT_EVALUATED asserted=0` and exits 0. That is correct behaviour under the rule
above, and it is announced by name in the `NOT EVALUATED` list — but nobody reads the
announcement if they were not told the pairing matters. **When you activate either, check
this gate's summary line says `PASSED` with a non-zero `asserted`.**

**Known limitation.** This makes the outcome greppable and last; it does not make it
visible on the run page itself. Making a dormant gate visible *in the job UI* would need
either task values (`dbutils.jobs.taskValues`, whose availability on serverless
`spark_python_task` is unverified here) or a summary table the gates write and a final
task that reads it. Neither is built.

---

## Phase 6 — Governance

### 6STOP — THE GROUP NAMES IN `apply_masks.sql` ARE PLACEHOLDERS AND DO NOT EXIST

**Do not run `apply_governance` (the full task) against `usnc_tds` until this is resolved.
It is not a hardening step you can take because it looks like the next one.**

This is README RECONCILE item #3, never done. `governance/apply_masks.sql` names four
account-level groups. **Checked in this workspace on 2026-08-25: three of the four do not
exist.**

| Named in `apply_masks.sql` | Exists in `usnc_tds`? |
|---|---|
| `hfig_data_engineering` | **no** |
| `hfig_analysts` | **no** |
| `hfig_commercials_reader` | **no** |
| `hfig_worker_pii_reader` | **no** |

The workspace uses a different convention entirely — `usnc_data_platform_bronze_layer_reader`,
`us_tds_bi_builder`, `usnc_data_custodian_fieldglass`, `pii_cleared_us`. Only `pii_cleared_us`
is real, and Task 1's mask probe used it.

**The specific consequence, which is worse than a failed task.** `apply_masks.sql:138` is

```sql
REVOKE ALL PRIVILEGES ON CATALOG `01_usnc_bronze_dev` FROM `account users`;
GRANT USE CATALOG ON CATALOG `01_usnc_bronze_dev` TO `hfig_data_engineering`;
```

The REVOKE is unconditional and **succeeds**. The GRANT that was meant to restore access
then **fails**, because the group does not exist. The result is that **nobody can read
Bronze** — on a catalog this bundle did not create and does not own. The statements run in
order in a single task, so the window is not theoretical.

**Two decisions are needed, and they are separate:**

1. **The group mapping.** Which real groups correspond to the four placeholders. This is a
   governance decision about who may read commercial and PII columns, not a rename.
2. **Whether this repo should revoke on a shared catalog at all.** `01_usnc_bronze_dev` is
   owned by the estate's Bronze pipeline, not by this accelerator. Revoking there is a
   cross-team action even when the group names are right.

**What is already split out, and is safe.** The mask *functions* are a prerequisite of the
load — `raw_vault` cannot declare a `MASK` clause naming a function that does not exist —
so they are created by their own task, `create_mask_functions`, which runs **before**
`raw_vault` and passes `--functions-only`. That path emits `CREATE OR REPLACE FUNCTION`
and nothing else: no `GRANT`, no `REVOKE`, no isolation change, enforced by an allowlist,
a denylist and a test asserting it cannot. Creating a function touches no permission.
**The permission half stays in `apply_governance`, in its original position, blocked.**

### 6a — Masks come from the table definition, not from this task

Understand this before running anything, because it inverts what the file name suggests.

Every vault object is a **streaming table** and every `_v1` is a **materialized view**.
For those, row filters and column masks must be set through the table definition —
`ALTER TABLE ... SET MASK` against a pipeline-owned table does not survive the next
pipeline update. So:

- **Which column gets which mask** is declared in `metadata/entities/*.yml` (`masks:`)
  and emitted by the factory into the table definition.
- **`governance/apply_masks.sql` creates the mask FUNCTIONS**, sets catalog isolation
  and applies grants. It applies no masks. That is correct, not an omission.

```bash
uv run python checks/apply_governance.py --catalog hfig_weu_tds --dry-run
```

Read the statements. Note that two are `REVOKE ALL PRIVILEGES ... FROM \`account users\``.
**Confirm with the human before running this against a shared catalog.**

```bash
databricks bundle run vault_load -t weu_tds --only apply_governance
databricks bundle run vault_load -t weu_tds --only assert_mask_survival
```

**Expected:** `MASK GATE PASSED: every declared sensitive column is masked` — including
on the `_v1` projections, which are checked separately because a mask on a base table
does not automatically protect a derived view.

### 6b — The refresh-as-owner trap. Get this wrong and you corrupt the vault.

Mask functions run with the **pipeline owner's** rights during a refresh, and
`is_account_group_member` evaluates against the pipeline's **run-as identity**, not the
querying user's.

So if the run-as identity is *not* privileged under a mask, a downstream materialized
view **materialises the masked value**: NULLs written into the vault as fact. Silver is
insert-only, so they stay.

#### There is no longer ONE privileged group. There are two, and which one you need
#### depends on which mask.

Changed 28 September, at the platform team's insistence (PLT-2). `mask_money` and
`mask_money_double` no longer admit `global_dataplatform_pipeline_job_runners` — that is an
*operational* group, and using it as the key to cleartext currency meant anyone added
merely to run jobs also read the money. The money masks now admit
`scope_unmask_currency_values`.

| mask | admits |
|---|---|
| `mask_money`, `mask_money_double` | `usnc_data_analyst_finance`, `scope_unmask_currency_values` |
| `mask_tokenised_account`, `mask_personal_name`, `mask_tax_reference` | `pii_cleared_us`, `global_dataplatform_pipeline_job_runners` |

Membership of the job-runner group therefore no longer unmasks a single amount. Do not
read a `true` from it as clearance to load.

#### STOP — four targets declare no `run_as` and load as a human.

`weu_tds`, `uks_tds`, `aue_tds` and `dev` carry no `run_as`, so their loads run as whoever
pressed Deploy. `usnc_tds` and the four production targets run as the service principal.

Until 28 September that was survivable by accident: the humans who deploy are in
`global_dataplatform_pipeline_job_runners`, which `mask_money` then admitted. **It is not
survivable now.** A load on one of those four, run by a human who is not in
`scope_unmask_currency_values`, evaluates the money masks under an identity they deny — and
the first materialized view over a masked column writes those NULLs into insert-only
silver, permanently. All nine targets reach at least one masked column, so none of the four
is safe by having nothing to load.

**Before deploying to `weu_tds`, `uks_tds`, `aue_tds` or `dev`, one of these must be true:**

1. the target declares `run_as.service_principal_name`; or
2. the identity you are about to run as returns `true` below.

`tests/test_accelerator.py` fails the build if a fifth target joins that list without being
recorded, or if one of the four is fixed and the record left behind.

Before the first load, confirm with the human and verify, **as the run-as identity**:

```sql
SELECT current_user(),
       is_account_group_member('scope_unmask_currency_values'),   -- money masks
       is_account_group_member('usnc_data_analyst_finance'),      -- money masks
       is_account_group_member('pii_cleared_us'),                 -- PII masks
       is_account_group_member('global_dataplatform_pipeline_job_runners')
```

```bash
databricks grants get function hfig_weu_tds.governance.mask_money
```

**Expected:** at least one `true` in each row of the table above for the masks this lake
actually declares, plus `EXECUTE` on the function. `is_account_group_member` returns `false`
identically for a non-member and for a group that does not exist, so a `false` you did not
expect is worth checking against the group name before concluding anything. If the run-as
identity is not privileged under a mask its load will touch, **stop** — do not run the load.
Report it.

After the first load, prove no masking leaked into stored data:

```sql
SELECT COUNT(*) AS nulls FROM hfig_weu_tds.silver_vault.sat_job_request_commercials_striive
WHERE pay_max IS NULL;
-- compare against the same count in Bronze. A large discrepancy means the mask
-- evaluated during write, not on read.
```

### 6c — The unprivileged-read test (manual; may change the model)

The gate proves masks *exist*. It cannot prove they behave. Ask the human for a
principal **not** in `hfig_commercials_reader`, then as that principal:

```sql
SELECT pay_max FROM hfig_weu_tds.silver_vault.sat_job_request_commercials_striive LIMIT 5;
SELECT pay_max FROM hfig_weu_tds.silver_vault.sat_job_request_commercials_striive_v1 LIMIT 5;
-- and through any Gold view that projects it
```

All three must return NULL. **If the value appears unmasked through the view**, stop and
report: sensitive columns must move to satellites Gold never projects, which is a
modelling decision for the human.

Also note the compute constraint: fine-grained access control requires serverless or a
recent runtime in standard access mode. An older dedicated cluster may be unable to
*read* a masked table at all. Confirm what the analysts actually use.

### 6d — Consider ABAC instead, before the estate grows

Attribute-based access control is now generally available and is a better fit for a
generated estate: tag a column as PII or financial once, and one account-level policy
covers every table — rather than a `masks:` block per entity that must be kept in step
across 30+ entities and 8 workspaces.

The same exemption discipline applies: ABAC policies on materialized views and streaming
tables are supported only when the pipeline owner and run-as identity are **exempt** from
the policy, added via the policy's `EXCEPT` clause, with `TO` naming who receives masked
data. It also requires **governed tags** defined at account level.

Recommendation: ship v1 with the per-column masks that are already declared, and raise
ABAC as the target state — it is a governance decision (who owns the tag taxonomy) as
much as a technical one, so it belongs with WISE intake and the AI Council.

## Phase 7 — Promotion and fan-out

Only after every gate above is green **and** the human has authorised it.

**Promote by environment, then by region. Never region-first** — deploying to a
production lake a shape no TDS workspace has seen defeats the point of having TDS.

```
1. weu_tds     <- this session
2. uks_tds, usnc_tds, aue_tds
3. conformance gate across all four TDS workspaces
4. weu   (preceded by weu_tds vs weu conformance)   <-- BLOCKED, see below
5. uks, usnc, aue   (each preceded by its own tds-vs-prod conformance)
```

### EU production is a legacy workspace — treat step 4 with extra care

`weu` maps to the older "Datalake" workspace rather than a purpose-built one. Nothing
about that blocks the promotion order, but it changes what step 4 has to prove:

- **Catalog binding is done and verified negatively** (Phase 0d), because this is the
  region where prod and TDS share a metastore and prod holds real payroll data.
- **`root_path` does not collide** with anything already in `/Shared`.
- **No legacy Hive object shadows a vault name.** A `hive_metastore` table called
  `hub_worker` will not break the bundle, but it will confuse every human who greps for
  one and finds two.
- **The conformance baseline stays `weu_tds`.** Comparing `weu_tds` against `weu` is now
  a tds-vs-prod comparison *within one metastore*, which is exactly what
  `conformance_check.py` is designed to accept as a two-target run.

### Step 3 — cross-region conformance, TDS

```bash
databricks bundle deploy -t uks_tds     # and usnc_tds, aue_tds
uv run python checks/conformance_check.py \
  --targets weu_tds,uks_tds,usnc_tds,aue_tds --baseline weu_tds --warehouse-id <id>
```

**Expected:** `CONFORMANCE GATE PASSED: all regions structurally identical`. It checks
the hash rulebook properties first, because that is the divergence that cannot be
recovered without re-keying.

Requires a CLI profile per workspace, named as in `checks/conformance_check.py`:
`hfig-weu-tds`, `hfig-weu`, `hfig-uks-tds`, and so on. Plus `pip install databricks-sdk`.

### Step 4 — environment conformance, then production

```bash
uv run python checks/conformance_check.py --targets weu_tds,weu --baseline weu_tds --warehouse-id <id>
databricks bundle validate -t weu
databricks bundle deploy   -t weu
databricks bundle run vault_load -t weu
```

The tool refuses to compare more than two targets across mixed environments, because
drift would be ambiguous between the region axis and the environment axis. Compare all
TDS, or all PROD, or exactly one region's two environments.

### What is forced, not chosen

Unity Catalog metastores are regional. Each of the 8 workspaces has its own metastore,
its own catalogs, and its own copies of the mask functions — so the "same" mask exists
8 times and can drift 8 ways. That is what the conformance gate compares.

### Residency — raise, do not decide

`usnc` and `aue` are outside the EEA. A hash key derived from a personal identifier is
pseudonymised personal data, not anonymous, so **resolving the same worker across weu
and aue is a cross-border transfer of personal data, not merely a join.**

Structural conformance is safe — it reads `information_schema` and table properties and
moves no personal data. Cross-region *identity resolution* is a governed decision for
WISE intake and the AI Council. If asked to build a global registry, stop and escalate.

## Phase 8 — Governance registration and handover

Confirm with the human that this is declared through **WISE intake**. It generates and
loads worker, job-request and timesheet structures and sits upstream of ML entity
resolution, so it likely falls under HFIG's high-risk AI classification — and an
AI-assisted automation that builds production pipelines is what the **Shadow AI**
framework asks to be declared. Registering it now is considerably cheaper than at
go-live. This is not a blocker to a dev deploy; it is a blocker to production.

Do not attempt to answer policy questions about residency, lawful basis or who may see
what. Those go to WISE intake and the AI Council.

### Write a handover report containing

- Phase 3 results: which of the seven API assumptions held, which were fixed and how.
- Phase 2 results: the reconciled rulebook constants, and the pre-hash strings and
  digests for the seven golden key vectors.
- Row counts per vault table after the first load, and after the idempotent re-run.
- Quarantine counts per entity with the top failure reasons.
- The mask-projection test result, stated plainly as pass or fail.
- Anything you changed in the repo, as individual commits.
- Anything you were unable to verify.

---

## Adding an entity later

One YAML file in `metadata/entities/`, no code:

```yaml
name: supplier                # the BUSINESS CONCEPT. Never supplier_striive: the same
                              # entity arrives from many systems, and validation rejects
                              # an entity name containing one of its own sources.
kind: hub
domain: party
key_style: federated          # federated | authored | tenant_scoped
business_keys: [supplier_reference]
sensitivity: internal         # internal | personal | financial | restricted
sources:                      # hubs fan IN: one table, one append flow per source
  - name: STRIIVE_EU
    bronze_table: hfig_eu.bronze.striive_supplier
    key_columns: [supplier_ref]
    applied_dts_column: last_modified_at
    cdc_op_column: _cdc_op
  - name: AFAS_EU
    bronze_table: hfig_eu.bronze.afas_creditor
    key_columns: [creditor_no]
    cdc_op_column: _cdc_op
```

For a **satellite**, the same file shape fans OUT instead: N sources produce N tables
named `sat_supplier_details_<source>`, each with its own payload mapping. One entity
declaration, one table per source, one-source-per-satellite guaranteed by construction.

Then `uv run python verify_repo.py` and redeploy. Validation will refuse to build, among
other things: a hub carrying attributes, a link carrying attributes, an NHL without a
transaction key, a satellite with two sources, an MSAT without a natural sub-key, a
reordered satellite payload, a `ctl_`/`ref_`/`reg_` object, or **a link whose parent is
another link**.

That last one will fire on the payment chain in EDM v0.4
(`nhl_payroll_transaction` → `timesheet_entry_id` → `nhl_bank_payment_txn`). It is not
a bug in the validator: chaining links breaks the unit of work, so one row can no
longer be read back as a complete business event. It needs a modelling decision from
the human before the pay-bill domain can be declared.
