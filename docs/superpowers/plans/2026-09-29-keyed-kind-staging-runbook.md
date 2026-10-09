# Keyed-Kind Staging — Migration Runbook (`usnc_tds`)

**Status: NOT SCHEDULED.** Adrian deferred this migration to the combined migration under
`spec/client-operating-company-split`, which re-versions seven of these same entities
again — doing the two separately would re-version six NHLs twice. This document therefore
has to stand on its own: whoever runs it will not have been in the session that measured
it, and the plan it came from (`2026-09-29-keyed-kind-staging.md`) describes the code
change, not the operation.

**What the migration is.** Eleven keyed entities — three links, six NHLs, two HALs —
stopped being written directly by the SDP pipeline and became batch-loaded tables that
`checks/load_hubs.py` builds from a staging log by anti-join. A streaming table cannot be
`INSERT`ed into by a job task, so each one needs a new physical version: the new table is
built and loaded beside the live one, proved on real data, and cut over by redefining the
stable view. Nothing is dropped until the cutover holds.

**What it is not.** It is not a shape change. The columns are identical on both sides. The
only thing that differs is who writes the table and how duplicates are eliminated.

Every number and every quoted refusal in this document was measured on 28–29 September
2026 against `usnc_tds`. Where a statement is read from the code rather than measured in
the lake, it says so.

---

## 0. Eleven entities are processed. FIVE of them need you.

`checks/load_hubs.py` scopes ITS TARGETS by KIND and by nothing else. It does take
`--active-sources` as of 29 September, but only to decide which declared column MASKS to
ALTER onto the table it creates (a CTAS copies the shape and not the masks, and an
inactive table has no payload column to mask) — not to decide what to build. Compare
`checks/load_satellites.py`, which uses the same list to SKIP an inactive satellite
outright. So the loader builds, loads and publishes a view for **all eleven** keyed
entities on every run, whether or not their binding is active in this lake. Only **five** of them carry data, and those five are exactly the five with an
ACTIVE binding in `usnc_tds`:

| stable name | binding | what you do |
|---|---|---|
| `hal_client_legal_entity_hierarchy` | `HUBSPOT` | **the full sequence in §3** |
| `nhl_general_journal_line` | `GP_US` | **the full sequence in §3** |
| `nhl_general_journal_line_closed_year` | `GP_US_HIST` | **the full sequence in §3** |
| `nhl_invoice_line` | `FIELDGLASS_US` | **the full sequence in §3** |
| `nhl_journal_line` | `UKG_US` | **the full sequence in §3** |
| `hal_consolidation_hierarchy` | `WORKDAY` | nothing — see below |
| `lnk_client_contracting_entity` | `CLIENT_PORTAL` | nothing — see below |
| `lnk_client_job_request` | `STRIIVE_EU` | nothing — see below |
| `lnk_legal_entity_consolidation` | `WORKDAY` | nothing — see below |
| `nhl_payroll_detail` | `UKG_US` (placeholder) | nothing — see below |
| `nhl_timesheet_line` | `STRIIVE_EU` | nothing — see below |

The active list is `databricks.yml`'s `active_sources` for the `usnc_tds` target.
`nhl_payroll_detail`'s `UKG_US` is named there as a *placeholder* bronze table, not the
real `ukg_raw.gl`, which is why it sits with the inactive six despite sharing a source name
with `nhl_journal_line`.

### What happens to the six without you touching them

Read from `checks/load_hubs.py` rather than measured in the lake, because this path has
not run against `usnc_tds` yet. Per inactive entity, in one `vault_load` run:

**AN INACTIVE ENTITY'S LOG IS NOT EMPTY. IT HOLDS ONE GHOST ROW,** and every number below
follows from that. `factory.build` calls `_emit_ghost(entity, target, ...)`
UNCONDITIONALLY (`src/accelerator/factory.py`, the `_emit_ghost` call at the end of the
per-table loop), and `target` is `naming.pipeline_table(entity.kind, table)` — which since
29 September is the STAGING LOG. So the once-only ghost flow writes its single zero-key row
at `load_dts` 1900-01-01 into `stg_<name>_rev<NEW>`, for an inactive entity exactly as for
an active one. An earlier draft of this section said "empty" in six places; each is
corrected below, and one of them changes what you should DO.

1. Its staging log exists and holds **one row** — the ghost. The pipeline creates the log
   for every DECLARED binding, so the inventory is identical across lakes, and the ghost
   flow is attached whether or not any binding is active.
2. `create_sql` creates `<name>_rev<NEW>` from that log's shape. Empty at creation.
3. The grain probe is **skipped**, and says so in the log: **1 staged row over 1 distinct
   key**, so no key repeats and no group can hold two rows. (`probe_is_needed(1, 1)` is
   False — the skip is unchanged, only its printed numbers are.)
4. The `INSERT ... NOT EXISTS` inserts **the ghost row** on the first run, and nothing on
   any run after it.
5. `stable_view_action()` returns `bootstrap`, because no stable view exists for any of
   the six today — **so the loader CREATES the stable view over the new `_rev<NEW>`,**
   which holds the ghost and nothing else.
6. It writes one `aud_table_load` row, **staged 1, accepted 1**.

**So there is no cutover to perform for the six: the view is already on the new version
when the load finishes.** Nothing was replaced, so no grants were dropped, and there was
nothing to grant in the first place.

### A correction, so you do not act on the earlier number

An earlier draft of this runbook said the six would each produce a refusal, and that the
split was corroborated by the 28 September `vault_load` printing

```
GATE SUMMARY :: publish_stable_views :: status=... asserted=5 not_evaluated=6
```

**That five/six split is real but it is not this loader's.** It is
`checks/publish_stable_views.py`'s — a different gate, which *does* skip inactive tables,
and which owned these eleven only while link/NHL/HAL were written directly by the SDP
pipelines. It is removed from the job as of 29 September (see §1.2). Do not carry its
scoping over to `load_hubs`.

**Check the six against the lake before concluding anything**, which is good practice
whichever way it lands:

```sql
SELECT table_name, view_definition
FROM `02_usnc_silver_edm_dev`.information_schema.views
WHERE table_schema = 'raw_vault' AND table_name IN (
  'hal_consolidation_hierarchy', 'lnk_client_contracting_entity',
  'lnk_client_job_request', 'lnk_legal_entity_consolidation',
  'nhl_payroll_detail', 'nhl_timesheet_line')
```

* **A row naming `_rev<NEW>`** — the expected shape. Nothing to do. Do not run the cutover.
* **A row naming `_rev1`** — the loader did not bootstrap (or a cutover already happened).
  Leave it. A view over an empty `_rev1` serves the same nothing as a view over a
  `_rev<NEW>` holding only its ghost row, and moving it buys you no data.
* **No row at all** — also fine, and it means the bootstrap did not happen. Leave it.

### If you run the tools on the six anyway

You will not break anything, but read the outcome correctly:

* `checks/cutover_vault_version.py` **refuses — but NOT on the refusal you would expect,
  and the difference is the one consequential thing in this section.** The middle refusal
  ("the target version exists but has never loaded — 0 rows") is the one that exists for
  precisely this case, and **it does not fire.** `target_rows` comes from a plain
  `SELECT COUNT(*)` over the target (`checks/cutover_vault_version.py`, the count printed
  before the decision), and that counts the ghost row the loader inserted at step 4 above.
  `target_rows == 1`, so `cutover_refusal()` falls through to the THIRD refusal — the
  target is ungated — and that one is cleared by exactly the `--gated-by-run` an operator
  would reach for next. The loader wrote an `aud_table_load` row for the run, so the gating
  lookup succeeds and the cutover proceeds: **a stable view cut over a ghost-only table,
  which is the outcome the middle refusal exists to prevent.**

  **So: do not run the cutover on the six at all, and do not pass `--gated-by-run` to a
  target you have not seen a real row count on.** The rule is not "the tool will stop you";
  on these six it will not. Read the row count the tool prints BEFORE the decision — if it
  says 1 row, that is the ghost, and there is nothing to cut over to.
* `checks/retire_vault_version.py` on their `_rev1` is **permitted** if the loader
  bootstrapped a view over `_rev<NEW>` (not live, not declared), and **refused** with

  > `<catalog>.raw_vault.<name>`: no view found -- cannot determine which version is live,
  > refusing to guess

  if no view exists. Either answer is correct. Dropping an empty `_rev1` gains nothing, so
  the default is to leave it until the binding becomes active or somebody cleans up
  deliberately.

**An open design question sits underneath this, and it is Adrian's, not the operator's:**
whether `load_hubs` SHOULD skip inactive keyed entities the way `load_satellites` does.
Recorded in `docs/superpowers/OPEN_ITEMS.md`. Until it is decided, the behaviour above is
what the code does, and this runbook describes the code.

---

## 1. Before anything runs

### 1.0 PREREQUISITE: the version bumps of §1.1 are not a tidiness rule. Without them, the deploy DUPLICATES EVERY REJECT EVER WRITTEN.

**Do not deploy this branch to an already-loaded lake without the version bumps (task 6b).
This is the reason, and it is stronger than "the loader will fail".**

Deploying the keyed-kind staging change renames every source flow in the vault pipelines.
The flow name is built from the table the pipeline writes (`factory._flow_name`), and that
table is now `stg_<table>` for every staged kind. SDP keys a flow's CHECKPOINT by its name,
so every renamed flow gets a fresh, empty checkpoint and **re-reads its bronze source from
the beginning** — which for a source flow is intended and self-correcting: the staging log
is new and empty, and the anti-join in `checks/load_hubs.py` collapses whatever arrives.

**The quarantine flow is renamed the same way and its TARGET IS NOT.**
`factory.build` sets the quarantine flow's `target=_quarantine_table(table)`, and
`_quarantine_table` (`src/accelerator/factory.py`) deliberately runs the name back through
`naming.unstg()` so that the twin keeps the name it has always had — which is
`qtn_<name>_rev<CURRENT>`, **the existing, populated, `delta.appendOnly` table.** New flow
name, old target: the re-read replays every rejected row that was ever written, and appends
it again. The twin is append-only, so **this cannot be undone in place.**

**The two gates that would catch it do not run.** `reconcile_loop1` and
`assert_landing_integrity` both sit downstream of `load_hubs` in the job graph, and on an
unbumped lake `load_hubs` fails first — its target `<name>_rev<CURRENT>` is a
pipeline-owned streaming table, which a batch task cannot `INSERT` into. So both gates are
SKIPPED. The pipelines themselves SUCCEED and commit the duplication. The only signal you
get is a red `load_hubs` whose message is about a streaming table, which reads as "the
version bump is missing" and says nothing at all about the twin.

**MEASURED IN THIS LAKE, 29 September: all four keyed quarantine twins are EMPTY** —
`qtn_invoice_line_rev1`, `qtn_invoice_line_rev2`, `qtn_invoice_line_rev3` and
`qtn_journal_line_rev1`, 0 rows each. So on `usnc_tds` as it stands today the replay would
duplicate nothing, and here the loud `load_hubs` failure really is the stronger reason. The
mechanism above is general and unchanged: it applies to any lake whose twins are populated,
and to `usnc_tds` the moment one of them is. Re-measure before you rely on this — it is a
row count on a date, not a property.

**With the version bumps in place the hazard does not exist:** `_quarantine_table` derives
the twin from the bumped name, so the flow writes into a NEW, EMPTY `qtn_<name>_rev<NEW>`
and there is nothing to replay into. The quarantine-flow naming is not a defect to fix on
its own; it is correct behaviour whose precondition is the version bump. That precondition
belongs to the combined migration.

### 1.1 The version bumps must already be in the model

The bumps are part of the change, not part of the run. At the time this was written the
model still declares the pre-migration versions, so confirm what is actually in
`metadata/entities/` before you start rather than trusting this table:

| entity | from | to |
|---|---|---|
| `nhl_invoice_line` | 3 | 4 |
| the five other NHLs (`general_journal_line`, `general_journal_line_closed_year`, `journal_line`, `payroll_detail`, `timesheet_line`) | 1 | 2 |
| both HALs (`client_legal_entity_hierarchy`, `consolidation_hierarchy`) | 1 | 2 |
| all three links (`client_contracting_entity`, `client_job_request`, `legal_entity_consolidation`) | 1 | 2 |

`nhl_invoice_line` is the odd one because it was already cut to `_rev3` on 28 September.

**AMENDED 29 September — §7.1 supersedes this table and is gated.** The combination with
the hub split made the scope larger than the eleven rows above describe (two hubs are
created and one is retired), and the table in §7.1 is asserted against
`metadata/entities/` by `tests/test_accelerator.py` rather than confirmed by hand. The
numbers agree with the ones above; read §7.1, and read this one only for its reasoning.

### 1.2 `publish_stable_views` is out of the job — do not put it back

**Already fixed on the branch that carries this migration.** Recorded here because the
symptom is confusing and someone will otherwise re-add the task.

`checks/publish_stable_views.py` computes its scope as
`naming.GENERATABLE - naming.STAGED_KINDS`. Once link, NHL and HAL joined `STAGED_KINDS`
that set is **empty**, and the gate fails closed on purpose:

```
GATE NOT EVALUATED: naming.GENERATABLE minus naming.STAGED_KINDS is empty --
no pipeline-owned kind is declared in the model.
```

It returns 1. While it was still a task in `resources/vault_job.yml` **every `vault_load`
run failed at it**, and it blocked `assert_freshness` and `assert_mask_survival` behind it.
That is fatal to this migration and not merely untidy: `cutover_vault_version.py` wants a
`--gated-by-run` naming a run the operator has confirmed green, and no run could be green.

The task was removed on 29 September, and both dependents keep their ordering transitively
through `load_satellites_business -> business_vault -> load_satellites -> load_hubs`. The
file was NOT deleted and its fail-closed branch was NOT softened to return 0 — that branch
is the proof that an emptied scope is a defect rather than a pass, and it is what a kind
added to `naming.GENERATABLE` but not to `naming.STAGED_KINDS` would trip. If that ever
happens, put the task back exactly as it was.

If you find `publish_stable_views` in the job when you come to run this, someone reverted
that change. Take it out again rather than softening the gate.

### 1.3 Identity

Both `cutover_vault_version.py` and `retire_vault_version.py` need the owning service
principal:

```
7732b208-8366-4aef-af09-60e9dec9cf86    Data Platform US Terraform TDS
```

It is the identity that OWNS `02_usnc_silver_edm_dev.control` and the vault schemas, and
it is `${var.service_principal}` on the `usnc_tds` target. A human identity gets
`PERMISSION_DENIED` on `MANAGE` and cannot replace a view or drop a table here. There is no
workaround; do not spend an afternoon on one.

There is no standing job for either tool. **The route used on 28 September was a temporary
bundle job** — a small `resources/*.yml` declaring one `spark_python_task` pointing at
`../checks/cutover_vault_version.py` (or `retire_vault_version.py`), with the arguments for
one entity, deployed, run with `databricks bundle run`, **and deleted immediately
afterwards**, in the same shape as `resources/grant_vault_access.yml`: a file someone can
read, run and delete. It was deleted. Keep it that way — a standing cutover job is a
standing risk of an automatic cutover, which is the thing the whole versioning mechanism
exists to prevent.

### 1.4 Preflight, every time

Eight near-identical workspaces. The realistic failure is artefacts landing in the wrong
lake because a profile was left pointing elsewhere; preflight matches on HOST, not on
profile name.

```bash
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
databricks bundle validate -t usnc_tds
```

Expect `PREFLIGHT PASSED: authenticated workspace matches the target.` If it fails, fix the
profile or `--target` — never the bundle.

### 1.5 Constants for every command below

```
catalog          02_usnc_silver_edm_dev
schema           raw_vault          (all eleven live here)
control-schema   control
target           usnc_tds
profile          hfig-usnc-tds
```

---

## 2. The three findings a naive runbook gets wrong

### 2.1 Replacing a stable view DROPS ITS GRANTS

`CREATE OR REPLACE VIEW` does not edit a view in place. Unity Catalog replaces the
securable and every grant held against the old one goes with it. A cutover is a
`CREATE OR REPLACE VIEW`, so **a cutover always revokes SELECT on the stable name.**

Measured 28 September: immediately after cutting `nhl_invoice_line` to `_rev3` the view was
unreadable —

> `User does not have SELECT on Table ...`

— and this is the part that wastes the hour: `information_schema` is permission-filtered,
so it reported the view as having **zero columns** rather than as forbidden. It looks like
an empty table, not like a permissions problem. The instinct is to go and check whether the
load produced any rows. Do not. Check the grants first.

**`grant_vault_access` must follow EVERY cutover**, before anyone is told the new version
is live:

```bash
databricks bundle run grant_vault_access -t usnc_tds
```

That job runs `checks/apply_governance.py` with `--emit-data-grants`, which the standing
`vault_load` deliberately does not set. It is manual, and it is manual on purpose.

### 2.2 `grant_vault_access` exits 1 by construction today

Do not read that exit code as a failed cutover.

18 of its statements fail with `TABLE_DOES_NOT_EXIST`. All 18 are satellites whose bindings
are inactive in this lake. The cause is in `checks/apply_governance.py`'s
`table_select_grants()`: it gates the quarantine twin and the stable view on the binding
being active, but it does **not** gate the physical table beside them, so it emits a
`GRANT SELECT ON TABLE` against a satellite table that was never created. A fix is queued
separately.

**The obvious one-line fix does not work, so do not spend the afternoon on it.** It was
attempted and reverted on 29 September: gating the physical-table grant the same way the
quarantine twin and the stable view are gated immediately reds two existing checks, because
`tests/test_accelerator.py`'s `_loader_built` derives what the loader builds from every
emitted staging log — and `load_satellites.py` skips inactive bindings while `load_hubs.py`
does not. The grant list and the check guarding it hold the SAME false premise, so they
agree with each other and disagree only with the lake. A check derived from the same wrong
model as the thing it checks is a mirror, not an oracle.

**The fix is three changes, not one**, named here so this section does not depend on a
document that may not be beside it:

1. **Gate the grant** — kind-aware, because `load_satellites.py:521` skips an inactive
   binding and never creates its table while `load_hubs.py` has no activity filter and
   builds every declared entity.
2. **Correct `_loader_built`** (`tests/test_accelerator.py:4039`) so it models what the
   loaders actually build rather than deriving it from every emitted staging log.
3. **Re-prove both against the live estate**, not against each other — which is the step
   that would have caught this, and the reason doing only the first two is not a fix.

The full analysis lives in `docs/superpowers/OPEN_ITEMS.md` **once `docs/grant-q1-finding`
merges**; it is not on this branch, so do not go looking for it here.

Until that fix lands:

> **Expect exactly 18 `TABLE_DOES_NOT_EXIST` failures, every one of them on a `sat_`,
> `msat_`, `esat_` or `csat_` table. ANY other failure — a different error, a different
> count, or a `TABLE_DOES_NOT_EXIST` on a `hub_`, `lnk_`, `nhl_`, `hal_`, `stg_` or `qtn_`
> name, or on a stable view — is real. Stop and read it.**

A `TABLE_DOES_NOT_EXIST` on the stable view you just cut over is the one that must never be
waved through: it means the cutover did not create what you think it created.

### 2.3 Both tools default to a DRY RUN, and the dry run is the point

Neither `cutover_vault_version.py` nor `retire_vault_version.py` changes anything without
`--apply`. Both print the whole decision first — row counts before the decision, so the
cost is on the record whether or not the action proceeds.

**Run the dry run first, every time, and read it.** Not as ceremony: the dry run is where
the empty-target refusal and the live-version refusal are printed, and those are the two
refusals that exist because the failure they prevent is silent. A view flipped to an empty
table breaks no invariant downstream — append-only holds, masks hold, reconciliation
compares zero against zero, every gate stays green, and the estate serves an empty answer.

---

## 3. The per-entity sequence

Do one entity at a time, start to finish, before starting the next. Do not batch the
cutovers: the whole benefit of versioning is that each one is independently reversible, and
that is lost the moment five views move on the same run.

### Step 1 — deploy

```bash
uv run python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
databricks bundle validate -t usnc_tds
databricks bundle deploy   -t usnc_tds
```

**THIS DEPLOY RE-READS ALL OF BRONZE, FOR ALL ELEVEN ENTITIES, NOT ONLY THE ONE YOU ARE
MIGRATING.** The flow name is built from the table name, and the table the pipeline writes
is now `stg_<table>` for every staged kind — so every source flow in the vault pipelines
gets a NEW NAME, and a new name means a NEW CHECKPOINT. A flow with a fresh checkpoint
re-reads its entire bronze source from the beginning. That includes
`nhl_general_journal_line_closed_year` at 14.6M rows and `nhl_general_journal_line` at
2.45M, whether or not they are the entity you are cutting over in this pass. Budget the
first `vault_load` after this deploy accordingly, and do not read a long first run as a
problem. (It is also why §6 puts the two large journals last.)

**AND IT RE-APPENDS EVERY REJECT EVER WRITTEN unless the version bumps are in — see §1.0.
That is the one consequence of the re-read that is NOT self-correcting.**

### Step 2 — load

```bash
databricks bundle run vault_load -t usnc_tds
```

Confirm the run is green and **write down its run id.** That id is what
`--gated-by-run` needs, and it is the only evidence the cutover accepts that the new
version has proved itself: gate outcomes are printed, never persisted, so a completed run
is the proxy. (See §1.2 if the run fails at `publish_stable_views` — that task should not
be in the job.)

### Step 3 — measure the new table against the old

Two numbers, both of them, on both versions. Row count alone will not tell you what you
need to know: the entire point of the change is that the new loader eliminates duplicate
hash keys the old streaming flow admitted, so **the row count is expected to be lower and
the distinct-key count is expected to be identical.**

```sql
SELECT count(*) AS rows, count(DISTINCT `<entity>_hk`) AS keys
FROM `02_usnc_silver_edm_dev`.`raw_vault`.`<name>_rev<OLD>`
```

```sql
SELECT count(*) AS rows, count(DISTINCT `<entity>_hk`) AS keys
FROM `02_usnc_silver_edm_dev`.`raw_vault`.`<name>_rev<NEW>`
```

Read the result like this:

* **new `keys` < old `keys`** — STOP. Keys have been lost. Do not cut over. Something
  upstream of the loader is dropping rows; the anti-join never can.
* **new `keys` = old `keys`, new `rows` < old `rows`** — the expected outcome. The
  difference is duplicates the old flow admitted. `nhl_invoice_line` is the known case:
  an in-stream `dropDuplicates` keeps its memory in the CHECKPOINT, and a checkpoint reset
  re-reads the whole staging log against an empty memory — measured 28 September, exactly
  that duplicated 1,164 rows into `nhl_invoice_line_rev2`. The anti-join asks the TARGET
  instead, and the target does not forget.
* **new `rows` = new `keys` = old `keys`** — also fine, and what a clean entity looks like.
* **new `rows` > old `rows`** — STOP and read the load log. The loader is `NOT EXISTS`, so
  it cannot insert a key that is already there.
* **new `rows` = 0** — the cutover will refuse anyway. Find out why the load produced
  nothing before you go near `--apply`.

### Step 4 — cutover, dry run

```bash
# via a temporary bundle job running as the service principal -- see 1.3
checks/cutover_vault_version.py \
  --catalog 02_usnc_silver_edm_dev \
  --schema raw_vault \
  --control-schema control \
  --stable-name <name> \
  --version <NEW> \
  --gated-by-run <the run id from step 2>
```

It prints the target's row count, whether that run gated it, and then either

```
cutover proven: ... exists, holds N row(s), and is gated by run '<id>'
DRY RUN -- nothing was changed. Re-run with --apply to repoint the view above.
```

or `CUTOVER REFUSED: ...` and exit 1. The three refusals are: the target does not exist;
the target exists but has never loaded (0 rows); the target has not passed its domain's
gates (no `--gated-by-run`, or that run wrote no audit row for this table). Clear the
refusal, do not route around it.

### Step 5 — cutover, apply

Same command with `--apply` appended. It issues one statement:
`CREATE OR REPLACE VIEW <stable> AS SELECT * FROM <name>_rev<NEW>`.

### Step 6 — re-grant, immediately

```bash
databricks bundle run grant_vault_access -t usnc_tds
```

§2.1 is why this is a step and not a footnote; §2.2 is how to read its exit code. Then
confirm by hand that the view is readable as a member of `scope_tds_edm_vault_read`:

```sql
SELECT count(*) FROM `02_usnc_silver_edm_dev`.`raw_vault`.`<name>`
```

A zero-column result from `information_schema` at this point means the grants did not
land, not that the table is empty. Re-read §2.1.

### Step 7 — retire the predecessor, dry run then apply

```bash
# dry run first, always
checks/retire_vault_version.py \
  --catalog 02_usnc_silver_edm_dev \
  --schema raw_vault \
  --stable-name <name> \
  --version <OLD>
```

It prints the row count it would destroy, whether the stable view is live over that
version, and the version the model declares. Then `--apply` to drop.

Retirement is never automatic and is never part of the standing job: read what it says it
will destroy, run it, take the temporary job back out.

---

## 4. Retirement's two refusals, and why they are separate runs

Both refusals must be **cleared**, never bypassed.

1. **The LIVE version may never be retired.** Dropping the table the stable view currently
   points at destroys the data the estate is serving and leaves a view over nothing. `is_live`
   is not new state — it is read from the stable view's own `view_definition` in
   `information_schema.views`, matching the physical name inside it. Clear it by cutting
   over first (steps 4–6), then retiring.

2. **The DECLARED version may never be retired**, even when the view points elsewhere. A
   rolled-back cutover leaves the declared version not-live, and retiring it would throw
   away the version the next load is about to rebuild. Clear it by making sure you are
   retiring the PREDECESSOR (`<OLD>`), never the version `metadata/entities/` now declares.

**This is why the version bump and the retirement are separate steps in separate runs.** In
one run, the model declares `<NEW>`, the view points at `<OLD>`, and neither version can be
dropped — `<OLD>` because it is live, `<NEW>` because it is declared. Only after the cutover
has moved the view does `<OLD>` become droppable, and only then. A migration that tries to
bump, cut over and retire in a single pass is one that has to disable a refusal to finish.

There is a third, earlier refusal worth recognising, because it looks like a bug and is not:

> `'<name>'` is not a stable view name any entity in the model declares. Refusing to guess
> which version is 'declared'.

That means the `--stable-name` is misspelled or the entity is not in `metadata/entities/`.

---

## 5. Rollback

Rolling back one entity is one cutover in the other direction, provided its predecessor has
not been retired:

1. `cutover_vault_version.py` with `--version <OLD>`, `--gated-by-run` naming a run that
   loaded `<OLD>`, and `--apply`.
2. `grant_vault_access` again — the rollback is also a `CREATE OR REPLACE VIEW` and drops
   the grants exactly the same way.

This is the reason step 7 is last and is a deliberate separate act. **Once the predecessor
is dropped there is no rollback.** If there is any doubt about an entity, do steps 1–6 and
leave step 7 for a later day; a superseded `_revN` table costs storage and nothing else.

Note that a rollback leaves the model declaring `<NEW>` while the view serves `<OLD>`. The
loaders will not undo that — `stable_view_action()` returns `elsewhere` and every loader
leaves the view alone, printing `LEFT ALONE ... it already points elsewhere`. That message
in a `vault_load` log is a rollback being respected, not an error.

---

## 6. Order

`nhl_invoice_line` first. It is the one whose duplicate behaviour was actually measured
(1,164 duplicated rows into `_rev2`), and it is the only one already carrying a version
history — it was cut from `_rev2` to `_rev3` on 28 September — so its cutover is a
rehearsal of a route that has been walked once before, on this exact entity.

Then `hal_client_legal_entity_hierarchy`, then `nhl_journal_line`, then
`nhl_general_journal_line` (2.45M rows), then `nhl_general_journal_line_closed_year`
(14.6M rows) last — largest last, so that by the time the run takes real time, nothing
about the procedure is still being worked out.

The reason a run takes real time at all is named in §3 Step 1: renaming every flow to
`stg_*` gives every flow a fresh checkpoint, so the FIRST `vault_load` after the deploy
re-reads the whole of bronze for all eleven entities at once — the 14.6M-row closed-year
journal included, on the very first run, whichever entity you are cutting over.

One open question for that last pair, which belongs to Adrian rather than to the operator:
the staging log is append-only and never pruned, so `stg_nhl_general_journal_line_rev<NEW>`
will hold at least 2.45M rows for ever. Sizing was flagged as unaddressed in the plan and
should be answered before the journal domain migrates.

---

## 7. The combined migration: the hub split rides on this one

**Everything in §0–§6 still applies.** This section is an extension, not a replacement, and
there is no second runbook on purpose — a second runbook is how an operator follows the
wrong one. Read §0–§6 first; what follows is only what the split adds.

Adrian's decision to defer the keyed-kind staging migration into this one (see the Status
note at the top) is what makes the numbers below what they are. Run separately, the eleven
keyed non-hub entities would have been versioned twice each — twenty version operations,
twenty cutovers, and twenty sets of dropped grants (§2.1). Combined, eight of the eleven
take ONE bump serving two reasons at once. That saving is the whole argument for the
combination and it is the reason the scope below is larger than either plan's text.

### 7.0 Thirteen version operations, and one retirement

The scope is **not** "the seven children plus the two new hubs". It is:

* **eleven bumps** — every keyed non-hub entity in the model (`naming.KEYED_KINDS` minus
  `hub`: three links, six NHLs, two HALs). All eleven move because the staging
  materialisation reaches all eleven; eight of them ALSO carry a renamed hash-key column
  from the split, and three carry only the staging reason.
* **one bump that this plan originally MISSED, added 29 September when the load found
  it** — `hub_supplier`. The split gave it a second binding (`GP_VENDOR`), which rebuilt
  its staging log with a different column order while `hub_supplier_rev1` kept the old
  one. `load_hubs` creates from the log and INSERTs positionally, and
  `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table, so the two silently
  disagreed until a `batch_id` was handed to `load_dts` and the cast failed. **Adding a
  binding to an existing entity is a shape change and needs a version**, which is the
  rule this row exists to record. It was the only failure among 23 keyed tables.
* **two creations** — `hub_client` and `hub_operating_company` are new. They are **built**,
  not cut over (§7.2).
* **one retirement** — `hub_organisation` no longer exists in the model and is dropped
  last of all (§7.4).

**And three new satellites, which are NOT version operations and are easy to miss for
exactly that reason.** `sat_client_details`, `msat_client_address` and
`sat_supplier_details` arrive with the GP party masters. They take no bump, appear in no
row of §7.1, and need no cutover — so nothing in this section's arithmetic counts them.
They still land as new objects in the lake, and they still need grants (§7.2). Fourteen
version operations plus three creations nobody versions.

### 7.1 The gated version table

**This table is asserted against `metadata/entities/` by `tests/test_accelerator.py`.** It
is not a snapshot to be confirmed by hand like §1.1's, and that is deliberate: this repo
has been bitten by a version table in a document going stale between writing and running,
and §1.1 carries the workaround ("confirm what is actually in `metadata/entities/` before
you start rather than trusting this table"). Here the suite does the confirming. Amend the
model without amending this table, or amend this table without amending the model, and the
suite goes red naming the row. The markers around it are what the checks locate, so if this
table ever moves, move them with it.

`predecessor` is the version the stable view still names, and therefore the version §3
step 7 retires. It is `_rev1` for ten of the eleven and **`_rev3` for `nhl_invoice_line`**,
which was already cut once on 28 September — retiring `_rev1` there would destroy a table
nothing is serving and leave the polluted `_rev2` and the live `_rev3` in place, while
believing the predecessor had been cleaned up.

<!-- VERSION-GATE START: asserted against metadata/entities/ by tests/test_accelerator.py -->

| stable name | predecessor | declared | operator action |
| --- | --- | --- | --- |
| `hal_client_legal_entity_hierarchy` | `_rev1` | 2 | §3 in full — `HUBSPOT` is active |
| `hal_consolidation_hierarchy` | `_rev1` | 2 | nothing — see §0 |
| `lnk_client_contracting_entity` | `_rev1` | 2 | nothing — see §0 |
| `lnk_client_job_request` | `_rev1` | 2 | nothing — see §0 |
| `lnk_legal_entity_consolidation` | `_rev1` | 2 | nothing — see §0 |
| `nhl_general_journal_line` | `_rev1` | 2 | §3 in full — `GP_US` is active |
| `nhl_general_journal_line_closed_year` | `_rev1` | 2 | §3 in full — `GP_US_HIST` is active |
| `nhl_invoice_line` | `_rev3` | 4 | §3 in full — `FIELDGLASS_US` is active |
| `nhl_journal_line` | `_rev1` | 2 | §3 in full — `UKG_US` is active |
| `nhl_payroll_detail` | `_rev1` | 2 | nothing — see §0 |
| `nhl_timesheet_line` | `_rev1` | 2 | nothing — see §0 |
| `hub_client` | none | 1 | built, never cut over — §7.2 |
| `hub_operating_company` | none | 1 | built, never cut over — §7.2 |
| `hub_supplier` | `_rev1` | 2 | §3 in full — `GP_VENDOR` is active |
| `hub_organisation` | `_rev1` | retired | dropped last of all — §7.4 |

<!-- VERSION-GATE END -->

The `operator action` column restates §0's five-of-eleven split and adds nothing to it. It
is prose, and the suite does not read it; the other three columns are gated.

### 7.2 The two new hubs are BUILT, not cut over

`hub_client` and `hub_operating_company` start at version 1, so there is no predecessor,
no `_rev<OLD>`, and **nothing to cut over**. `checks/load_hubs.py` creates
`hub_client_rev1` and `hub_operating_company_rev1`, loads them from their staging logs, and
`stable_view_action()` returns `bootstrap` for both because no stable view exists for
either name. The loader creates the view. That is the whole operation.

Consequences, so that the tools are read correctly on these two:

* **Do not run `cutover_vault_version.py` on either.** There is no version to move to. Its
  first refusal (the target does not exist) would fire on `_rev2`, which is the right
  answer to the wrong question.
* **Nothing was replaced, so no grants were dropped** — but nothing was granted either.
  The bootstrap creates the view with no grants at all, so `grant_vault_access` is still
  required before anyone is told the two hubs are readable. §2.1's zero-column
  `information_schema` symptom presents identically on a freshly bootstrapped view as on a
  freshly replaced one.
* **`hub_client` and `hub_operating_company` are ACTIVE in `usnc_tds`** — `GP_CUSTOMER`
  binds `hub_client` to `rm00101` and `GP_VENDOR`/`GP_US` reach
  `hub_operating_company` — so unlike the inactive six of §0 these two will hold real rows
  on the first load, and the row counts of §3 step 3 are worth reading even though there is
  no predecessor to compare them against.

#### The three new satellites are built the same way, and are equally ungranted

`sat_client_details`, `msat_client_address` and `sat_supplier_details` are new at version
1. `checks/load_satellites.py` creates each table and bootstraps its stable view, exactly
as `load_hubs.py` does for the two hubs above, and **every consequence in the three bullets
above applies to them unchanged**: no cutover, nothing replaced, no grants dropped — and
none granted.

All three are **ACTIVE** in `usnc_tds`: `GP_CUSTOMER` binds `sat_client_details` and
`msat_client_address` to `rm00101`/`rm00102`, and `GP_VENDOR` binds `sat_supplier_details`;
both source names are in the target's `active_sources`. So all three hold real rows on the
first load.

**Why this is worth its own paragraph rather than a footnote.** An ungranted stable view
does not raise — it presents as §2.1's zero-column, zero-row symptom, which is
indistinguishable from "the load produced nothing". These three are the objects most likely
to meet that: they are the only new objects in this migration that no version table lists,
no cutover step names, and no row-count comparison covers, because there is no predecessor
to compare them against. A reader working the §7.1 table to completion would finish the
migration having never looked at them.

**They do not change §2.2's count.** The 18 expected `TABLE_DOES_NOT_EXIST` failures are
all satellites whose bindings are INACTIVE in this lake; these three are active, so they
are created and granted normally. If the count moves off 18, these are not the reason —
read it as §2.2 says.

### 7.3 `key_derivation_guard` WILL say `RE_KEYED`. Do not pass `--force`.

Expect this, on this branch, before anything else goes wrong:

```
the model was RE-KEYED
```

**It is the gate working, and the prescription it prints — drop and reload — is wrong for
this change.** The reason is in `checks/key_derivation_guard.py`: `key_differences`
classifies **by key NAME**. The split renames sixteen hash-key COLUMNS
(`organisation_hk` → `client_hk` / `operating_company_hk`, and `client_legal_entity_hk` →
`engaging_legal_entity_hk`), so to a name-based comparison a rename is not a move — it is a
removal plus an addition. The tables behind the removed names hold rows in `usnc_tds`, so
`keys_at_risk` marks them risky and `verdict()` returns `RE_KEYED`.

**Not one hash-key EXPRESSION moved.** That is asserted byte for byte against
`tests/golden_key_expressions.json`, which was rendered BEFORE the split and is not
regenerated by it, and it was reproduced independently at review: 91 expressions each side,
75 identities unchanged with zero expression changes, 16 renamed and all 16 byte-identical.
A rename is a recoverable schema migration; a digest change is not, and the gate cannot
tell them apart from names alone. It is right not to guess.

**What to do:**

* **Do NOT pass `--force`,** and do not edit the gate. The rows really are keyed under
  names the model no longer declares; the resolution is the version bumps of §7.1, which
  put every renamed column on a NEW physical table that has never been keyed at all.
* Once the bumps are deployed, the removed names' tables are the PREDECESSORS
  (`_rev<OLD>`), which §3 step 7 retires. The verdict clears when the predecessors are
  gone, not before.
* `docs/superpowers/OPEN_ITEMS.md` records this under *"Do NOT deploy
  `feat/client-opco-split` alone"*.

**How to tell THIS `RE_KEYED` from a different one — read the CLAUSES, not the names.**
An earlier draft of this section said to confirm the verdict "names those sixteen". It
cannot be done, and following it would mean either mis-reading a real finding as this one
or stalling on a correct one. `verdict()` in `checks/key_derivation_guard.py` prints at
most **eight** names per clause (`risky_removed[:8]`, then `, …`), and both lists are
first filtered to keys whose tables actually hold rows — so neither the names nor the
count is the sixteen this section is about.

What IS readable is which of two clauses the message carries:

| clause in the message | meaning | is it this section? |
|---|---|---|
| `N recorded key(s) are no longer declared AND their tables hold rows` | keys vanished by NAME — which is what a rename looks like to a name-based comparison | **yes** — this is the expected verdict |
| `N key(s) now derive differently AND their tables hold rows` | a key kept its name and its EXPRESSION changed | **NO — STOP.** Not a rename. This is the unrecoverable case the gate exists for, and nothing in this section applies |

The split renames columns; it moves no expression. So the second clause must be **absent**.
If it appears, `--force` is not merely inadvisable — the gate is telling you something this
migration does not predict, and the 91-expression byte-comparison this section relies on no
longer describes what you are deploying.

Both clause strings above are gated against `key_derivation_guard.py` by
`tests/test_accelerator.py`, so a reworded message cannot leave this table quietly matching
nothing.

### 7.4 `hub_organisation` is retired LAST, and `retire_vault_version.py` CANNOT do it

`hub_organisation` is gone from the model — split into the two halves of §7.2. Retiring
`hub_organisation_rev1` is the last act of the migration, and it happens only after **both**
new hubs are live AND every predecessor table that still holds an `organisation_hk` column
has itself been retired (§3 step 7). That is the eight of §7.1 which carry the rename;
`hal_consolidation_hierarchy`, `lnk_legal_entity_consolidation` and
`hal_client_legal_entity_hierarchy` never pointed at this hub and do not gate it.

The satellites the split re-pointed do NOT gate it either, and it is worth knowing why so
that nobody waits for them: a satellite hangs off its parent's hash key
(`general_journal_line_hk`, `invoice_line_hk`), not off the hub's, so none of them carries
an `organisation_hk` column and none of them needed a version of its own. Only the keyed
kinds carry parent hash keys as columns.

Why last, and not merely "tidily last": every child that still reads `organisation_hk` is
reading its own predecessor table. Dropping `hub_organisation_rev1` before its last child
has moved leaves that child's foreign key pointing at a hub row that no longer exists —
silently, because nothing in the vault enforces the reference.

#### `retire_vault_version.py` refuses this entity by construction, and NOT on the refusal §4 describes

**Read this before you deploy a temporary retirement job for it, because the tool will
exit 1 and the message will look like a typo.** Read from
`checks/retire_vault_version.py` rather than measured, and the order of its own code is the
whole point:

```
'hub_organisation' is not a stable view name any entity in the model declares.
Refusing to guess which version is 'declared'.
```

That guard runs **first** — before the physical name is built, before the table-exists
check, before the row count, and before the view is read at all. `main()` walks
`model.entities` looking for one whose `stable_tables()` yields `hub_organisation`; there
is none, `declared_version` stays `None`, and it returns 1 immediately.

**So NEITHER of §4's two refusals ever evaluates on this entity.** In particular the LIVE
refusal — the one that exists because dropping the table a stable view serves is silent and
unrecoverable — is never reached. It is tempting to read "not declared" as *cleared*, since
§4 says both refusals must be cleared and the declared one obviously is; the code is the
other way round. **Being undeclared is not a cleared refusal, it is an earlier one,** and it
takes the live check down with it.

That also disposes of the obvious workaround: **do not add `hub_organisation` back to
`metadata/entities/` to satisfy the tool.** `spec.validate_model` now refuses a model
declaring that name outright, and re-declaring it beside the two halves is the partial
revert the split's own checks exist to prevent — three hubs, the third holding the union of
the other two, and nothing failing.

#### So the retirement is manual, and the safety is yours

As the owning service principal (§1.3), in this order, reading each answer before the next:

```sql
-- 1. ESTABLISH THAT YOU CAN SEE THIS SCHEMA AT ALL, and read what the drop destroys.
--    Thirteen rows when the split was argued (hub_client.yml records the breakdown: one
--    client, AEE1/Ameren, and eleven operating companies). Read the number here rather
--    than trusting that one -- it is a count on a date.
--    THIS STEP IS ALSO THE POSITIVE CONTROL FOR STEP 2. If it returns
--    TABLE_OR_VIEW_NOT_FOUND you lack USE SCHEMA, every NOT_FOUND below is meaningless,
--    and you must STOP and get the right identity -- not conclude anything is absent.
SELECT count(*) FROM `02_usnc_silver_edm_dev`.`raw_vault`.`hub_organisation_rev1`;
```

```sql
-- 2. IS ANYTHING STILL SERVING IT? This is the live check the tool would have done.
--    Read the ERROR CODE, and read WHICH OBJECT it names. Do not read an empty result
--    set from anywhere as an answer -- see the note below on why.
SELECT count(*) FROM `02_usnc_silver_edm_dev`.`raw_vault`.`hub_organisation`;
```

| Response | Meaning | Action |
|---|---|---|
| `TABLE_OR_VIEW_NOT_FOUND` naming **`hub_organisation`** | the view is genuinely absent | live check PASSES, proceed |
| `TABLE_OR_VIEW_NOT_FOUND` naming **any other object** (e.g. `hub_organisation_rev1`) | the view EXISTS and is DANGLING — its base table is already gone | **STOP** — step 3's `DROP VIEW` still applies; the view is real and something may still reference it |
| `INSUFFICIENT_PERMISSIONS` | the view EXISTS and you are not the right identity | **STOP** — you are not the service principal of §1.3; re-run the whole step as it |
| a row count | the view exists and you can read it | **STOP** — drop the view first, per step 3 |

**Why an empty result set is never the answer here.** `information_schema.views` and
`SHOW TABLES` are both permission-filtered: under an identity without SELECT on the view,
both return empty — the same answer they give when nothing is there. **Measured
29 September against `02_usnc_silver_edm_dev`:** `information_schema.views` returned `[]`
while `hub_organisation` was in fact present, because the querying identity lacked the
grant. A check that cannot distinguish "absent" from "you cannot see it" fails OPEN, in
the one step whose whole job is to prevent an unrecoverable drop. That is why step 2 reads
an error code and not a result set, and why step 1 exists at all.

**What the control does and does not prove.** Measured the same day: a deliberately
nonexistent name in this schema returns `TABLE_OR_VIEW_NOT_FOUND`, so Unity Catalog is not
masking absence behind a permission error *at the privilege level that probe ran at*. It
does **not** prove the same one level down — an identity lacking `USE SCHEMA` can be told
NOT_FOUND for an object that exists. Step 1 is what closes that gap operationally: it
fails first, on an object you know is there. Re-run both on a different workspace rather
than assuming they carry over.

```sql
-- 3. THE VIEW FIRST, THEN THE TABLE. This is the one place in the whole migration where a
--    stable view is REMOVED rather than repointed, because hub_organisation has no
--    successor version to point at -- its successors are two differently named hubs.
DROP VIEW IF EXISTS `02_usnc_silver_edm_dev`.`raw_vault`.`hub_organisation`;
DROP TABLE `02_usnc_silver_edm_dev`.`raw_vault`.`hub_organisation_rev1`;
```

Also drop `stg_hub_organisation_rev1` and `qtn_organisation_rev1` if they exist, for the
same reason and in the same pass — they are the staging log and quarantine twin of a table
no pipeline declares any more, so nothing will ever write to them again and nothing will
ever clean them up either.

**This gap is `retire_vault_version.py`'s, not this migration's**, and it is recorded in
`docs/superpowers/OPEN_ITEMS.md`: the tool cannot retire the last version of an entity the
model has removed, which is exactly the case in which an operator most needs its live
check. Retiring a REMOVED entity is a different operation from retiring a SUPERSEDED
version, and the tool only models the second.

### 7.5 Order

§6's order stands for the five active entities and is unchanged. The combined migration
adds three bookends:

1. **The two new hubs first**, before any child cuts over. A child's foreign key is only
   meaningful once the hub row it points at exists, and `hub_client` /
   `hub_operating_company` are parents of seven of the eleven (three under `client`,
   four under `operating_company`; `lnk_client_contracting_entity`'s two parents are both
   `legal_entity`, so it does not wait for them).
2. **Then §6's order, unchanged**: `nhl_invoice_line`,
   `hal_client_legal_entity_hierarchy`, `nhl_journal_line`, `nhl_general_journal_line`
   (2.45M), `nhl_general_journal_line_closed_year` (14.6M) last.
3. **`hub_organisation` retired last of all**, per §7.4 — and only after step 7 has
   retired the predecessors of the children, since a child's `_rev<OLD>` is the table whose
   `organisation_hk` points into the hub being dropped.

The six inactive entities of §0 need nothing at any point, and that has not changed.
