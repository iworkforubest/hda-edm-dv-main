# HFIG Data Vault Accelerator — SDP-native, metadata-driven

**v0.2.0** · hash rulebook `1.0.0` · see `CHANGELOG.md`

Generic Data Vault 2.0 generator for the HFIG EDM. Metadata in, Lakeflow Spark
Declarative Pipelines out. EU first; the other three regional lakes receive the same
committed artifact once the conformance gate is green.

ARB decision basis: SDP-native (option C). No dbt, no second framework, no
Lakeflow↔dbt boundary rule to ratify.

Superpowers methodology layer: `superpowers@claude-plugins-official` v6.3.0. Enabling
the plugin is developer-local and deliberately not tracked here. What is tracked is the
gate mapping that binds Superpowers phases to this repo's hard gates:
`.claude/skills/dv-accelerator-gates/SKILL.md`.

---

## Why this shape

| Decision | Rationale |
|---|---|
| **Metadata in the repo, not in a table** | A structural change to the vault becomes a pull request rather than an `UPDATE`. It also makes the four lakes identical *by construction*: same commit, same model, same keys. |
| **Append flows only in the vault** | Append is the default flow type for a streaming table, so insert-only is the native path here rather than something we fight for. `AUTO CDC` is Bronze-only and never touches a vault table. |
| **Multi-source hubs = N append flows into one table** | `create_streaming_table()` then one `append_flow` per source. Seven sources on `hub_job_request` is seven flows, so one source's schema change or backfill affects exactly one flow. |
| **Change detection delegated to Bronze** | A streaming table cannot read itself. Rather than reintroduce a stateful lookup (and with it a load order, killing restartability), Bronze emits the change stream — natively for CDC sources, via `AUTO CDC FROM SNAPSHOT` for snapshot sources. `hashdiff` is still computed and stored for audit and cross-source parity. |
| **Type 2 is a view** | `<table>_v1` computes `valid_from` / `valid_to` / `is_current` from `load_dts` + `sub_seq`. Nothing in Silver stores an end date. |
| **Quarantine is a second flow** | Every source binding emits a valid flow and an invalid flow, so `landed + (quarantined - superseded) = approved` reconciles for loop 1 -- `superseded` is a reject a later run legitimately re-accepted (control.ctl_quarantine_superseded), bounded by `over_subtracted()` so the subtraction itself cannot exceed what was ever quarantined. Silent drop is a control-plane violation, not a style choice. |
| **Catalog isolation before any grant** | PROD and TDS share a regional metastore, so `apply_masks.sql` sets isolation mode as its first statement and the workspace binding is verified *negatively* — the prod catalog must be absent when listed from TDS. |
| **Grain is declared, not inferred** | The GL journal is `grain: aggregate`, `aggregates_from: payroll_detail`, `aggregate_drops: [worker, pay_period]`. An account-level total that never says it is one gets joined to a worker eventually. Validation refuses an aggregate that also claims a grain it dropped. |
| **Aggregate invariants are gates, not expectations** | Expectations are row-level. `SUM(debits) = SUM(credits)` per journal spans rows, so it lives in `journal_integrity_check.py`. Shipping it as an expectation would look like coverage and provide none. |
| **Reference type is part of identity** | Workday references are (type, value) pairs. `BRPLM` as a `Company_Reference_ID` and as an `Organization_Reference_ID` are different assertions, so the type is in the business key. |
| **A NULL business key is a defect, not a value** | Hashing the null token into a hub would create one identity accumulating every unkeyed record from every source. Missing keys quarantine instead. |
| **Hash parity is gate zero** | `checks/hash_parity_check.py` runs before any load and proves this region's Spark output equals the pure-Python reference implementation over 7 key and 7 hashdiff golden vectors. A divergence means keys in this lake cannot join to keys in another. |
| **Hard gates are tasks, not conventions** | Append-only, loop-1 reconciliation and mask survival each fail the job. A rule that depends on nobody typing `--full-refresh` eventually fails. |
| **Expectations from Unity Catalog** | Data quality expectations are governed configuration held in UC, not literals in code. |
| **Every lake declares every source; only some load** | A source is in Bronze in some lakes and not others. `create_streaming_table()` is emitted for every declared binding everywhere, so the inventory is identical and conformance compares like with like; `append_flow()` only for the bindings a target names in `active_sources`. An inactive binding yields an empty table, not a missing one, and nothing fails at definition time. Activity is *declared*, not probed: a catalog probe cannot tell a source absent by design from one absent by accident. A name matching no binding is refused. |
| **Region and environment are not inputs** | The generator never sees which of the 8 workspaces it is building for. Structural change replicates by *deploy*; runtime DDL replication would breach residency and diverge within a quarter. |

---

## Getting this project

Extract the archive; do not download files individually. The bundle
uses relative paths so a flat copy cannot be deployed.

```bash
unzip hfig-dv-accelerator-v0.2.zip && cd hfig-dv-accelerator-v0.2
uv sync
uv run python verify_repo.py          # repo integrity; starts with a layout check
uv run python tests/test_accelerator.py
```

Both run offline — no workspace, no profile, no credentials — and `.github/workflows/verify.yml`
runs them on every push to `main` and every pull request, on Python 3.11 (the declared
floor) and 3.13. The gates that read a live lake stay in the `vault_load` job against a
named target, because a profile is passed explicitly and never auto-selected.

There is a third suite, and it needs more than the other two:

```bash
uv run --extra spark python tests/test_spark_derivation.py
```

It runs `factory._stage_full` in a local Spark session for every binding and compares the
digests Spark produces against the pure-Python reference. It exists because `factory.py`
cannot be imported without pyspark — the offline suite stubs pyspark so it runs anywhere with
no JVM — which meant the loader's key derivation was executed by no test in this repo at all
until 4 September 2026. It needs a JVM: CI installs Temurin 17, and locally the suite looks
for one at `~/.local/share/jdk/current` when `JAVA_HOME` is unset.

**It is not gate zero.** `checks/hash_parity_check.py` runs in the workspace, on the real
runtime, and it is the authority on what this lake's digests are. Local OSS Spark agreeing is
necessary, not sufficient.

## High-variability sources (Fieldglass: 300+ per-client customisations)

A source with a standard column set plus per-client custom columns is split by
**stability**, not by client:

| | Structure | Onboarding a client |
|---|---|---|
| standard columns | `sat_job_request_details_fieldglass_eu` — typed, shared by all clients | no change |
| custom columns | `msat_job_request_custom_field_fieldglass_eu` — multi-active on the field name | **adds rows, not columns** |
| promoted fields | `csat_job_request_custom_promoted` — Business Vault, registry-driven pivot | rebuild, not migration |

Rejected: one wide satellite (thousands of sparse columns, and the column *names* leak
client taxonomy — a mask cannot hide a column's existence); one satellite per client
(300 tables per entity, onboarding needs a deploy); a VARIANT column (hashdiff unstable
unless JSON keys are canonicalised, and **a column mask cannot apply to a field inside a
variant**, so sensitive custom fields become unmaskable).

`ref_fieldglass_custom_field` — the registry of client, field, type, meaning, sensitivity —
is platform-owned config estate and is what stops the key-value satellite becoming a swamp.

**Tenant-scoped keys.** If a source's references are unique only *within* a buyer instance,
`key_style: tenant_scoped` is mandatory and `tenant_key` must name the tenant columns, which
must be part of `business_keys`. Validation enforces all three, because the failure it
prevents — two clients' records merging into one hub row — is silent.

## Capacity

```bash
python tools/estimate_footprint.py                      # all lakes
python tools/estimate_footprint.py --region uks --pit-grain month_end
```

Structure comes from the model (real column counts, key widths, fan-out); **volumes come
from `metadata/volumes.yml` and are placeholders**. The output is built to show which inputs
the answer is sensitive to rather than to produce a number worth quoting.

Three things it makes visible: the PIT snapshot grain is the biggest single storage lever
(daily → month-end is roughly a 30× reduction), compression is the widest unknown (~4×
across a plausible range), and volume concentrates in two or three NHLs — the many small
satellites from per-source splitting are not the cost driver.

Replace the guesses by measuring: load one domain into `usnc_tds`, read bytes-per-row and
DBU-per-load from the metrics vault, put the actuals in `volumes.yml`.

## ERD

```bash
python tools/render_erd.py     # -> docs/hfig_dv_erd.html and .pdf
```

**Generated from the metadata, never drawn.** One overview page plus a detail page per
domain, A3 landscape. Satellites appear once per source, because that is what the factory
creates. Aggregate-grain tables get a dashed border and a dashed `reconciles` edge to their
transaction-grain counterpart; masked columns are marked. The verifier checks the rendered
ERD still contains every current table, so a stale diagram fails the build rather than
quietly misleading someone.

## Layout

```
metadata/entities/*.yml     the model — the only thing that changes to add an entity
src/accelerator/
  hashing.py                THE HASH RULEBOOK. Nothing else may build a hash.
  naming.py                 prefixes + technical column standard
  spec.py                   load + validate metadata; every rule fails the build
  factory.py                emits SDP tables and flows from metadata
src/pipelines/
  bronze_ingest.py          the only place AUTO CDC is permitted
  silver_vault.py           shared entry point for BOTH vault pipelines; all structure
                            comes from metadata. hfig.vault_layer (raw|business) picks
                            which entities each pipeline declares -- an SDP pipeline
                            targets one schema, so raw_vault and business_vault run
                            this same file with different configuration.
governance/apply_masks.sql  UC mask functions, application, grants (raw_vault AND
                            business_vault schemas)
checks/
  append_only_check.py      HARD GATE — DESCRIBE HISTORY shows zero mutation
  loop1_reconciliation.py   HARD GATE — landed + (quarantined - superseded) = approved
  mask_survival_check.py    HARD GATE — declared-sensitive ⇒ masked
  preflight_target.py       PREFLIGHT — authenticated workspace == intended target
  hash_parity_check.py      HARD GATE — Spark digests == reference implementation
  journal_integrity_check.py HARD GATE — debits = credits; lines = control total;
                            column names read from each entity's accounting: block
  aggregate_reconciliation_check.py
                            HARD GATE — raw payroll detail == GL journal, per account
  conformance_check.py      CROSS-REGION GATE — run before deploying region two
  publish_metadata.py       projects the repo metadata into UC for lineage
resources/*.yml             two vault pipelines (raw_vault, business_vault) + job with
                            the gates wired in
databricks.yml              usnc_tds active; weu/uks/aue declared but gated
tests/test_accelerator.py   pure Python checks, no workspace needed (count printed at runtime)
tests/test_spark_derivation.py  factory.py's key derivation in a real Spark session; needs
                            `--extra spark` and a JVM, so it is not part of the offline suite
tools/render_erd.py         ERD -> docs/*.html + *.pdf, from metadata
tools/estimate_footprint.py storage projection from model + declared volumes
verify_repo.py              repo-integrity checks, offline (count printed at runtime)
CHANGELOG.md                what changed in v0.2, and what is still open
tests/golden_hash_vectors.json  pinned digests + the exact pre-hash strings
```

## Adding an entity

One file. No code.

```yaml
name: supplier          # the business concept -- never supplier_striive
kind: hub
domain: party
key_style: federated
business_keys: [supplier_reference]
sources:
  - name: STRIIVE_EU
    bronze_table: hfig_eu.bronze.striive_supplier
    key_columns: [supplier_ref]
    cdc_op_column: _cdc_op
```

`python tests/test_accelerator.py` then validates it; CI blocks the merge if it
violates a Data Vault rule.

## What validation refuses to build

Each of these is silent at run time and expensive to unwind, so it fails the build:

- a hub carrying descriptive attributes
- a link carrying descriptive attributes
- an NHL without a transaction key (parent hub keys alone are never unique)
- a satellite with more than one source
- a multi-active satellite without a natural sub-key
- **a link whose parent is another link** — the unit-of-work violation in the current ERD
- a payload whose existing column order changed (invalidates every stored hashdiff)
- any `ctl_` / `ref_` / `reg_` / `agg_` object — platform-built per the ARB boundary rule
- an entity pinned to a superseded hash rulebook version

These are rules about the **declaration**. Since DEF-26 they are also rules about what
lands: `factory._projection` narrows every entity to its declared model — a hub to its
hash key, its declared business keys, the readable `_bk` and the system columns, and
nothing else — and a quarantine twin to exactly what its target holds plus
`failure_rule` and `failure_detail`. If a column is not named by `business_keys`,
`parent_keys`, `transaction_key`, `payload`, `mas_key` or the system column set, it does
not reach the table. Before that, `_stage` appended the whole staged Bronze row: 92
columns on a hub declaring four business keys, and — because the factory masks only an
entity that *declares* masks, which a hub never does — 2.7 million readable amounts on a
column that was masked one table over.

---

## RECONCILE BEFORE FIRST LOAD

Three things in here are the platform default as evidenced in the spike documents and
EDM v0.4 ERD, **not** an authority. Check them against `TECH_COLUMNS_STANDARD` in the
sample-data workbook and change them once:

1. **`hashing.py` constants.** Two are now RATIFIED and guarded at import, so they
   cannot change without bumping `RULEBOOK_VERSION` in the same reviewed commit:
   **SHA-256 / BINARY(32)**, and **`HASHDIFF_UPPERCASE = False`** (payload case is
   preserved — a case correction is a change, and folding it means the new casing is
   never stored at all).

   `||` and `^^` remain the platform default and should still be confirmed against the
   workbook — but the choice is no longer load-bearing: any key component containing
   the delimiter, equal to the null token, or NULL is **quarantined with a named
   reason** rather than silently producing a wrong identity
   (`hashing.key_safety_rules`). The golden tests fail loudly on any change, which is
   the point.
2. **`naming.py` column names** — the ERD says `ingested_at` and `record_hash`; the
   spike says `LOAD_DATETIME` / `ldts` and `HASHDIFF`. These cannot all be right.
3. **Mask group names** in `apply_masks.sql` — `hfig_worker_pii_reader` etc. are
   placeholders for real account-level groups.

## Day-1 verification list

| # | Task | Why it is day 1 |
|---|---|---|
| 1 | **Mask survival through projection.** Read a masked satellite column through `_v1`, a PIT MV and a Gold view as an unprivileged principal. | If masks do not propagate, personal/financial columns must move to satellites Gold never projects. That is a *modelling* change, so it must be known before entities are declared. |
| 2 | ~~**`change_detection: antijoin` flow shape.**~~ **RETIRED 29 Sep 2026 — nothing to verify.** The mode was accepted by `spec.validate` and implemented in no code path; it is now refused. | The fallback this row held in reserve — a batch job task outside the pipeline — is what actually runs: `checks/load_hubs.py` anti-joins the target for every keyed kind, `checks/load_satellites.py` hashdiff-compares for satellites. `cdc` is the only accepted value. |
| 3 | **NHL re-run duplication.** Replay one timesheet file twice and assert the uniqueness check still passes. **Still worth running; the reason changed on 29 Sep 2026.** | NHLs have no hashdiff, and the in-batch `dropDuplicates` guard is not cross-batch idempotency — which is why `c3bd6c1` stopped relying on it: an NHL now stages to a `stg_` log and `checks/load_hubs.py` loads it with the same `NOT EXISTS` as row 2. The replay verifies that anti-join against real re-delivery, not the checkpoint. |
| 4 | **Ghost `once=True` flow on re-deploy.** Confirm the ghost row is not re-appended when the pipeline is redeployed. | A duplicated zero key breaks PIT equi-joins. |

## Not built yet (deliberate)

Scoped out of v1 so the first domain can land: PIT and bridge generation, effectivity
satellites, record-tracking and status-tracking satellites, computed (business vault)
satellites beyond the metadata contract, the metrics vault, and the `AUTO CDC FROM
SNAPSHOT` wiring per Bronze feed. The metadata schema already accommodates `esat`,
`csat` and `msat`, so these are additions rather than redesigns.

## Deploy

```bash
databricks bundle validate -t usnc_tds
databricks bundle deploy   -t usnc_tds
databricks bundle run vault_load -t usnc_tds
```

8 workspaces: 4 lakes (`weu`, `uks`, `usnc`, `aue`) x 2 environments (PROD, TDS). TDS is
a separate workspace, not a catalog. EU prod is the legacy workspace still named
**"Datalake"**; the target is `weu` and preflight matches on host, not name.

**Only 4 metastores.** Unity Catalog allows one per region, so PROD and TDS in a region
share it — `hfig_weu` and `hfig_weu_tds` sit in the same West Europe metastore and are
visible from both workspaces by default. Separate catalogs are not isolation; catalog
binding is a required step, not hardening. See `DEPLOY.md` Phase 0d.

Promote by environment then by region — see `DEPLOY.md` Phase 7. Run preflight before
every deploy; with 8 near-identical workspaces, landing artefacts in the wrong lake is
the realistic failure.

```bash
python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
```

```bash
# cross-region drift, all TDS
python checks/conformance_check.py --targets usnc_tds,weu_tds,uks_tds,aue_tds \
  --baseline usnc_tds --warehouse-id <id>

# environment drift, one region
python checks/conformance_check.py --targets usnc_tds,usnc --baseline usnc_tds --warehouse-id <id>
```

---

## Governance

This accelerator generates and loads worker, job-request and timesheet structures and
sits upstream of ML entity resolution. That is candidate and worker data driving
employment-adjacent processes, so it is likely in scope for HFIG's high-risk AI
classification and should be declared through **WISE intake** — including the
generator itself, since an AI-assisted automation that builds production pipelines is
what the **Shadow AI** framework asks to be declared. Registering it while it is a
spike is considerably cheaper than doing so once Job Request consumers are live.

Anything the model surfaces that ranks or screens people stays a recommendation for
human review, with the decision and its author recorded.

Residency constraints per region, lawful basis, and which masks apply to whom are
governed decisions for **WISE intake and the AI Council**, not settled in this repo.
What this repo does is make the architectural consequence of those decisions
enforceable.
