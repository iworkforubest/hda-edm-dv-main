# Gold schema topology — design

**Date:** 27 September 2026
**Status:** proposed
**Scope:** subsystem A of four (see §8). Creates the gold layer's schemas. Generates no
gold data and grants no designed access.

---

## 1. Intent

Stand up the gold layer's schema topology in `03_<lake>_gold_edm<env>` so the
`wd_fin_export` project has somewhere to be built, and so `reference_data` and
`master_data` exist as the shared schemas every project schema reads from.

Stated by Adrian, 27 September: the three schemas are `reference_data`, `master_data`
and `wd_fin_export`; `wd_fin_export` is the main project schema; `reference_data` and
`master_data` "can be accessed by any project schema".

**What this spec deliberately does not do.** TDS is a test/development/staging
environment. Adrian's ruling: *"I would postpone all the security and governance
aspects to be implemented and tested before we push this to prod."* The role model —
data engineers, data modelers, data architect, data analysts, and what each may do —
is subsystem B and gets its own spec. This spec therefore contains no designed access
control, only the TDS convenience in §6.

## 2. Two corrections to the repo's own record

Both were true when written and are false now. They are stated here because code
comments still assert them.

**The gold catalog exists.** `src/accelerator/gold_quality.py` opens with a
measurement from 29 August: *"`03_usnc_gold_edm_dev` does not exist -- `databricks
schemas list` answers 'Catalog ... does not exist.'"* Measured again 27 September:
the catalog exists and contains exactly one schema, `information_schema`. Nothing has
ever been built in it. The docstring's conclusion still holds; its premise does not.

**The schema names in `databricks.yml` disagree with this spec.** The comment above
`gold_export_schema` reads *"The planned schemas are reference, master, wd_fin_export,
governance and control"*. Adrian chose `reference_data` and `master_data`. That comment
is updated in the same change, so the repo carries one story rather than two.

`wd_fin_export` needs no decision: `gold_export_schema: wd_fin_export` is already set
on all four targets and matches.

## 3. The five schemas

| schema | holds | readable |
|---|---|---|
| `reference_data` | shared conformed reference — calendars, code lists, currency | shared (§6) |
| `master_data` | shared conformed master entities — legal entity, worker, supplier | shared (§6) |
| `wd_fin_export` | the project schema; Workday finance export tables | project (§6) |
| `governance` | gold's mask functions and grant DDL | nobody |
| `control` | `aud_load_run`, `aud_table_load`, `aud_table_discard` | nobody |

Five, not the three named, because `governance/control_objects_gold.sql` **already**
creates `${gold_catalog}.${control_schema}` with those three audit tables. That schema
arrives the moment the file is applied whether or not this spec plans for it, so it is
planned for. `governance` joins it for symmetry with silver, where masks and grants
live apart from the load audit.

`governance` and `control` are readable by nobody, which is silver's posture: the
control surface is inside the Phase 6 STOP and is not read directly.

### 3.1 How "readable by any project schema" actually resolves

A Unity Catalog schema is not a principal and cannot be granted anything. The
requirement resolves into two distinct mechanisms, and the first is the one that
matters:

**Ownership chaining.** All five schemas are owned by the same service principal that
owns the vault schemas (`7732b208-8366-4aef-af09-60e9dec9cf86`). A view in
`wd_fin_export` that reads `reference_data` or `master_data` therefore resolves for any
principal holding SELECT on *that view*, with no grant on the shared schemas at all.
This is what lets any project schema build on the shared ones, and it needs no
per-project grant bookkeeping.

**Direct reads of the shared schemas** — an analyst querying `master_data` itself — need
a real grant. That grant is subsystem B's to design. §6 is a TDS stand-in, not it.

## 4. What creates the schemas

A new emitter and a new generated artefact, following the pattern
`governance/control_objects_gold.sql` already establishes:

```
src/accelerator/gold_layout.py   -- SCHEMAS: the single source of the five names
tools/emit_gold_schemas.py       -- renders the DDL from gold_layout.SCHEMAS
governance/gold_schemas.sql      -- generated, committed, byte-gated against drift
```

**Why a separate file from `control_objects_gold.sql`.** That file's contract is the
control surface, and `checks/control_conformance_check.py` gates it against
`control_standard.LAYER_TABLES["gold"]`. Folding four unrelated `CREATE SCHEMA`
statements into it would widen a gate whose value is its narrowness.

**Why a module for five strings.** The names appear in the emitter, the SQL, the
verification checks, the grant job and `databricks.yml`. This repo has already paid for
restatement — `gold_quality.py`'s own comment warns that a duplicated list is "a fourth
thing to keep in step". `gold_layout.SCHEMAS` is the one source; everything else derives.

Every statement is `CREATE SCHEMA IF NOT EXISTS`, so the task is idempotent and safe to
re-run.

## 5. The job

A new bundle resource, `resources/gold_job.yml`, declaring job `gold_build` with a
single task `create_gold_schemas`.

**Not a task in `vault_load`.** Adrian, 26 September: *"we have a job with almost 30
task, this is not ok."* Gold is a different layer on a different cadence, and the
domain-split spec already rules that layered jobs are the direction. `gold_build` is
where subsystem C's export tasks will land, and where `invoice_export` moves when it is
implemented.

`invoice_export` **stays in `vault_load` and stays a stub** under this spec. It
currently prints `NOT IMPLEMENTED` and exits 0, which is why the 30-task load is green
while writing nothing to gold. Moving it is subsystem C's change, not this one.

## 6. TDS visibility — explicitly temporary

With no grants, the gold schemas are invisible to Adrian, exactly as the vault was on
27 September until `grant_vault_access` ran.

`grant_vault_access` is extended to cover the gold catalog: `USE CATALOG`, `USE SCHEMA`
on `reference_data`, `master_data` and `wd_fin_export`, and per-object `SELECT` to
`scope_tds_edm_vault_read`. `governance` and `control` get nothing.

Three constraints carry over unchanged:

- **Per object, never schema-level.** DEF-40 is not suspended because this is gold. A
  `GRANT SELECT ON SCHEMA` would cover any SDP `__materialization_*` twin that appears
  later, and `checks/schema_grant_check.py` fails the build on one.
- **It grants nothing today.** Gold has zero tables, so the per-object emission is
  empty. It must say so rather than report a successful run over nothing — the failure
  mode `apply_governance` already guards with its `NO DATA GRANTS EMITTED` message.
- **It is labelled as a stand-in.** This is a TDS convenience so the layer is visible
  while it is built, and subsystem B replaces it before prod.

## 7. Verification

Added to the offline suites. Every check must be demonstrated failing before it is
believed — this repo has shipped checks that could never fail, and the discipline is
that the next unfailable one is assumed to be present.

**Structural (`verify_repo.py`):**

1. `governance/gold_schemas.sql` is byte-identical to what `tools/emit_gold_schemas.py`
   renders — the emit → commit → byte-gate contract.
2. The DDL names exactly `gold_layout.SCHEMAS` — no schema created that the module does
   not declare, and none declared that the DDL does not create.
3. Every statement is `CREATE SCHEMA IF NOT EXISTS`; no `DROP`, no `CREATE OR REPLACE`.
   A gold schema must never be replaced: DEF-61 established that replacing a securable
   discards every grant held against it, and a schema is a securable too.
4. Every target's `gold_export_schema` value is a member of `gold_layout.SCHEMAS`. This
   ties `databricks.yml`'s independent naming to the module instead of duplicating it.
5. Every schema in the rendered DDL is qualified with `${gold_catalog}` and never
   `${catalog}` —
   creating these in silver is the plausible wrong outcome.
6. No bundle resource declares a gold dashboard. This check **already exists** and
   **stays**: its premise is that gold has nothing to show, and creating empty schemas
   does not change that. It is scoped to `quality_gold*` keys, so it is not tripped by
   this work.

**Behavioural:**

7. `checks/schema_grant_check.py` extends its sweep to the gold catalog. It currently
   inspects silver, control and bronze and does not look at gold at all, so a broad
   grant there would pass unseen today.

## 8. Non-goals, and where they went

| deferred | to |
|---|---|
| Role model: engineer / modeler / architect / analyst rights | **B** — before prod |
| Column masking in gold, and whether a gold write materialises masked or cleartext values | **B**, after the groups are defined (Adrian's ruling) |
| Gold table generation from the AME invoice rules and Logan's DCDD | **C** |
| Customers, Suppliers, Journals, Customer + Supplier Invoices migration; CSV export; Snaplogic; DT/DTS | **C** |
| Delta Sharing to the EU global catalog; the XML/API app; scheduled flows | **D** |
| Moving `invoice_export` into `gold_build` | **C** |
| The gold quality dashboard | stays undeclared until gold holds data |

## 9. Open risks

**The masking question is now ANSWERED, and this paragraph replaces what it said.**
Measured 27 September 2026, after the gold schemas were created.

The original text named `is_account_group_member('hfig_commercials_reader')` as the
deciding predicate. **That was wrong.** No mask function in `governance/apply_masks.sql`
references that group. The masks exempt `pii_cleared_us` (PII), `usnc_data_analyst_finance`
(money), and — in every one of the five functions —
`global_dataplatform_pipeline_job_runners`.

**The risk is the opposite of the one recorded here.** The fear was that a gold write would
read cleartext and materialise unmasked money into gold, where gold tables carry no mask.
It will not: `resources/vault_job.yml` records, beside the withdrawn journal gate, that
*the run-as service principal is NOT in `global_dataplatform_pipeline_job_runners`*, so
every `debitamt`/`crdtamnt` it reads returns NULL. The mask survives into gold by default
and there is no leak.

**What there is instead is a hard dependency, and subsystem C must not start without it.**
The writing identity cannot read the money it is meant to export. A `wd_fin_export` table
projecting invoice amounts would be written with NULL in every amount column, and the load
would report success. That is the same failure that forced `assert_journal_integrity` out
of the job: the gate reads zeros and correctly refuses to assert on them.

**PLT-2 is the fix** — "add the load identity to
`global_dataplatform_pipeline_job_runners`" — and it is unsent. It must land before any
gold export carries money, or the export is built and found empty at the end.

Measurements, taken as `adrian.turcu@vertage.com`, who IS in the job-runner group:
`nhl_general_journal_line` returned `debit_visible = 2,453,131` of `rows_total =
2,453,132` — cleartext. An identity outside that group reads NULL; the 25 August DEF-40
measurement recorded the same column as `count(debitamt) = 0`.

**`gold_export_schema` has no default, deliberately.** If a new target is added without
setting it, configuration fails naming the target. Check 4 above preserves that property
rather than introducing a default to satisfy the module.

**The TDS grant is a known temporary.** If subsystem B slips, `scope_tds_edm_vault_read`
retains read on gold by default. That is the intended behaviour for a test lake, and the
wrong behaviour for prod; B is the control.
