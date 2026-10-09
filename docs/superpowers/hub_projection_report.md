# DEF-26 — projection to the declared model, and the catalogue-wide mask assertion

Date: 2026-08-25 · Branch: `main` · Status: **built and tested offline. NOT deployed, NOT reloaded.**

---

## 1. The projection rule, per kind

`factory._projection(entity, src)` returns the table's column list from the **declaration
alone** — no Spark, no frame — so a table's shape is a modelling fact that can be asserted
without a workspace. `_project(df, entity, src)` applies it in one `select`, so every
expression is evaluated against the input frame and a business key whose model name
collides with a source column cannot read a value some earlier step overwrote.

| kind | columns, in this order |
|---|---|
| **hub** | `<entity>_hk`, one column per declared `business_keys` entry, `<entity>_bk`, system columns |
| **link / hal** | `<entity>_hk`, each parent's `<parent>_hk`, `transaction_key` (empty for a plain link), system columns |
| **nhl** | the same, plus the declared `payload` |
| **sat / esat / csat** | `<parent>_hk`, `hashdiff`, declared `payload`, system columns |
| **msat** | the same plus the generated `mas_key`, third in order |
| **quarantine twin** | exactly what its target holds, plus `failure_rule`, `failure_detail` |

System columns are `naming.SYSTEM_COLUMNS`: `load_dts`, `applied_dts`, `sub_seq`,
`rec_src`, `batch_id`, `manifest_id`, `cdc_op`.

**If a column is not named by `business_keys`, `parent_keys`, `transaction_key`,
`payload`, `mas_key` or the system column set, it does not reach the table.**

Three decisions inside that rule:

- **A binding's `payload` wins where it declares one** — `src.payload or entity.payload`,
  the same rule `hashdiff_expr` already used, so the columns that are hashed and the
  columns that are stored cannot drift apart. `gl20000`'s binding names `openyear` where
  `gl30000`'s names `hstyear`; they are separate tables, and each takes its own.
- **A hub's business keys are renamed to the model's names and cast to STRING.**
  `business_keys` are the model's names (`reference_type`, `fiscal_year`); the frame
  carries the source's (`input_db`, `openyear`, `jrnentry`) — or nothing at all where the
  binding supplies a `key_literal`. Six bindings feed `hub_organisation` under six
  different source column names for one key position, so a hub keyed on source names
  could not have one shape at all. STRING because the positions must agree across
  bindings: `fiscal_year` is `openyear` (INT) from GP and the literal `'NOT_APPLICABLE'`
  from UKG. `CAST(... AS STRING)` is also exactly what the hash rulebook does to a key
  component before hashing it, so the stored key and the hashed key are the same value.
- **A duplicate collapses to its first position.** An NHL's `transaction_key` is normally
  also in its payload (`seqnumbr`), and one column cannot appear twice in a select.
- **A declared column that collides with a system column raises**, named. Under the dedup
  above it would have won and the system column would have vanished silently.

**Order inside the flow is load-bearing in both directions.** `_stage_full` returns the
whole staged row and `_register_source_flows` evaluates the violation predicate against
it — expectations are governed configuration written into UC by `apply_governance.py` and
may name **any** source column, not only a modelled one. Only then does it project. The
quarantine flow does the same and adds its two reason columns **after** the projection, so
the twin is never wider than the table it shadows.

`_stage()` is now `_project(_stage_full(...))`, which is what `_derived_schema_ddl` reads —
so a declared schema is the declared model's shape by construction rather than by
agreement with a second description.

---

## 2. What the ghost flow needed

DEF-25's lesson — *declaring a schema converts every previously-cosmetic inconsistency
between flows into a hard failure* — applies to the projection, because the projection is
what makes every declared table's shape explicit.

- Where a schema is declared (`entity.masks` and at least one active binding),
  `_emit_ghost` is now built from **the same field list `_emit_target` declared**
  (`_derived_schema_fields`), not from a hand-kept subset. It cannot drift.
- `_ghost_column_sql` gives the **zero key to every `*_hk` column, not only the entity's
  own**. A link's parent hash keys were previously absent from the ghost row; they now
  carry the zero key, which is what keeps a PIT join to the parent hub an equi-join.
- Every other declared column — a business key, a payload column, the hashdiff — gets
  `CAST(NULL AS <declared type>)`. A ghost row asserts an identity that is deliberately
  absent, not a value. Typed, so `debitamt` arrives as `decimal(18,2)` and matches
  `governance.mask_money(v DECIMAL(18,2))`.
- System columns keep their literals, `cdc_op` included (DEF-25).
- A declared **NOT NULL** column the ghost cannot supply is **refused at definition time**
  with the column named, rather than failing on the first append with
  `DELTA_MISSING_NOT_NULL_COLUMN_VALUE`.

Where no schema is declared (no masks, or no active binding at all) SDP still infers the
table from its flows and the ghost stays hash key plus system columns — the ghost-only
tables are 8 columns today and stay 8 columns.

`_ghost_columns_sql` is pure and returns SQL strings, so the property that matters — *the
ghost supplies every declared column, in declared order* — is asserted against the declared
field list itself rather than inferred from a flow nobody can run offline.

---

## 3. The catalogue-wide mask assertion, and its non-vacuity

### What was wrong with the gate

`checks/mask_survival_check.py` walked only the entities that **declare** masks, plus their
`_v1` views, and looked up those entities' own tables. It never asked the opposite
question — *does this column name appear, unmasked, on a table that declares nothing?* —
so it reported **PASSED over a total bypass of the vault's only PII defence.** The fourth
check found on this branch whose condition could not fail.

### What was added

`unmasked_elsewhere(declared_masked, columns, masked, is_exempt)` — assertion 3. Any column
**name** declared masked anywhere in the metadata must carry a mask on **every** object in
the vault schema where that name appears, quarantine twins included. `schema_columns()`
reads `information_schema.columns` (not just `column_masks`, which can only confirm masks
that already exist).

It is a **name-level** assertion, deliberately. It does not ask whether the values are the
same values. The false positive — an unrelated column sharing a masked name — costs a mask
nobody needed. The false negative cost 2.7 million readable amounts.

The function is **pure and Spark-free**, so it can be fired in both directions offline.
A sweep that read zero columns is reported as a failure, not a pass.

### Non-vacuity: it fails against the vault as it stands

Run against `02_usnc_silver_edm_dev.raw_vault` (read-only, `information_schema` only, via
`--profile hfig-usnc-tds`; nothing deployed):

```
catalogue: 2,120 columns over 60 objects; 6 masked; 14 declared masked names
CATALOGUE ASSERTION -> 30 PROBLEM(S)
```

across **10 objects**:

| object | unmasked occurrences of a declared-masked name |
|---|---|
| `hub_accounting_journal` | `debitamt`, `crdtamnt`, `debit`, `credit` |
| `hub_organisation` | `debitamt`, `crdtamnt`, `debit`, `credit` |
| `hub_ledger_account` | `debit`, `credit` |
| `hub_pay_period` | `debit`, `credit` |
| `qtn_accounting_journal` | `debitamt`, `crdtamnt`, `debit`, `credit` |
| `qtn_organisation` | `debitamt`, `crdtamnt`, `debit`, `credit` |
| `qtn_general_journal_line` | `debitamt`, `crdtamnt` |
| `qtn_general_journal_line_closed_year` | `debitamt`, `crdtamnt` |
| `qtn_journal_line` | `debit`, `credit` |
| `qtn_ledger_account`, `qtn_pay_period` | `debit`, `credit` |

Only `nhl_general_journal_line`, `nhl_general_journal_line_closed_year` and
`nhl_journal_line` carry masks today — 6 masked occurrences against 30 unmasked ones of
the same names.

Offline, `tests/test_accelerator.py` fires the same predicate against a snapshot of that
vault (must produce exactly 4 problems on the reduced snapshot), against the shape the
projection produces (must produce none), and against that same post-fix shape with one
mask removed (must produce exactly 1) — so the silence comes from the assertion holding,
never from it being unable to run. The same property is also asserted at the **metadata**
level, so a model can be refused before a lake is ever built.

### It already found a second defect

`nhl_payroll_detail` is `sensitivity: restricted` and declared `rate` in its payload with
no mask, while `nhl_timesheet_line` masks a column of the same name. `rate` is now masked
on both. Found by the assertion, not by review.

### Two exclusions, both with a printed reason

- **Platform-owned** `ref_` / `ctl_` / `reg_` / `agg_` / `doc_` — read by join, never
  generated here (the ARB boundary rule).
- **SDP `__materialization_mat_*` backing tables** — the runtime's own storage under a
  streaming table. **This one is a recorded limitation, not a clean boundary.** Measured
  25 Aug 2026: the backing table under the *already-masked* `nhl_general_journal_line`
  carries `debitamt` and `crdtamnt` with no mask of their own. The mask lives on the
  streaming table, not on the storage underneath it — a platform property that predates
  this gate and is identical for every masked table in every lake, so asserting over these
  would fail permanently and prove nothing about this model. **What it means in practice:
  read access to `raw_vault`'s `__`-prefixed internals must be treated as access to
  unmasked values.** That is a grant question and `checks/apply_governance.py` owns it —
  it is worth confirming those internals are not readable by the analyst groups before the
  reload.

Both exclusions are counted and printed with their reason; neither is silent. The offline
suite asserts that dropping the exclusions makes those rows fire, so an exemption that
changes nothing cannot survive.

---

## 4. Expected column counts after the fix

Measured today (`information_schema.columns`) against the projection computed from
metadata. Every binding of a multi-source table produces an identical column list — that
is asserted offline.

| table | now | after | shape |
|---|---:|---:|---|
| `hub_accounting_journal` | 92 | **13** | hk + 4 business keys + bk + 7 system |
| `hub_organisation` | 92 | **11** | hk + 2 + bk + 7 |
| `hub_ledger_account` | 62 | **11** | hk + 2 + bk + 7 |
| `hub_pay_period` | 22 | **11** | hk + 2 + bk + 7 |
| `nhl_general_journal_line` | 83 | **26** | hk + 3 parent hks + 15 binding payload (incl. `seqnumbr`, `openyear`) + 7 |
| `nhl_general_journal_line_closed_year` | 83 | **26** | same, with `hstyear` |
| `nhl_journal_line` | 25 | **17** | hk + 4 parent hks + 5 payload + 7 |
| `qtn_accounting_journal` | 94 | **15** | target + 2 |
| `qtn_organisation` | 94 | **13** | target + 2 |
| `qtn_ledger_account` | 64 | **13** | target + 2 |
| `qtn_pay_period` | 24 | **13** | target + 2 |
| `qtn_general_journal_line` | 85 | **28** | target + 2 |
| `qtn_general_journal_line_closed_year` | 85 | **28** | target + 2 |
| `qtn_journal_line` | 27 | **19** | target + 2 |

**Unchanged:** the 16 ghost-only tables (`hub_job_request`, `hub_worker`,
`lnk_client_job_request`, the satellites, `nhl_payroll_detail`, `nhl_timesheet_line`) stay
at 8 columns — hash key plus 7 system columns — because they have no active binding, so
nothing is declared and the ghost stays minimal.

Not-yet-loaded tables, for when their bindings activate: `nhl_payroll_detail` 28,
`nhl_timesheet_line` 20, `sat_accounting_journal_header_ukg_us` 21,
`msat_journal_line_worktag_ukg_us` 13, `msat_journal_line_external_code_ukg_us` 12,
`msat_worker_skills_*` 13, `sat_job_request_details_*` 16,
`sat_job_request_commercials_striive_eu` 13, `csat_payroll_line_classification` 16,
`csat_job_request_custom_promoted` 17, `hub_job_request` 10, `hub_worker` 10,
`lnk_client_job_request` 10, `msat_job_request_custom_field_fieldglass_eu` 15.

---

## 5. What the reload will involve

**Nothing has been deployed. Nothing has been dropped. This section is what to authorise.**

The vault is **insert-only** and these are streaming tables owned by an SDP pipeline. A
column set cannot be narrowed in place: `delta.appendOnly = true` is set on every generated
table, the pipeline owns the table definition, and a `create_streaming_table(schema=...)`
that no longer matches the existing table is a schema conflict, not a migration. So this is
a **full drop and reload of the affected tables**, not a refresh.

1. **Confirm the blast radius.** 7 vault tables and 7 quarantine twins, listed in §4. The
   16 ghost-only tables are untouched in shape but will be recreated by the same pipeline
   update; they hold one ghost row each and nothing else.
2. **Confirm nothing downstream reads the columns that are going away.** No Gold objects
   exist yet (`03_usnc_gold_edm_dev` does not exist — see CHANGELOG "Still open"), and no
   `_v1` views are emitted in this lake because every satellite is deferred. The risk is
   ad-hoc queries and any saved dashboard against `hub_*` descriptive columns, which by
   construction should never have existed. Worth one sweep of query history before the
   drop, because "nobody could legitimately be reading it" is exactly what was true of the
   unmasked amounts too.
3. **Full refresh, not an update.** Stop the `vault_load` job, then a **full refresh** of
   the raw-vault pipeline for the 7 targets. If the runtime refuses to narrow the declared
   schema on refresh, drop the 14 tables explicitly first and let the pipeline recreate
   them. Bronze is untouched, so the reload is a re-read of `01_usnc_bronze_dev`, not a
   re-ingest.
4. **Reload volume.** ~2.76 M hub rows, ~2.45 M + closed-year NHL rows from `gl20000` /
   `gl30000`, plus UKG's `gl`. Same source data, same hash rulebook (`RULEBOOK_VERSION`
   stays `1.0.0`, `hashing.py` untouched, golden vectors untouched), so **every hash key
   must come out byte-identical**. `metadata/key_composition.json` is unchanged and
   `verify_repo.py` asserts the digests. That is the property to check first after the
   reload: the row counts and the keys should match what is there today, and only the
   column set should differ.
5. **Preflight before the deploy** (`checks/preflight_target.py`, `--profile
   hfig-usnc-tds`), and `create_mask_functions` must run before `raw_vault` as it already
   does — the quarantine twins now declare MASK clauses, so a twin's definition names
   `governance.mask_money`, which must exist first.
6. **Run the gates after, and read the new one.** `assert_mask_survival` will now scan the
   whole schema. Expect it to go from 30 problems to 0. If it does not, the projection did
   not land — the gate is the check on the fix, which is the arrangement the reviewer asked
   for.
7. **Then the grant question from §3**: confirm the `__materialization_mat_*` internals in
   `raw_vault` are not readable by any analyst group, since the mask does not sit on them.

---

## 6. Verification

```
uv run python tests/test_accelerator.py    371 checks, ALL CHECKS PASSED   (was 335)
uv run python verify_repo.py               552 checks, VERIFICATION PASSED (was 532)
databricks bundle validate --profile hfig-usnc-tds -t usnc_tds   OK (pre-existing warning only)
```

`src/accelerator/hashing.py` untouched. `RULEBOOK_VERSION` `1.0.0`. Golden vectors
untouched. `metadata/key_composition.json` unchanged and re-asserted.
