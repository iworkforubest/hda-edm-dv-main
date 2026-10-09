# Changelog

Accelerator version is separate from the **hash rulebook version**, deliberately. The
accelerator can change freely; the rulebook cannot, because changing it re-keys every row
in every lake. Rulebook stayed at `1.0.0` through this release.

---

## Unreleased

### Defects fixed

**DEF-26, CRITICAL — every vault table carried the whole source row, defeating the mask
control.** `factory._stage` never projected an entity to its declared shape: the entire
staged Bronze frame was appended, so `hub_accounting_journal` shipped **92 columns while
declaring four business keys** and `nhl_general_journal_line` shipped 83 while declaring
one transaction key and fourteen payload columns. `README.md`'s "what validation refuses
to build" already lists *a hub carrying descriptive attributes* — the rule was enforced on
the declaration and broken in the implementation.

It was Critical rather than untidy because it **bypassed the vault's only PII defence**.
`_emit_target` emits MASK clauses only for an entity that *declares* masks, and a hub
declares none, so the same column was masked on one table and clear on another:

```
nhl_general_journal_line   2,453,132 rows            0 readable debitamt   (masked)
hub_accounting_journal     2,759,294 rows    2,759,292 readable debitamt   (clear)
```

`_emit_quarantine` had the same gap from the other side: it passed no `schema=`, so no
MASK clause, while being fed the identical frame.

Every entity is now projected to its declared model — hub: hash key, declared business
keys, readable `_bk`, system columns; link/hal: hash key, parent hash keys, transaction
key, system columns; NHL: the same plus its declared payload; satellites: parent hash key,
hashdiff, payload, `mas_key`, system columns; quarantine twin: exactly its target plus
`failure_rule` and `failure_detail`. A binding's `payload` wins where it declares one, the
same `src.payload or entity.payload` rule `hashdiff` already used. A hub's business keys
are **renamed to the model's names and cast to STRING** — six bindings feed
`hub_organisation` under six different source column names, so nothing else gives one hub
one shape. The flows stage the full row, evaluate expectations against it (an expectation
is governed configuration and may name any source column), then project.

**The ghost flow now supplies every declared column.** DEF-25's lesson applies again:
declaring a schema converts every previously-cosmetic inconsistency between flows into a
hard failure. Where a schema is declared the ghost is built from the same field list, zero
keys a link's parent hash keys too, and a NOT NULL column it cannot supply is refused at
definition time with the column named rather than at first append.

**`nhl_payroll_detail.rate` was unmasked** while `nhl_timesheet_line` masked a column of
the same name — on a `restricted` entity. Found by the new assertion below, not by review.

### New: the catalogue-wide mask assertion

`checks/mask_survival_check.py` walked only entities that *declare* masks, plus their
`_v1` views. It never asked whether a masked column *name* appears unmasked elsewhere, so
it **reported PASSED over a total bypass** — the fourth check found on this branch whose
condition could not fail. It now asserts that any column name declared masked anywhere in
the metadata carries a mask on **every** object in the vault schema where that name
appears, quarantine twins included.

Proven non-vacuous against `02_usnc_silver_edm_dev.raw_vault` as it stands: **30 problems
across 10 objects** (4 hubs, 6 quarantine twins). The predicate is pure and Spark-free, so
both directions are fired offline in `tests/test_accelerator.py`, and the same property is
asserted at the metadata level so a model can be refused before a lake is ever built.

Two exclusions, both stating a reason that is printed: platform-owned `ref_`/`ctl_`/`reg_`/
`agg_`/`doc_` objects, and SDP's `__materialization_mat_*` backing tables. The second is a
**recorded limitation, not a clean boundary** — measured, the backing table under the
already-masked `nhl_general_journal_line` carries `debitamt` unmasked. The mask lives on
the streaming table, not the storage under it, so read access to those internals must be
treated as access to unmasked values. That is a grant question.

### Reload required

This changes the schema of loaded tables. The seven loaded vault tables and their seven
quarantine twins must be dropped and reloaded — see `.superpowers/hub-projection-report.md`.

---

## v0.2.0

Second packaged release. Adds the Workday/UKG finance and payroll domains, fixes two
defects that would have failed in a workspace, and makes three modelling properties
enforceable that were previously only conventions.

### Defects fixed

**Link staging hashed columns that never existed.** A link's hash was built from
`worker_hk`, `client_hk` and so on — columns that are not present in staged Bronze. A
one-parent example hid it; a journal line with three parents could not. Links and NHLs now
declare `parent_keys`, mapping each parent to the source columns carrying its business
key, and the link hash derives from those business key columns rather than from parent hash
columns. Hashing a hash would mean casting `BINARY` to `STRING`, which is lossy and would
have put Spark out of step with the reference implementation.

**Masks were applied by the wrong mechanism.** `ALTER TABLE ... SET MASK` does not survive
a pipeline update on a streaming table or materialized view — for those, masks must be set
through the table definition. Masks are now declared per column in metadata and emitted by
the factory into each generated table's definition, including the `_v1` projections.
`apply_masks.sql` creates only the functions, isolation and grants.

**Six shapes with negative width or height.** Invalid OOXML in the reference deck
(separate artifact) — recorded here because the same connector helper pattern now guards
the diagram code.

**Six undeclared bundle variables** would have failed `databricks bundle validate`.

**`${catalog}` placeholders in the governance SQL were never substituted** — the SQL task
passed no parameters. Replaced with `checks/apply_governance.py`, which substitutes
explicitly, refuses non-identifier values, and has an offline `--dry-run`.

### New: grain is declared, not inferred

The GL journal is an aggregate — one line carries every worker's HSA deduction for a batch
summed into one figure. Nothing about the row's shape says so, and the failure it invites
is silent: deriving a per-worker fact from an account-level total.

- `grain: transaction | aggregate` on every entity.
- An aggregate must declare `aggregates_from` (its transaction-grain counterpart) and
  `aggregate_drops` (which parents are summed away).
- Validation refuses an aggregate that drops nothing, that names a counterpart which is
  itself an aggregate, or that **also claims a grain it dropped**.
- `checks/aggregate_reconciliation_check.py` derives its assertions from those declared
  pairs, in both directions: a journal line with no detail behind it, and detail that
  reaches no journal line.

### New: domains

| Domain | Added |
|---|---|
| `finance` | Workday/UKG accounting journal (aggregate grain): journal, company and ledger account hubs; journal line NHL with `line_order` as a dependent child key; header satellite; worktag and external-code link satellites |
| `payroll` | Raw payment grain: pay period hub; payroll register detail NHL (worker × period × code); payroll code classification computed satellite with `rule_version` |

### New: modelling capabilities

- **Link satellites.** A satellite may hang off a link or NHL. Worktags and external codes
  belong to a journal *line*, not to any hub. Link *chaining* is still refused — the two
  look similar and are opposites.
- **Entities are source-independent.** The source lives in the `sources:` binding and in
  the generated table name, never in the entity name. One `sat_job_request_details`
  declaration fans out to three per-source tables. Validation rejects an entity name
  containing one of its own sources.
- **Reference type in the business key.** Workday references are `(type, value)` pairs, so
  `BRPLM` as a `Company_Reference_ID` and as an `Organization_Reference_ID` are different
  assertions.

### New: hash rulebook hardening

- `HASHDIFF_UPPERCASE = False` **ratified and guarded**. Case-folding the payload made a
  case correction invisible *and* unstored, since no row is inserted. Business keys are
  still case-folded; the asymmetry is deliberate.
- Pure-Python **reference implementation** plus 14 golden vectors carrying both the digest
  and the exact pre-hash string, so a mismatch says which normalisation step diverged.
- `checks/hash_parity_check.py` runs as **gate zero**, before Bronze, asserting Spark's
  digests equal the reference.
- `key_safety_rules` quarantine any key component that is NULL, blank, contains the
  delimiter, or equals the null token — so the delimiter choice stops being load-bearing.

### New: gates

| Gate | Asserts |
|---|---|
| `hash_parity_check.py` | Spark digests == reference implementation (runs first) |
| `journal_integrity_check.py` | debits = credits per journal; lines agree with any declared control total; the line ordering column dense and unique. Column names come from each entity's `accounting:` block, not from this gate — the sources disagree (`debitamt`/`crdtamnt`/`seqnumbr` in GP, `debit`/`credit` and no ordinal in UKG), and a property it cannot evaluate is reported as NOT EVALUATED rather than skipped |
| `aggregate_reconciliation_check.py` | raw payroll detail == GL journal, per account, both directions |
| `preflight_target.py` | authenticated workspace == intended target |
| `mask_survival_check.py` | every metadata-declared mask present, on every generated table and its `_v1` |

Aggregate invariants live in gates, not expectations: expectations are row-level, and
`SUM(debits) = SUM(credits)` spans rows. Shipping it as an expectation would look like
coverage and provide none.

### New: 8-workspace topology

Four lakes × two environments, targets mirroring the workspace names. EU production is the
legacy workspace still named "Datalake"; the target is `weu` and preflight matches on host,
not name.

**Correction to v0.1:** Unity Catalog allows one metastore *per region*, so PROD and TDS in
a region **share** it — 4 metastores, not 8. `hfig_weu` and `hfig_weu_tds` are catalogs in
the same metastore and visible from both workspaces by default. Separate catalogs are not
isolation, so `apply_masks.sql` now sets catalog isolation as its first statement, before
any grant, and the workspace binding is verified *negatively*.

### New: high-variability source pattern

Fieldglass carries a standard column set plus per-client custom columns across 300+ clients.
Split by **stability** rather than by client: standard columns join the typed satellite as a
fourth source binding; custom columns go to a multi-active satellite keyed on the field name,
so onboarding a client adds rows rather than columns. Promotion to typed columns happens in a
registry-driven Business Vault satellite, where a schema change is a rebuild.

`key_style: tenant_scoped` is now enforced: it requires `tenant_key`, and those columns must
be part of `business_keys`. If a source's references are unique only within a buyer instance,
keying on source + reference merges two clients' records into one hub row — silently.

### New: capacity estimator

`tools/estimate_footprint.py` projects storage from the real model structure plus volumes
declared in `metadata/volumes.yml`. Structure is accurate; volumes are placeholders and are
labelled as such, because an unlabelled guess gets quoted into a capacity request. The output
is designed to expose sensitivity rather than produce a headline: PIT snapshot grain is the
largest lever, compression the widest unknown, and volume concentrates in two or three NHLs.

On placeholder staffing volumes the whole Silver vault lands around 300 GB at seven years
across all four lakes — so storage is not the constraint. Compute concurrency and Azure
regional quota lead time are.

### New: generated ERD

`tools/render_erd.py` renders `docs/hfig_dv_erd.html` and `.pdf` from the metadata — an
overview plus a detail page per domain. Because it is generated, it cannot drift from the
model; it can only be stale by one run. The verifier asserts the rendered ERD still contains
every current table, so a stale diagram fails the build.

### New: verification

- `verify_repo.py` grew substantially, and now reports its own count at runtime, including
  a layout precheck that diagnoses a flat or partial copy (files downloaded individually
  rather than the archive extracted).
- `tests/test_accelerator.py`: pure Python checks, no workspace needed (count printed at
  runtime).
- Version stamped onto every generated table as `hfig.accelerator.version`, so when two
  lakes disagree the first question — were they built by the same code — is answerable.

### Still open

- Bronze table names in metadata are **placeholders**; the pipeline fails at definition
  time until they are real.
- Delimiter `||` and null token `^^` await confirmation against `TECH_COLUMNS_STANDARD`.
- Seven API assumptions await workspace verification — see DEPLOY.md Phase 3.
- `hub_pay_period` vs the ERD's `ref_period` split needs agreeing.
- `ref_payroll_code` and `ref_workday_reference_type` are platform-owned and not generated;
  the latter is generatable from the XSD, which is not yet built.
- **Gold catalog grants are absent from `governance/apply_masks.sql`.** `03_usnc_gold_edm_dev`
  does not exist yet, and a GRANT against an absent catalog fails the `apply_governance` job
  task, which blocks `assert_mask_survival`. The grants go back in when the gold catalog is
  created — the file carries the exact statements to restore, in position.

---

## v0.1.0

First packaged release: SDP-native metadata-driven factory (hubs, links, NHLs, satellites,
MSATs), hash rulebook with ratified SHA-256, append-only and loop-1 gates, Unity Catalog
governance, DAB with regional targets, conformance gate across regions, 92 verification
checks.
