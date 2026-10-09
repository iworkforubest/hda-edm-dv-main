# Closing the materialization bypass

**Date:** 25 August 2026
**Status:** approved (approach A), implementing
**Supersedes:** the open "materialization-grant question" in `OPEN_ITEMS.md`

## 1. The defect

`raw_vault` holds 30 streaming tables and, in the same schema, 30
`__materialization_mat_<pipeline-id>_<table>_1` backing tables — the runtime's own
storage, one per streaming table. The mask lives on the streaming table. The backing
table carries the same values with no mask.

Measured on 25 August 2026, as `adrian.turcu@headfirst.group`, for whom
`is_account_group_member('hfig_commercials_reader')` returns **false**:

| object | `count(debitamt)` |
|---|---:|
| `raw_vault.nhl_general_journal_line` (masked) | **0** |
| its `__materialization_*` twin | **2,453,131** |

The mask works. It is bypassed by reading one object to the side.

`governance/apply_masks.sql:126` is what makes this exploitable rather than latent:

```sql
GRANT SELECT ON SCHEMA `${catalog}`.`${vault_schema}` TO `hfig_data_engineering`;
```

A schema-level grant covers every table in the schema. When `apply_governance` runs,
everyone in that group receives cleartext money through the backing tables — exactly
the people `mask_money` exists to stop.

## 2. Why the obvious fixes do not work

- **Mask the backing table.** `apply_masks.sql:103` already records that
  `ALTER TABLE … SET MASK` against a pipeline-owned table does not survive the next
  update. The generator never creates these tables, so it cannot declare a mask in
  their definition either. Their names embed the pipeline id.
- **Grant the schema, revoke the table.** Unity Catalog has no `DENY`; grants are
  additive. Revoking a privilege never granted at table level is a no-op.

`mask_survival_check.exemption()` already reached this conclusion and routed the
problem explicitly: *"read access to raw_vault's internal objects must be treated as
access to unmasked values, which is a GRANT question: checks/apply_governance.py owns
it."* The intent was recorded; the implementation contradicts it. This closes that gap.

## 3. The fix

**No vault schema ever carries a schema-level `SELECT` grant.** Readers get
`USE SCHEMA` plus one `GRANT SELECT ON TABLE` per *declared* table.

The safety property is structural rather than vigilant: the grant list is derived from
the same metadata the factory builds from, and a backing table is never declared. It
cannot appear in the list, however the model changes.

```sql
REVOKE ALL PRIVILEGES ON SCHEMA `cat`.`raw_vault` FROM `account users`;
GRANT USE SCHEMA      ON SCHEMA `cat`.`raw_vault` TO `hfig_data_engineering`;
GRANT SELECT ON TABLE `cat`.`raw_vault`.`hub_accounting_journal`   TO `hfig_data_engineering`;
GRANT SELECT ON TABLE `cat`.`raw_vault`.`nhl_general_journal_line` TO `hfig_data_engineering`;
GRANT SELECT ON TABLE `cat`.`raw_vault`.`qtn_general_journal_line` TO `hfig_data_engineering`;
-- … one per declared table. The __materialization_* twins receive nothing.
```

Quarantine twins are granted: they hold rejected rows, loop-1 reconciliation depends on
them, and they carry the same masks as their targets by construction.

### Activity

Quarantine twins exist only for bindings active in this lake, and a grant against a
table that does not exist fails. The grant list therefore respects `--active-sources`,
read through `spec.active_table_bindings` — the same declared list `factory.build` uses,
so the two cannot disagree about which tables exist. This follows the pattern
`mask_survival_check` already established.

### The raw/business split

`BUSINESS_KINDS = {"csat"}` currently lives in `src/pipelines/silver_vault.py`, which
imports pyspark and so cannot be read by a Spark-free check. It moves to
`naming.py` and both read it from there. One definition, two consumers.

## 4. The gate

`checks/schema_grant_check.py`, new, blocking.

Per-table grants are only as good as the absence of a broader one, and a single
hand-run `GRANT SELECT ON SCHEMA` reopens the hole silently and permanently. The gate
asserts the negative:

1. No `SELECT` (or `ALL PRIVILEGES`) assignment exists **ON SCHEMA** for any vault
   schema.
2. No `SELECT` (or `ALL PRIVILEGES`) assignment exists **ON CATALOG** for the silver
   catalog — catalog-level grants cascade to every schema and would bypass 1.

Both are read from `SHOW GRANTS`. The gate names the principal and the privilege when
it fails, because "someone granted something somewhere" is not actionable.

It runs **before** `apply_governance` in the job graph, not after: the point is to catch
a broad grant that already exists, before adding more on top of it.

## 5. What this does not fix

**`assert_journal_integrity` stays red.** Its run-as identity is not privileged under
`mask_money`, so it reads NULLs and cannot do its arithmetic. Per `DEPLOY.md` 6b that
identity must be a member of every mask's privileged group. That is an account-admin
action and a business decision, not a code change.

**`apply_governance` stays under its STOP.** The groups it grants to do not exist in
this workspace (README RECONCILE #3), and its unconditional `REVOKE` on the shared
bronze catalog would land while the `GRANT` failed, leaving nobody able to read Bronze.
This change makes the grant half correct; it does not make the file safe to run. The
STOP lifts when the group mapping is settled, which is a separate decision.

So the deliverable is a fix that is **built, tested and blocking** — and correctly not
yet applied.

## 6. Files

| file | change |
|---|---|
| `src/accelerator/naming.py` | `BUSINESS_KINDS`; `vault_schema_for()` |
| `src/pipelines/silver_vault.py` | read `BUSINESS_KINDS` from `naming` |
| `checks/apply_governance.py` | `table_select_grants()`; `--active-sources`, `--reader-group` |
| `governance/apply_masks.sql` | drop both schema-level `GRANT SELECT`; record why |
| `checks/schema_grant_check.py` | **new** — the gate |
| `resources/vault_job.yml` | gate task before `apply_governance`; new args |
| `tests/test_accelerator.py` | generator + gate predicate, both directions |
| `.claude/skills/dv-accelerator-gates/SKILL.md` | add the gate to the table |

## 7. Testing

Offline, in `tests/test_accelerator.py`:

- `table_select_grants` emits one `GRANT SELECT ON TABLE` per declared table and for
  every table the factory emits under the same `--active-sources` — the two inventories
  are compared directly, so they cannot drift.
- It emits **no** statement matching `GRANT … ON SCHEMA`.
- It never names a table starting `__`, asserted against a model deliberately polluted
  with one.
- The rendered `apply_masks.sql` contains no `GRANT SELECT ON SCHEMA`.
- The gate's predicate returns a problem for a schema-level `SELECT`, for
  `ALL PRIVILEGES`, and for a catalog-level `SELECT`; and none for `USE SCHEMA` alone.

Each assertion is checked to fail on the pre-fix input, not merely to pass on the
post-fix one — the four checks-that-could-never-fail found earlier in this project are
the reason that is a standing requirement here.
