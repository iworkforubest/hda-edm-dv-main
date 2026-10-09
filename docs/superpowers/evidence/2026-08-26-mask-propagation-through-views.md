# Does a Unity Catalog mask propagate through a view?

**Measured:** 26 August 2026, workspace `db-usnc-datalakehouse-tds`, profile `hfig-usnc-tds`,
as `adrian.turcu@headfirst.group` — an identity in **neither** `usnc_data_analyst_finance`
nor `global_dataplatform_pipeline_job_runners`, so `mask_money` denies to it.

Run because `sat_x_v1` has to be created by `checks/load_satellites.py` rather than by the
pipeline (DEF-52), and three satellites declare masks. The question was how that view
carries them.

## A view cannot declare a mask. Two forms, both rejected

```sql
CREATE OR REPLACE VIEW governance._probe_v1 (k, amt MASK governance.mask_money) AS SELECT ...
```
→ syntax error at the `AS SELECT`.

```sql
ALTER VIEW governance._probe_v1 ALTER COLUMN amt SET MASK governance.mask_money
```
→ syntax error at `ALTER COLUMN`.

## It does not need to. The mask propagates

```sql
ALTER TABLE governance._probe_v1_src
  ALTER COLUMN amt SET MASK `02_usnc_silver_edm_dev`.governance.mask_money
```

Registered, confirmed in `information_schema.column_masks`:

```
_probe_v1_src   amt   02_usnc_silver_edm_dev.governance.mask_money
```

Then, reading the same row two ways as the unprivileged identity:

| read through | `amt` |
|---|---|
| the base table | *(empty — masked)* |
| a plain view over it | *(empty — masked)* |

The row's real value is `10.00`.

**Note the catalog qualification.** The first attempt used `governance.mask_money` and
failed to resolve — DEF-21's finding, met again: a MASK clause's function must be
catalog-qualified.

## Why this differs from the materialized-view finding

Sub-project 3a §1.3 recorded that *"an MV projection of a masked column that declares no
mask of its own is an unmasked copy"*, and `mask_survival_check` was built on it. That
remains true — and it is true **because a materialized view copies data**. A plain view
copies nothing; it is a query, so the base table's mask applies at read time.

The distinction the gate does not currently make:

| object | stores data | needs its own mask |
|---|---|---|
| table / streaming table | yes | **yes** |
| materialized view | yes, a copy | **yes** |
| plain view | no | **no — it inherits** |

## What follows

1. `sat_x_v1` is created as a **plain view**, and carries no mask declaration. Simpler and
   safer than the pipeline's materialized `_v1`, which must redeclare.
2. `mask_survival_check` must stop requiring a declared mask on a plain view. It currently
   sweeps `information_schema.columns`, which includes view columns, so it would flag
   `sat_x_v1.control_total_amount` as unmasked — a false positive that would block every
   load once a masked satellite is activated.

   The safety argument for relaxing it: a view exposes only what some storing object holds,
   and every storing object is still asserted. If a mask is missing, the gate fails on the
   table — the root cause — rather than on a view over it.

Probe objects `governance._probe_v1` and `governance._probe_v1_src` were dropped and their
absence confirmed.
