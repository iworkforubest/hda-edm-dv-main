# Request to the Bronze / ingestion team: manifest ids on four feeds

**Raised:** 26 August 2026. **Revised:** 27 August 2026 — the manifest moves to
Bronze's own control schema. Plain-text mirror of
`docs/loop1_control_table_request.html`, which is the version to send.
**From:** the HFIG Data Vault accelerator (`data_platform_operations`)
**Owner needed:** whoever lands GP and UKG data into `01_usnc_bronze_dev`

## Correction to the earlier draft

An earlier version of this asked the platform team to create
`governance.ctl_approval_manifest` and `governance.ref_dq_expectation`. **That was our
mistake.** The `governance` schema is owned by the accelerator team — we created it, and
it already held our pipeline event logs. Both tables now exist; nobody needed to be asked,
and we should have checked before writing.

What remains is genuinely yours.

## Updated again, 27 August — this one is not a mistake

The schema named in the earlier draft was right, and stays right for Silver's own
manifest. **Where you write is what changes.** Each layer's control schema records what
THAT layer did, not what a downstream layer needs, so your deliveries belong in your own
control schema and Silver's reconciliation reads them from there.

This is a **bigger ask** than the version before it, and we would rather say so than have
you discover it: Bronze has no `control` schema today — 47 schemas in
`01_usnc_bronze_dev`, inventoried 24 August, none of them called `control` — so the first
item below is to stand one up.

## What we need

Three things: one to set up once, then two for every batch you land into the four feeds
below.

1. **Stand up a `control` schema** in `01_usnc_bronze_dev`. It needs four tables — the
   three mandatory audit tables every layer's control schema carries, `aud_load_run`,
   `aud_table_load` and `aud_table_discard`, which are **append-only**: they record what
   happened and must never be rewritten. Plus one table specific to Bronze,
   `ctl_delivery_manifest`, which is **mutable**: it records what should happen and stays
   correctable. Exact columns and types for all four: `control_contracts/bronze.yaml` in
   the accelerator repository.

2. **Write one row per batch** to `01_usnc_bronze_dev.control.ctl_delivery_manifest`:

   | column | meaning |
   |---|---|
   | `manifest_id` | any stable id for the batch |
   | `delivered_count` | how many rows it delivered |
   | `delivered_at` | when |
   | `delivered_by` | who or what landed it |
   | `source_system` | which feed |

3. ~~**Stamp that same `manifest_id` onto every row** in the batch, in a column named
   `_manifest_id`.~~ **Withdrawn 1 September — do not add a column.** Measured that day: all
   four feeds already carry `input_file_name`, which identifies a delivery on every row (3
   files for `ukg_raw.gl`, 7 for `gl20000`, 5 for `joborders`), and our loader already uses it
   for ordering. We group on that instead. **No change to any `_raw` table.**

| source | bronze table |
|---|---|
| `GP_US` | `01_usnc_bronze_dev.great_plains_raw.gl20000` |
| `GP_US_HIST` | `01_usnc_bronze_dev.great_plains_raw.gl30000` |
| `GP_US` | `01_usnc_bronze_dev.great_plains_raw.gl00100` |
| `UKG_US` | `01_usnc_bronze_dev.ukg_raw.gl` |

`_manifest_id` was not a new convention — **nine other bindings in our model declare
it**, and the Striive timesheet feed uses `approval_manifest_id`. These four simply did
not carry one. That is now moot for this request: we group on `input_file_name`, which
every one of them already has. It was not part of the earlier Bronze work requests
(BRZ‑1 … BRZ‑11).

## What the delivery manifest does, and what it does not do

Writing `ctl_delivery_manifest` gives us the **cross-layer delivery control** — did Silver
stage everything you delivered. That is worth having and it is the honest thing for Bronze to
state, because it is a fact about what Bronze did.

**It does not, on its own, light up loop-1**, and an earlier version of this page implied it
would. Loop-1 compares what landed in one Silver table against a count taken before the load,
and for three of the four feeds below our loader collapses re-delivered rows: `gl20000` carries
about 1.8 copies of every business row across 7 deliveries, `ukg_raw.gl` exactly 2 across 2. So
your `delivered_count` sits above what our loader accepts — by design, not by error — and the
two numbers are not interchangeable.

What loop-1 needs from you is item 2, the `delivered_count` you assert — **not** item 3, which is withdrawn. The count it compares against
has to be taken *after* our deduplication, which is ours to compute, not yours.

## Why

`checks/loop1_reconciliation.py` asserts one identity per load:

```
landed + (quarantined - superseded) = approved
```

`superseded` counts the rejects a later run legitimately re-accepted, recorded in
`control.ctl_quarantine_superseded`. It is a correction to the quarantined term, not
a relaxation of the identity: the gate reports any manifest superseding more rejects
than it quarantined as its own failure rather than absorbing it into the arithmetic.

It is the only check we run that proves **everything that was supposed to arrive did**.
Every other gate checks the shape or the content of what arrived. Without this one, a load
that silently dropped rows reports success and is indistinguishable from a complete one.

The gate joins the vault to the manifest on `manifest_id`. Measured on the loaded vault,
26 August 2026:

```
nhl_general_journal_line   2,453,132 rows   0 non-null manifest_id
```

Every row is unaccounted for. The gate now says so explicitly and fails; until the feeds
carry an id there is nothing to reconcile against.

## What we are not asking for

- **`ref_dq_expectation`** is created and empty, and that is fine. The generator falls back
  to its two compiled-in key-safety rules, so nothing is blocked. It exists for when a data
  steward has a rule to add.
- **A per-target count.** `delivered_count` is a count of the rows in your batch, nothing
  more. We only reconcile targets that take one row per approved row — the non-historised
  links. Our hubs deduplicate (4,444,172 GP rows become 2,221,108 hub rows) and our
  satellites store only changed rows, so the identity is false for them by design and the
  gate refuses to reconcile them at all. You do not need to know anything about our model.
