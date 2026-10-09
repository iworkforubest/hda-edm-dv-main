# Bronze conformance dashboard — design

**Date:** 29 August 2026
**Status:** approved, implementing

## What this builds

A quality dashboard over **source contract conformance**: does Bronze deliver what
Silver's contracts require, per contracted table, over time. It is the reporting face of
`checks/source_conformance_check.py`, which today prints a verdict and forgets it.

## Why not the obvious alternatives

Three things were measured before choosing, on 29 August:

1. **No gate persists its verdict.** `source_conformance_check` writes nothing; no check in
   `checks/` writes a gate-results table. A conformance dashboard has nothing to read today.
2. **Bronze has no `control` schema.** `control_standard.tables_for("bronze")` declares
   `ctl_delivery_manifest` and three `aud_*` tables, but `01_usnc_bronze_dev` has no
   `control` schema at all. A dashboard pointed at them renders green over nothing.
3. **Bronze publishes real signal already** — ten `*_freshness_volume_monitoring` views,
   showing feeds 5× to 192× past their own 24-hour SLA. That is the Bronze team's statement
   about their own delivery, and reporting it is not the same as controlling ours.

The dashboard therefore reports **what our gate asserted**, not a second computation of it.

### Rejected: compute conformance live in SQL

Publishing the contract as a table and joining it against Bronze's
`information_schema.columns` at view time is always-current and needs no writer. It was
rejected for two reasons. It cannot see whether values actually **cast** — that needs a
`try_cast` probe over real data, which a static join cannot express — so it would read green
while data is uncastable. And it would be a **second independent derivation** of a judgement
the gate already makes, free to disagree with it. This repo has been bitten by that shape
repeatedly (`BUSINESS_KINDS`, the system-column set, `RECONCILABLE_KINDS`), and each time the
fix was to make one definition authoritative and assert the agreement.

## Architecture

### The table

`ctl_source_conformance`, in **silver's** control schema. Bronze's catalog belongs to another
team and has no control schema; this is our assertion about Bronze, so it lives in our lake.

Grain: **one row per (job_run_id, contract_table)**.

| column | type | meaning |
|---|---|---|
| `job_run_id` | STRING | the run that measured it |
| `recorded_at` | TIMESTAMP | when |
| `target` | STRING | bundle target the contract belongs to |
| `contract_table` | STRING | three-part Bronze table name |
| `status` | STRING | `CONFORMANT` / `ABSENT` / `NON_CONFORMANT` / `NOT_EVALUATED` |
| `missing_columns` | BIGINT | count |
| `lossy_casts` | BIGINT | count |
| `findings` | ARRAY\<STRING\> | the gate's own `KIND: detail` lines |

**Why not one row per finding.** A per-finding grain cannot express "checked, nothing
wrong". A table holding only failures makes coverage uncomputable and leaves the denominator
empty — the DEF-48 trap at dashboard scale. Every checked table gets a row whether or not it
failed; detail comes from exploding `findings`, the idiom `meta_vault_model.generated_tables`
already uses.

`status` is stored rather than derived so the row records what the gate concluded, including
`NOT_EVALUATED` when `--skip-cast-probes` left the castability half unmeasured. A dashboard
recomputing status from the counts would call that row conformant.

### The writer

A pure `conformance_rows(...)` builds the rows; `main()` writes them, keeping Spark confined
where it already is. Two rules:

- **Recording never changes the exit status.** The verdict stays `gate_status()`'s alone.
- **A failed write is fatal.** A dashboard reading a table that silently stopped being
  written is precisely the failure this design exists to avoid.

### Wiring

`assert_source_conformance` depends only on `assert_hash_parity` today. It will now write
into the control schema that `create_control_objects` creates, so it gains that dependency.
Without it the first run writes into nothing.

### The dashboard

`src/accelerator/bronze_quality.py`, mirroring `quality.py`: `datasets()` and `tiles()`,
coverage-first for the same reason silver is — contracts exist for two of nine targets, and
the current run passes, so a bare pass-rate would be true, thin, and indistinguishable from
"we barely measure anything".

Tiles, in reading order:

1. **Non-conformant tables, latest run** — counter, over a single-row dataset.
2. **Runs recorded** — counter. This is the honesty tile: an empty table must read as
   *empty*, not as perfect.
3. **Conformance by table, latest run** — table, status and both counts.
4. **Findings detail** — `findings` exploded, so a reader sees the gate's own words.
5. **Conformance trend** — by run, so BRZ items landing shows as movement.

### Emitter and artefacts

Extend `tools/emit_quality_dashboard.py` with a second family rather than writing a new
generator — it already owns `_widget`, the spec versions, and the byte-identical
regeneration discipline. Emits `quality_bronze_<target>.lvdash.json` and the `_synthetic`
twin, for the targets that have a contract, via the existing `configured_targets()`.

### Gates inherited for free

Adding `ctl_source_conformance` to `control_standard.LAYER_TABLES["silver"]` and
`APPEND_ONLY` means `control_conformance_check` and `append_only_check` cover it with no new
code. Note that silver's `governance/control_objects.sql` is **verified against** the
standard rather than generated from it, so its DDL is added by hand and the existing
conformance gate proves the two agree.

New assertions: datasets reference only `ctl_source_conformance`; `RATE_GUARD` on every
rate; counters only over `SINGLE_ROW_DATASETS`.

## Testing

Offline suites, plus mutation proofs that carry their own weight:

- a finding lands a row with that finding in it;
- a table with **no** findings lands a `CONFORMANT` row (the coverage denominator);
- a `NOT_EVALUATED` verdict is stored as such and not recomputed to conformant;
- a failed write fails the task.

Then deploy, run, and **read the live dashboard** rather than trusting that it renders.

## Known limitation, stated rather than hidden

On first deployment this dashboard shows all-green, because the conformance gate currently
passes. That is honest only because the coverage and runs-recorded tiles sit beside it
saying how little is under contract — two of nine targets. Without those two tiles it would
be a green light over an almost-empty measurement.
