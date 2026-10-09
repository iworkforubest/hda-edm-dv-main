# The ETL control framework: one run identity, owned by us, honoured by every tool

**Date:** 2026-09-28 (rev 2 — after an independent review returned REWORK)
**Status:** DRAFT — for Adrian's review.

> **Origin and scope of the borrowing.** The *ideas* come from a control framework Adrian
> designed for an IBM insurance programme around 2012: a run identity everything hangs off,
> delivered counts you can reconcile against, source freshness as a declared state, business
> period separate from load date, record-level logging, metrics as rows.
>
> **[REV2] The DataStage implementation is not carried over.** That was an imperative
> sequence engine; this is Databricks with Spark Declarative Pipelines. Master sequences,
> per-layer execution gates and job-sequence control flow are not translated — they are
> replaced by what this platform already has: jobs, pipeline updates, `depends_on`, and the
> audit tables below. Where a concept survives it is re-derived in Databricks terms, not
> ported.
>
> **Revision note.** Rev 1 was reviewed and returned REWORK with six Critical findings. Four
> of its assertions about this repo were false and its sequencing rested on a category the
> repo excludes. Corrections are marked **[REV2]**.

---

## 1. What problem this solves

Three controls assert nothing today, all for one reason: **nothing in the data says which
delivery it came from.**

| control | state today |
|---|---|
| loop-1 reconciliation | `status=NOT_EVALUATED asserted=0` — "no ACTIVE reconcilable binding declares a manifest_column" |
| silent-feed detection | measured in `monitoring.*`, never declared. `hub_invoice` landed 3 times, then nothing for 13 days |
| period attribution | absent — the estate records *when we loaded*, never *what period the data is for* |

## 2. Decisions taken (Adrian, 28 September)

1. **We own the identity; the ingesting mechanism writes the row.**
2. **The whole framework**, staged across three branches (**[REV2]** — rev 1 proposed eight
   tasks on one branch and its dependency chain was broken at the root; see §8).
3. **Bronze and Silver, one model.**
4. **[REV2] Phase evidence is kept; restart logic is not built.** See §2b.
5. **[REV2] No DataStage constructs.** Databricks and SDP semantics throughout.

## 2a. Technology independence

**No part of this framework may depend on which tool produced the data.** Snaplogic, Synapse,
SDP, and whatever replaces them, conform to one identity logic managed in the control tables.

**[REV2] `etl_run_id` is an opaque UUID.** Rev 1 specified
`<source_system>|<UTC yyyyMMddHHmmss>|<mechanism>|<unique token>`. Wrong twice:

- **It put the mechanism inside the identity** — the one artefact this section is about. A tool
  whose name is not yet in our vocabulary could not mint a conforming id, and a composite
  invites parsing, which turns the format into an interface we can never change.
- **The delimiter collides with the hashing conventions.** `hashing.DELIMITER` is `||`,
  `NULL_TOKEN` is `^^`, and `hashing.py` emits a `key_no_delimiter_*` rule rejecting `||` in key
  text. An empty middle segment yields `||` inside a value that feeds `CONCAT_WS('||', …)`
  hashdiffs.

So: an opaque UUID. `source_system`, `started_at` and `ingestion_mechanism` stay the columns they
already are. Nothing parses the id because there is nothing in it to parse.

**The tool's own id is an attribute, never the key.** `mechanism_native_run_id` lets an operator
reach the vendor's log. Never joined on, nullable — a mechanism without one is still a
first-class member.

**`ingestion_mechanism` tells an operator who to call**, and must never appear in identity,
reconciliation or control flow. **[REV2]** `producer` (the job or pipeline that ran) is likewise
descriptive; rev 1 called it `job_sequence` "named in that mechanism's own terms", which is both
DataStage vocabulary and a lineage orphan at a tool swap.

**The test of the design is a tool that does not exist yet:** it joins by minting a UUID and
writing two rows — no branch in our code, no new column, no change to any gate.

## 2b. [REV2] Phase evidence — already present, needs only a link

The original's five Y/N layer indicators encoded *resume from the layer that failed*. Rev 1
dropped them claiming the DAG replaces them; that was a wrong equivalence, and the reviewer was
right to say so.

**Restart is not built.** SDP is declarative with full-refresh semantics and does not resume the
way an imperative sequence engine did. Building it means fighting the execution model.

**But no new table is needed to keep the evidence, because it already exists.** Measured:

    aud_load_run     job_run_id, phase, target, active_sources, recorded_at
    aud_table_load   job_run_id, pipeline_update_id, table_name, written_by,
                     staged, accepted, recorded_at

`phase` and `target` are already recorded per run, and `pipeline_update_id` is already the
SDP-native unit. What is missing is a **link to the delivery that fed them** and anything that
*consumes* the record. So §5 adds `etl_run_id` to `aud_load_run` and nothing else. Rev 1 was
going to add a `ctl_phase_completion` table; that would have duplicated `aud_table_load`.

## 3. What is dropped

- **Schedule-as-data** — schedules live in `resources/*.yml`, reviewed in git.
- **Source-config-as-data** — this is `metadata/source_unions.yml` and the entity bindings.
- **Per-layer execution gates** — replaced by `depends_on` and SDP's own update semantics; their
  evidence survives as §2b.

**Status becomes an event stream.** Control objects here are classified append-only or mutable and
guarded by `append_only_check`, so status is appended rather than updated. Every transition is
kept rather than only the last.

## 4. [REV2] Where these objects actually live

Rev 1 said every table is added to `governance/control_objects.sql`. **That is silver's
hand-maintained file.** Corrected:

| layer | file |
|---|---|
| bronze | `governance/control_objects_bronze.sql` — **generated** by `tools/emit_control_contract.py` from `control_standard`, byte-gated, with `control_contracts/bronze.yaml` published alongside |
| silver | `governance/control_objects.sql` — hand-maintained |
| gold | `governance/control_objects_gold.sql` |

- Bronze tables are **declared in `control_standard` and emitted**, never hand-appended.
- **The emitter has no view support at all.** `ctl_etl_run_current` cannot be emitted today. Either
  the emitter gains views (preferred — one capability, reusable) or the view lives elsewhere with
  a stated reason. A work item, not a detail.
- **Every control table must be registered** in `control_standard.APPEND_ONLY` or `MUTABLE`;
  `append_only_check.classify_control_table` returns `undeclared` otherwise.
  `ctl_delivery_manifest` is **`MUTABLE` BY DESIGN and stays that way** (**[REV3]**).
  `control_standard.py:216-218` gives the reason: "a config table records WHAT SHOULD
  HAPPEN: an expectation gets corrected, a manifest gets superseded. Making those
  append-only would mean a mistyped rule could never be withdrawn." Rev 2 proposed
  reclassifying it on an argument about writers rather than about modelling; that is
  withdrawn. The split is coherent as it stands: **a manifest is a claim that can be
  corrected, run status is history that must not be.** New tables are `APPEND_ONLY`; the
  manifest is not; and rev 1's "every table here is append-only" was simply wrong.
- **[REV2] The `ref_` claim is withdrawn.** Rev 1 said this accelerator never creates `ref_`
  objects; `governance/control_objects.sql` creates `ref_dq_expectation`. `naming.PLATFORM_OWNED`
  gates the *entity generator*, not governance DDL — and it contains `ctl_` too, which would have
  forbidden this entire framework.

## 5. The tables

**Bronze** (`01_usnc_bronze_dev.control`, declared in `control_standard`, emitted):

- **`ctl_etl_run`** — `etl_run_id` (UUID), `source_system`, `ingestion_mechanism`,
  `mechanism_native_run_id`, `producer`, `started_at`, `created_at`.
- **`ctl_etl_run_status`** — `etl_run_id`, `status`, `status_at`, **`recorded_at`**, `written_by`.
  **[REV2]** `recorded_at` is *ours* and is new: rev 1 ordered by `status_at`, the mechanism's
  clock, with no tie-break — equal timestamps gave a nondeterministic winner and clock skew let
  `STARTED` beat `COMPLETED`. The view orders by `(recorded_at DESC, status_precedence DESC)`.
- **`ctl_delivery_manifest`** — exists; gains `etl_run_id`, `table_name` and **`arrived_count`**
  (ours, beside the sender's `delivered_count` — see §7). Grain `(etl_run_id, table_name)`.
- **`ctl_source_system`**, **`ctl_source_system_status`** — `UP` means *refreshed*, not reachable.
- **`ctl_sequence_source`** — `(source_system, producer)` plus `run_when_down`.
- **`ctl_business_period`**, **`ctl_business_period_run`** — the m:n makes re-delivery expressible
  rather than destructive. Fieldglass periods derive from the **week-ending date** (Adrian,
  28 September); `weekending_date` is already a payload column on `nhl_invoice_line`.

**Silver** (`02_usnc_silver_edm_dev.control`):

- **`aud_load_run` gains `etl_run_id`** — the join that makes "one model" real, and the whole of
  §2b. It is in `control_standard.CORE`, so DDL, standard and checks change in one commit.
- **`ctl_etl_log`** — record-level, with `edw_record_hk BINARY(32)`: a row's warehouse identity
  here is its hash key, not a surrogate integer.
- **`ctl_metric_definition`**, **`ctl_metric_result`**.

## 6. [REV2] What loop-1 actually reconciles, and the missing piece

Rev 1 implied bronze's `ctl_delivery_manifest` feeds loop-1. It does not: `loop1_reconciliation`
reconciles against **silver's `ctl_approval_manifest`**, and **nothing writes that table**.

And `spec.py:936` **raises `SpecError` if a reconcilable binding declares both `dedup_by` and
`manifest_column`** — all three active reconcilable bindings dedup. Rev 1's instruction to set
`manifest_column` on them would have made the model refuse to load, killing the build.

The missing piece neither document had: **the post-de-duplication count**. Loop-1 becoming
evaluable requires, in order — the post-dedup count, a change to the validate rule that refuses
the combination, and only then the binding declarations.

## 7. [REV2] What was missing entirely

- **Retry idempotency.** DEF-56 repair runs reuse `{{job.run_id}}`. A retry must re-register the
  *same* `etl_run_id` and append a status event, or it becomes a second delivery with a second
  count and loop-1 balances twice.
- **Supersede and correct — [REV3] branch 2.** A wrong `ctl_etl_run_status` event cannot be
  edited. A supersede row is needed, modelled on `ctl_quarantine_superseded`. Not branch 1:
  nothing writes these tables until branch 2, so there is nothing yet to correct.
- **Orphan ids — [REV3] branch 2.** Nothing stops a mechanism stamping an id it never registered. An orphan-id gate
  joins stamped values back to `ctl_etl_run` and fails on any that are absent.
- **Retention — [REV3] branch 3, with the tables it bounds.** Stated for `ctl_etl_log` only in rev 1. `ctl_metric_result` and
  `ctl_etl_run_status` are unbounded too, at GL scale (19,057,752 rows).
- **`delivered_count` is unverifiable by construction** — it is the sender's claim, which is the
  point, since only the sender knows what it sent. Recording `arrived_count` beside it makes the
  two able to disagree **visibly**, which is the nearest thing to verification available.

## 8. [REV2] Sequencing — three branches

Rev 1 put the SDP share first, arguing it needs nobody's agreement. **That rested on a category
this repo excludes:** `resources/vault_pipeline.yml` states there is no bronze-ingest pipeline and
bronze ingestion is out of scope (decision D4), `src/pipelines/bronze_ingest.py` is unwired and
slated for deletion, and `verify_repo.py` hard-asserts no bronze-ingest task. Nothing in this
bundle lands bronze, so there is no SDP share for us to write.

| branch | contents | proves |
|---|---|---|
| **1 — the shape** | the mechanism map; `ctl_etl_run` + status + registry entries; the emitter's view support; the manifest extension | the tables, the emitter and the classification work, before anyone is asked to write to them |
| **2 — the control** | the post-dedup count, the validate-rule change, metrics, loop-1 | the first control that asserts nothing today starting to assert something |
| **3 — the capability** | source status and run-when-down, business periods, record-level log, the `aud_load_run` link | new capability, none of it blocking |

**Branch 1 depends on nobody.** If the mechanism map returns all-`unknown`, branch 1 still lands —
the tables and the emitter are ours. Branches 2 and 3 are scoped by what the map resolves.

## 9. What must not regress

`append_only_check` green with every new table registered; per-object grants only (DEF-40); the
bronze emitter's byte-gate; `control_standard` and the emitted DDL in lockstep; no semicolon in
any SQL string or comment; check counts never decrease.
