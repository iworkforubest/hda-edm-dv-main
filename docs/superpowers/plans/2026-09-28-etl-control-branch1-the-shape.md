# ETL Control Framework — Branch 1: the shape

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the run-identity tables in bronze control, emitted through the existing generator, so a delivery can be registered and its status tracked — before any team is asked to write to them.

**Architecture:** Tables declared in `src/accelerator/control_standard.py`, emitted to `governance/control_objects_bronze.sql` by `tools/emit_control_contract.py`, registered in `APPEND_ONLY`, published as `control_contracts/bronze.yaml`. Run status is an append-only event stream with a current-status view.

**Tech Stack:** Python 3.11/3.13, PySpark on Databricks serverless, Delta, Unity Catalog, DABs.

**Spec:** `docs/superpowers/specs/2026-09-28-etl-control-framework-design.md` (rev 2, with [REV3] corrections)

**Scope:** Branch 1 of three, and **it depends on no other team**. Branch 2 (post-dedup count, validate-rule change, metrics, loop-1, supersede, orphan-id gate) and branch 3 (source status, business periods, record-level log, the `aud_load_run` link, retention) get their own plans.

> **[REV3] This plan contains no check CODE, deliberately.** Plan-authored check code has failed five times out of five in this repo — word-presence matching a comment, an eagerly-evaluated f-string `detail`, a vacuous `all()`, an unfailable text match, and code that did not parse. An implementer caught every one. Each step states **what must be asserted** and **what mutation must prove it**; the implementer writes the code against the live suite, where a syntax error surfaces in seconds instead of surviving into review.

## Global Constraints

- **Bronze control DDL is GENERATED.** Declare in `control_standard`, run `tools/emit_control_contract.py`, commit the emitted `governance/control_objects_bronze.sql` and `control_contracts/bronze.yaml`. **Never hand-edit an emitted file.** `governance/control_objects.sql` is *silver's* and is not touched here.
- **Adding to `LAYER_TABLES["bronze"]` auto-publishes into `control_contracts/bronze.yaml`**, whose owner is the Bronze / ingestion team. That is a contract change — read what the emitted header currently asserts before assuming it still holds.
- **Every control table must be registered** in `APPEND_ONLY` or `MUTABLE`, or `append_only_check.classify_control_table` returns `undeclared`.
- **`ctl_delivery_manifest` stays `MUTABLE`.** It is mutable by design — a manifest is a claim that gets corrected. Do not reclassify it.
- **The test suite is NOT pytest.** `check(name, condition, detail)` in `tests/test_accelerator.py`, inserted **BEFORE** the `if FAILURES:` / `print("ALL CHECKS PASSED")` block.
- **`tests/test_accelerator.py` STUBS pyspark** (line 1783). Anything needing a real session goes in `tests/test_spark_derivation.py`, a separate CI job.
- **An exception inside a check's CONDITION aborts the whole suite** — including arguments evaluated at a call site outside a helper's `try`, and **eagerly-evaluated f-string `detail` arguments**.
- **Every check mutation-proven by name**; a mutation that merely "exits 1" is NOT a proof, since an abort also exits 1. Each proof must show **PASS+FAIL equal to the full check count AND the summary block printed**.
- **No check may pass vacuously** — no `"X" in <file text>`, no two values degrading to the same sentinel and comparing EQUAL, no `all()` over an empty collection.
- **Do NOT use `_raises`** (defined twice; line 4700 shadows line 50 and returns `False` for any non-`ValueError`).
- Counts never decrease. `verify_repo.py` is at the repo root. No semicolon in any SQL string or comment. No `QUALIFY`. Re-parse rendered statements in local Spark (`~/.jdk/jdk-17.0.20.1+1`).

## Review Focus

1. **A retry becoming a second delivery.** DEF-56 repair runs reuse `{{job.run_id}}`. A retry must re-register the same `etl_run_id`, or it is a second delivery with a second count and reconciliation balances twice.
2. **The emitted contract changing under the Bronze team.** These tables publish into their contract file, whose header currently states what bronze declares.
3. **Two status events with the same timestamp, or arriving out of order.**
4. **A run with no terminal status** — visible as unterminated, not assumed complete.
5. **A declaration the emitter does not render.** The byte-gate catches a hand-edit; only a check catches a table declared and never emitted.

---

### Task 1: The source-to-mechanism map

**Files:** create `tools/measure_ingestion_mechanisms.py` and `metadata/ingestion_mechanisms.yml`; modify `tests/test_accelerator.py`

**Interfaces:** produces `{source_system: {mechanism, evidence}}`, consumed by branches 2 and 3 to decide who is asked for what.

The bronze schema names do not reveal this: `snaplogic` covers the Snaplogic flows only, and the estate also ingests through Synapse and through pipelines outside this bundle.

- [ ] **Step 1: Write the read-only measuring tool.** For each schema in `01_usnc_bronze_dev`, record what evidence exists and leave `mechanism: unknown` where the evidence does not decide. `unknown` is an output, not a failure — it is the list to ask about.

- [ ] **Step 2: Run it, resolving the target BY HOST, and commit the result.** Read the host from `databricks.yml`'s target and match it against the profiles; do not hardcode a profile name.

- [ ] **Step 3: Assert four things**, each its own check, each guarded so a malformed file fails a check rather than aborting the suite:
  - **(a)** the file loads as a mapping of mappings and is non-empty;
  - **(b)** every `mechanism` is one this estate actually has;
  - **(c)** every entry carries non-empty `evidence` — an entry without it is a guess, and a guess puts a request in front of the wrong team;
  - **(d)** the map covers every non-system bronze schema. **The expected count must come from `information_schema`, not from the file being checked** — sourcing it from the file makes the check tautological.

- [ ] **Step 4: Prove all four.** Mutations: an invented mechanism value; a blanked `evidence`; the file truncated to one entry; the file replaced with a list. Each must give a named red with the full count and the summary block printed.

- [ ] **Step 5: Commit.**

---

### Task 2: `ctl_etl_run` and `ctl_etl_run_status`

**Files:** modify `src/accelerator/control_standard.py`; regenerate `governance/control_objects_bronze.sql` and `control_contracts/bronze.yaml`; modify `tests/test_accelerator.py`

- [ ] **Step 1: Declare both tables**, registered in `APPEND_ONLY`.

```python
"ctl_etl_run": {
    "etl_run_id": "STRING",              # opaque UUID -- nothing parses it
    "source_system": "STRING",
    "ingestion_mechanism": "STRING",     # who to call. NEVER in identity or control flow
    "mechanism_native_run_id": "STRING", # the vendor's id: recorded, never joined on, nullable
    "producer": "STRING",                # the job or pipeline that ran, descriptive
    "started_at": "TIMESTAMP",           # the mechanism's clock
    "created_at": "TIMESTAMP",           # ours
},
"ctl_etl_run_status": {
    "etl_run_id": "STRING",
    "status": "STRING",                  # PENDING / STARTED / COMPLETED / FAILED
    "status_at": "TIMESTAMP",            # the mechanism's clock
    "recorded_at": "TIMESTAMP",          # OURS -- the ordering column, see Task 3
    "written_by": "STRING",
},
```

- [ ] **Step 2: State the retry rule in `etl_run_id`'s column comment** — a retry re-registers the **same** id and appends a status event. Minting a new one turns a retry into a second delivery with a second count (Review Focus 1).

- [ ] **Step 3: Read the emitter's emitted header BEFORE regenerating.** `tools/emit_control_contract.py` currently writes a header asserting what bronze declares of its own; adding two tables makes that false. Update the header text in the same commit, and record in the report what it said before.

- [ ] **Step 4: Regenerate and commit the emitted artefacts.** Never hand-edit them.

- [ ] **Step 5: Assert three things** — that `control_standard` and the emitted DDL agree on both tables, **derived from both sides rather than from a literal list**; that both classify as `assert`; and that re-running the emitter is a no-op.

- [ ] **Step 6: Prove each, re-parse the emitted statements in local Spark, and commit.**

---

### Task 3: View support in the emitter, and the current-status view

**Files:** modify `tools/emit_control_contract.py` and `src/accelerator/control_standard.py`; regenerate the bronze DDL; modify `tests/test_accelerator.py` and `tests/test_spark_derivation.py`

**[REV3] The emitter capability and the view it renders are ONE task.** Splitting them left the emitter's check asserting over an empty declaration set — vacuous, and banned by this plan's own constraints.

- [ ] **Step 1: Add a `VIEWS` declaration** to `control_standard`, keyed by layer like `LAYER_TABLES`, and emit views after the tables into the same generated file so the byte-gate covers them.

- [ ] **Step 2: Declare `ctl_etl_run_current`** — a `LEFT JOIN` from `ctl_etl_run` to its latest status, selected with `row_number() OVER (PARTITION BY etl_run_id ORDER BY recorded_at DESC, <status precedence> DESC) = 1`. No `QUALIFY`.

  Three deliberate choices, each answering a Review Focus item:
  - **`LEFT JOIN`**, so a run with no status row still appears, as one that never terminated. An inner join hides exactly the failure this view exists to surface.
  - **`ORDER BY recorded_at`** — our clock, not `status_at`, which is the mechanism's. Clock skew would otherwise let `STARTED` beat `COMPLETED`.
  - **A status precedence tie-break**, because two events can share a timestamp and `row_number()` would otherwise choose nondeterministically.

- [ ] **Step 3: Assert the SELECTED ROW, not the SQL text — in `tests/test_spark_derivation.py`**, because `test_accelerator.py` stubs pyspark and has no session. Over a fixture: equal `recorded_at` with `STARTED` and `COMPLETED` selects `COMPLETED`; a skewed `status_at` does not change the winner; a run with no status row appears with a NULL status.

- [ ] **Step 4: Assert offline, in `test_accelerator.py`**, that every declared view is present in the emitted file — compared as parsed statements, never as substrings of the file.

- [ ] **Step 5: Prove each.** Remove the tie-break; order by `status_at`; change `LEFT JOIN` to `JOIN`; declare a view the emitter does not render. Four named reds, each with the full count and the summary printed.

- [ ] **Step 6: Re-parse in local Spark and commit.**

---

### Task 4: Extend `ctl_delivery_manifest`

**Files:** modify `src/accelerator/control_standard.py`; regenerate the bronze DDL and contract; modify `tests/test_accelerator.py`

- [ ] **Step 1: Add `etl_run_id`, `table_name` and `arrived_count`.** The grain becomes `(etl_run_id, table_name)`, so a run delivering four tables produces four counted rows.

  `delivered_count` is the sender's claim and is **unverifiable by construction** — that is the point, since only the sender knows what it sent. `arrived_count` is ours. Recording both lets them **disagree visibly**, which is the nearest thing to verification available.

- [ ] **Step 2: Do NOT reclassify it — it stays `MUTABLE`.** `control_standard.py:216-218` states why: a config table records what SHOULD happen, an expectation gets corrected and a manifest gets superseded, and making those append-only would mean a mistyped row could never be withdrawn. Rev 2 proposed reclassifying on an argument about writers rather than about modelling, and that was wrong. **Any existing check asserting it is `MUTABLE` must stay green** — if one reds, stop and report rather than editing it.

- [ ] **Step 3: Assert** that the grain is declared, that all three counts are `BIGINT`, and that `classify_control_table("ctl_delivery_manifest")` still returns `mutable`.

- [ ] **Step 4: Prove each, re-parse, and commit.**

---

## Self-review

**Spec coverage.** §5 bronze tables → Tasks 2, 4. §4 emitter, registry and contract → Tasks 2, 3, 4. §7 retry idempotency → Task 2 Step 2, as a stated rule; the enforcing writer is branch 2. §8 branch 1 → this plan. Supersede, the orphan-id gate and retention are assigned to branches 2 and 3 in spec §7 and are correctly absent here: nothing writes these tables until branch 2, so there is nothing yet to correct or orphan.

**No check code, deliberately** — see the note under Spec. Each step names the assertion and the mutation instead.

**Review Focus coverage.** 1 → Task 2 Step 2. 2 → Task 2 Step 3. 3 → Task 3 Steps 2, 3, 5. 4 → Task 3's `LEFT JOIN`. 5 → Task 3 Step 4 and Task 2 Step 5.

**Open at plan time:** how many bronze source schemas the coverage check requires. Task 1 Step 3 takes it from `information_schema` — not from a guess, and not from the file it is checking.
