# Sub-project 3a — Vault layering, the journal domains, and change detection

Date: 2026-08-24
Status: REVISED after Task 1 proved a prerequisite false
Revision: 2 — `antijoin` does not work in this workspace; scope is reduced accordingly
Parent: `docs/superpowers/specs/2026-08-24-subproject3-source-rebinding-design.md`
Accelerator: v0.2.0 · hash rulebook 1.0.0 (unchanged by this work)

---

## 1. What Task 1 proved

The plan's first task existed to test two prerequisites before anything was built on them.
One failed. Revision 1's scope is not deliverable and is reduced here.

### 1.1 `change_detection: antijoin` does not work — DBR 18.3, channel CURRENT

Two independent refusals, and the probe target holds **0 rows** after five updates:

- A batch `@append_flow` into a streaming table is rejected at graph analysis:
  `CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED`. All five updates FAILED.
- The `NOT EXISTS` half is impossible regardless. A fully-qualified read of the
  pipeline's own target raises `UnresolvedDatasetException` — *"defined in the pipeline
  but could not be resolved"*. **No flow type can compare a satellite against itself
  in-pipeline.**

Two variants returned exactly the row count a naive pass-test looks for **without ever
running the anti-join**: `once=True` (goes IDLE after one load and never executes again)
and a stream-static rewrite (the checkpoint consumed the source on run one). Both would
have been false passes. `once=True` must be explicitly refused by validation — it stops
silently after a single load, which looks like success.

**This vindicates `factory.py` decision 3 rather than contradicting it.** A streaming
table cannot read itself, which is exactly why the design delegates change detection to
Bronze. `cdc` needs no self-comparison — it filters an operation flag Bronze supplies.
`antijoin` was the stopgap for sources without one, and the stopgap does not exist.

### 1.2 Masks: views propagate, materialized views do not

A plain Unity Catalog view propagates a column mask at read time. A **materialized view
does not** — and `_v1` is a materialized view. Proven in both directions:

- An MV with no mask declared stored `***` **permanently**: relaxing the base mask to
  pass-through without refreshing left the MV serving `***` while the base returned
  plaintext.
- The mirror case leaked: with the base mask set to deny-all, the MV still served
  plaintext. `information_schema.column_masks` shows no row for the MV.

No modelling change is forced — `_emit_v1_view` already redeclares the mask. But two
things are now proven rather than assumed:

1. **A mask must be redeclared on every materialization**, not inherited.
2. **The pipeline's run-as principal must be privileged under every mask it materializes
   through**, or the MV bakes `***` into storage permanently. That is data loss, not a
   display problem.

Also confirmed: `dp.materialized_view(schema=... MASK ...)` works on 18.3, closing an open
keyword question in `_emit_target`.

### 1.3 Provenance of the Task 1 evidence, and the residue it left

`probes/` and the five `probe_*` pipelines were throwaway and are deleted in Task 8. The
findings above are the whole of what they proved; the detail that only lived in
`probes/README.md` is recorded here so nothing is lost with the directory.

**Where it was measured.** Workspace `db-usnc-datalakehouse-tds`
(`adb-2593897084138079.19`), target `usnc_tds`, profile `hfig-usnc-tds`, on 2026-08-24.
Pipeline channel `CURRENT`, `dbr_version 18.3` read from the `create_update` event,
serverless and Photon. Run-as and reader `adrian.turcu@headfirst.group`, a member of
`us_tds_data_engineer`, `data_platform_operations` and `users` but **not** of
`pii_cleared_us` — the asymmetry that let one principal demonstrate both the masked and
the unmasked outcome. Source `01_usnc_bronze_dev.great_plains_raw.gl20000`
(4,444,172 rows), read only; the probe slice was a pure filter
(`jrnentry BETWEEN 43 AND 60`), never a `.limit()`, so it is byte-identical on every run:
1,344 rows, 136 distinct hashdiffs. A doubled count would have been 2,688.

**How the two false passes were told apart from real ones**, which matters to anyone who
re-runs this: neither `once=True` (1344 → 1344) nor the stream-static rewrite
(1344 → 1344) executed the anti-join at all, and `COUNT(*)` cannot tell you that. The
`once` flow reported `IDLE, waiting for new data` on run 2; the stream variant's
checkpoint had consumed the source on run 1, and every row in both of its runs carried
`probe_path = fallback_full`, i.e. the anti-join branch threw and the fallback ran.
**Check the flow's run-2 event and the branch marker, not the row count.**

**`dp.view` was not usable for the mask question at all.** A `dp.view` is pipeline-local
and never registered in Unity Catalog, so a separate principal cannot read it; the probe
had to use a materialized view, which is what `_v1` is anyway.

**The mask finding generalises past `_v1`.** *Every* downstream materialization — `_v1`,
the PIT MVs of parent sub-project 5, and any Gold materialization — must redeclare the
mask, because an MV projection of a masked column that declares no mask of its own is an
**unmasked copy**. `checks/mask_survival_check.py` asserting the mask per generated table
is therefore evidence-backed, not precautionary.

**Residue in the workspace, and what the Task 8 deploy did with it.** The probes left a
whole schema behind. `02_usnc_silver_edm_dev.silver_vault` held **eight objects and
nothing else** — `probe_antijoin_target` (0 rows), `probe_antijoin_once`,
`probe_antijoin_stream`, `probe_antijoin_diag`, `probe_masked_base`, `probe_masked_mv1`,
`probe_plain_base`, `probe_plain_v` — plus the functions `probe_mask` and
`probe_mask_priv` in `02_usnc_silver_edm_dev.governance`. Note the schema name: E3a-1
renamed the vault schema to `raw_vault`, so `silver_vault` is now **entirely** probe
residue, and the superseded `[usnc_tds] hfig silver vault` pipeline that once targeted it
managed zero tables. Removing the five `probe_*` resources therefore made the Task 8
deploy a destructive one — six pipeline deletions, the five probes and that empty
predecessor — over a schema that has never held vault data. Verified table by table
before approving, because `bundle deploy` cannot distinguish an empty superseded pipeline
from a loaded one. The `probe_*` tables and the two mask functions are not bundle-managed
and survive the deploy; dropping them is a workspace clean-up for whoever holds the grant.

---

## 2. Decisions

### E3a-1 — Split the vault into `raw_vault` and `business_vault` schemas

`vault_schema` is a single value and the pipeline writes everything to one schema, so the
Raw Vault and the Business Vault are currently indistinguishable in the catalog. They are
different things with different dependencies: the Raw Vault loads from Bronze; the
Business Vault computes from the Raw Vault.

| Schema | Holds |
|---|---|
| `raw_vault` | hubs, links, NHLs, satellites, multi-active satellites |
| `business_vault` | computed satellites (`csat`), and later PIT and bridge tables |

An SDP pipeline targets one schema, so this is **two pipelines**: `raw_vault` runs first,
`business_vault` runs after and reads its output. That dependency is real and currently
implicit — the `csat` entities already declare `BUSINESS_VAULT` as their source name and
read a vault table as their `bronze_table`.

It also matches the fallback in section 3: a second stage reading the first stage's output
is precisely the shape a separate change-detection task needs.

The pipeline entry point gains a layer filter alongside its existing domain filter.
`BUSINESS_KINDS = {"csat"}` today, anticipating `pit` and `bridge` from parent
sub-project 5.

Touches `databricks.yml` (all nine targets), `resources/vault_pipeline.yml`,
`resources/vault_job.yml`, `src/pipelines/silver_vault.py`, `checks/apply_governance.py`,
`checks/conformance_check.py` (its `--schema` default), `governance/apply_masks.sql`,
`verify_repo.py`, both `csat` entity files, and the docs.

### E3a-2 — Two journal entities, not one

Revision 1 conflated them. Dynamics GP's `gl20000` is the **general** ledger — payables,
receivables, payroll, everything, undifferentiated. The model's `nhl_journal_line`
declares `aggregates_from: payroll_detail`: it is the **payroll** journal.

Evidence they cannot be the same entity: GP's journal has no pay-period column (only
`periodid`, a *fiscal* period), and payroll-originated entries are not distinguishable —
`trxsorce` is `GLTR` for essentially every row. **Posting to GP loses the pay period.**

| Entity | Source | Parents | Transaction key |
|---|---|---|---|
| `nhl_journal_line` (payroll) | `ukg_raw.gl` | journal, organisation, ledger_account, **pay_period** | `userdefined1` |
| `nhl_general_journal_line` (new) | GP `gl20000` + `gl30000` | journal, organisation, ledger_account | `seqnumbr` |

Dynamics GP remains the book of record for the general ledger. `ukg_raw.gl` is the payroll
GL posting that feeds it (flow 2050) and is the only place the pay period survives, so it
is **not** superseded — parent decision E8 is narrowed to the general journal.

`hub_accounting_journal` takes two bindings under the reference-type pattern:
`('UKG_Batch_ID', batchid)` and `('GP_Journal_Entry', jrnentry)`. Whether a GP journal
entry corresponds to a UKG batch is a same-as question, downstream.

### E3a-3 — `pay_period` becomes a parent; `aggregate_drops` becomes `[worker]`

The SME confirms: a batch is unique within company and pay period; supplemental runs get
their own batch; a company/batch sits in exactly one pay period, and a pay period may hold
several. Verified against the extract — `max_periods_per_batch = 1`. Their answer on
modelling was "Parent".

These are one change, not two. `spec.py:483-500` requires `aggregate_drops` to be disjoint
from the entity's own parents, so making `pay_period` a parent **forces**
`aggregate_drops: [worker]`. Validation enforces the consistency.

`hub_pay_period` therefore returns to scope, sourced from `ukg_raw.gl`'s
`payperiodstartdate` / `payperiodenddate` — a real pay period from the payroll system.
It is **not** bound to `fiscalperiods`, which is an accounting month.

The SME also asked where worker appears in the GL file. It does not, and that is what the
declaration records: `aggregate_drops` names parents of the raw counterpart that were
summed away, not columns present in the source.

### E3a-4 — Correction: the UKG duplication is re-delivery, not double-entry

The parent spec states `ukg_raw.gl` holds "exactly two rows per key — the debit side and
the credit side". That is wrong.

```
files 2 | rows 520 | distinct keys 260 | keys+amounts 260 | keys+input_file_name 520
```

The same 260 lines were delivered twice, identical in every business column including the
amounts. Each line is either a debit or a credit (348 debit rows = 174 lines, 172 credit =
86 lines). The natural key is `(company, batchid, account, userdefined1)`, and the
doubling is the same `_raw` overlapping-delivery pattern as GP's 1.8x.

### E3a-5 — `input_db` is the legal-entity code (BRZ-5 closed)

`BARM` and `BRPLM` both appear as `input_db` values, and the SME names BARM as a company.
`BRPLM` also appears as `company` in `ukg_raw.gl`. Same code, same legal entity, two
systems.

Eleven values: `CER`, `BARM`, `CSS`, `BRPLM`, `GGI`, `CSGH`, `INSPV`, `GUSPV`, `GLSPV`,
`BPSPV`, `CESPV`. The `*SPV` suffixes are consistent with special-purpose vehicles. The
mapping to legal entity *names* is still wanted but no longer blocks.

### E3a-6 — Capabilities

Unchanged from revision 1 except that the N-table binding stays deferred to 3b:

| Capability | Needed by |
|---|---|
| Staging deduplication | every binding — `_raw` re-delivery |
| Literal key component | `hub_organisation`, `hub_accounting_journal` reference types |
| Per-binding type coercion | GP's `debitamt`/`crdtamnt` are `double` |
| Key-composition guard | nothing currently guards `business_keys` changes |

---

## 2b. OPEN GAP IN PARENT DECISION D5 — the inventory guarantee holds, the layout guarantee does not

**Read this before deploying a second lake.** `active_sources` (parent D5) is implemented:
`create_streaming_table()` is emitted for every declared binding in every lake and
`append_flow()` only for active ones, so **every lake has the same table names**. That is
the inventory guarantee, and it holds.

**The layout guarantee does not.** `_emit_target` passes no explicit schema, so SDP infers
a streaming table's columns from the flows that write to it. A table with no active
binding has only the ghost flow, which supplies the hash key and the six system columns
and no payload. So an inactive table is **eight columns wide where an active lake has it
at full width** — same name, same properties, wrong shape.

`checks/conformance_check.py:124` compares `information_schema.columns` for every table
matching `VAULT_PREFIXES`. Under `usnc_tds`'s current `active_sources`, **18 of 25 vault
tables are ghost-only**, so a cross-lake comparison against a lake where those sources are
active would report a column-layout difference on all 18.

**Why it is not fixed here.** The generator cannot know a payload column's *type*: types
are inferred from the Bronze table at runtime, and for an inactive binding that Bronze
table is not in this lake. There is no local fix — only a design choice, and it is a real
one:

| Option | Cost |
|---|---|
| Declare column types in metadata | Every binding's full payload typed by hand; the metadata stops being a mapping and starts being a schema, and it can drift from Bronze silently |
| Make `conformance_check.py` active-set aware | It would need each lake's `active_sources` to compare only tables active in *both*; conformance then proves less, and proves nothing about a table active nowhere |
| Do not create inactive tables at all | Abandons D5's inventory guarantee outright, and a missing table is exactly what D5 exists to prevent |

**Why it does not block 3a.** `conformance_check.py` compares two or more lakes and only
`usnc_tds` exists. The gate is already dormant for this sub-project by stated condition
(section 5), so nothing fails today.

**What to do about it.** Whoever deploys the second lake meets this as a *decision*, not
as a surprise gate failure. Take one of the three options above before running
`conformance_check.py` across lakes, and record which. The mask path is already handled:
mask clauses and the `_v1` projection are suppressed for an inactive table, because the
columns they name do not exist on it.

---

## 3. Change detection: what is possible now

**`cdc` is the design's answer and needs Bronze to supply an operation flag.** No `_raw`
source has one. **BRZ-1 — Change Data Feed on `_raw` — is therefore on the critical path,
not an optimisation.** Without it there is no in-pipeline way to load a satellite
correctly.

Three routes, and 3a takes the third:

- **Wait for CDF.** `cdc` then works exactly as designed, no code change. Gated on another
  team.
- **The documented fallback: a separate Lakeflow job task** outside the pipeline, per
  `factory.py` decision 3. Unblocks satellites now, at the cost of the SDP-native premise
  for satellite loading. E3a-1's two-pipeline split establishes the shape.
- **Reduce scope to what needs no change detection.** Hubs, links and NHLs are
  insert-only by nature and carry no hashdiff — nothing to compare. They load correctly
  today.

### 3.1 Revised scope for 3a

**In:** `hub_organisation`, `hub_accounting_journal`, `hub_ledger_account`,
`hub_pay_period`, `nhl_journal_line`, `nhl_general_journal_line`, plus the `raw_vault` /
`business_vault` split and the four capabilities.

**Deferred until CDF lands or the fallback is built:** every satellite, including
`sat_accounting_journal_header`. They are declared and left inactive under parent D5's
`active_sources`, exactly as the unsourced payroll entities are.

This still delivers the thing that matters: **first data in the vault, and gate zero
executing before a load.** NHL uniqueness in `append_only_check.py` also becomes the
sharpest available test of E3a-6's deduplication, since NHLs have no hashdiff to hide a
duplicate behind.

---

## 4. Deliberately not decided here

`fiscalperiods` is not bound to anything. Whether `ukg_raw.gl` is retained as a
reconciliation cross-check against GP is BRZ-11. The mapping of the eleven `input_db`
codes to legal entity names is still wanted.

---

## 5. Definition of done

`preflight_target` passes; `bundle validate` is clean apart from the known DEF-43 warning;
the hub and NHL entities are loaded into `02_usnc_silver_edm_dev.raw_vault`; and these
gates have **executed**:

| Gate | Asserts |
|---|---|
| `hash_parity_check.py` | gate zero — Spark digests equal the reference implementation |
| `append_only_check.py` | no mutation; **NHL uniqueness** — the dedup test |
| `loop1_reconciliation.py` | landed + quarantined = approved |
| `journal_integrity_check.py` | debits = credits within tolerance; `line_order` dense and unique |

`mask_survival_check.py` is deferred with the satellites. `aggregate_reconciliation_check`
and `conformance_check` remain dormant — no transaction-grain counterpart, no second lake.

---

## 7. Task 8 — the first deployment, and what it stopped

The bundle deployed to `usnc_tds`. **No data was loaded.** Five defects were found, in a
chain, each one hidden behind the one before it — and not one of them is visible to
`verify_repo.py`, `tests/test_accelerator.py` or `bundle validate`. They exist in the
execution model and in generated SQL, so only a real run against a real workspace finds
them.

| # | Defect | Status |
|---|---|---|
| DEF-12 | serverless `spark_python_task` binds no `__file__`; every gate died on its bootstrap | fixed |
| DEF-13 | the hashdiff tail-strip regex was unescaped, so it stripped nothing in Spark | fixed |
| DEF-14 | serverless marks a gate that `sys.exit(0)`s as a FAILED task | fixed |
| DEF-15 | an SDP library cannot locate itself; the code object's path is a transient one | fixed |
| DEF-16 | the generated `MASK` clause omitted the column type and was not valid DDL | fixed |
| DEF-17 | `current_pipeline_update_id()` does not exist on this runtime | fixed |
| DEF-18 | `@dp.expect_all_or_drop` cannot stack under `@dp.append_flow` | fixed |
| DEF-19 | `schema=` is the whole schema, so a mask needs all 85 columns declared | fixed — §2b decided |
| DEF-20 | the mask functions did not exist; the job created them AFTER the load | fixed (split); grants stay blocked |
| DEF-21 | a MASK function must be catalog-qualified, not schema-qualified | fixed |
| DEF-22 | the mask function DDL was never valid SQL — `COMMENT` after `RETURN` | fixed |
| DEF-23 | Delta will not `CLUSTER BY` a BINARY column, and every hash key is BINARY | fixed (layout regression recorded) |
| DEF-24 | `load_dts`, the clustering fallback, is outside Delta's 32-column stats window | fixed (clustering dropped) |
| DEF-25 | the ghost flow never supplied `cdc_op`, which is NOT NULL | fixed |
| DEF-26 | the append-only gate filtered on `MANAGED` and saw no vault table at all | fixed |
| DEF-27 | the uniqueness grain was an arbitrary `_hk` taken from a set | fixed |
| DEF-28 | hubs are not deduplicated, within a flow or across flows | **open — design decision** |

Gate zero **passes**: `7 keys and 7 hashdiffs match the reference exactly`, including the
schema-evolution assertion. Four of the five are defects in how the code is *executed*;
DEF-16 is the only one in what the generator *produces*.

Note what the order of discovery cost. DEF-12 and DEF-14 between them meant **no possible
outcome of any gate could produce a green task** — failure crashed on the bootstrap,
success raised `SystemExit(0)` — so until both were fixed the gates could neither pass nor
fail informatively. DEF-13 was invisible until they were, and DEF-16 was invisible until
DEF-15 let the pipeline reach `factory.build`. A green run was never available to be had;
the question was only how far down the chain a run could get before stopping.

### DEF-12 — serverless `spark_python_task` defines no `__file__` (FIXED here)

DEPLOY.md Phase 3 lists "spark_python_task + environment_key works on serverless jobs" as
an assumption to VERIFY. It half-holds: the task launches, but Databricks serverless reads
the file and `exec(compile(source, path, "exec"))`s it in an ipykernel command context
where **`__file__` is not bound**. Every check script begins

```python
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
```

so all seven of them raised `NameError` on their first executable line — before importing
`accelerator`, before reading `golden_hash_vectors.json`, before reading
`metadata/entities`. **Every gate in the job failed without asserting anything**, and the
run page showed a normal task failure with a Python traceback: exactly the "a gate that
passes without asserting" failure this sub-project exists to prevent, in its louder form.

The fix does not touch a single assertion. `compile()` still records the real path in the
code object, so the path is recoverable where `__file__` is not bound:

```python
if "__file__" not in globals():
    import inspect as _inspect
    __file__ = _inspect.currentframe().f_code.co_filename
```

Applied to all ten scripts in `checks/`. `append_only_check.py` never referenced
`__file__` and needed no change beyond DEF-14. Verified by exec'ing the files the way
serverless does, with no `__file__` in globals, and confirming the bootstrap resolves and
the golden vectors load. **Offline suites cannot catch this**: run under `python file.py`,
`__file__` is always bound.

**The pipeline entry point has the same symptom and a different cause — see DEF-15.** The
fix above is correct for a `spark_python_task` and wrong for an SDP library; applying it
there removed the `NameError` and replaced it with a silently wrong path.

### DEF-13 — the hashdiff tail-strip was a no-op in Spark (FIXED here)

Gate zero ran, and **failed**, on the one assertion DEPLOY.md §5a calls the most important
line in its output: *appending a null column must not change the hashdiff*.

```
  PASS  baseline                     806e2e950bba07ca...
  FAIL  appended_null_column         806e2e950bba07ca...
  FAIL  appended_two_null_columns    806e2e950bba07ca...
HASH PARITY GATE FAILED -- 3 divergence(s)
```

`hashing.py` builds the tail-strip as `REGEXP_REPLACE(payload, CONCAT('(', '||^^', ')+$'), '')`.
The delimiter and null token are **interpolated into a regular expression unescaped**, and
both are made entirely of regex metacharacters. The pattern Spark compiles is

```
(||^^)+$
```

— an alternation of two *empty* branches and two start-of-input anchors. It matches the
empty string at the end of the payload and **strips nothing**. The reference implementation
escapes correctly (`re.escape(DELIMITER + NULL_TOKEN)` → `(?:\|\|\^\^)+$`), so the two
implementations disagree for every payload whose trailing columns are NULL. Reproduced
offline: applying the unescaped pattern to the golden payload yields sha256
`16adba78a6ad76ef…` and `2579ea0b93090ff6…`, matching the gate's reported `got` digests
byte for byte. Root cause confirmed, not inferred.

**Consequences, had it shipped.** The docstring's promise that "a column added but not yet
populated leaves previously-stored hashdiffs valid and does not reinsert every row" was
false in Spark: adding a column to any satellite would have reinserted every row in the
estate. Spark-computed hashdiffs could also never have equalled reference-computed ones
once any trailing payload column was NULL.

**Fixed here, and it is NOT a rulebook change.** The rulebook's *definition* was always
correct: all seven golden hashdiff vectors reproduce byte-for-byte from
`reference_hashdiff`, and `baseline`, `appended_null_column` and
`appended_two_null_columns` all yield `806e2e95...` in the reference — the schema-evolution
property is exactly what the vectors already pin. The pure-Python reference is right, the
vectors are right, and only the **SQL generator** was wrong. So `RULEBOOK_VERSION` stays
`1.0.0`, `tests/golden_hash_vectors.json` is untouched, and those untouched vectors are
what prove the fix correct.

The fix escapes the constants for regex context at the one call site that consumes them as
a pattern, via a new `_regex_literal_sql()`. **Two escaping layers, both load-bearing:**
regex (`||^^` becomes a backslash-escaped pattern) and then SQL string literal (Spark
processes backslash escapes inside one, so every backslash is doubled). Spark parses the
emitted literal back to a pattern matching the literal text `||^^`.

`DELIMITER` and `NULL_TOKEN` are interpolated **raw** everywhere else — `_concat` builds
`CONCAT_WS('||', ...)` and `normalise` builds `COALESCE(..., '^^')`, where the raw value is
correct and escaping would itself be a bug. The helper's docstring says so explicitly, so
nobody later "makes it consistent". No rulebook constant, the algorithm, the trim/case
rules or the reference implementation were touched, and both import-time guards
(`ALGORITHM_RATIFIED`, `HASHDIFF_UPPERCASE_RATIFIED`) are untouched and still pass.

Note what held: all seven business keys passed, and so did `baseline`,
`appended_populated_column`, `case_correction`, `value_change` and `reordered_payload`. The
hubs, links and NHLs that §3.1 puts in scope use `key()`, not `hashdiff()`, so **the defect
did not touch anything Task 8 was going to load** — it blocked the load only because gate
zero is correctly unconditional, which is exactly the behaviour wanted.

### DEF-14 — serverless marks a PASSING gate as a failed task (FIXED here)

With DEF-13 fixed, gate zero printed
`HASH PARITY GATE PASSED: 7 keys and 7 hashdiffs match the reference exactly`
— and the task still came back **FAILED**, with `SystemExit: 0`.

Every check ended `sys.exit(main())`. The serverless ipykernel wrapper surfaces
`SystemExit` as an exception and fails the task **for exit code 0 as readily as for 1**. So
a gate that passed failed its task and blocked every task behind it — the exact inverse of
DEF-12, and worse to diagnose, because the gate's own output says PASSED while the run page
says FAILED. Between them, DEF-12 and DEF-14 meant **no possible outcome of any gate could
produce a green task**: failure crashed on the bootstrap, success raised `SystemExit(0)`.

Exit explicitly only on failure; falling off the end of the module is exit 0 anyway:

```python
_rc = main()
if _rc:
    sys.exit(_rc)
```

Applied to all ten scripts in `checks/` that used the pattern, including
`conformance_check.py` and `preflight_target.py`, which are not job tasks today but would
meet this the moment they became one. Behaviour from a shell is unchanged and was verified
both ways: `preflight_target.py` still exits 0 on a match and 1 on a mismatch.

**Neither DEF-12 nor DEF-14 changes an assertion, a threshold or an entity.** They change
whether a gate can run and whether its verdict is believed.

### DEF-15 — an SDP pipeline library cannot locate itself (FIXED here)

With the gates working and gate zero green, the raw vault still failed at graph analysis,
on every retry, with nothing loaded. Two refusals in sequence, and **the second is the
dangerous one because it looks like the bootstrap succeeded**:

1. `__file__` is unbound in an SDP library, exactly as in a serverless
   `spark_python_task` (DEF-12).
2. Recovering the path from the code object — which is *correct* for a
   `spark_python_task`, and is what `checks/*.py` now do — returns a **transient
   ipykernel path** here:
   `/home/spark-<id>/.ipykernel/34/command-42949672961-2650799212`.
   The `/Workspace/...` path shown in the traceback header is display metadata; it is not
   `co_filename`. So `sys.path` received a junk directory, `import accelerator` failed,
   and `METADATA_DIR` would have pointed at nothing.

Applying DEF-12's fix here therefore turned a loud `NameError` into a quiet wrong answer.
It is recorded as its own defect rather than folded into DEF-12 precisely because the two
have the same symptom, the same-looking fix, and different correct answers.

**The location is configuration, not something to derive.** Both pipelines now pass
`hfig.source_root: ${workspace.file_path}` — which the bundle resolves to the deployed
`.../files` directory — and the entry point reads it from `spark.conf` before importing
anything, falling back to `__file__` only for a local run where it is bound and correct:

```python
_SOURCE_ROOT = spark.conf.get("hfig.source_root", "").strip()
ROOT = Path(_SOURCE_ROOT) if _SOURCE_ROOT else Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
METADATA_DIR = ROOT / "metadata" / "entities"
```

The import is wrapped so that a missing `hfig.source_root` names itself rather than
surfacing as a bare `ModuleNotFoundError`. **Do not "simplify" this back to
`Path(__file__)`** — the comment in the entry point says so, because the simplification
looks obviously right and fails only at runtime, in the pipeline, at graph analysis.

**Method note.** This was diagnosed rather than guessed: the entry point was temporarily
made to raise with `__file__`, its resolved form and the head of `sys.path` in the message,
and one pipeline update returned the ipykernel path that made the cause unambiguous. Three
of these four defects are invisible to any offline suite and to `bundle validate`; they
exist only in the execution model, and only a real run finds them.

### DEF-16 — the generated MASK clause carried no column type (FIXED here)

With DEF-12/13/14/15 fixed, gate zero green and the entry point importing correctly, the
raw vault reached `factory.build` and failed there. This is the first defect in the
**generator's own output** rather than in how it is executed.

```
ParseException: [PARSE_SYNTAX_ERROR] Syntax error at or near 'governance'. (line 1, pos 92)

== SQL ==
CREATE TABLE `02_usnc_silver_edm_dev`.`raw_vault`.`nhl_general_journal_line`
  (debitamt MASK governance.mask_money, crdtamnt MASK governance.mask_money)
--------------------------------------------------------------^^^
```

`_mask_clauses` emits `", ".join(f"{col} MASK {fn}")` — **column name and mask function,
with no data type**. A column entry in a table definition needs one, so the parser reads
`debitamt` as the name, takes `MASK` as the type, and rejects `governance`. Compare the
Task 1 probe, which *did* work and which §1.2 cites as settling the keyword question: it
passed `schema="... sensitive_text STRING MASK <fn> ..."` — hand-written, **with types**.
The keyword is accepted; the generator has simply never emitted a well-formed clause.
`_emit_v1_view` uses the same helper, so the `_v1` materialized view carries the identical
defect and would fail the same way the moment satellites return.

This is **DEPLOY.md Phase 3 assumption 8 failing**, and the fallback that entry names —
*"declare the table in SQL inside the pipeline"* — is a generator redesign, not a
configuration change. The prohibitions it attaches still hold: **do not** fall back to
`ALTER TABLE ... SET MASK` (it does not survive a pipeline update) and **do not** ship the
table unmasked.

**Fixed, and deliberately NOT by solving the general problem.** The type is already
declared for every case that reaches DDL, and §2b's open gap is untouched.

`_mask_clauses` now takes the table's **active** bindings and reads each masked column's
type from that binding's `cast:` block, emitting `col TYPE MASK fn`:

```
debitamt DECIMAL(18,2) MASK governance.mask_money, crdtamnt DECIMAL(18,2) MASK governance.mask_money
```

Two facts make this safe rather than a guess, and both were checked rather than assumed:

- **Every masked column on an active binding already declares a cast.**
  `general_journal_line` and `general_journal_line_closed_year` cast
  `debitamt`/`crdtamnt`, and `journal_line` casts `debit`/`credit` — all to
  `DECIMAL(18,2)`. That is the six columns §5f expects.
- **`DECIMAL(18,2)` is the only value that can be correct.**
  `governance/apply_masks.sql:75` declares `mask_money(v DECIMAL(18,2))`, and a mask
  function's parameter type must agree with the column it masks. The cast is not merely
  an available number; it is forced.

Every masked column *without* a cast — `payroll_detail`, `timesheet_line`,
`accounting_journal_header`, `job_request_commercials`, `payroll_line_classification` —
belongs to an entity with **no active binding**, which emits no mask clause at all. The
gap therefore never reaches DDL.

**The guard that makes the coupling safe.** A masked column on an **active** binding with
no declared cast **raises `SpecError`** at build time, naming the entity, the columns, and
the type to declare. It does not emit a clause without a type, and it does not invent a
default — which would let SDP infer a type and write it permanently into an insert-only
vault. Both directions are tested offline: a masked column with a cast produces a
well-formed clause, one without refuses.

A visible consequence, and a correct one: **"every binding active" is no longer a
configuration this model can legally take.** The five deferred masked entities must
declare their casts before they can be activated. DEPLOY.md §5f already warns that
`payroll_detail/UKG_US` and `payroll_line_classification/BUSINESS_VAULT` must be activated
together; whoever does that now meets a build-time refusal naming exactly what to add,
rather than a `ParseException` pointing at a mask function's name.

**This is not a general type system and must not grow into one.** The generator still does
not know payload column types in general — §2b's decision stays open and untouched. Types
are needed only for masked columns, only on active bindings, and there they are already
declared. The docstring says so, at the call site, for the next reader.

**Scope.** Six masked columns across the three journal tables in scope — matching DEPLOY.md
§5f's `mask_survival asserted=6` — plus five more entities that are deferred with the
satellites. Every one of them is blocked on this.

### DEF-17 — `current_pipeline_update_id()` does not exist (FIXED here)

With the mask clause valid, the pipeline got past graph analysis and into the flows, where
every one of them failed:

```
[UNRESOLVED_ROUTINE] Cannot resolve routine `current_pipeline_update_id` on search path
[`system`.`session`, `system`.`builtin`, `system`.`ai`, `hive_metastore`.`default`]
  File .../factory.py, line 124, in _system_columns
    .withColumn(naming.COL["batch_id"], F.expr("current_pipeline_update_id()"))
```

**DEPLOY.md Phase 3 assumption 4, refuted.** Its documented fallback is "pass a batch id
via pipeline `configuration` and read it with `spark.conf.get`", and that is what was done
— with one correction, because the fallback as written has a trap in it. A value
hardcoded in the bundle is **identical for every update**, and a load-run id that never
changes is worse than none at all: it looks like provenance and records nothing. `batch_id`
maps to `hub_load_run` in the metrics vault, so a constant would quietly make every row in
the estate's history belong to one run.

`_batch_id(spark)` therefore reads the first key that carries a value from
`hfig.batch_id` (an operator override), `pipelines.updateId`, `pipelines.update.id`,
`pipelines.updateContext.updateId` and `spark.databricks.pipelines.updateId`.

**On this runtime, none of them does — and the update id cannot be looked up either.**
Three routes were tried and all are refused inside an SDP Python pipeline:

| Route | Result |
|---|---|
| `current_pipeline_update_id()` | `UNRESOLVED_ROUTINE` |
| `spark.sql("SET")` inside a flow | `UNSUPPORTED_COMMAND_IN_QUERY_DEFINITION` |
| `spark.sql("SET")` at build scope | `UNSUPPORTED_SPARK_SQL_COMMAND` — *"not supported in spark.sql(...) API in SDP Python"* |
| `spark.sparkContext.getConf().getAll()` | `PY4J_BLOCKED_API` on serverless |

So **the platform's update id is not reachable from inside an SDP Python pipeline here.**
The fallback is a generator-assigned id resolved **once per update in `build()`**:
`gen-<utc timestamp>-<8 hex>`. It keeps the column's actual contract — one distinct value
per load run, constant across every table in that run — and the `gen-` prefix means it can
never be mistaken for the platform's id. Correlating a load run to a platform update is
done through the pipeline event log, which `vault_pipeline.yml` already materialises.

Resolving it in `build()` rather than per flow is not just a workaround for where `SET` is
allowed; it is the correct semantics. One update is one load run, not one per table.

**Residue.** If a future runtime exposes the update id under a conf key, adding it to
`_BATCH_ID_CONF_KEYS` is the whole change, and the generated id stops being used.

No gate reads `batch_id`; it is lineage metadata. That is why a fallback is acceptable here
at all, and it is worth being explicit that the same reasoning would not license guessing a
value for anything a gate asserts on.

### DEF-18 — `@dp.expect_all_or_drop` cannot stack under `@dp.append_flow` (FIXED here)

With the batch id resolved, every flow in the pipeline still failed to resolve, all with
the same error:

```
AttributeError: 'DatasetOrExpectationDecoratorResult' object has no attribute '__globals__'
```

`_register_source_flows` declared

```python
@dp.append_flow(target=table, name=...)
@dp.expect_all_or_drop(rules)
def _valid(...):
```

The inner decorator returns a `DatasetOrExpectationDecoratorResult`, not a function, and
the outer one then reaches for `__globals__` on it. **DEPLOY.md Phase 3 assumption 3,
refuted** — and note the blast radius: it is not one flow that fails but *every* flow, at
graph analysis, so the whole pipeline is down.

The fallback that entry names is taken exactly: **the expectations become a filter in the
query, and the quarantine flow stays the record of rejects.** One improvement on the
literal instruction — a new `_violation_expr(rules)` is the single definition of "this row
violates", and **both** flows read it, so the rows that land and the rows that are
quarantined are complements *by construction*. That is not tidiness: loop-1 asserts
`landed + quarantined = approved`, and two separately-maintained predicates are exactly how
that arithmetic goes quietly wrong. A row violates if the rule's SQL is FALSE **or NULL** —
an expectation that cannot be evaluated is not a pass — which also makes `failed` non-null
and `~failed` safe as the valid flow's predicate.

**Cost, recorded rather than hidden.** No expectation is declared to the runtime any more,
so **expectation metrics no longer appear in the pipeline event log**. The quarantine table
becomes the only record of what was rejected and why. DEPLOY.md's fallback anticipates this
("keep the quarantine flow as the record of rejects"), but it is a real loss of
observability and should be weighed if a future runtime supports the stacking.

### DEF-19 — `schema=` is the WHOLE schema, not a mask overlay (FIXED here — §2b decided)

DEF-16's fix is correct and necessary — `col TYPE MASK fn` is well-formed where `col MASK
fn` was not, and the pipeline now gets past parsing. It is not sufficient. The next update
failed with:

```
Table '02_usnc_silver_edm_dev.raw_vault.nhl_general_journal_line' has a user-specified
schema that is incompatible with the schema inferred from its query.

Declared schema:                    Inferred schema:
 |-- debitamt: decimal(18,2)         |-- debitamt: decimal(18,2)
 |-- crdtamnt: decimal(18,2)         |-- crdtamnt: decimal(18,2)
                                     |-- general_journal_line_hk: binary
                                     |-- load_dts: timestamp (nullable = false)
                                     ... 85 columns in total
```

**`create_streaming_table(schema=...)` does not accept a partial column list as an overlay
on the inferred schema.** Whatever is passed *is* the table's schema, and it must match
what the flows produce. So declaring a mask through `schema=` requires declaring **every
column** — all 85 of them, with exact types, order and nullability.

This is **exactly spec §2b's open gap**, and it is no longer hypothetical or deferred to
"whoever deploys the second lake": *"The generator cannot know a payload column's type:
types are inferred from the Bronze table at runtime."* It now blocks the first load of the
first lake.

The earlier ruling — take the type from the binding's `cast:` block — was right about the
masked columns and right that no guessing was needed for them. What it could not
anticipate is that the runtime demands the *other 83* as well.

**The options, and why none is a quick fix:**

| Option | What it costs |
|---|---|
| Derive the full schema at build time by calling `_stage()` and reading `df.schema` | Tractable — `spark` and Bronze are both available at build scope, and `.schema` resolves without scanning. But it must reproduce names, types, order **and nullability** exactly (`load_dts` is `nullable = false`), and reproduce the *union* across a table's several active bindings the way SDP computes it. Any mismatch is this same error; a mismatch that is accepted but wrong is a mis-typed insert-only table. |
| Declare payload types in metadata | §2b's first option. Every binding's full payload typed by hand; the metadata stops being a mapping and becomes a schema that can drift from Bronze silently. |
| Declare the table in SQL inside the pipeline | DEPLOY.md Phase 3 assumption 8's own fallback. Same problem: the DDL still needs all 85 columns. |
| Drop the mask from the streaming table and mask only `_v1` | **Refused.** It ships the base table unmasked, which Phase 3 assumption 8 forbids explicitly, and §1.2 proved an MV projection without its own mask is an unmasked copy. |

**Decided: derive the full schema from `_stage(...).schema`.** The alternatives were
rejected for reasons worth recording. Re-applying masks after the load leaves a window in
which the base table holds unmasked PII, and under parent decision D3 the column mask is
the vault's **only** PII defence — so a window is not a cost that can be paid. Deferring
masked tables loads nothing at all, since all three active journal entities carry
`mask_money`.

**The schema is READ, not reconstructed**, and that distinction is the safety property.
`_derived_schema_ddl` calls `_stage()` — the very function the flow calls — and renders
its `.schema`. Names, types, order and nullability are therefore what the flow produces
*by construction*, rather than agreeing with a second description that could drift. The
frame is lazy: `.schema` resolves at definition time without executing the query or
scanning a row.

Three details that had to be right, because they are written permanently into insert-only
tables:

- **Nullability.** `load_dts` is `nullable = False`; a declared schema that says otherwise
  is rejected. Rendered as `NOT NULL` from `field.nullable`.
- **The cast ordering.** `_stage` applies `cast:` *after* dedup and *before*
  `_system_columns`, so the staged schema already carries `DECIMAL(18,2)` for `debitamt`.
  Read it before the cast and the declaration would say `DOUBLE`, and
  `mask_money(v DECIMAL(18,2))` would not bind. Verified by reading `_stage`, not assumed.
- **Quoting.** GP delivers a column literally called `timestamp`. Identifiers are
  backtick-quoted.

**Multi-binding tables are unioned by column name, and disagreement RAISES** — never
widens, never picks one. A silent widening writes a type nobody chose into a table nobody
can alter. No table needs the union today (every masked table has exactly one active
binding; the multi-binding hubs carry no masks), but the rule is in place before it is
first needed rather than after.

**Both checks are kept.** `_mask_clauses` still asserts every masked column declares a
`cast:` type (DEF-16), and the derived schema then reads the type the flow actually
produces (DEF-19). The first is what would catch a masked column the second carried
through with whatever Bronze happened to supply. Two independent checks on one property is
correct where the cost of being wrong is permanent.

**One path is knowingly left incomplete.** `_emit_v1_view` still passes only the mask
clauses, which is the partial schema this defect is about. It is unreachable today — every
satellite is deferred (§3.1), so no `_v1` is emitted in this lake — and an MV's schema is
its projection's, not `_stage`'s, so deriving it needs the real thing to test against.
It is commented in place: **whoever re-activates satellites meets this first.**

### DEF-21 — a MASK function must be catalog-qualified (FIXED here)

With the full schema derived, the three masked flows — and only those three — failed:

```
[SCHEMA_NOT_FOUND] The schema `spark_catalog`.`governance` cannot be found
```

`metadata/entities/*.yml` declares masks two-part, as `governance.mask_money`, and that is
right: the catalog differs per lake and must not be baked into the model. But inside a
pipeline a two-part function name resolves against **`spark_catalog`**, not the pipeline's
own catalog, even though the pipeline sets `catalog:`.

The catalog now comes from `hfig.catalog`, which the bundle sets from `${var.catalog}` on
both pipelines — configuration rather than another runtime-API gamble, after DEF-17 showed
how many of those are refused. `_qualified_mask_fn` prefixes a two-part name and leaves a
three-part one alone. The catalog is backtick-quoted because these catalogs begin with a
digit (DEF-1). **The metadata stays lake-independent**, and a test asserts it: if a mask
were ever declared three-part in an entity file, that check fails.

### DEF-20 — the mask functions did not exist, and the job created them AFTER the load (FIXED by splitting; the grant half stays blocked)

This is where Task 8 stops, and it is not a code defect I can fix alone.

`02_usnc_silver_edm_dev.governance` contains **two functions, both probe residue**:

```
routine_catalog        | routine_schema | routine_name
02_usnc_silver_edm_dev | governance     | probe_mask
02_usnc_silver_edm_dev | governance     | probe_mask_priv
```

**`mask_money` has never been created.** A `MASK` clause naming a function that does not
exist cannot be declared, so the three masked journal tables cannot be created, so nothing
loads.

**The ordering in `resources/vault_job.yml` is inverted.** `apply_governance` — the task
that creates the mask functions — depends on `assert_append_only`, `reconcile_loop1` and
`assert_aggregate_reconciliation`, so it runs *after* `raw_vault`. But `raw_vault` cannot
define a masked table until the functions exist. DEPLOY.md Phase 6a states the premise
plainly — *"`governance/apply_masks.sql` creates the mask FUNCTIONS ... It applies no
masks"* — without drawing the consequence that function creation must therefore **precede**
any pipeline that declares one.

**Why the reordering is not mine to make.** `apply_governance` renders 16 statements, and
creating the four functions is only four of them:

| # | Statement | Concern |
|---|---|---|
| 1 | `ALTER CATALOG 02_usnc_silver_edm_dev SET ISOLATION MODE ISOLATED` | changes catalog-level visibility |
| 8, 11 | `REVOKE ALL PRIVILEGES ON SCHEMA ... FROM account users` | removes access for every principal |
| **14** | **`REVOKE ALL PRIVILEGES ON CATALOG 01_usnc_bronze_dev FROM account users`** | on the **shared Bronze catalog this bundle did not create** |
| 9,10,12,13,15,16 | `GRANT ... TO hfig_data_engineering` | the group's existence is **unverified** |

DEPLOY.md Phase 6a: *"Note that two are `REVOKE ALL PRIVILEGES ... FROM account users`.
**Confirm with the human before running this against a shared catalog.**"* Statement 14 is
exactly that case, and the failure mode is concrete: if `hfig_data_engineering` does not
exist at account level, the REVOKE succeeds and the GRANT fails, **leaving nobody with
access to Bronze**.

**Resolved by splitting, and the danger was confirmed rather than suspected.** Checking the
workspace: `hfig_data_engineering`, `hfig_analysts`, `hfig_commercials_reader` and
`hfig_worker_pii_reader` **do not exist**. This estate uses a different convention entirely
— `usnc_data_platform_bronze_layer_reader`, `us_tds_bi_builder`,
`usnc_data_custodian_fieldglass`, `pii_cleared_us` — and only the last is real. That is
README RECONCILE item #3, never done.

So `apply_masks.sql:138` would have run

```sql
REVOKE ALL PRIVILEGES ON CATALOG `01_usnc_bronze_dev` FROM `account users`;  -- succeeds
GRANT USE CATALOG ON CATALOG `01_usnc_bronze_dev` TO `hfig_data_engineering`;  -- fails
```

The REVOKE is unconditional and lands; the GRANT meant to restore access fails on a
nonexistent group. **Nobody could read Bronze**, on a catalog this repo does not own.

**The split.** A new `create_mask_functions` task runs `--functions-only` **before**
`raw_vault`. It emits `USE CATALOG`, `CREATE SCHEMA IF NOT EXISTS` and the four
`CREATE OR REPLACE FUNCTION` statements — six in total — and nothing else. Creating a
function touches no permission, so this half is safe to automate.

It is a **filter over the same rendered SQL, not a second file**: `apply_masks.sql` stays
the one definition of what a mask function is, and a function added there is picked up
without being added anywhere else. The selection is **allowlist-first and
denylist-verified** — the allowlist decides what is included, then the denylist re-reads
what was selected and refuses on `REVOKE`, `GRANT`, `ALTER CATALOG`, `SET MASK`,
`ALTER TABLE` or `DROP`. Selecting *no* `CREATE FUNCTION` is also refused, so the file
changing shape fails loudly rather than shipping tables whose `MASK` names nothing.

Both layers are tested, and separately: a standalone `GRANT` is never selected, and a
forbidden token hiding **inside** an allowlisted statement still raises — the case the
allowlist alone cannot catch. The whole value of the split is that one half *cannot* do
what the other half does, so that is asserted as a property rather than trusted as a habit.

**`apply_governance` keeps everything else, in its original position, blocked.** The group
mapping is a governance decision about who may read commercial and PII columns, and
separately there is a question of whether this repo should revoke on a shared catalog it
does not own at all. Neither was decided here, no group name was substituted, and no
revoke was commented out. The finding is recorded in `DEPLOY.md` beside Phase 6a, with the
four placeholders, the real groups and the specific consequence — the note exists to stop
someone running governance because it looked like the next step.

### DEF-22 — the mask function DDL has never been valid SQL (FIXED here)

With the split in place, `create_mask_functions` ran — the first time any part of
`apply_masks.sql` has ever executed — and the two setup statements succeeded while **all
four functions failed**:

```
  ok    1. USE CATALOG `02_usnc_silver_edm_dev`
  ok    2. CREATE SCHEMA IF NOT EXISTS governance
  FAIL  3. CREATE OR REPLACE FUNCTION governance.mask_tokenised_account(v STRING)
        [PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. (line 6, pos 0)
  FAIL  4..6  (the same, for the other three)
```

A SQL UDF's `RETURN` clause is **terminal**. `COMMENT` must precede it. All four were
written the other way round:

```sql
CREATE OR REPLACE FUNCTION governance.mask_money(v DECIMAL(18,2))
RETURN CASE ... END
COMMENT '...';          -- does not parse
```

So the mask functions could never have been created, by any route, since the file was
written. That is consistent with everything else observed: DEF-20 found the schema empty
of everything but probe residue, and this is why. **Nothing had ever run this file** — the
task that would have was positioned after the load it was a prerequisite for, so the two
defects concealed each other perfectly.

Fixed by swapping the clauses for all four. This is a pure syntax property, cheap to
assert offline, and now asserted per function — it should never have required a live
workspace to discover.

**A second defect surfaced in the same edit, and it is worth recording because it is a
property of the tooling rather than of the SQL.** `checks/apply_governance.py` splits the
file on `;` **before** stripping comments, so a semicolon inside a comment ends a
statement. That is harmless when the semicolon is the last character on the line — the
next chunk then begins with `--` and is discarded — and **not** harmless when prose
follows it on the same line, because that prose escapes the comment and becomes a bogus
statement. A comment I added mid-line did exactly that and silently swallowed the whole
first `CREATE FUNCTION` with it: the statement count dropped from 6 to 5 and one function
simply disappeared from the plan. `verify_repo.py` caught it (*"every statement split out
of apply_masks.sql begins with SQL"*), which is the check earning its place. Both the
clause order and the semicolon rule are now asserted in the offline suite.

**The group names remain placeholders and remain unresolved** — that is DEF-20's other
half, and creating a function does not depend on the group existing: membership is
evaluated per query, not at definition. A mask over a nonexistent group denies to
everyone, which fails safe.

### DEF-23 / DEF-24 — nothing in this model can be a clustering column (FIXED, with a recorded regression)

With the mask functions created, the pipeline created its tables — `nhl_journal_line` and
`nhl_general_journal_line` among them, the first masked vault tables that have ever
existed — and then failed updating `hub_ledger_account`:

```
[DELTA_CLUSTERING_COLUMNS_DATATYPE_NOT_SUPPORTED]
CLUSTER BY is not supported because the following column(s): ledger_account_hk : BINARY
```

`_cluster_by` clusters on join keys, and every one of them is a hash key. Hash keys are
**BINARY by the ratified rulebook** (`BINARY_OUTPUT = True`, chosen as *"smaller, faster
joins"*), so the columns the design most wants to cluster on are exactly the ones Delta
refuses.

DEPLOY.md Phase 3 assumption 1 is about whether `cluster_by` is *accepted as an argument*;
it is. Its documented fallback — move it to `ALTER TABLE ... CLUSTER BY` — does **not**
help, because the restriction is on the column's type, not on where the clause is written.

**Dropping the key and keeping `load_dts` was the obvious fix, and it failed too.** DEF-24:

```
[DELTA_CLUSTERING_COLUMN_MISSING_STATS] Liquid clustering requires clustering columns to
have stats. Couldn't find clustering column(s) 'load_dts' in stats schema
```

Delta collects statistics for the **first 32 columns** only. A GP journal table is **85
columns wide** and the system columns are appended last, so `load_dts` falls outside the
stats window on exactly the tables this project loads. Both candidate columns are
therefore unusable, for unrelated reasons, and `cluster_by` is dropped entirely — which is
the fallback DEPLOY.md names for assumption 1. The argument is **omitted**, not passed
empty.

**This is a real regression against the design's intent, recorded rather than papered
over.** Clustering is a layout hint: no gate reads it, no join depends on it, nothing about
what is stored changes. What is lost is read performance on hash-key joins — precisely the
access path a Data Vault is built around.

Two ways back, both decisions rather than fixes:

- Raise `delta.dataSkippingNumIndexedCols`, or move the system columns to the front of the
  projection, to bring `load_dts` into the stats window. A write-cost trade on every table.
- Cluster on the key itself, which needs `BINARY_OUTPUT = False` — a rulebook change, with
  a `RULEBOOK_VERSION` bump and every stored key rewritten.

Asserted offline in both directions: no entity clusters on a `_hk` column while keys are
BINARY, and dropping the key must not also drop `load_dts`.

### DEF-25 — the ghost flow never supplied `cdc_op`, and a declared schema made that fatal (FIXED here)

The pipeline reached the flows, ran them, and then the ghost flows for the two masked
journal tables failed — repeatedly, until the update gave up:

```
[DELTA_MISSING_NOT_NULL_COLUMN_VALUE] Column cdc_op, which has a NOT NULL constraint,
is missing from the data being written into the table
```

`factory._system_columns` writes `cdc_op` onto every row of every table, building it from
`F.lit("I")` when the binding declares no CDC column — **a non-nullable literal**. But
`naming.SYSTEM_COLUMNS`, the declared inventory of "columns present on every generated
vault table", omitted it, and so did `_emit_ghost`. One row in every table lacked a column
all the others had.

**That stayed invisible for exactly as long as schemas were inferred.** SDP infers a
table's schema from the union of its flows, and the union of *"the source flow supplies a
non-nullable cdc_op"* and *"the ghost flow supplies nothing"* is simply a **nullable**
column — so the ghost row landed with `NULL` and nobody noticed. The moment a masked table
**declares** its schema (DEF-19), the `NOT NULL` becomes a real constraint and the ghost
flow cannot satisfy it.

**Fixed by the ghost supplying the column, not by weakening the schema to nullable.** A
ghost *is* an inserted row: `'I'` is exactly what it means, it is what `_stage` already
defaults to for a source with no CDC column, and it removes the asymmetry rather than
papering over it. `cdc_op` is also added to `SYSTEM_COLUMNS`, so the inventory now matches
what is actually written.

**Consequence for §2b, recorded because it changes a stated number.** A ghost-only table is
now **eight** columns wide, not seven — the hash key plus seven system columns.
`DEPLOY.md` and §2b are updated. This does not change the D5 argument at all; the inventory
guarantee still holds and the layout guarantee still does not.

**The general lesson, which is the reason this is worth a defect number.** Declaring a
schema converts every previously-cosmetic inconsistency between flows into a hard failure.
Inference is forgiving in a way that hides exactly this class of bug, and the forgiveness
disappears the moment one table needs a mask. The offline suite now asserts the general
property — the ghost flow emits **every** column in `SYSTEM_COLUMNS` — rather than just
the one instance that happened to bite.

### DEF-26 — the append-only gate could not see a single vault table (FIXED here)

The load succeeded and `assert_append_only` reported:

```
FAIL no vault tables found in 02_usnc_silver_edm_dev.raw_vault
```

with 30 vault tables sitting in that schema. `vault_tables()` filtered
`information_schema.tables` on `table_type = 'MANAGED'`, and **every vault object is a
`STREAMING_TABLE`**. The only `MANAGED` rows in the schema are SDP's internal
`__materialization_mat_<pipeline-id>_<table>_1` tables, whose names carry no vault prefix
and are filtered out by the next clause.

So **the sharpest assertion in this repo — NHL uniqueness, the one real test of the
staging deduplication — could never have seen a table**, in any lake, ever.

It failed loudly only because of the explicit `if not tables:` guard. **Without that guard
it would have exited 0 having asserted nothing** — a gate reporting success over an empty
list. That is precisely the pathology this sub-project exists to prevent, and it was one
`if` away from happening.

Fixed by accepting `MANAGED` and `STREAMING_TABLE`. The guard stays, and is now asserted
by a test in its own right.

### DEF-27 — the uniqueness grain was an arbitrary hash key (FIXED here)

With the gate finally able to see the tables, it reported five failures — three of which
were not defects at all:

```
* nhl_general_journal_line: 794888 duplicate rows at grain (accounting_journal_hk)
* nhl_general_journal_line_closed_year: 1964403 duplicate rows at grain (accounting_journal_hk)
* nhl_journal_line: 49 duplicate rows at grain (ledger_account_hk)
```

Those grains are **parent** keys. An NHL has many lines per journal and many lines per
ledger account — that is its defining shape, not a duplicate. The check did

```python
hk = next((c for c in cols if c.endswith("_hk")), None)
```

over `cols`, which is a **set**. It took an arbitrary hash key, non-deterministically: a
hub carries exactly one so it was right there by luck, while an NHL or link carries its own
key *and* one per parent, and the check landed on a parent.

The grain is now **derived from the table name** — `nhl_journal_line` →
`journal_line_hk` — which is `naming.hk()` of the entity the table belongs to, and it
errors if that column is absent rather than silently substituting another.

**With the correct grain, all three NHLs pass.** Confirmed independently in SQL before
the fix, so the gate was checked against the data rather than trusted:

| Table | Rows | Distinct own hash key |
|---|---|---|
| `nhl_general_journal_line` | 2,453,132 | **2,453,132** |
| `nhl_general_journal_line_closed_year` | 6,086,493 | **6,086,493** |
| `nhl_journal_line` | 261 | **261** |

**The staging deduplication works, on genuinely re-delivered data.** `gl20000` carries
~1.81 copies of every row across 7 file deliveries and `ukg_raw.gl` exactly 2 across 2, and
each NHL holds exactly one row per key. This is the assertion the whole sub-project was
built to reach.

### DEF-28 — hubs are not deduplicated (OPEN — a design decision, and partly the Task 1 problem again)

With NHLs passing, two real failures remain, both hubs:

```
* hub_accounting_journal: 538186 duplicate rows at grain (accounting_journal_hk)
* hub_organisation:           11 duplicate rows at grain (organisation_hk)
```

A hub's defining property is **one row per business key**. `_register_source_flows`
applies the dedup guard only to NHLs:

```python
if entity.kind == "nhl":
    df = df.dropDuplicates([entity.hk_column])
```

The duplication has **two distinct causes, and only one of them is fixable in the
pipeline**:

**Within a flow.** GP's `gl20000` holds many lines per journal, so one hub row is emitted
per *line*: 2,759,294 rows for 2,221,108 distinct journals. A `dropDuplicates` inside the
flow — exactly what NHLs already do — collapses this.

**Across flows, and this is the harder half.** `hub_organisation` holds 24 rows for 12
keys: two bindings (`GP_US` and `GP_US_HIST`) each emit the same twelve organisations, one
row each. No in-flow dedup can remove that, because the two flows never see each other —
and a flow **cannot read its own target** to check what is already there. That is precisely
what Task 1 proved impossible (§1.1, `UnresolvedDatasetException`), now arriving for hubs
rather than satellites.

**Why it is not fixed here.** The within-flow half is a small change, but it does not make
the gate pass, and shipping half a fix would leave the gate red for a reason nobody could
read off the output. The cross-flow half needs the same answer as satellites: a separate
job task outside the pipeline, or Bronze-side deduplication. That is a design decision
about how multi-source hubs load, and it belongs with the CDF/fallback conversation in §3
rather than being taken at speed.

**Not relaxed.** The gate is correct and its failure is information. `hub_ledger_account`
passes (1,906,877 rows, 1,906,877 keys) because its key includes `actindx`, which is
already distinct per surviving row — so the gate is discriminating, not simply red
everywhere.

### Two workspace prerequisites are absent

`02_usnc_silver_edm_dev.governance` holds **zero tables**. Neither
`ctl_approval_manifest` (which `loop1_reconciliation.py` reads unconditionally, and which
§5d of DEPLOY.md says this repo must never create) nor `ref_dq_expectation` (the
`expectations_table` the pipelines are configured with) exists. `reconcile_loop1` is
therefore blocked on a platform-owned prerequisite independently of gate zero. That is a
conversation with the platform team, not a repo change.

All four active Bronze sources exist and are readable —
`great_plains_raw.{gl00100,gl20000,gl30000}` and `ukg_raw.gl` — so nothing else stands
between this bundle and a first load.

### Status of §5's definition of done

`preflight_target` passes; `bundle validate` is clean apart from DEF-43; **the hub and NHL
entities are loaded into `02_usnc_silver_edm_dev.raw_vault`** — ~13.2M rows, the first data
this vault has ever held. Of the four gates §5 requires to have *executed*:

| Gate | Status |
|---|---|
| `hash_parity_check.py` | **PASSED** — 7 keys, 7 hashdiffs, including the schema-evolution assertion |
| `append_only_check.py` | **EXECUTED, FAILED on hubs.** NHL uniqueness **passes** on all three journals — the dedup assertion this sub-project exists to make. The failure is DEF-28, and it is real |
| `loop1_reconciliation.py` | **BLOCKED** — `ctl_approval_manifest` does not exist. A platform prerequisite, not a code defect |
| `journal_integrity_check.py` | **EXECUTED, FAILED and correctly refused to assert** — every amount column reads NULL through `mask_money`, whose privileged group does not exist (DEF-20's open half). The stored data is intact; see below |

**The masked amounts are NOT lost.** The gate reads through the mask and sees NULL, and it
rightly refuses to assert balance on zeros — but the values are stored correctly. Verified
by comparing the streaming table against its own materialization:

| Read path | Rows | Non-null `debitamt` |
|---|---|---|
| `nhl_general_journal_line` (mask applies) | 2,453,132 | **0** |
| its internal `__materialization_…` table | 2,453,132 | **2,453,131** |

Every row but the ghost carries its amount. The mask is doing exactly what §1.2 proved a
mask does on a *streaming table* — denying at read time, storing nothing — and because
`hfig_commercials_reader` does not exist, `is_account_group_member` is false for everyone
including the gate's own identity. **That is fail-safe behaviour, not corruption.** Note
that the gate's own diagnosis names the refresh-as-owner trap, which is the *materialized
view* failure mode (§1.2) and is wrong here; its message should distinguish "stored
masked" from "masked on read".

§5 is therefore **substantially met**: the vault is loaded, gate zero passes, and the two
gates that could run both executed and produced real verdicts. Two gates are blocked on
things outside this repo (the manifest, the group mapping), and one genuine modelling
defect is open (DEF-28).

---

## 6. What 3b inherits

The VMS domain when BRZ-2 and BRZ-4 land, the N-table binding capability, and **every
satellite in the model** — which now waits on CDF or on the separate-job-task fallback
rather than on a source.

---

## 11. CRITICAL, RESOLVED (DEF-26) — hubs carried the whole source row, defeating the mask control

Found by the final whole-branch review, verified against loaded data on 25 Aug 2026.

`src/accelerator/factory.py`'s `_stage` never projects a hub down to its key. The entire
staged Bronze frame is appended, so:

```
hub_accounting_journal    92 columns     hub_ledger_account   62 columns
hub_organisation          92 columns     hub_pay_period       22 columns
```

`README.md:176` lists **"a hub carrying descriptive attributes"** under what validation
refuses to build. That rule is enforced on the *declaration* — an entity cannot declare a
payload on a hub — and broken in the *implementation*.

**The mask control is bypassed as a direct consequence.** `_emit_target` applies MASK
clauses only to entities that declare masks; a hub declares none, because a hub is
supposed to hold only keys. So the same column is masked on one table and clear on
another:

```
nhl_general_journal_line   2,453,132 rows            0 readable debitamt
hub_accounting_journal     2,759,294 rows    2,759,292 readable debitamt
```

`_emit_quarantine` has the same gap: it passes no `schema=`, so no MASK clause, while
being fed the identical staged frame.

**`checks/mask_survival_check.py` structurally cannot detect this.** It walks only
entities that *declare* masks, plus their `_v1` projections. It never asks whether a
masked column *name* appears unmasked elsewhere in the schema, so it reports PASSED over
a total bypass — of the control `factory.py` itself calls the vault's only PII defence.
This is the fourth check found on this branch whose condition cannot fail.

Today the exposure is `sensitivity: financial` (commercial amounts), not personal data.
It becomes personal the moment a worker-bearing entity loads.

**RESOLVED 25 Aug 2026 — both fixes are in, offline. The reload is not.**

1. `factory._projection` / `_project` narrow every entity to its declared model, per kind,
   and the flows stage the full row (expectations may name any source column), filter,
   then project. `_emit_quarantine` now declares a schema, so the twin carries the same
   MASK clauses and is its target plus exactly `failure_rule` and `failure_detail`. The
   ghost is built from the same declared field list. `hub_accounting_journal` 92 -> 13
   columns; `nhl_general_journal_line` 83 -> 26.
2. `mask_survival_check.unmasked_elsewhere` asserts, catalogue-wide, that any column name
   declared masked anywhere carries a mask on every object in the vault schema where it
   appears. Proven non-vacuous against the live `raw_vault`: 30 problems across 10 objects.
   It also found `nhl_payroll_detail.rate` unmasked on a `restricted` entity while
   `nhl_timesheet_line` masked the same name.

THE LOADED TABLES ARE STILL WRONG. This changes the schema of insert-only tables, so it
needs a deliberate drop-and-reload, authorised separately. See
`.superpowers/hub-projection-report.md`.

**Two fixes, both needed:**

1. Project hubs and links to hash key, business key columns and system columns only — or
   carry the MASK onto every table that receives a masked column, quarantine twins
   included.
2. Add a catalogue-wide assertion to `mask_survival_check.py`: any column *name*
   declared masked anywhere must carry a mask on every table where it appears.

The second matters more than the first. The first is a bug; the second is why nothing
caught it.
