# Keyed-kind staging and anti-join — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every keyed kind (`nhl`, `hal`, `link`) a staging log and the anti-join loader that hubs already use, so a re-read of a source cannot duplicate vault rows.

**Architecture:** `naming.STAGED_KINDS` widens to `KEYED_KINDS | SATELLITE_KINDS`. Because `naming.pipeline_table()` is the single switch every emitter routes through, the flows re-target themselves to `stg_*` logs with no emitter change. `load_hubs.py` widens its kind set to `KEYED_KINDS` and gains no new SQL — its `row_number()=1 AND NOT EXISTS` is already the right shape for any one-row-per-hash-key table. Eleven entities then need a new physical version, because a UC streaming table cannot be `INSERT`ed into by a job task.

**Tech Stack:** Python 3.11 and 3.13, PySpark on Databricks serverless, Unity Catalog, Lakeflow Declarative Pipelines, Databricks Asset Bundles.

**Spec:** `docs/superpowers/specs/2026-09-28-keyed-kind-staging-design.md` (branch `spec/keyed-kind-staging`)

## Preconditions

1. **`fix/stream-generation` must be merged into `main` first.** Measured 29 September:
   `main` declares `nhl_invoice_line` at `version: 2`, the branch declares `version: 3`,
   and the workspace serves `nhl_invoice_line_rev3`. Deploying `usnc_tds` from an
   unmerged `main` rebuilds the `_rev2` that was dropped on 28 September. This plan's
   baseline is `main` **after** that merge.
2. The spec's migration table is stale in one row and is corrected in Task 6 below:
   `nhl_invoice_line` is now at `v2` on main / `v3` on the branch, not the dirty `v2` the
   spec describes.
3. **Line numbers below are post-merge.** `src/accelerator/spec.py` shifts by roughly 84
   lines when `fix/stream-generation` lands, because that branch adds
   `SourceBinding.stream_generation` and `BINDING_KEYS`. Cited as `spec.py` ~373
   (`change_detection` field), ~949 (the keyed multi-source refusal) and ~976 (its
   validation); on today's unmerged `main` the same three sit at 321, 865 and 892. Every
   citation is a search anchor, not an address — find the symbol, not the line.
   `naming.py:397`, `load_hubs.py:125` and `:164`, and `publish_stable_views.py:75` are
   unaffected by the merge.

## Global Constraints

- **No check code in this plan.** Plan-authored `check()` code has failed 5 times out of 5
  in this repo — f-strings split across adjacent literals, helpers that do not exist. Every
  test step below names the assertion and the mutation that must fail it; the implementer
  writes the code against the live file.
- **The test idiom is NOT pytest.** `check(name, condition, detail)` in
  `tests/test_accelerator.py`. New checks go BEFORE the `if FAILURES:` summary block.
- **An exception inside a check's CONDITION aborts the whole suite.** Compute into a
  sentinel inside `try/except` first, then compare in the condition. The sentinel must be a
  value no expected result can equal.
- **A mutation that "exits 1" is not a proof.** Every new check must be proven by a mutation
  that produces `PASS + FAIL == the full check count` AND prints the summary block. An abort
  also exits 1.
- **Choose each mutation from the failure feared, not from the check's own logic.**
- **Never write a check count down.** Counts change; assert relationships, not totals.
- **Both Python legs must pass.** `uv run --frozen --python 3.11 python tests/test_accelerator.py`
  and `uv run --python 3.13 --with pyyaml --with jsonschema --with referencing python tests/test_accelerator.py`.
  Revert any `uv.lock` drift before committing.
- **`delta.appendOnly = true`** is set by `factory._properties()` on every vault table. No
  column may be renamed or narrowed in place; a shape change is a new `_rev<N>`.
- **Hash values must not move.** `key_scope_of()` returns the binding name, never the entity
  name or kind, so staging a kind must not change any key. Asserted in Task 1.
- **`RULEBOOK_VERSION`** must be bumped in the same commit as any change to a RATIFIED
  constant in `hashing.py`. This plan touches none.

## Review Focus

The five failure modes the spec implies that no single task's tests would otherwise exercise:

1. **A derived scope silently empties.** `GENERATABLE - STAGED_KINDS` becomes `∅`, so
   `publish_stable_views.PIPELINE_OWNED_KINDS` empties and the gate passes having published
   nothing. Owned by Task 4.
2. **A staged kind that no loader claims.** `STAGED_KINDS` minus every loader's kind set must
   be empty. Widening one without the other leaves a kind staged and never loaded — its log
   fills and its vault table stays empty, under a green build. Owned by Task 2.
3. **The `row_number()=1` winner is not deterministic.** Two rows sharing
   `(hash key, load_dts, rec_src)` with different payload make the survivor arbitrary, so two
   runs of the same log can land different data. Owned by Task 3.
4. **A hash key moves.** If staging changed any key, every existing child's parent key stops
   joining and the vault fans out silently rather than failing. Owned by Task 1.
5. **A quarantine twin is left unstaged while its target is staged**, so loop-1's
   `landed + (quarantined − superseded) = approved` compares a staged count against an
   unstaged one. Owned by Task 2.

---

### Task 1: Widen the kind sets, and prove no key moved

**Files:**
- Modify: `src/accelerator/naming.py` (the `STAGED_KINDS` definition, currently `frozenset({"hub"}) | SATELLITE_KINDS`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `naming.KEYED_KINDS` = `{hub, link, nhl, hal}`, `naming.SATELLITE_KINDS`, `naming.GENERATABLE`
- Produces: `naming.STAGED_KINDS` == `KEYED_KINDS | SATELLITE_KINDS`, consumed by
  `naming.pipeline_table()`, `spec.py`'s keyed-kind refusal, `publish_stable_views`,
  `apply_governance`, `mask_survival_check`, and `load_hubs.HUB_LOADER_KINDS`.

- [ ] **Step 1: Record the baseline keys before changing anything**

Run the model offline and write, for every entity and binding, the hash-key expression
`spec.hash_key_columns()` produces, to a scratch file. This is the evidence Step 4 compares
against. Do not skip it — after the change there is nothing left to compare to.

- [ ] **Step 2: Widen the definition**

In `src/accelerator/naming.py`, change `STAGED_KINDS` to `KEYED_KINDS | SATELLITE_KINDS`.
Update the comment above it, which currently explains why `SATELLITE_KINDS` is kept as its
own name, to also say why every keyed kind is staged: a keyed table needs one row per hash
key, and a streaming flow cannot check whether the key already landed.

- [ ] **Step 3: Run the suite and read what breaks**

Run: `uv run --frozen --python 3.11 python tests/test_accelerator.py`
Expected: FAILURES, not an abort. Existing checks assert the old membership. Record the
list; each one is either a check to update (it pinned the old set) or a real defect.

- [ ] **Step 4: Add the key-invariance check**

Assert: for every entity and every binding, the hash-key column expression is byte-identical
to the Step 1 baseline.
Detail must name the first entity/binding that moved.
**Mutation that must fail it:** make `key_scope_of()` return the entity kind instead of the
binding name. If the check still passes, it is not reading what it claims to.

- [ ] **Step 5: Add the partition checks**

Assert both halves, because "no overlap" alone passes on two empty sets:
(a) `STAGED_KINDS` is exactly `KEYED_KINDS | SATELLITE_KINDS`;
(b) every kind in `GENERATABLE` is in `STAGED_KINDS` — i.e. `GENERATABLE - STAGED_KINDS` is
empty, stated as the fact Task 4 then has to defend rather than as an accident.
**Mutations:** drop `hal` from `KEYED_KINDS`; add a fictional kind to `GENERATABLE`.

- [ ] **Step 6: Both Python legs green, then commit**

```bash
git add src/accelerator/naming.py tests/test_accelerator.py
git commit -m "feat(naming): every keyed kind is staged"
```

---

### Task 2: Give the keyed loader its new kinds

**Files:**
- Modify: `checks/load_hubs.py` (`HUB_LOADER_KINDS` at ~line 125; `staged_entities()`; the module docstring; the `GATE NOT EVALUATED` message at ~line 164)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `naming.STAGED_KINDS`, `naming.SATELLITE_KINDS` from Task 1
- Produces: `HUB_LOADER_KINDS` == `naming.KEYED_KINDS`; `staged_entities(model)` returns hubs, links, NHLs and HALs in `base_table` order

- [ ] **Step 1: Widen the loader's kind set**

`HUB_LOADER_KINDS` is currently `naming.STAGED_KINDS - naming.SATELLITE_KINDS`, which
already evaluates to `KEYED_KINDS` after Task 1. Make that explicit — set it to
`naming.KEYED_KINDS` — and update the `staged_entities()` docstring, which says "Hubs only".
The subtraction form was chosen so a new staged kind "is forced to declare which loader owns
it"; that decision is being made now, and the code should say so rather than inherit it.

- [ ] **Step 2: Update the messages that say "hub"**

The `GATE NOT EVALUATED` message reads "no staged hub in the model". It now covers four
kinds. The module docstring opens "THE HUB LOADER: staging log -> vault table, by
anti-join" — the mechanism is unchanged, the scope is not.

- [ ] **Step 3: Add the loader-coverage check**

Assert: `STAGED_KINDS` minus the union of every loader's kind set is empty — that is,
`HUB_LOADER_KINDS | load_satellites`' kind set covers `STAGED_KINDS` exactly. Derive both
from the modules, never from a hand-typed list.
**Mutation that must fail it:** widen `STAGED_KINDS` with a fictional kind and leave both
loaders alone. This is Review Focus 2: a staged kind nobody loads fills a log and leaves its
vault table empty under a green build.

- [ ] **Step 4: Add the quarantine-twin check**

Assert: for every active binding on a staged kind, the quarantine twin is staged on the same
side as its target — both `stg_` or neither.
**Mutation:** make `naming.pipeline_table()` return the unstaged name for `qtn_` tables only.
This is Review Focus 5.

- [ ] **Step 5: Both Python legs green, then commit**

```bash
git add checks/load_hubs.py tests/test_accelerator.py
git commit -m "feat(loader): the keyed loader owns links, NHLs and HALs"
```

---

### Task 3: Settle the `row_number()` tie-break by measurement, not invention

**Files:**
- Modify: `checks/load_hubs.py` (the INSERT builder, and a new pre-insert assertion)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `staged_entities()` from Task 2
- Produces: a refusal function, pure and Spark-free, that decides whether a staging log's
  `(hash key, load_dts, rec_src)` grain is ambiguous — the shape `classify_control_table()`
  and `retire_refusal()` already use, so it can be exercised without a session.

**This task resolves the spec's open question 1.** The plan deliberately does NOT invent a
tie-break column. A hub's log holds one row per key per source per delivery; an NHL's will
hold one per transaction, and nobody has measured whether two rows can share the ordering
triple with different payload. Inventing an ordering to cover a case that may not exist adds
an untested branch; asserting the case does not arise turns an unknown into either a proven
non-issue or a loud failure.

- [ ] **Step 1: Measure the real logs first**

Against `usnc_tds`, for each existing `stg_` log, count rows sharing
`(<entity>_hk, load_dts, rec_src)`. Record the counts. If any log has such a group, this
task's design changes and the finding goes to Adrian before Step 2 — that is a plan
deviation to raise, not to absorb.

- [ ] **Step 2: Add the ambiguity refusal to the loader**

Before the INSERT, for each entity, fail the task when a staging log holds more than one row
at `(hash key, load_dts, rec_src)` **with differing payload**. Identical duplicates are fine
— they are what the log is for and the anti-join removes them. The message must name the
entity, the key and the count, and say that the loader's choice of survivor would otherwise
be arbitrary.

- [ ] **Step 3: Add checks for the refusal function**

Assert: the pure function returns a refusal for an ambiguous group and "" for an unambiguous
one, exercised on fabricated inputs so no session is needed.
**Mutation that must fail it:** make the function return "" unconditionally. A refusal that
never fires reads as protection and is not.

- [ ] **Step 4: Both Python legs green, then commit**

```bash
git add checks/load_hubs.py tests/test_accelerator.py
git commit -m "fix(loader): refuse an ambiguous staging grain rather than pick arbitrarily"
```

---

### Task 4: `publish_stable_views` must fail closed on an empty scope

**Files:**
- Modify: `checks/publish_stable_views.py` (`PIPELINE_OWNED_KINDS` at ~line 75, and `main()`)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `naming.GENERATABLE`, `naming.STAGED_KINDS`
- Produces: a non-zero exit when the computed scope is empty **and** the model declares
  entities that a loader should have published

`PIPELINE_OWNED_KINDS = frozenset(naming.GENERATABLE) - naming.STAGED_KINDS` becomes empty
after Task 1. That is semantically correct — every kind's stable view is published by its
loader — and it is exactly how a gate in this repo goes vacuous. This is Review Focus 1.

- [ ] **Step 1: Make the empty scope explicit rather than incidental**

In `main()`, when `PIPELINE_OWNED_KINDS` is empty, print what that means — no kind is
pipeline-owned, every stable view is a loader's responsibility — and return non-zero unless
the loaders are confirmed to have published. Do not simply pass: the caller believed this
gate was asserting something.

- [ ] **Step 2: Add the fail-closed check**

Assert: with an empty scope, the gate's decision function reports a problem rather than
success.
**Mutation that must fail it:** make the empty-scope branch return 0. This is the exact
defect the spec warns about, so the mutation is the defect itself.

- [ ] **Step 3: Both Python legs green, then commit**

```bash
git add checks/publish_stable_views.py tests/test_accelerator.py
git commit -m "fix(views): an empty publish scope is a defect, not a pass"
```

---

### Task 5: Retire the dead `antijoin` mode and the `stream_generation` warning

**Files:**
- Modify: `src/accelerator/spec.py` (`change_detection` field ~line 373, its validation ~line 976, and the `stream_generation` docstring's `DO NOT BUMP` paragraph)
- Modify: `src/accelerator/factory.py` (the module docstring's point 3, which describes `antijoin`)
- Modify: `docs/superpowers/OPEN_ITEMS.md`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing new
- Produces: `change_detection` accepting `cdc` only

- [ ] **Step 1: Remove `antijoin`**

`change_detection: "antijoin"` is accepted by validation and implemented nowhere — no entity
declares it, and its only behavioural effect is that `changed_only` stays False. Its own
description flags the flow shape as unverified and names `load_hubs.py` as the fallback,
which is what this plan makes universal. Remove the value from the accepted set and from
`factory.py`'s description, leaving a note that says where the anti-join now lives.

- [ ] **Step 2: Delete the `DO NOT BUMP AN NHL BINDING` paragraph**

At `SourceBinding.stream_generation`. It is true only while NHLs are unstaged. Replace it
with one sentence recording that a bump now re-reads into the staging log and the loader's
anti-join removes the duplicates — with the date, so the reason survives.

- [ ] **Step 3: Remove the multi-source refusal that can no longer fire**

`spec.py` ~line 949 refuses a keyed kind that is `not in STAGED_KINDS` and declares more
than one source. After Task 1 the first condition is never true for a keyed kind, so the
refusal is unreachable — and its message names the fix as *"this kind must be added to
naming.STAGED_KINDS and given a loader in checks/load_hubs.py"*, which is exactly what this
plan does. A refusal that cannot fire reads as protection and is not.

Remove it, and record in its place that a multi-source link, NHL or HAL is now buildable
because the loader anti-joins the target — the benefit the spec claims, made real here.

Assert: an entity declaring a keyed kind with two sources loads without a `SpecError`.
**Mutation that must fail it:** keep the old refusal in place. If the check still passes,
it is not exercising the path that changed.

- [ ] **Step 4: Close the OPEN_ITEMS entry**

Mark the NHL-idempotence entry resolved, naming the commit. Leave the `test_accelerator.py`
line-1524 suite-abort entry open — it is a separate defect and this plan does not fix it.

- [ ] **Step 5: Add the antijoin refusal check**

Assert: a model declaring `change_detection: antijoin` is refused with a message naming the
replacement.
**Mutation:** accept any string. Note the `_raises` hazard — two definitions exist in
`tests/test_accelerator.py` and the later one returns False for anything that is not a
`ValueError`, so a `SpecError` refusal written with it reads as "did not raise" and passes
while asserting nothing. Use a locally-named helper.

- [ ] **Step 6: Both Python legs green, then commit**

```bash
git add src/accelerator/spec.py src/accelerator/factory.py docs/superpowers/OPEN_ITEMS.md tests/test_accelerator.py
git commit -m "chore(spec): retire the unimplemented antijoin mode"
```

---

### Task 6: Version bumps and the migration runbook

**Files:**
- Modify: 11 entity files under `metadata/entities/` (see table)
- Create: `docs/superpowers/plans/2026-09-29-keyed-kind-staging-runbook.md`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: everything above
- Produces: 11 entities at a new `version:`, and a runbook an operator follows per entity

Every one of these becomes a batch-loaded table where it was an SDP streaming table, and a
streaming table cannot be `INSERT`ed into by a job task. Versions measured on `main` on
29 September, **assuming `fix/stream-generation` is merged first** (which takes
`nhl_invoice_line` to 3, hence 3 → 4):

| entity | from | to |
|---|---|---|
| `nhl_invoice_line` | 3 | 4 |
| `nhl_general_journal_line`, `nhl_general_journal_line_closed_year`, `nhl_journal_line`, `nhl_payroll_detail`, `nhl_timesheet_line` | 1 | 2 |
| `hal_client_legal_entity_hierarchy`, `hal_consolidation_hierarchy` | 1 | 2 |
| `lnk_client_contracting_entity`, `lnk_client_job_request`, `lnk_legal_entity_consolidation` | 1 | 2 |

- [ ] **Step 1: Bump the versions**

One `version:` per file, each with a comment saying this is a materialisation change and not
a shape change — the columns are identical, the table is now loaded by anti-join rather than
written by a streaming flow.

- [ ] **Step 2: Update the checks that pin a version**

At least two exist for `nhl_invoice_line` (`tests/test_accelerator.py`, the "declares
version" and "physical table is" pair). Find every pinned version across the 11 and update
it. Do not delete these checks — they are what makes a version change deliberate.

- [ ] **Step 3: Write the runbook**

Per entity, in order: deploy → `vault_load` → measure the new table's row count and distinct
hash-key count against the old → `cutover_vault_version.py` dry run → `--apply` →
**`grant_vault_access`** → `retire_vault_version.py` on the predecessor.

Record the two findings from 28 September that a naive runbook gets wrong:
- **Replacing a stable view drops its grants.** The view is unreadable until
  `grant_vault_access` runs, and `information_schema` reports it as having zero columns
  rather than as forbidden, so it looks like an empty table rather than a permissions
  problem.
- **Retirement and cutover need the owning service principal.** The human identity gets
  `PERMISSION_DENIED` on `MANAGE`. Both tools default to a dry run that prints the decision;
  run that first, every time.

- [ ] **Step 4: Add the version-consistency check**

Assert: no entity in the model is at a version whose physical table the stable view does not
name — that is, the declared version and the live view agree, or the entity is explicitly
listed as mid-migration.
**Mutation:** bump one entity's version without updating its expected physical name.

- [ ] **Step 5: Both Python legs green, then commit**

```bash
git add metadata/entities/ tests/test_accelerator.py docs/superpowers/plans/2026-09-29-keyed-kind-staging-runbook.md
git commit -m "feat(vault): version the eleven keyed entities for anti-join loading"
```

---

## Not in this plan

- **The migration's execution.** Task 6 produces the runbook; running it against
  `usnc_tds` is an operator action with eleven cutovers and eleven retirements, each needing
  the service principal.
- **`spec/client-operating-company-split`**, which is sequenced after this and re-versions
  seven of the same entities again. Worth deciding whether the two migrations combine into
  one before Task 6 is executed — doing them separately means re-versioning six NHLs twice.
- **The `test_accelerator.py` line-1524 suite abort**, recorded in `OPEN_ITEMS.md`.
- **Staging-log sizing.** `nhl_general_journal_line` is 2.45M rows and its log will hold at
  least that, append-only and never pruned. The spec flags this as unaddressed; it should be
  answered before the journal domain migrates, and it is a question for Adrian rather than a
  task here.
