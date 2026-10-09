# Source Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate `source_contracts/<target>.yaml` from the model, stating exactly what Silver requires of each Bronze table it reads, so Bronze can only break us knowingly.

**Architecture:** A new emitter, `tools/emit_source_contract.py`, mirroring `tools/emit_data_contract.py`: pure functions producing a dict, `yaml.safe_dump` to a committed file, and a `verify_repo` gate asserting regeneration is a no-op. It reuses `emit_data_contract.targets_and_variables()` and `spec.active_table_bindings()` rather than restating either. No workspace access, no clock read.

**Tech Stack:** Python 3.11 floor, PyYAML, the repo's `check(name, condition, detail)` test idiom (NOT pytest).

**Spec:** `docs/superpowers/specs/2026-08-28-source-contract-design.md`

## Global Constraints

* **Python 3.11 is the floor.** No backslash inside an f-string expression part; no assignment expression in a comprehension's iterable. CI runs 3.11 and 3.13 and has been red on both of these.
* **Run suites with `/mnt/projects/hda-edm-dv/.venv/bin/python`** (3.13). Reproduce the 3.11 leg with `uv run --frozen --python 3.11 python <suite>`.
* **Suites are `python verify_repo.py` (813 checks at plan time) and `python tests/test_accelerator.py`.** NOT pytest. Both use `check(name, condition, detail)`.
* **A check that cannot fail is a defect, not a nit.** Every check added must be mutation-proven: break the thing it guards, observe a named FAIL, restore. Derive the mutation from the check's own prose, not from the plan text.
* **Commit before mutation-testing. Never `git checkout` to undo a mutation** — copy the file aside and copy it back. `git checkout` has destroyed uncommitted work in this project twice.
* **No workspace access.** Do not run any `databricks` command, do not deploy, do not query a lake. Only one of eight profiles is non-production and choosing it is the human's decision.
* **`yaml.safe_dump(..., sort_keys=True, default_flow_style=False, allow_unicode=True)`** — matching `emit_data_contract.render()`. `allow_unicode=True` is required: without it an em dash publishes as `—`.
* **The model is the sole authority.** `metadata/entities/*.yml`. The emitter never hard-codes a column, table, or target name that the model or `databricks.yml` already states.

---

### Task 1: Per-binding requirements, by role

**Files:**
- Create: `tools/emit_source_contract.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `accelerator.spec.Entity`, `accelerator.spec.SourceBinding`.
- Produces: `binding_requirements(entity, src) -> dict` with keys `business_keys`,
  `parent_keys`, `transaction_key`, `payload`, `applied_dts`, `cdc_op`, `manifest`,
  `dedup_by`, `required_casts`, `declared_not_required`. Every value is a sorted list except
  `required_casts` (a dict column->type) and the three single-column roles
  (`applied_dts`/`cdc_op`/`manifest`, each a string or `None`).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accelerator.py`, in a new section. `_esc` is the module under test.

```python
print("\n== source contract: per-binding requirements ==")

_esc_spec = _ilu2.spec_from_file_location(
    "esc", ROOT / "tools" / "emit_source_contract.py")
_esc = _ilu2.module_from_spec(_esc_spec); _esc_spec.loader.exec_module(_esc)

# The real GP_US binding on gl20000, read from the model rather than fabricated, so this
# test moves when the model does.
_esc_e = [x for x in MODEL.entities if x.name == "general_journal_line"][0]
_esc_s = [s for s in _esc_e.sources if s.name == "GP_US"][0]
_esc_req = _esc.binding_requirements(_esc_e, _esc_s)

check("parent-key columns are collected across all parents, deduplicated and sorted",
      _esc_req["parent_keys"] == ["actindx", "input_db", "jrnentry", "openyear"],
      f"got {_esc_req['parent_keys']!r} -- input_db is declared under three parents and must "
      f"appear once")
check("the entity's transaction_key is carried onto the binding's requirements",
      _esc_req["transaction_key"] == ["seqnumbr"],
      f"got {_esc_req['transaction_key']!r} -- transaction_key is declared on the ENTITY, not "
      f"the binding, so a binding-only walk would miss it")
check("declared casts become required types",
      _esc_req["required_casts"] == {"crdtamnt": "DECIMAL(18,2)", "debitamt": "DECIMAL(18,2)",
                                     "orcrdamt": "DOUBLE", "ordbtamt": "DOUBLE"},
      f"got {_esc_req['required_casts']!r}")
check("applied_dts is carried, and absent roles are None rather than missing keys",
      _esc_req["applied_dts"] == "dex_row_ts" and _esc_req["cdc_op"] is None
      and _esc_req["manifest"] is None,
      f"applied_dts={_esc_req['applied_dts']!r} cdc_op={_esc_req['cdc_op']!r} "
      f"manifest={_esc_req['manifest']!r} -- a missing key reads as 'not asked about', a None "
      f"reads as 'asked and not required'")

# DEDUP_ORDER IS NOT A REQUIREMENT, and this is the check that keeps it out. Measured 28 Aug:
# factory.py:367 says dedup_order is "DECLARED but not applied" -- a partitioned ranking window
# is not streaming-legal -- and that is the runtime's only mention of it. Worse,
# input_file_name appears in most dedup_order declarations and occurs NOWHERE in
# src/accelerator/ as a column: it is Spark's file-metadata function, not something Bronze
# holds. Publishing it would send another team defending a column that does not exist.
check("dedup_by IS required and dedup_order is NOT",
      _esc_req["dedup_by"] == ["input_db", "jrnentry", "openyear", "seqnumbr"]
      and "input_file_name" not in str(_esc_req["dedup_by"])
      and _esc_req["declared_not_required"]["dedup_order"] == ["dex_row_ts",
                                                               "input_file_name"],
      f"dedup_by={_esc_req['dedup_by']!r} "
      f"declared_not_required={_esc_req['declared_not_required']!r}")
check("no required role anywhere carries a dedup_order-only column",
      "input_file_name" not in {c for k, v in _esc_req.items()
                                if k != "declared_not_required"
                                for c in (v if isinstance(v, (list, dict)) else [v])
                                if isinstance(c, str)},
      "input_file_name is Spark file metadata, not a Bronze column -- it must not appear in "
      "any REQUIRED role")
```

`MODEL` already exists in `tests/test_accelerator.py` as the loaded model; if the name in that file differs, use the existing one rather than loading the model a second time.

- [ ] **Step 2: Run it to verify it fails**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: an error that `tools/emit_source_contract.py` does not exist. That is the correct first failure.

- [ ] **Step 3: Write the module**

Create `tools/emit_source_contract.py`:

```python
"""The source contract: what Silver requires of each Bronze table it reads.

A CONSUMER-SIDE contract. data_contracts/ describes what the vault produces; this describes
what it depends on, generated from the same authority, so Bronze can only break us knowingly.

We do not model Bronze and are not proposing to. What is modelled -- completely, and already
gated -- is our DEPENDENCY on Bronze: every SourceBinding names its bronze_table and the
columns Silver reads from it.

PURE, AND OFFLINE BY DESIGN. No Spark, no workspace, no clock. The artefact is committed and
verify_repo asserts regeneration is a no-op, which is the only reason to trust it -- reading
anything from a live lake at emit time would destroy that property.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

from accelerator import spec  # noqa: E402

OUTPUT_DIR = ROOT / "source_contracts"


def binding_requirements(entity: spec.Entity, src: spec.SourceBinding) -> dict:
    """What Silver requires of one Bronze table, through one binding, grouped by ROLE.

    Role rather than a flat column list, because the consequences differ: dropping a payload
    column degrades one target, dropping a key column breaks joins across the whole lake.

    transaction_key is read from the ENTITY -- it is declared there, not on the binding.
    """
    parent_cols: set[str] = set()
    for _parent, cols in src.parent_keys:
        parent_cols.update(cols)

    return {
        "business_keys": sorted(src.key_columns),
        "parent_keys": sorted(parent_cols),
        "transaction_key": sorted(entity.transaction_key or ()),
        "payload": sorted(src.payload),
        "applied_dts": src.applied_dts_column,
        "cdc_op": src.cdc_op_column,
        "manifest": src.manifest_column,
        "dedup_by": sorted(src.dedup_by),
        "required_casts": {col: typ for col, typ in src.cast},
        # DECLARED, NOT REQUIRED. dedup_order is not consumed by the streaming path
        # (factory.py:367: "DECLARED but not applied" -- a partitioned ranking window is not
        # streaming-legal), and input_file_name, which appears in most declarations, is Spark's
        # file-metadata function rather than a Bronze column. Requiring either would publish a
        # dependency we do not have. Carried so the day a batch latest-wins path lands, the
        # intent is already written down.
        "declared_not_required": {"dedup_order": list(src.dedup_order)},
    }
```

- [ ] **Step 4: Run it to verify it passes**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: ALL CHECKS PASSED.

- [ ] **Step 5: Mutation-prove every check**

Commit first. Then, one at a time, copy `tools/emit_source_contract.py` aside, apply the
mutation, run the suite, record the observed FAIL line, and copy the file back:

1. `parent_cols.update(cols)` -> `parent_cols = set(cols)` (last parent wins)
   Expected FAIL: "parent-key columns are collected across all parents"
2. `sorted(entity.transaction_key or ())` -> `[]`
   Expected FAIL: "the entity's transaction_key is carried onto the binding's requirements"
3. `{col: typ for col, typ in src.cast}` -> `{}`
   Expected FAIL: "declared casts become required types"
4. `"cdc_op": src.cdc_op_column,` deleted from the dict
   Expected FAIL: "applied_dts is carried, and absent roles are None rather than missing keys"
5. `"dedup_by": sorted(src.dedup_by)` -> `sorted(src.dedup_by) + list(src.dedup_order)`
   Expected FAIL: both "dedup_by IS required and dedup_order is NOT" and "no required role
   anywhere carries a dedup_order-only column". If only one fires, say which in the report.

- [ ] **Step 6: Commit**

```bash
git add tools/emit_source_contract.py tests/test_accelerator.py
git commit -m "Per-binding source requirements, grouped by role"
```

---

### Task 2: Merge across bindings, and exclude what is not Bronze

**Files:**
- Modify: `tools/emit_source_contract.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `binding_requirements` from Task 1.
- Produces: `NOT_BRONZE_SOURCE = "BUSINESS_VAULT"`;
  `bronze_tables(model, active) -> dict[str, dict]`, keyed by fully-qualified bronze table,
  each value `{"read_by": [...], "requires": {...}}` where `requires` is the union of every
  contributing binding's roles. `active` is `frozenset[str] | None`, passed straight to
  `spec.active_table_bindings`.

- [ ] **Step 1: Write the failing test**

```python
print("\n== source contract: merge and exclusion ==")

# MOST BRONZE TABLES ARE READ BY SEVERAL BINDINGS. Measured 28 Aug: 21 distinct bronze tables
# across 36 bindings; ukg_raw.gl alone is read by five. An emitter that assigns per binding
# instead of merging would publish whichever binding happened to come last, silently dropping
# the columns the other four need -- and it would look correct, because the file would still
# name every table.
_esc_all = _esc.bronze_tables(MODEL, None)
_esc_gl = _esc_all["01_usnc_bronze_dev.ukg_raw.gl"]
check("a bronze table read by five bindings lists all five",
      len(_esc_gl["read_by"]) == 5,
      f"got {_esc_gl['read_by']!r}")
check("its requirements are the UNION across those bindings, not the last one",
      set(_esc_gl["requires"]["payload"]) >=
      set(_esc.binding_requirements(
          *[(e, s) for e in MODEL.entities for s in e.sources
            if s.bronze_table == "01_usnc_bronze_dev.ukg_raw.gl"][0])["payload"]),
      "a merge that overwrote would lose the earlier bindings' payload columns")

# NOT EVERY bronze_table IS IN BRONZE. Measured 28 Aug: three bindings named BUSINESS_VAULT
# read hfig_*.raw_vault.* -- our OWN vault feeding business-vault entities. Publishing those to
# the Bronze team would name tables they do not own and have never heard of.
check("BUSINESS_VAULT bindings are excluded from the contract",
      not [t for t in _esc_all if ".raw_vault." in t],
      f"raw_vault tables leaked into the contract: {[t for t in _esc_all if '.raw_vault.' in t]}")

# TWO CRITERIA THAT MUST AGREE. Excluding by source NAME is a string convention; excluding by
# table PATH is structural. They agree exactly today (3 and 3). Asserting both directions means
# a future BUSINESS_VAULT binding that really reads Bronze, or a differently-named binding that
# reads raw_vault, fails here rather than silently changing what gets published.
_esc_by_name = {f"{e.name}/{s.name}" for e in MODEL.entities for s in e.sources
                if s.name == _esc.NOT_BRONZE_SOURCE}
_esc_by_path = {f"{e.name}/{s.name}" for e in MODEL.entities for s in e.sources
                if ".raw_vault." in s.bronze_table}
check("the BUSINESS_VAULT name and the raw_vault path identify the SAME bindings",
      _esc_by_name == _esc_by_path and len(_esc_by_name) == 3,
      f"by name: {sorted(_esc_by_name)}; by path: {sorted(_esc_by_path)} -- if these diverge, "
      f"excluding by name publishes or hides the wrong thing")
```

- [ ] **Step 2: Run it to verify it fails**

Expected: `AttributeError: module 'esc' has no attribute 'bronze_tables'`.

- [ ] **Step 3: Implement**

Append to `tools/emit_source_contract.py`:

```python
# THE THREE BINDINGS THAT ARE NOT BRONZE. Measured 28 Aug: job_request_custom_promoted,
# payroll_line_classification and organisation each declare a BUSINESS_VAULT source whose
# `bronze_table` is hfig_*.raw_vault.* -- our own vault, feeding a business-vault entity. The
# field is named bronze_table because that is what it is for every other binding. Publishing
# these would name tables the Bronze team does not own.
#
# Excluded by NAME, and tests/test_accelerator.py asserts the name and the raw_vault path pick
# out the same three bindings, in both directions -- so a future divergence fails there rather
# than quietly changing what is published.
NOT_BRONZE_SOURCE = "BUSINESS_VAULT"

_MERGEABLE_LIST_ROLES = ("business_keys", "parent_keys", "transaction_key", "payload",
                         "dedup_by")
_SINGLE_ROLES = ("applied_dts", "cdc_op", "manifest")


def bronze_tables(model: spec.Model, active) -> dict:
    """Every Bronze table an ACTIVE binding reads, with the union of what is required of it.

    Keyed by fully-qualified table. Several bindings legitimately read one table -- ukg_raw.gl
    is read by five -- so roles are UNIONED, never overwritten.
    """
    out: dict = {}
    for entity in model.entities:
        for src in entity.sources:
            if src.name == NOT_BRONZE_SOURCE:
                continue
            if not spec.active_table_bindings(entity, src, active):
                continue
            req = binding_requirements(entity, src)
            slot = out.setdefault(
                src.bronze_table,
                {"read_by": [], "requires": {r: set() for r in _MERGEABLE_LIST_ROLES}},
            )
            slot["read_by"].append(f"{entity.name}/{src.name}")
            for role in _MERGEABLE_LIST_ROLES:
                slot["requires"][role].update(req[role])
            for role in _SINGLE_ROLES:
                if req[role]:
                    slot["requires"].setdefault(role, set()).add(req[role])
            slot["requires"].setdefault("required_casts", {}).update(req["required_casts"])
            slot["requires"].setdefault("declared_not_required", {}).setdefault(
                "dedup_order", set()).update(req["declared_not_required"]["dedup_order"])

    # sets are for merging; lists are what YAML should carry, sorted so the artefact is stable
    for slot in out.values():
        slot["read_by"] = sorted(slot["read_by"])
        req = slot["requires"]
        for role, value in list(req.items()):
            if isinstance(value, set):
                req[role] = sorted(value)
        req["declared_not_required"]["dedup_order"] = sorted(
            req["declared_not_required"]["dedup_order"])
    return out
```

- [ ] **Step 4: Run it to verify it passes**

Expected: ALL CHECKS PASSED.

- [ ] **Step 5: Mutation-prove**

Commit first, then one at a time:

1. `slot["requires"][role].update(req[role])` -> `slot["requires"][role] = set(req[role])`
   Expected FAIL: "its requirements are the UNION across those bindings, not the last one"
2. Delete the `if src.name == NOT_BRONZE_SOURCE: continue` guard
   Expected FAIL: "BUSINESS_VAULT bindings are excluded from the contract"
3. `NOT_BRONZE_SOURCE = "BUSINESS_VAULT"` -> `"BUSINESS_VAULTS"`
   Expected FAIL: both the exclusion check and the two-criteria check
4. Delete the `if not spec.active_table_bindings(...): continue` guard
   Expected: this may NOT fail with `active=None` (None means every binding is active). Say so
   plainly in the report rather than claiming a pass — Task 3's per-target checks are what
   cover the active filter, and if nothing here can prove it, that is worth stating.

- [ ] **Step 6: Commit**

```bash
git add tools/emit_source_contract.py tests/test_accelerator.py
git commit -m "Merge requirements per bronze table, exclude non-bronze sources"
```

---

### Task 3: The artefact — emit, render, main

**Files:**
- Modify: `tools/emit_source_contract.py`
- Create: `source_contracts/*.yaml` (nine files, generated)
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `bronze_tables` from Task 2; `emit_data_contract.targets_and_variables()`.
- Produces: `LIMITS` (a two-key dict), `emit(model, target, variables) -> dict`,
  `render(structure) -> str`, `main() -> None`, `contract_path(target) -> Path`.

- [ ] **Step 1: Write the failing test**

```python
print("\n== source contract: the artefact ==")

_esc_struct = _esc.emit(MODEL, "usnc_tds", dict(_esc_vars_for("usnc_tds")))
check("the contract names only bronze tables an ACTIVE binding reads in that target",
      all(".raw_vault." not in t for t in _esc_struct["bronze_tables"])
      and "hfig_eu.bronze.striive_job_request" not in _esc_struct["bronze_tables"],
      f"got {sorted(_esc_struct['bronze_tables'])!r} -- STRIIVE_EU is not in usnc_tds's "
      f"active_sources, and a contract listing it sends Bronze after a table this lake "
      f"never reads")

# THE LIMITS MUST BE IN THE FILE, and this is the check that keeps them there. Five defects on
# the preceding branch were a claim that outlived the thing justifying it; a caveat that can be
# silently deleted is worth little. Both limits are load-bearing: without the first, a reader
# takes required types as observations about Bronze; without the second, the column list looks
# complete when governed expectation SQL may name any source column at runtime.
check("both stated limits appear in the emitted structure",
      set(_esc.LIMITS) == {"types_are_requirements", "expectation_columns_not_listed"}
      and all(isinstance(v, str) and len(v) > 40 for v in _esc.LIMITS.values())
      and _esc_struct["limits"] == _esc.LIMITS,
      f"LIMITS keys={sorted(_esc.LIMITS)}, in structure={'limits' in _esc_struct}")
check("the limits survive rendering to YAML text",
      all(k in _esc.render(_esc_struct) for k in _esc.LIMITS),
      "a limit present in the dict but lost in render() is not in the artefact anyone reads")
check("render uses allow_unicode, so an em dash is not published as an escape",
      "\\u2014" not in _esc.render({"limits": {"a": "an em dash — here"}}),
      "emit_data_contract.render passes allow_unicode=True; without it the artefact carries "
      "\\u2014 where a human wrote an em dash")
check("the contract states it is not an agreement Bronze countersigned",
      "not an agreement" in _esc.render(_esc_struct).lower(),
      "presenting an unnegotiated document as a binding contract is a claim we cannot "
      "evidence -- the reason the SLA block was withdrawn from data_contracts/")
```

Add near the top of this block, since two tasks need it:

```python
def _esc_vars_for(target):
    """That target's resolved bundle variables, from the one authority for them."""
    return dict([v for t, v in _esc.targets_and_variables() if t == target][0])
```

- [ ] **Step 2: Run it to verify it fails**

Expected: `AttributeError: module 'esc' has no attribute 'emit'`.

- [ ] **Step 3: Implement**

Append to `tools/emit_source_contract.py`:

```python
from emit_data_contract import targets_and_variables  # noqa: E402

# REUSED, NOT RESTATED. targets_and_variables() resolves a target's variables the way the
# bundle does, and spec.active_table_bindings is "THE definition of an inactive table" already
# relied on by three gates. A second statement of either is the duplicate-definition trap this
# repo has been bitten by twice (BUSINESS_KINDS, the system-column set).

LIMITS = {
    "types_are_requirements": (
        "Every type here is a type Silver REQUIRES, never an observation about what Bronze "
        "holds. Bronze's own column types are not modelled in this repository, so this "
        "document cannot and does not describe them."
    ),
    "expectation_columns_not_listed": (
        "This lists the columns the MODEL reads, plus the two compiled-in key-safety rules. "
        "Governed data-quality rules live in control.ref_dq_expectation and their SQL is read "
        "at pipeline runtime, where it may name ANY source column. So a change to a column "
        "only a governed rule references can break a load this document never mentioned. "
        "Reading those rules here would make a committed, offline-reproducible artefact "
        "depend on workspace state, so the gap belongs to a live check instead."
    ),
}

STANDING = (
    "This is Silver's stated dependency on Bronze, generated from "
    "metadata/entities/*.yml. It is NOT an agreement Bronze has countersigned."
)


def contract_path(target: str) -> Path:
    return OUTPUT_DIR / f"{target}.yaml"


def emit(model: spec.Model, target: str, variables: dict) -> dict:
    """The contract structure for one target. Pure: same inputs, same dict, no I/O, no clock."""
    active = spec.resolve_active_sources(model, variables.get("active_sources"))
    return {
        "target": target,
        "standing": STANDING,
        "generated_from": "metadata/entities/*.yml",
        "limits": dict(LIMITS),
        "bronze_tables": bronze_tables(model, active),
    }


def render(structure: dict) -> str:
    """The YAML text for one contract. sort_keys=True so the diff is reviewable, and
    allow_unicode=True so an em dash is a character rather than an escape."""
    return yaml.safe_dump(structure, sort_keys=True, default_flow_style=False,
                          allow_unicode=True)


def main() -> None:
    model = spec.load_model(ROOT / "metadata" / "entities")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for target, variables in targets_and_variables():
        contract_path(target).write_text(render(emit(model, target, variables)),
                                         encoding="utf-8")
        print(f"wrote {contract_path(target).relative_to(ROOT)}")


if __name__ == "__main__":
    main()
```

`spec.resolve_active_sources(model, declared)` is verified to exist at
`src/accelerator/spec.py:949`. It takes the MODEL as its first argument, accepts the
comma-separated string the bundle supplies, returns `frozenset[str] | None`, and returns None
when nothing is declared — meaning every binding is active. It raises `SpecError` naming any
entry that matches no binding, which is deliberately fatal. Do not write a second parser.

- [ ] **Step 4: Generate and inspect**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tools/emit_source_contract.py`
Then read one file end to end: `sed -n '1,60p' source_contracts/usnc_tds.yaml`.
Confirm by eye: no `raw_vault` table, no inactive source's table, both limits present,
`dedup_order` only under `declared_not_required`.

- [ ] **Step 5: Run both suites, then mutation-prove**

Commit first. Then:

1. Delete `"limits": dict(LIMITS),` from `emit`
   Expected FAIL: "both stated limits appear in the emitted structure"
2. `allow_unicode=True` -> `allow_unicode=False`
   Expected FAIL: "render uses allow_unicode, so an em dash is not published as an escape"
3. `active = spec.resolve_active_sources(...)` -> `active = None`
   Expected FAIL: "the contract names only bronze tables an ACTIVE binding reads in that target"
4. Delete the `STANDING` entry from `emit`
   Expected FAIL: "the contract states it is not an agreement Bronze countersigned"

- [ ] **Step 6: Commit**

```bash
git add tools/emit_source_contract.py source_contracts tests/test_accelerator.py
git commit -m "Emit source_contracts/<target>.yaml, with its limits stated in the artefact"
```

---

### Task 4: The gates

**Files:**
- Modify: `verify_repo.py` — append a new `[source contracts]` section immediately after the
  existing `[contracts]` section that gates `data_contracts/`.

**Interfaces:**
- Consumes: `emit_source_contract.emit`, `.render`, `.contract_path`,
  `.targets_and_variables`, `.LIMITS`, `.bronze_tables`.
- Produces: nothing importable.

**Depends on the `[contracts]` section having run:** that section already does
`sys.path.insert(0, str(ROOT / "tools"))` (around `verify_repo.py:1894`) and imports
`emit_data_contract`. Append AFTER it and reuse that path entry. Do NOT add a second
`sys.path.insert` and do NOT add a second `emit_data_contract` import.

- [ ] **Step 1: Write the gate**

```python
# --------------------------------------------------------------------------- #
print("\n[source contracts] what Silver requires of Bronze matches the model")

import emit_source_contract as _esc2  # noqa: E402

# THE DIFF IS THE REVIEW -- the discipline metadata/key_composition.json and data_contracts/
# already carry. CAUGHT rather than raised: an exception escaping a module-level block aborts
# verify_repo.py and turns every later check silently ABSENT, which is worse than one red check
# carrying the message. Errors and staleness are reported SEPARATELY, because "run the emitter
# and review the diff" is the wrong remedy for a broken emitter and will only reproduce the
# crash.
_sc_stale, _sc_errors, _sc_missing = [], [], []
# `model` and `spec` are already loaded at verify_repo.py:551-553 -- reuse them.
# Reloading the model here would be a second parse of the same files and a
# second place for the entities directory to be named.
_sc_model = model
for _sc_target, _sc_vars in _esc2.targets_and_variables():
    _sc_path = _esc2.contract_path(_sc_target)
    if not _sc_path.is_file():
        _sc_missing.append(_sc_target)
        continue
    try:
        if _sc_path.read_text(encoding="utf-8") != _esc2.render(
                _esc2.emit(_sc_model, _sc_target, _sc_vars)):
            _sc_stale.append(_sc_path.name)
    except Exception as _exc:  # noqa: BLE001
        _sc_errors.append(f"{_sc_path.name}: {type(_exc).__name__}: {_exc}")

check("every target in scope has a committed source_contracts/<target>.yaml",
      not _sc_missing,
      f"missing: {_sc_missing} -- run tools/emit_source_contract.py")
check("generating every source contract raises no error",
      not _sc_errors,
      f"the emitter itself is broken, not stale: {_sc_errors} -- fix "
      f"tools/emit_source_contract.py; re-running it will only reproduce this crash")
check("every committed source contract matches what the model generates",
      not _sc_stale,
      f"stale: {_sc_stale} -- run tools/emit_source_contract.py and review the diff")

# NO INVENTED COLUMN, IN BOTH DIRECTIONS. A contract that omits a column Silver reads tells
# Bronze a dependency does not exist; one that invents a column sends them defending something
# nothing uses. The second is the easier mistake to make and the harder to notice, because the
# file still looks complete.
_sc_declared_cols = set()
for _e in _sc_model.entities:
    _sc_declared_cols.update(_e.transaction_key or ())
    for _s in _e.sources:
        _sc_declared_cols.update(_s.key_columns)
        _sc_declared_cols.update(_s.payload)
        _sc_declared_cols.update(_s.dedup_by)
        _sc_declared_cols.update(_s.dedup_order)
        for _p, _cols in _s.parent_keys:
            _sc_declared_cols.update(_cols)
        for _c in (_s.applied_dts_column, _s.cdc_op_column, _s.manifest_column):
            if _c:
                _sc_declared_cols.add(_c)
        _sc_declared_cols.update(col for col, _t in _s.cast)

_sc_emitted_cols, _sc_required_dedup_order = set(), []
for _sc_target, _sc_vars in _esc2.targets_and_variables():
    for _tbl, _slot in _esc2.emit(_sc_model, _sc_target, _sc_vars)["bronze_tables"].items():
        _req = _slot["requires"]
        for _role, _value in _req.items():
            if _role == "declared_not_required":
                continue
            if isinstance(_value, dict):
                _sc_emitted_cols.update(_value)
            else:
                _sc_emitted_cols.update(_value)
        for _dorder in _req["declared_not_required"]["dedup_order"]:
            for _role, _value in _req.items():
                if _role != "declared_not_required" and _dorder in set(_value):
                    _sc_required_dedup_order.append(f"{_tbl}.{_role}.{_dorder}")

check("every column a source contract names is declared in the model",
      not (_sc_emitted_cols - _sc_declared_cols),
      f"invented: {sorted(_sc_emitted_cols - _sc_declared_cols)} -- a column in the contract "
      f"that the model never declares sends Bronze defending something nothing reads")
check("no dedup_order column appears among a contract's REQUIRED roles",
      not _sc_required_dedup_order,
      f"{_sc_required_dedup_order} -- dedup_order is not applied at runtime "
      f"(factory.py:367) and input_file_name is Spark file metadata, not a Bronze column")
# SPEC SECTION 6 ROW 3, IN BOTH DIRECTIONS. A dict key cannot repeat, so "exactly once" is
# really set equality: every bronze table an active non-BUSINESS_VAULT binding reads appears,
# and nothing else does. Omission tells Bronze a dependency does not exist; an extra key sends
# them after a table this lake never reads. The self-review of this plan found this row had no
# task -- it is here because a spec requirement with no check is the gap that mutation-proving
# is structurally blind to.
_sc_key_drift = []
for _sc_target, _sc_vars in _esc2.targets_and_variables():
    _sc_active = spec.resolve_active_sources(_sc_model, _sc_vars.get("active_sources"))
    _sc_want = {s.bronze_table for e in _sc_model.entities for s in e.sources
                if s.name != _esc2.NOT_BRONZE_SOURCE
                and spec.active_table_bindings(e, s, _sc_active)}
    _sc_got = set(_esc2.emit(_sc_model, _sc_target, _sc_vars)["bronze_tables"])
    if _sc_want != _sc_got:
        _sc_key_drift.append(
            f"{_sc_target}: missing {sorted(_sc_want - _sc_got)}, extra {sorted(_sc_got - _sc_want)}")
check("each target's contract names exactly the bronze tables its active bindings read",
      not _sc_key_drift,
      f"{_sc_key_drift} -- a missing table tells Bronze a dependency does not exist; an extra "
      f"one sends them after a table this lake never reads")

check("no source contract names a raw_vault table",
      not [t for _sc_t, _sc_v in _esc2.targets_and_variables()
           for t in _esc2.emit(_sc_model, _sc_t, _sc_v)["bronze_tables"]
           if ".raw_vault." in t],
      "BUSINESS_VAULT bindings read our own vault; publishing those names tables the Bronze "
      "team does not own")
check("both stated limits appear in every committed source contract",
      not [p.name for p in
           [_esc2.contract_path(t) for t, _ in _esc2.targets_and_variables()]
           if p.is_file() and not all(k in p.read_text(encoding="utf-8") for k in _esc2.LIMITS)],
      "a caveat that can be silently deleted is worth little -- five defects on the preceding "
      "branch were a claim outliving what justified it")
```

`verify_repo.py:551` already does `from accelerator import hashing, naming, spec`, and
`:553` already does `model = spec.load_model(ROOT / "metadata" / "entities")`. Both names are
in scope at module level — verified. Use `spec` and `model` directly; do not import either
again, and do not re-parse the model.

- [ ] **Step 2: Run and confirm green**

Run both suites. `verify_repo.py` should report **more** checks than the 813 baseline
(seven new here, plus per-file glob checks over the new emitter). Record the new number.

- [ ] **Step 3: Mutation-prove every gate**

Commit first, then one at a time, restoring by file copy:

1. Edit a committed `source_contracts/*.yaml` by hand -> FAIL "every committed source contract
   matches what the model generates", naming that file only
2. Edit the EMITTER without regenerating -> FAIL the same check, naming every file
3. `mv source_contracts/dev.yaml aside` -> FAIL "every target in scope has a committed
   source_contracts/<target>.yaml", and the suite must still run to completion
4. Inject `import nonexistent_module_xyz` into `emit()` -> FAIL "generating every source
   contract raises no error", NOT the staleness check
5. In `binding_requirements`, add `"payload": sorted(src.payload) + ["not_a_real_column"]`,
   regenerate -> FAIL "every column a source contract names is declared in the model"
6. In `binding_requirements`, `"dedup_by": sorted(src.dedup_by) + list(src.dedup_order)`,
   regenerate -> FAIL "no dedup_order column appears among a contract's REQUIRED roles"
7. Delete the `NOT_BRONZE_SOURCE` guard, regenerate -> FAIL "no source contract names a
   raw_vault table" AND "each target's contract names exactly the bronze tables its active
   bindings read"
9. In `bronze_tables`, `continue` on the FIRST binding of each table (e.g. skip when
   `src.bronze_table in out`) -> FAIL "each target's contract names exactly the bronze tables
   its active bindings read" only if a whole table disappears; if it does not fire, say so and
   find a mutation that removes one table key entirely
8. Delete one limit key from a committed YAML by hand -> FAIL "both stated limits appear in
   every committed source contract"

- [ ] **Step 4: Verify the 3.11 leg**

```bash
uv run --frozen --python 3.11 python tests/test_accelerator.py
uv run --frozen --python 3.11 python verify_repo.py
```

- [ ] **Step 5: Commit**

```bash
git add verify_repo.py
git commit -m "Gate the source contracts: staleness, invented columns, limits present"
```

---

### Task 5: Record it

**Files:**
- Modify: `docs/superpowers/OPEN_ITEMS.md`

- [ ] **Step 1: Add the section**

Insert before the most recent dated section, matching the file's heading style. Cover:

* What landed: `source_contracts/<target>.yaml`, nine files, generated from the model and
  gated so regeneration is a no-op.
* **What it does NOT do:** it does not check Bronze. Nothing reads a live lake. Whether Bronze
  satisfies the contract is a separate live assertion against `information_schema` and belongs
  with `control_conformance_check.py`. **That check is where the value lands**, because it
  would catch a breaking Bronze change before a pipeline run does — and it is deliberately a
  second spec.
* The two stated limits, and that they are asserted present rather than trusted.
* That `dedup_order` is excluded from requirements, with both measurements: `factory.py:367`
  says it is declared but not applied, and `input_file_name` is Spark file metadata rather
  than a Bronze column.
* That three `BUSINESS_VAULT` bindings are excluded because their `bronze_table` is our own
  `raw_vault`, and that the name and the path are asserted to identify the same bindings.
* That the contract is not an agreement Bronze countersigned, and says so in its own text.

- [ ] **Step 2: Run both suites and commit**

```bash
/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py
git add docs/superpowers/OPEN_ITEMS.md
git commit -m "Record the source contract"
```
