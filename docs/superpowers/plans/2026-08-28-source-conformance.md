# Source Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A check that answers whether Bronze still provides what `source_contracts/<target>.yaml` says Silver requires — before a pipeline run finds out.

**Architecture:** `checks/source_conformance_check.py`, in the shape `checks/control_conformance_check.py` uses: pure decision functions with Spark confined to `main()`, so fabricated `information_schema` rows and fabricated cast counts exercise the same decisions the live path makes. `catalog_missing` is imported from that module, never redefined.

**Tech Stack:** Python 3.11 floor, PyYAML, PySpark (in `main()` only), the repo's `check(name, condition, detail)` test idiom — NOT pytest.

**Spec:** `docs/superpowers/specs/2026-08-28-source-conformance-design.md`

## Global Constraints

* **Python 3.11 is the floor.** No backslash inside an f-string expression part; no assignment expression in a comprehension's iterable. CI runs 3.11 and 3.13 and has been red on both.
* **Run suites with `/mnt/projects/hda-edm-dv/.venv/bin/python`.** Reproduce the 3.11 leg with `uv run --frozen --python 3.11 python <suite>`.
* **Suites:** `python verify_repo.py` (828 in the main clone at plan time; a worktree without a `.venv` reports fewer because verify_repo globs `.venv/site-packages` — **compare check SETS, never counts**) and `python tests/test_accelerator.py`.
* **A check that cannot fail is a defect, not a nit.** Mutation-prove every check: break the thing it guards, observe a NAMED FAIL, restore. Derive the mutation from the check's own prose, not from this plan's list.
* **A check that reddens on correct output is worse than a blind one.** It teaches people to distrust the gate. Baseline must stay green after every change.
* **Commit before mutation-testing. Never `git checkout` to undo a mutation** — copy the file aside and copy it back. `git checkout` has destroyed uncommitted work in this project twice.
* **No workspace access while building.** No `databricks` command, no lake query, no deploy. Only `hfig-usnc-tds` of eight profiles is non-production and choosing it is the human's decision.
* **DEF-14:** `main()` returns an int; `sys.exit` only on a truthy rc. Serverless surfaces `SystemExit` as a failure even for exit code 0.
* **Reuse, do not restate.** `catalog_missing` is imported from `checks/control_conformance_check.py`. `source_contracts` are read through `tools/emit_source_contract.py`'s helpers. A second copy of either is the duplicate-authority trap this repo has been bitten by three times.

---

### Task 1: The two pure decisions

**Files:**
- Create: `checks/source_conformance_check.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `GATE = "source_conformance"`
  - `missing_columns(required: set, deployed: set) -> list[str]` — pure; sorted.
  - `lossy_casts(rows) -> list[str]` — pure; `rows` is an iterable of dicts shaped
    `{"table": str, "column": str, "type": str, "lossy": int}`, already collected.
  - `cast_probes(requires: dict) -> list[tuple[str, str]]` — pure; `(column, type)` pairs, one per
    required type, sorted.
  - `FINDINGS = ("ABSENT", "MISSING COLUMN", "LOSSY CAST")`
  - `finding(kind: str, detail: str) -> str` — pure; raises `ValueError` on an unknown kind.
  - `gate_status(problems: list, not_evaluated: list) -> str` — pure; one of `FAILED`,
    `NOT_EVALUATED`, `PASSED`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accelerator.py`, **BEFORE the file's closing pass/fail gate**. That file ENDS with `print("=" * 62)`, then `if FAILURES: ... sys.exit(1)`, then `print("ALL CHECKS PASSED")`. Appending after the gate would place these checks past it: their failures would land in a list nothing reads again and the suite would print ALL CHECKS PASSED and exit 0. Insert immediately after the existing `== source contract: the artefact ==` section.

`_ilu2` (importlib.util) is already imported at `tests/test_accelerator.py:3397`. The loaded model is `model`, lowercase, at line 128 — there is no `MODEL`.

```python
print("\n== source conformance: the pure decisions ==")

_scc_spec = _ilu2.spec_from_file_location(
    "scc", ROOT / "checks" / "source_conformance_check.py")
_scc = _ilu2.module_from_spec(_scc_spec); _scc_spec.loader.exec_module(_scc)

check("missing_columns names what the contract requires and the lake lacks",
      _scc.missing_columns({"a", "b", "c"}, {"a", "c"}) == ["b"],
      f"got {_scc.missing_columns({'a', 'b', 'c'}, {'a', 'c'})!r}")
check("missing_columns is silent when the lake has more than required",
      _scc.missing_columns({"a"}, {"a", "b"}) == [],
      "a Bronze table carrying EXTRA columns is not a conformance failure -- we state what we "
      "read, never what Bronze may not hold")

# ONE PROBE PER (COLUMN, REQUIRED TYPE), not per column. required_casts is a column to a sorted
# LIST of types, because Ruling T2-B established that two bindings casting one column to two
# types is a CONJUNCTION: Bronze must supply a column castable to BOTH. Every list holds one type
# today, so a singular implementation would pass its tests and silently skip the second type the
# day one appears.
check("cast_probes emits one probe per required type, not per column",
      _scc.cast_probes({"required_casts": {"amt": ["DECIMAL(18,2)", "DOUBLE"],
                                           "qty": ["INT"]}})
      == [("amt", "DECIMAL(18,2)"), ("amt", "DOUBLE"), ("qty", "INT")],
      f"got {_scc.cast_probes({'required_casts': {'amt': ['DECIMAL(18,2)', 'DOUBLE'], 'qty': ['INT']}})!r}")
check("cast_probes is empty when nothing declares a cast",
      _scc.cast_probes({"required_casts": {}}) == []
      and _scc.cast_probes({}) == [],
      "a table with no declared cast has no castability to measure, and a missing key must not "
      "raise -- see Ruling T1-B")

check("lossy_casts fires on a non-zero count and names the table, column and type",
      [p for p in _scc.lossy_casts([{"table": "t", "column": "amt",
                                     "type": "DECIMAL(18,2)", "lossy": 12}])
       if "t" in p and "amt" in p and "DECIMAL(18,2)" in p and "12" in p],
      f"got {_scc.lossy_casts([{'table': 't', 'column': 'amt', 'type': 'DECIMAL(18,2)', 'lossy': 12}])!r}")
check("lossy_casts is silent on zero",
      _scc.lossy_casts([{"table": "t", "column": "amt", "type": "DOUBLE", "lossy": 0}]) == [],
      "zero rows destroyed means the cast is safe against today's data, which is a pass")

# SPEC §3 REQUIRES THE THREE FINDINGS TO BE DISTINGUISHABLE, NOT ONE CATEGORY. An ABSENT table
# and a wrong-columns table are different problems with different owners, and collapsing them
# sends the wrong person to look. Asserted through a pure function rather than by grepping the
# source for three literals, so the property is behavioural.
check("the three finding kinds produce distinguishable, non-overlapping labels",
      len({_scc.finding(k, "x").split(":")[0] for k in _scc.FINDINGS}) == 3
      and set(_scc.FINDINGS) == {"ABSENT", "MISSING COLUMN", "LOSSY CAST"},
      f"labels={[_scc.finding(k, 'x') for k in _scc.FINDINGS]!r}")
check("finding() refuses an unknown kind rather than inventing a fourth category",
      _scc_raised(lambda: _scc.finding("PROBABLY FINE", "x"), ValueError),
      "a typo'd kind must not become a silent fourth class of finding")

# SPEC §8: A REPORTED TYPE CANNOT CHANGE THE EXIT STATUS, and §6: a skipped cast probe is
# NOT_EVALUATED rather than a pass. Both are properties of the status decision, so both are
# testable on a pure function instead of read off main()'s control flow.
check("gate_status depends on problems and not_evaluated, and on nothing else",
      _scc.gate_status([], []) == "PASSED"
      and _scc.gate_status([], ["a"]) == "NOT_EVALUATED"
      and _scc.gate_status(["p"], []) == "FAILED"
      and _scc.gate_status(["p"], ["a"]) == "FAILED",
      f"got {[_scc.gate_status(*a) for a in (([], []), ([], ['a']), (['p'], []), (['p'], ['a']))]!r}"
      f" -- a skipped probe must never read as a pass, and a real problem outranks a skip")
```

`tests/test_accelerator.py` has `expect_error(name, fn, substring)`, which CALLS `check` itself
rather than returning a boolean — so it cannot be embedded in a condition. Define one tiny local
helper beside these checks instead, and do not touch `expect_error`:

```python
def _scc_raised(fn, exc_type) -> bool:
    try:
        fn()
    except exc_type:
        return True
    except Exception:
        return False
    return False
```

- [ ] **Step 2: Run it to verify it fails**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: an error that `checks/source_conformance_check.py` does not exist. That is the correct first failure.

- [ ] **Step 3: Write the module's pure half**

Create `checks/source_conformance_check.py`:

```python
"""Does Bronze still provide what Silver requires of it?

source_contracts/<target>.yaml states what Silver reads from each Bronze table. This checks
whether the lake still provides it -- before a pipeline run finds out. The contract spec named
this as where the value actually lands, and left it to a separate spec deliberately.

THE COVERAGE IS UNEVEN AND SAYS SO. Measured on great_plains_urw.gl20000: the contract names 19
required columns and a type for FOUR of them, because Bronze's own column types are not modelled
in this repository -- a type appears only where Silver declares a `cast`. So existence is
asserted for every required column, castability only for the columns carrying a declared cast,
and the actual Bronze type of the rest is REPORTED and never asserted.

PURE DECISIONS, SPARK IN main() ONLY -- the shape checks/control_conformance_check.py uses, so a
fabricated information_schema row and a fabricated cast count exercise the same decisions the
live path makes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "checks"))

# IMPORTED, NOT REDEFINED. catalog_missing exists because control_conformance_check.main() once
# CRASHED on a nonexistent catalog instead of reporting the not-instrumented state its own
# docstring promised. A second copy here would be the duplicate-authority trap this repo has been
# bitten by three times, and tests/test_accelerator.py asserts this module does not define one.
from control_conformance_check import catalog_missing  # noqa: E402,F401

GATE = "source_conformance"


def missing_columns(required: set, deployed: set) -> list:
    """Required columns the lake does not have. Sorted.

    EXTRA columns in Bronze are NOT a finding: the contract states what Silver reads, never what
    Bronze may not hold, so a one-directional comparison is the correct one here.
    """
    return sorted(set(required) - set(deployed))


def cast_probes(requires: dict) -> list:
    """[(column, required type)] to measure, one per required type.

    required_casts maps a column to a sorted LIST of types, per Ruling T2-B: two bindings casting
    one column to two types is a conjunction of requirements, so Bronze must supply a column
    castable to both. Every list holds exactly one type as of 28 Aug 2026, which is why a
    per-column implementation would pass and then silently skip the second type.
    """
    out = []
    for column, types in sorted((requires.get("required_casts") or {}).items()):
        for required_type in sorted(types):
            out.append((column, required_type))
    return out


FINDINGS: tuple = ("ABSENT", "MISSING COLUMN", "LOSSY CAST")


def finding(kind: str, detail: str) -> str:
    """One problem line, labelled by kind.

    THREE KINDS, KEPT DISTINCT. An ABSENT table and a wrong-columns table are different problems
    with different owners; control_conformance_check learned that collapsing them sends the wrong
    person to look. A kind outside FINDINGS raises rather than becoming a silent fourth class.
    """
    if kind not in FINDINGS:
        raise ValueError(f"unknown finding kind {kind!r}; expected one of {FINDINGS}")
    return f"{kind}: {detail}"


def gate_status(problems: list, not_evaluated: list) -> str:
    """FAILED / NOT_EVALUATED / PASSED, from those two inputs and nothing else.

    A SKIPPED PROBE IS NOT A PASS. --skip-cast-probes leaves the castability half unmeasured, and
    a green result that concealed that would be the gate-goes-quiet failure this repo keeps
    finding. A real problem still outranks a skip.

    Pure, and deliberately blind to everything else main() prints -- which is what makes the
    REPORTED-not-asserted block unable to change the exit status.
    """
    if problems:
        return "FAILED"
    if not_evaluated:
        return "NOT_EVALUATED"
    return "PASSED"


def lossy_casts(rows) -> list:
    """One problem line per (table, column, type) whose cast would destroy rows.

    `rows` is already collected -- dicts shaped {"table","column","type","lossy"} -- so a
    fabricated list exercises the same decision the live count does.
    """
    problems = []
    for r in rows:
        if r["lossy"]:
            problems.append(
                f"{r['table']}.{r['column']}: try_cast to {r['type']} destroys "
                f"{r['lossy']} row(s) that are not null today. Silver casts this column, so "
                f"those rows reach the vault as NULL"
            )
    return problems
```

- [ ] **Step 4: Run it to verify it passes**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: ALL CHECKS PASSED.

- [ ] **Step 5: Mutation-prove every check**

Commit first. Then one at a time, copy `checks/source_conformance_check.py` aside, mutate, run, record the observed FAIL line, copy it back:

1. `sorted(set(required) - set(deployed))` -> `sorted(set(deployed) - set(required))`
   Expected FAIL: "missing_columns names what the contract requires and the lake lacks" AND
   "missing_columns is silent when the lake has more than required"
2. In `cast_probes`, `for required_type in sorted(types)` -> `for required_type in sorted(types)[:1]`
   Expected FAIL: "cast_probes emits one probe per required type, not per column"
3. `(requires.get("required_casts") or {})` -> `requires["required_casts"]`
   Expected FAIL: "cast_probes is empty when nothing declares a cast" — and confirm it is a
   NAMED FAIL, not a KeyError traceback. Ruling T1-B: a crash mid-suite means every check after
   it does not run and reports nothing.
4. In `lossy_casts`, `if r["lossy"]:` -> `if True:`
   Expected FAIL: "lossy_casts is silent on zero"
5. In `lossy_casts`, drop `{r['column']}` from the message
   Expected FAIL: "lossy_casts fires on a non-zero count and names the table, column and type"

**VERIFY EACH MUTATION ACTUALLY LANDED** before trusting its result — assert the replacement count, or diff against the backup. A replace that matches nothing looks identical to a check that holds, and that error has been made repeatedly on this project.

- [ ] **Step 6: Commit**

```bash
git add checks/source_conformance_check.py tests/test_accelerator.py
git commit -m "The two pure decisions behind source conformance"
```

---

### Task 2: The import assertion, and the report-presence gate

**Files:**
- Modify: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `source_conformance_check` as loaded in Task 1 (`_scc`), `GATE`.
- Produces: nothing importable.

**Why this is its own task:** both checks assert properties of the SPEC's own prose rather than of behaviour, and both exist because a traceability pass found §5 and §2 asserting nothing. A reviewer can reject these independently of Task 1's logic.

- [ ] **Step 1: Write the failing test**

Append to the same section:

```python
# §5 SAYS A SECOND COPY WOULD BE THE DUPLICATE-AUTHORITY TRAP, so assert it rather than warn
# about it. The spec's own argument -- that catalog_missing must be imported from
# control_conformance_check and never redefined -- was prose with no gate until this check, which
# is the defect class this repo keeps producing: a claim about a risk with nothing enforcing it.
_scc_src = (ROOT / "checks" / "source_conformance_check.py").read_text(encoding="utf-8")
_scc_defs = [_n.name for _n in ast.walk(ast.parse(_scc_src))
             if isinstance(_n, ast.FunctionDef)]
check("source_conformance_check IMPORTS catalog_missing rather than defining its own",
      "catalog_missing" not in _scc_defs
      and _scc.catalog_missing is _ccc.catalog_missing,
      f"functions defined here: {sorted(_scc_defs)} -- catalog_missing exists because "
      f"control_conformance_check.main() once crashed on a nonexistent catalog instead of "
      f"reporting not-instrumented; a second copy is the trap §5 names")
```

`_ccc` is the already-loaded `control_conformance_check` module in that file. `ast` is imported at `tests/test_accelerator.py:15`.

- [ ] **Step 2: Run it to verify it fails**

Temporarily add a `def catalog_missing(exc): return False` to `checks/source_conformance_check.py`, run the suite, and confirm a NAMED FAIL. Then remove it. This proves the check can fail BEFORE you rely on it — the assertion here is about absence, and an absence check that cannot fail is the easiest kind to write by accident.

- [ ] **Step 3: Run it to verify it passes**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py`
Expected: ALL CHECKS PASSED.

- [ ] **Step 4: Commit**

```bash
git add tests/test_accelerator.py
git commit -m "Assert catalog_missing is imported, not redefined"
```

---

### Task 3: The live path

**Files:**
- Modify: `checks/source_conformance_check.py`
- Test: `tests/test_accelerator.py`

**Interfaces:**
- Consumes: `missing_columns`, `cast_probes`, `lossy_casts`, `catalog_missing`, `GATE` from Task 1.
- Produces: `contracts() -> dict`, `main() -> int`.

- [ ] **Step 1: Write the failing test**

```python
# THE REPORTED TYPES MUST BE PRESENT, NOT MERELY HARMLESS. §8 requires that a reported type
# cannot change the exit status -- which a report that silently VANISHED would satisfy perfectly.
# Harmless and absent are indistinguishable to that check alone. This is the same shape as the
# caveat that could be blanked while its key survived, found on the preceding branch inside the
# check written to prevent it.
_scc_main_src = _scc_src[_scc_src.index("def main("):]
check("main() reports the observed type of columns it does not assert a type for",
      "REPORTED" in _scc_main_src and "not asserted" in _scc_main_src.lower(),
      "the observed Bronze type of a non-cast column is the raw material for a future decision; "
      "a report that can silently vanish satisfies 'cannot change the exit status' trivially")

check("main() is DEF-14 compliant: returns an int and never exits 0 explicitly",
      "return 1 if" in _scc_main_src and "sys.exit(0)" not in _scc_src,
      "serverless surfaces SystemExit as a failure even for exit code 0, so falling off the end "
      "is the only correct success path")

check("the gate name is stated once and used in the summary line",
      _scc.GATE == "source_conformance" and "GATE SUMMARY" in _scc_src,
      f"GATE={_scc.GATE!r}")

check("main() runs only for targets with a committed contract",
      "contracts()" in _scc_main_src or "configured_targets" in _scc_src,
      "a target with no contract has not declared its Bronze, and there is nothing to conform to")
```

- [ ] **Step 2: Run it to verify it fails**

Expected: FAIL on all four, since `main()` does not exist yet. `_scc_src.index("def main(")` will raise `ValueError` — that is an acceptable first failure, but note it in your report, and if it aborts the suite rather than producing named FAILs, guard the index with a sentinel so the later checks still run.

- [ ] **Step 3: Implement the live path**

Append to `checks/source_conformance_check.py`:

```python
def contracts() -> dict:
    """{target: parsed contract} for every committed source contract.

    Read through the emitter's own path helpers, so the location of these files is stated once.
    """
    import yaml  # noqa: PLC0415 -- kept local; the pure half must import nothing heavy

    import emit_source_contract as esc

    out = {}
    for target, _variables in esc.configured_targets():
        path = esc.contract_path(target)
        if path.is_file():
            out[target] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--catalog", required=True,
                    help="the bronze catalog for this target, e.g. 01_usnc_bronze_dev")
    ap.add_argument("--skip-cast-probes", action="store_true",
                    help="report the cast columns as not_evaluated instead of measuring them; "
                         "for a caller without SELECT on the _raw tables")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    all_contracts = contracts()
    if args.target not in all_contracts:
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        print(f"{args.target}: no committed source contract. not instrumented -- a target that "
              f"has not declared its Bronze has nothing to conform to.")
        return 0

    tables = all_contracts[args.target].get("bronze_tables") or {}

    # COLLECT FIRST, THEN DECIDE. information_schema carries every column of every table in the
    # catalog; one query, then the pure functions.
    try:
        rows = spark.sql(
            f"SELECT lower(table_schema) s, lower(table_name) t, lower(column_name) c, "
            f"lower(data_type) d "
            f"FROM `{args.catalog}`.information_schema.columns").collect()
    except Exception as exc:  # noqa: BLE001 -- narrowed immediately by catalog_missing()
        if not catalog_missing(exc):
            raise
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        print(f"{args.catalog} does not exist. not instrumented -- this is the expected state "
              f"until the owning team stands the lake up.")
        return 0

    # KEYED LOWERCASE ON BOTH SIDES. The contract states a table name as the model declares it;
    # information_schema is queried lowered. Comparing the two without normalising both was a bug
    # in an earlier draft of this plan: `deployed` was matched case-insensitively while `observed`
    # was not, so the REPORTED block silently printed "not present" for every column.
    deployed: dict = {}
    observed: dict = {}
    for r in rows:
        key = f"{args.catalog}.{r['s']}.{r['t']}".lower()
        deployed.setdefault(key, set()).add(r["c"])
        observed[(key, r["c"])] = r["d"]

    # SPEC §6: VERIFY try_cast EXISTS, DO NOT ASSUME IT. A silently-unsupported function would
    # turn every cast measurement into a false zero -- the worst outcome available here, because
    # it reports safety it never measured. One cheap probe, before any real one.
    cast_probes_usable = not args.skip_cast_probes
    if cast_probes_usable:
        try:
            spark.sql("SELECT try_cast('x' AS INT) AS probe").collect()
        except Exception as exc:  # noqa: BLE001
            cast_probes_usable = False
            print(f"try_cast is unavailable in this runtime ({type(exc).__name__}), so "
                  f"castability was NOT measured: {exc}")

    problems: list = []
    not_evaluated: list = []
    probe_rows: list = []

    for table in sorted(tables):
        requires = tables[table].get("requires") or {}
        required = {c for role, v in requires.items()
                    if isinstance(v, list) for c in v}
        required |= set((requires.get("required_casts") or {}))
        key = table.lower()
        if key not in deployed:
            # ABSENT ends this table. Reporting its columns as missing too would bury one real
            # problem under nineteen derived ones.
            problems.append(finding("ABSENT",
                                    f"{table} is named in the contract and does not exist"))
            continue
        have = deployed[key]
        for col in missing_columns(required, have):
            problems.append(finding("MISSING COLUMN", f"{table}.{col}"))

        for col, required_type in cast_probes(requires):
            if col not in have:
                continue  # already reported as MISSING COLUMN
            if not cast_probes_usable:
                not_evaluated.append(f"{table}.{col} -> {required_type}")
                continue
            lossy = spark.sql(
                f"SELECT COUNT(*) n FROM {table} "
                f"WHERE `{col}` IS NOT NULL "
                f"AND try_cast(`{col}` AS {required_type}) IS NULL").collect()[0]["n"]
            probe_rows.append({"table": table, "column": col,
                               "type": required_type, "lossy": lossy})

    problems += lossy_casts(probe_rows)

    # REPORTED, NOT ASSERTED. The contract states a type only where Silver declares a cast, so
    # every other required column's Bronze type is raw material for a future decision and nothing
    # more. Printed unconditionally, and it cannot change the exit status -- `problems` is never
    # appended to from here.
    print(f"REPORTED (not asserted) -- observed Bronze types for columns the contract does not "
          f"type:")
    for table in sorted(tables):
        key = table.lower()
        requires = tables[table].get("requires") or {}
        typed = {c for c, _t in cast_probes(requires)}
        required = {c for role, v in requires.items() if isinstance(v, list) for c in v}
        for col in sorted(required - typed):
            print(f"  {table}.{col}: {observed.get((key, col), 'not present')}")

    for line in problems:
        print(f"  {line}")
    if not_evaluated:
        print(f"  NOT EVALUATED: cast probes skipped for {len(not_evaluated)} column(s) "
              f"(--skip-cast-probes): {not_evaluated}")

    print(f"GATE SUMMARY :: {GATE} :: status={gate_status(problems, not_evaluated)} "
          f"asserted={len(tables)} "
          f"not_evaluated={len(not_evaluated)}")
    return 1 if problems else 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for exit code 0. Exit explicitly
    # only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
```

- [ ] **Step 4: Run both suites**

Run: `/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py` then `python verify_repo.py`.
Expected: both green. `verify_repo` gains per-file glob checks over the new module (parses, builds no hash of its own, DEF-14 shape) — record the new count and confirm the additions are those.

- [ ] **Step 5: DO NOT run main() against a workspace**

Running it is a separate consented act and the profile is the human's choice. Confirm in your report that you did not, and state which of §8's rows are therefore asserted only by the offline checks.

- [ ] **Step 6: Mutation-prove**

Commit first, then:

1. Delete the `REPORTED (not asserted)` print block
   Expected FAIL: "main() reports the observed type of columns it does not assert a type for"
2. `return 1 if problems else 0` -> `return 0`
   Expected FAIL: "main() is DEF-14 compliant: returns an int and never exits 0 explicitly"
3. `GATE = "source_conformance"` -> `GATE = "src_conf"`
   Expected FAIL: "the gate name is stated once and used in the summary line"
4. Delete the `try_cast` availability probe from `main()`
   CORRECTED after the final whole-branch review (Fix 7, 2026-08-28): this step originally
   claimed no offline check would fire here, because spec §6 then treated "the check verifies
   `try_cast` exists in the runtime" as untestable offline. That claim was wrong -- the review
   built an offline `main()` harness (`_run_source_conformance` in `tests/test_accelerator.py`)
   that drives this exact probe with a fake Spark that can raise on it, and both branches are
   now asserted: the probe succeeding, and it raising (every cast column lands in
   `not_evaluated`, naming `try_cast is unavailable in this runtime`, and no COUNT probe is
   attempted). Expected FAIL now: "FIX 7: try_cast-unavailable, driven through the harness,
   lands every cast column in not_evaluated" (and its sibling checks). The genuinely untestable
   claim is narrower than this step used to state: only whether Databricks' REAL `try_cast`
   behaves as documented, which no fabricated input can settle.

- [ ] **Step 7: Commit**

```bash
git add checks/source_conformance_check.py tests/test_accelerator.py
git commit -m "The live source-conformance path, with the cast probe measured per required type"
```

---

### Task 4: Non-vacuity, and the record

**Files:**
- Modify: `tests/test_accelerator.py`
- Modify: `docs/superpowers/OPEN_ITEMS.md`

- [ ] **Step 1: Write the non-vacuity check**

```python
# §8 REQUIRES A NON-VACUITY GUARD. If contracts() ever returned {}, every table loop below it
# would iterate nothing and the check would pass having compared nothing -- the hollow-gate mode
# that DEF-48 exists for, and that the source-contract section needed its own guard against.
check("source conformance has contracts to check: contracts() is non-empty and matches the "
      "committed files",
      bool(_scc.contracts())
      and set(_scc.contracts()) == {p.stem for p in (ROOT / "source_contracts").glob("*.yaml")},
      f"contracts()={sorted(_scc.contracts())} against "
      f"{sorted(p.stem for p in (ROOT / 'source_contracts').glob('*.yaml'))} -- an empty or "
      f"short result here lets every conformance decision pass having compared nothing")
```

- [ ] **Step 2: Run it, then mutation-prove it**

Run both suites; expect green. Then make `contracts()` `return {}` and confirm a NAMED FAIL. Restore by file copy.

- [ ] **Step 3: Add the OPEN_ITEMS section**

Insert before the most recent dated section, matching the file's heading style. Cover:

* What landed: `checks/source_conformance_check.py`, its two pure decisions, and the live path.
* **It has never been run against a workspace.** Say so plainly. `main()` is driven end-to-end
  by the offline harness (`_run_source_conformance` in `tests/test_accelerator.py`), and twelve
  named checks depend on it, but no fabricated input can stand in for a real Databricks
  warehouse -- the live path itself is not asserted against one.
* The uneven coverage: existence for every required column, castability only for the columns carrying a declared cast, and the observed type of the rest reported and asserted-present but never asserted-correct.
* That the cast probe is one measurement per `(column, required type)` pair, because `required_casts` is a list per Ruling T2-B, and that every list holds one type today so the second-type path is untested in practice.
* **The genuinely narrow residual limit:** whether Databricks' REAL `try_cast` behaves as
  documented against a live warehouse. The offline harness exercises both branches of the probe
  (succeeding, and raising) with a fake Spark; what it cannot settle is whether the actual
  runtime's `try_cast` matches that documented behaviour.
* That `--skip-cast-probes` reports `not_evaluated` rather than passing, and why: a green result must never conceal that the stronger half was skipped.
* What it does not cover: the columns governed expectations touch, whose SQL lives in `control.ref_dq_expectation` and is read at pipeline runtime.

- [ ] **Step 4: Run both suites, the 3.11 leg, and commit**

```bash
/mnt/projects/hda-edm-dv/.venv/bin/python verify_repo.py
/mnt/projects/hda-edm-dv/.venv/bin/python tests/test_accelerator.py
uv run --frozen --python 3.11 python verify_repo.py
git add tests/test_accelerator.py docs/superpowers/OPEN_ITEMS.md
git commit -m "Guard source conformance against vacuity, and record what it does not cover"
```
