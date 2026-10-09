# Workday Customer-Invoice Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce the four-file CSV set Workday's DT/DTS tool loads for a customer invoice, from data already in this vault, refusing to write anything it cannot stand behind.

**Architecture:** The AME business rules exist as tested pure Python. The pipeline needs them as Spark SQL `derived_columns`. Rather than choose one, both exist and a **parity gate proves they agree over golden vectors** — the pattern `hash_parity_check.py` already uses for hashing. Downstream, a declared mapping turns vault columns into DCDD fields, validation runs before any file is written, and the two external dependencies (PLT-2 for money, subsystem E for references) surface as loud refusals rather than blank columns.

**Tech Stack:** Python 3.11 and 3.13, PyYAML, Databricks serverless `spark_python_task`, Spark SQL, Unity Catalog volumes, Delta.

**Spec:** `docs/superpowers/specs/2026-09-27-wd-fin-export-design.md`

## Global Constraints

- **Two implementations are permitted ONLY with a parity gate.** The AME rules exist in `src/accelerator/invoice_rules.py` (pure Python, unit-tested) and as Spark SQL in a conform profile. Neither is authoritative alone; the gate proving they agree is. This mirrors `checks/hash_parity_check.py`, which proves Spark digests equal a pure-Python reference over golden vectors.
- **AME002 (Invoice Date) and AME006 (Line Number) are NEVER derived.** They are issued once into `control.ctl_invoice_issuance` and read from there. A satellite is recomputed; anything recomputed can change, and a changed invoice date means the customer's copy no longer matches ours while every gate stays green.
- **No file is written unless every validation passes.** A partially valid CSV is worse than none: it looks loadable.
- **The two external dependencies are LOUD and not suppressible.** No flag disables them — a flag is how a temporary suppression becomes permanent.
- **The DCDD workbooks are read-only.** `metadata/workday/dcdd/*.xlsx` are committed verbatim with sha256 digests in `PROVENANCE.json`. Never edit one; never write mapping results back into one.
- **Tests are NOT pytest.** `tests/test_accelerator.py` and `verify_repo.py` are plain scripts of `check(name, condition, detail)` calls. **An exception inside a check's condition aborts the whole suite** — worse than a red check. Guard everything that can raise.
- **Where checks go differs per file.** Append at the END of `verify_repo.py`. **INSERT BEFORE the PASS/FAIL summary block in `tests/test_accelerator.py`** — that block holds its only `sys.exit(1)`, and a check after it goes red while the suite prints `ALL CHECKS PASSED` and exits 0.
- **Check counts never decrease.** `tests/test_accelerator.py` is 1601 and `verify_repo.py` is 1427 at the start of this plan. Confirm the live numbers before you start; they rise on their own when new modules appear.
- **Every new check must be proven red** by mutating what it guards, then restored. Verify the **exit code** with `echo $?`; never infer it from output text.
- **No semicolons in any generated comment text.** `apply_governance.statements()` splits SQL on `;`, and a semicolon inside a `--` comment destroys the statement that follows it. That defect shipped on 27 September and cost a whole schema.
- **No new third-party dependencies.** Python 3.11 and 3.13 must both work.
- **Do not modify** `src/accelerator/hashing.py`, `RULEBOOK_VERSION`, or `tests/golden_hash_vectors.json`.

## Review Focus

1. **A rule whose SQL and Python disagree on an edge input** — empty string, NULL, a task code with no pipe, a line whose module columns are all populated. The parity gate must cover these, not just the happy path. — Task 2.
2. **An invoice with no lines, or a line whose invoice id is absent from the header file.** The four CSVs are a set joined by `Customer_Invoice_ID`; an orphan on either side is a load failure at Workday, silent here. — Task 5.
3. **A re-run after values were issued.** `ctl_invoice_issuance` must return the SAME invoice date and line number, never recompute them, even if the underlying satellite changed. — Task 3.
4. **Every money column NULL.** The PLT-2 symptom, and indistinguishable from a legitimately zero invoice unless it is asserted explicitly. — Task 6.
5. **A `CHECKREFERENCES` field validated against an EMPTY reference table.** Passing because there is nothing to contradict you is not passing. — Task 6.

---

### Task 1: The AME golden vectors

**Files:**
- Create: `tests/golden_ame_vectors.json`
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Produces: `tests/golden_ame_vectors.json` with a top-level `{"rules": {<ame_id>: [{"inputs": {...}, "expected": <value>}, ...]}}` shape.

The vectors come FIRST because both implementations are measured against them. Writing them after either one would let that one define correctness.

- [ ] **Step 1: Write the vectors**

`tests/golden_ame_vectors.json` — cover the happy path AND the edges named in Review Focus 1:

```json
{
  "note": "Golden vectors for the AME invoice rules. BOTH the pure-Python implementation in src/accelerator/invoice_rules.py AND the Spark SQL derived_columns are measured against these. Neither implementation defines correctness; this file does.",
  "rules": {
    "AME004_AME005_worker_name": [
      {"inputs": {"last": "Smith", "first": "Jane"}, "expected": "Smith, Jane"},
      {"inputs": {"last": "Smith", "first": ""}, "expected": "Smith, "},
      {"inputs": {"last": "", "first": "Jane"}, "expected": ", Jane"},
      {"inputs": {"last": "  Smith  ", "first": "  Jane  "}, "expected": "Smith, Jane"}
    ],
    "AME007_line_type": [
      {"inputs": {"source_type": "Tax"}, "expected": "TAX"},
      {"inputs": {"source_type": "tax"}, "expected": "TAX"},
      {"inputs": {"source_type": "Labor"}, "expected": "ITEM"},
      {"inputs": {"source_type": ""}, "expected": "ITEM"}
    ],
    "AME011_task_number": [
      {"inputs": {"task_code": "ABC|123"}, "expected": "ABC"},
      {"inputs": {"task_code": "ABC(123)"}, "expected": "ABC"},
      {"inputs": {"task_code": "PLAIN"}, "expected": "PLAIN"}
    ],
    "AME012_AME013_module_value": [
      {"inputs": {"line_item_type": "TS", "ts": "a", "es": "b", "mi": "c"}, "expected": "a"},
      {"inputs": {"line_item_type": "ES", "ts": "a", "es": "b", "mi": "c"}, "expected": "b"},
      {"inputs": {"line_item_type": "MI", "ts": "a", "es": "b", "mi": "c"}, "expected": "c"}
    ]
  }
}
```

**Before writing the file, run each case through `invoice_rules.py` and record what it ACTUALLY returns.** If a function disagrees with a value above, the value above is wrong — fix the vector, not the rule. The rules were authorised by Amy's workbook; this file is a measurement of them, not a second opinion.

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'src')
from accelerator import invoice_rules as r
print(repr(r.worker_name('Smith','Jane')))
print(repr(r.worker_name('Smith','')))
print(repr(r.line_type('Tax')), repr(r.line_type('Labor')), repr(r.line_type('')))
print(repr(r.task_number('ABC|123')), repr(r.task_number('ABC(123)')), repr(r.task_number('PLAIN')))
print(repr(r.module_value('TS','a','b','c')), repr(r.module_value('ES','a','b','c')))
"
```

If `worker_name('Smith','')` raises rather than returning `"Smith, "`, record that: the vector becomes `{"inputs": {...}, "raises": "ValueError"}` and the parity gate must assert both sides raise.

- [ ] **Step 2: Write the failing check**

Insert into `tests/test_accelerator.py` immediately BEFORE the PASS/FAIL summary block:

```python
print("\n== the AME golden vectors, and the Python side of the parity ==")
import json as _json  # noqa: E402
from accelerator import invoice_rules as _ir  # noqa: E402

_ame_path = ROOT / "tests" / "golden_ame_vectors.json"
try:
    _ame = _json.loads(_ame_path.read_text(encoding="utf-8"))
    _ame_err = ""
except Exception as _e:  # noqa: BLE001 -- must not abort the suite
    _ame, _ame_err = {"rules": {}}, f"{type(_e).__name__}: {_e}"

check("the AME golden vectors load and declare at least four rules",
      not _ame_err and len(_ame.get("rules") or {}) >= 4,
      f"{_ame_err or sorted(_ame.get('rules') or {})} -- both implementations are "
      f"measured against this file, so an empty or unreadable one means the parity "
      f"gate below compares nothing")

_ame_fns = {
    "AME004_AME005_worker_name": lambda i: _ir.worker_name(i["last"], i["first"]),
    "AME007_line_type": lambda i: _ir.line_type(i["source_type"]),
    "AME011_task_number": lambda i: _ir.task_number(i["task_code"]),
    "AME012_AME013_module_value": lambda i: _ir.module_value(
        i["line_item_type"], i["ts"], i["es"], i["mi"]),
}
_ame_bad = []
for _rule, _cases in (_ame.get("rules") or {}).items():
    _fn = _ame_fns.get(_rule)
    if _fn is None:
        _ame_bad.append(f"{_rule}: no reference function bound")
        continue
    for _c in _cases:
        try:
            _got = _fn(_c["inputs"])
            if "raises" in _c:
                _ame_bad.append(f"{_rule}{_c['inputs']}: returned {_got!r}, expected raise")
            elif _got != _c["expected"]:
                _ame_bad.append(f"{_rule}{_c['inputs']}: {_got!r} != {_c['expected']!r}")
        except Exception as _exc:  # noqa: BLE001
            if _c.get("raises") != type(_exc).__name__:
                _ame_bad.append(f"{_rule}{_c['inputs']}: raised {type(_exc).__name__}")

check("invoice_rules.py agrees with every AME golden vector",
      not _ame_bad,
      f"{_ame_bad[:6]} -- the Python side of the parity. If this is red the vectors "
      f"and the rules disagree, and one of them is wrong before any SQL exists")

check("every rule in the vector file has a reference function bound",
      all(r in _ame_fns for r in (_ame.get("rules") or {})),
      f"unbound: {sorted(set(_ame.get('rules') or {}) - set(_ame_fns))} -- a rule with "
      f"no binding is measured against nothing and silently passes")
```

- [ ] **Step 3: Run and confirm green**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$?"
```

- [ ] **Step 4: Prove the checks can fail**

```bash
cp tests/golden_ame_vectors.json /tmp/av.bak
.venv/bin/python - <<'PY'
import json; p="tests/golden_ame_vectors.json"; d=json.load(open(p))
d["rules"]["AME007_line_type"][0]["expected"] = "WRONG"
open(p,"w").write(json.dumps(d, indent=2))
PY
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/av.bak tests/golden_ame_vectors.json
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```

- [ ] **Step 5: Commit**

```bash
git add tests/golden_ame_vectors.json tests/test_accelerator.py
git commit -m "Golden vectors for the AME rules, measured against the Python reference"
```

---

### Task 2: The SQL side, and the parity gate that makes two implementations safe

**Files:**
- Modify: `metadata/source_unions.yml` (add the GIE conform profile)
- Modify: `metadata/entities/csat_invoice_line_gie.yml` (add `conform:` to its binding)
- Create: `checks/ame_parity_check.py`
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Consumes: `tests/golden_ame_vectors.json` from Task 1.
- Produces: conform profile `fieldglass_us_invoice_gie` with `derived_columns` for the 11 GIE payload columns; `checks/ame_parity_check.py` runnable as a `spark_python_task`.

**This is DEF-58.** `csat_invoice_line_gie` declares 11 payload columns and its source, `nhl_invoice_line`, provides **none** of them. `derived_columns` is the mechanism (`factory.py:355`), attached through a `conform:` profile.

- [ ] **Step 1: Read what the source actually provides**

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0,'src')
from accelerator import spec
m=spec.load_model('metadata/entities')
e=m.get('invoice_line')
print('payload:', sorted(e.payload))
print('transaction_key:', list(e.transaction_key))
for s in e.sources:
    print(s.name, 'derived:', [n for n,_ in s.derived_columns])
"
```

Every `derived_columns` expression you write may reference ONLY columns this prints, plus other derived columns defined before it. An expression naming a column that does not exist fails at staging with `UNRESOLVED_COLUMN`, and that failure is invisible to the offline suites — it surfaced on 27 September only in the Spark CI leg.

- [ ] **Step 2: Add the conform profile**

Append to `metadata/source_unions.yml`, in the existing `unions:` list. **No semicolons anywhere in these strings** — `statements()` splits SQL on `;` and a semicolon inside a comment destroyed a whole `CREATE SCHEMA` on 27 September:

```yaml
  # THE GIE DERIVATION -- DEF-58. csat_invoice_line_gie declares 11 payload columns and
  # nhl_invoice_line provides none of them, so they are computed here.
  #
  # THESE EXPRESSIONS ARE A SECOND IMPLEMENTATION OF src/accelerator/invoice_rules.py,
  # and that is permitted ONLY because checks/ame_parity_check.py proves the two agree
  # over tests/golden_ame_vectors.json. That is the same bargain hash_parity_check.py
  # strikes for hashing -- a pure-Python reference and real Spark, measured against
  # golden vectors, as gate zero. Change a rule in one place and the gate goes red.
  - name: fieldglass_us_invoice_gie
    derived_columns:
      line_type: "CASE WHEN UPPER(TRIM(COALESCE(invoice_line_item_type, ''))) = 'TAX' THEN 'TAX' ELSE 'ITEM' END"
      rule_version: "'1.0.0'"
```

Start with exactly these two. `line_type` is AME007 and is the simplest rule with a real branch; `rule_version` is a constant that proves the plumbing. The remaining nine are added in Step 6 once the parity gate exists to catch them.

- [ ] **Step 3: Bind the profile to the satellite**

In `metadata/entities/csat_invoice_line_gie.yml`, add to the `BUSINESS_VAULT_GIE` source, immediately above `bronze_table:`:

```yaml
      conform: fieldglass_us_invoice_gie
```

- [ ] **Step 4: Write the parity check**

`checks/ame_parity_check.py`:

```python
#!/usr/bin/env python3
"""HARD GATE: the AME rules agree between Spark SQL and pure Python.

WHY TWO IMPLEMENTATIONS ARE ALLOWED HERE AT ALL. The rules must run in the pipeline,
where only SQL reaches, and must be unit-testable, where only Python reaches. Rather
than pick one and lose the other, both exist and this gate proves they agree over
tests/golden_ame_vectors.json. It is the same bargain checks/hash_parity_check.py
strikes for hashing, and it fails the same way: loudly, naming the rule and the input.

WITHOUT THIS GATE the SQL and the Python drift silently, and the first symptom is an
invoice that disagrees with the rule somebody authorised.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import invoice_rules  # noqa: E402

VECTORS = ROOT / "tests" / "golden_ame_vectors.json"

#: rule id -> (python callable, SQL template with {placeholders} for inputs)
RULES = {
    "AME007_line_type": (
        lambda i: invoice_rules.line_type(i["source_type"]),
        "CASE WHEN UPPER(TRIM(COALESCE({source_type}, ''))) = 'TAX' "
        "THEN 'TAX' ELSE 'ITEM' END",
    ),
}


def sql_literal(v) -> str:
    """A SQL string literal, with embedded quotes doubled."""
    return "'" + str(v).replace("'", "''") + "'"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="print the SQL without a Spark session")
    args = ap.parse_args()

    vectors = json.loads(VECTORS.read_text(encoding="utf-8"))["rules"]
    covered = [r for r in RULES if r in vectors]
    print(f"{len(covered)} rule(s) with both a SQL form and golden vectors")
    if not covered:
        print("AME PARITY NOT EVALUATED -- no rule has both. A gate that compares "
              "nothing asserts nothing.")
        return 1

    if args.dry_run:
        for rule in covered:
            _py, tmpl = RULES[rule]
            for case in vectors[rule]:
                print("  " + tmpl.format(**{k: sql_literal(v)
                                            for k, v in case["inputs"].items()}))
        return 0

    from pyspark.sql import SparkSession  # noqa: PLC0415
    spark = SparkSession.builder.getOrCreate()

    failures = []
    compared = 0
    for rule in covered:
        py, tmpl = RULES[rule]
        for case in vectors[rule]:
            expr = tmpl.format(**{k: sql_literal(v) for k, v in case["inputs"].items()})
            spark_val = spark.sql(f"SELECT {expr} AS v").collect()[0]["v"]
            try:
                py_val = py(case["inputs"])
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{rule}{case['inputs']}: python raised "
                                f"{type(exc).__name__}, Spark gave {spark_val!r}")
                continue
            compared += 1
            if str(spark_val) != str(py_val):
                failures.append(f"{rule}{case['inputs']}: Spark {spark_val!r} != "
                                f"Python {py_val!r}")

    print(f"{compared} comparison(s) made")
    for f in failures:
        print(f"  FAIL {f}")
    if failures:
        print(f"\nAME PARITY FAILED -- {len(failures)} disagreement(s). The SQL in "
              f"metadata/source_unions.yml and src/accelerator/invoice_rules.py have "
              f"drifted, and an invoice would carry whichever one the pipeline used.")
        return 1
    print(f"\nAME PARITY PASSED: {compared} comparison(s), Spark equals the reference")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Exercise the dry run and the model**

```bash
.venv/bin/python checks/ame_parity_check.py --dry-run
.venv/bin/python -c "
import sys; sys.path.insert(0,'src')
from accelerator import spec
m=spec.load_model('metadata/entities')
b=m.get('invoice_line_gie').sources[0]
print('conform:', b.conform)
print('derived:', [n for n,_ in b.derived_columns])
"
```
Expected: the dry run prints one `CASE WHEN` per AME007 vector, and the model reports `conform: fieldglass_us_invoice_gie` with `line_type` and `rule_version` derived.

- [ ] **Step 6: Add the remaining nine derived columns**

Now that the parity gate exists, add SQL for `description`, `line_description`, `amount`, `invoice_amount`, `accounting_date`, `project_number`, `task_number`, `expenditure_type` and `expenditure_organization`, and extend `RULES` in `checks/ame_parity_check.py` with a SQL template for each rule that has a Python counterpart.

**`invoice_amount` (AME003) is the one that will not fit.** It is the invoice total repeated on every line, so it needs a window or a join rather than a row-local expression, and `invoice_rules.py` has no AME003 function. **STOP and report** rather than inventing one: whether AME003 exists is an open question the spec records, and guessing it is how a wrong number reaches a customer invoice.

- [ ] **Step 7: Write the offline checks**

Insert into `tests/test_accelerator.py` before the summary block:

```python
print("\n== the GIE derivation, and the parity that keeps its two forms honest ==")
_gie = _m2.get("invoice_line_gie")
_gie_src = _gie.sources[0]
_gie_derived = {n for n, _ in _gie_src.derived_columns}
_gie_want = set(_gie_src.payload or _gie.payload)

check("the GIE binding names a conform profile, without which it derives nothing",
      bool(_gie_src.conform),
      "csat_invoice_line_gie declares 11 payload columns and nhl_invoice_line provides "
      "none of them; derived_columns is the only mechanism that closes that gap")

check("no GIE payload column is left underived",
      not (_gie_want - _gie_derived),
      f"underived: {sorted(_gie_want - _gie_derived)} -- a payload column with no "
      f"expression is projected from a source column that does not exist, which fails "
      f"at staging with UNRESOLVED_COLUMN and is invisible to this suite")

check("no derived column is declared that the payload does not want",
      not (_gie_derived - _gie_want),
      f"extra: {sorted(_gie_derived - _gie_want)} -- computed and discarded")

_apc = (ROOT / "checks" / "ame_parity_check.py").read_text(encoding="utf-8")
check("every rule the parity gate covers has golden vectors, and vice versa",
      all(r in (_ame.get("rules") or {}) for r in re.findall(r'"(AME[\w]+)":', _apc)),
      "a SQL form with no vectors is unmeasured, and vectors with no SQL form mean the "
      "gate silently skips the rule")

check("the parity gate refuses to pass when it compared nothing",
      "NOT EVALUATED" in _apc and "return 1" in _apc,
      "a gate that finds no comparable rule must fail, not report success over an "
      "empty set")
```

- [ ] **Step 8: Run, then prove the parity check can fail**

```bash
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "tests exit=$?"
cp metadata/source_unions.yml /tmp/su.bak
sed -i "s/THEN 'TAX' ELSE 'ITEM' END/THEN 'TAXX' ELSE 'ITEM' END/" metadata/source_unions.yml
echo "  (SQL now disagrees with Python; the parity gate must catch it in Spark CI)"
cp /tmp/su.bak metadata/source_unions.yml
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```

The parity gate itself needs Spark. If a JDK is available (`~/.jdk/jdk-17.0.20.1+1`), run it for real and capture the FAIL:

```bash
JH=~/.jdk/jdk-17.0.20.1+1
JAVA_HOME="$JH" PATH="$JH/bin:$PATH" uv run --frozen --python 3.11 --extra spark \
  python checks/ame_parity_check.py
```

- [ ] **Step 9: Commit**

```bash
git add metadata/source_unions.yml metadata/entities/csat_invoice_line_gie.yml \
        checks/ame_parity_check.py tests/test_accelerator.py
git commit -m "DEF-58: derive the GIE columns in SQL, with a parity gate against the Python rules"
```

---

### Task 3: The issuance ledger

**Files:**
- Modify: `checks/invoice_issue.py` (it is a NOT IMPLEMENTED stub)
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Produces: rows in `control.ctl_invoice_issuance`, one per invoice line, carrying the issued invoice date (AME002) and line number (AME006).
- Consumes: `csat_invoice_line_gie` from Task 2.

**The rule this task exists to enforce:** an issued value is issued ONCE. A re-run returns what was issued before, never a recomputation. `metadata/entities/csat_invoice_line_gie.yml` states why — a changed invoice date means the customer's copy no longer matches ours, every load succeeds, every gate stays green, and the first symptom is a dispute.

- [ ] **Step 1: Read the stub and the table it writes**

```bash
sed -n '1,40p' checks/invoice_issue.py
grep -n -A12 "ctl_invoice_issuance" governance/control_objects.sql
```

Use the columns that DDL already declares. Do not add columns to it in this task.

- [ ] **Step 2: Write the failing checks**

Insert into `tests/test_accelerator.py` before the summary block:

```python
print("\n== issued values are issued once ==")
_ii = (ROOT / "checks" / "invoice_issue.py").read_text(encoding="utf-8")

check("invoice_issue no longer reports itself NOT IMPLEMENTED",
      "NOT IMPLEMENTED" not in _ii,
      "the stub printed a notice and exited 0, which is why the load was green while "
      "issuing nothing")

check("invoice_issue writes only rows that do not already exist",
      ("NOT EXISTS" in _ii.upper() or "LEFT ANTI" in _ii.upper()),
      "an issued value is issued ONCE. Re-issuing on a re-run changes an invoice date "
      "that a customer already has, and nothing fails")

check("invoice_issue never recomputes an existing issued value",
      "UPDATE" not in _ii.upper() and "MERGE" not in _ii.upper(),
      "an UPDATE or MERGE against the issuance ledger is how a frozen value thaws")

check("invoice_issue reads AME002 and AME006 from nothing derived",
      "invoice_rules" not in _ii,
      "AME002 and AME006 are deliberately absent from invoice_rules.py. Importing it "
      "here is the first step toward deriving what must be issued")
```

- [ ] **Step 3: Run to verify they fail**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -4
```
Expected: the NOT IMPLEMENTED check is red.

- [ ] **Step 4: Implement**

Replace the stub's `main()` with argv parsing for the parameters `resources/vault_job.yml` already passes, and an anti-join insert: read the GIE satellite's current rows, left-anti-join against `control.ctl_invoice_issuance` on the invoice-line key, and insert only the rows with no existing issuance. Print the count issued and the count already present — a run that issues nothing must say so, not look idle.

Import pyspark lazily inside `main()` so `--dry-run` works with no JVM.

- [ ] **Step 5: Run and confirm green**

```bash
.venv/bin/python checks/invoice_issue.py --dry-run --catalog 02_usnc_silver_edm_dev \
  --control-schema control --business-vault-schema business_vault
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "tests exit=$?"
```

- [ ] **Step 6: Prove the re-issue guard can fail**

```bash
cp checks/invoice_issue.py /tmp/ii.bak
sed -i 's/NOT EXISTS/EXISTS/' checks/invoice_issue.py
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/ii.bak checks/invoice_issue.py
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```

- [ ] **Step 7: Commit**

```bash
git add checks/invoice_issue.py tests/test_accelerator.py
git commit -m "Issue AME002 and AME006 once, and never recompute them"
```

---

### Task 4: The declared mapping, held to the DCDD in both directions

**Files:**
- Create: `metadata/workday/mapping_customer_invoice.yml`
- Create: `src/accelerator/wd_mapping.py`
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Produces: `wd_mapping.load_mapping(path) -> dict[str, dict]` — DCDD field name to `{"csv_file": str, "source": str, "rule": str | None, "money": bool}`.
- Produces: `wd_mapping.dcdd_populated_fields(xlsx_path) -> dict[str, dict]` — the populated DCDD rows, read with `tools/_xlsx.py`.

- [ ] **Step 1: Write the module**

`src/accelerator/wd_mapping.py` — pure, no pyspark:

```python
"""The mapping from vault columns to DCDD fields, and the DCDD's own view of itself.

THE MAPPING LIVES HERE, NOT IN THE WORKBOOK. metadata/workday/dcdd/*.xlsx are committed
verbatim with sha256 digests in PROVENANCE.json. Editing one breaks the record of which
bytes a mapping came from, and a binary is not reviewable in a diff.

BOTH DIRECTIONS ARE CHECKED, because one alone is worthless: "every DCDD field has a
mapping" misses a mapping entry pointing at a field that does not exist, and "every
mapping entry names a real field" misses a field nobody mapped. Together a drifting
mapping goes red instead of exporting a blank column.
"""
from __future__ import annotations

from pathlib import Path

import yaml


def load_mapping(path: Path) -> dict[str, dict]:
    """DCDD field name -> its mapping entry."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(raw.get("fields") or {})


def dcdd_populated_fields(xlsx_path: Path, sheet: str) -> dict[str, dict]:
    """The DCDD rows that are NOT 'Do Not Populate', keyed by WD Field Name."""
    import sys  # noqa: PLC0415
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
    from _xlsx import load  # noqa: PLC0415

    rows = load(str(xlsx_path))[sheet]
    header = {v: k for k, v in rows[0][1].items()}
    out: dict[str, dict] = {}
    for _r, cells in rows[1:]:
        get = lambda key: cells.get(header.get(key, ""), "")  # noqa: E731
        if get("Required/Optional") in ("Do Not Populate", ""):
            continue
        name = get("WD Field Name")
        if name:
            out[name] = {
                "csv_file": get("csv File Name"),
                "required": get("Required/Optional"),
                "validations": [v.strip() for v in (get("VALIDATIONS") or "").split("\n")
                                if v.strip()],
            }
    return out
```

- [ ] **Step 2: Author the mapping file**

`metadata/workday/mapping_customer_invoice.yml`. Start with the five must-populate fields — the rest are added incrementally and the checks tell you what is missing:

```yaml
# VAULT COLUMN -> DCDD FIELD, for Submit_Customer_Invoice.
#
# The DCDD is read-only: metadata/workday/dcdd/Submit_Customer_Invoice_DCDD.xlsx is
# committed verbatim with a sha256 digest. This file is where the mapping lives.
#
# `money: true` marks a field whose value comes from a masked column. The export refuses
# to write a file whose money fields are null on every row, because the run-as service
# principal cannot read masked money until PLT-2 lands -- and a CSV of NULL amounts looks
# exactly like a correct one.
fields:
  Customer_Invoice_ID:
    csv_file: All CSVs
    source: control.ctl_invoice_issuance.invoice_id
    money: false
  Customer_Invoice_Line_Reference_ID:
    csv_file: Submit_Customer_Invoice_Lines
    source: control.ctl_invoice_issuance.line_reference
    money: false
  Company_Reference_ID:
    csv_file: Submit_Customer_Invoice
    source: hub_organisation.reference_id
    money: false
  Customer_Reference_ID:
    csv_file: Submit_Customer_Invoice
    source: hub_invoice.buyer_tenant
    money: false
  Submit:
    csv_file: Submit_Customer_Invoice
    source: "constant:1"
    money: false
```

- [ ] **Step 3: Write the failing checks**

Insert into `tests/test_accelerator.py` before the summary block:

```python
print("\n== the customer-invoice mapping agrees with the DCDD, both ways ==")
from accelerator import wd_mapping as _wm  # noqa: E402

try:
    _map = _wm.load_mapping(ROOT / "metadata" / "workday" /
                            "mapping_customer_invoice.yml")
    _dcdd = _wm.dcdd_populated_fields(
        ROOT / "metadata" / "workday" / "dcdd" / "Submit_Customer_Invoice_DCDD.xlsx",
        "Submit_Customer_Invoice_DCDD")
    _map_err = ""
except Exception as _e:  # noqa: BLE001
    _map, _dcdd, _map_err = {}, {}, f"{type(_e).__name__}: {_e}"

check("the mapping and the DCDD both load",
      not _map_err and bool(_map) and bool(_dcdd),
      f"{_map_err} -- the two checks below compare nothing if either is empty")

check("no mapping entry names a field the DCDD does not contain",
      not (set(_map) - set(_dcdd)),
      f"unknown fields: {sorted(set(_map) - set(_dcdd))[:6]} -- a typo here exports a "
      f"column Workday will not recognise")

_must = {n for n, d in _dcdd.items()
         if d["required"] in ("Required", "Design Requirement", "Constant Value")}
check("every field the DCDD says MUST be populated has a mapping entry",
      not (_must - set(_map)),
      f"unmapped must-populate fields: {sorted(_must - set(_map))} -- these are the five "
      f"the DCDD refuses to load without")

check("every mapping entry declares whether its source is money",
      all("money" in e for e in _map.values()),
      f"missing the money flag: "
      f"{sorted(n for n, e in _map.items() if 'money' not in e)} -- the export cannot "
      f"refuse a null-money file if it does not know which fields are money")
```

- [ ] **Step 4: Run, then prove both directions fail**

```bash
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "tests exit=$?"
cp metadata/workday/mapping_customer_invoice.yml /tmp/mp.bak
.venv/bin/python - <<'PY'
import yaml; p="metadata/workday/mapping_customer_invoice.yml"
d=yaml.safe_load(open(p)); d["fields"]["Not_A_Real_Field"]={"csv_file":"x","source":"y","money":False}
open(p,"w").write(yaml.safe_dump(d, sort_keys=False))
PY
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/mp.bak metadata/workday/mapping_customer_invoice.yml

.venv/bin/python - <<'PY'
import yaml; p="metadata/workday/mapping_customer_invoice.yml"
d=yaml.safe_load(open(p)); d["fields"].pop("Company_Reference_ID")
open(p,"w").write(yaml.safe_dump(d, sort_keys=False))
PY
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/mp.bak metadata/workday/mapping_customer_invoice.yml
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```

Both mutations must go red, and they must go red on DIFFERENT checks. If one check catches both, the pair is not bidirectional.

- [ ] **Step 5: Commit**

```bash
git add metadata/workday/mapping_customer_invoice.yml src/accelerator/wd_mapping.py \
        tests/test_accelerator.py
git commit -m "Declare the customer-invoice mapping, held to the DCDD in both directions"
```

---

### Task 5: The CSV writer and the join contract

**Files:**
- Modify: `checks/invoice_export.py` (it is a NOT IMPLEMENTED stub)
- Modify: `resources/vault_job.yml` (add the export volume parameter)
- Modify: `databricks.yml` (declare `gold_export_volume`, no default)
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Consumes: `wd_mapping.load_mapping` from Task 4, the issuance ledger from Task 3.
- Produces: four CSV files under the configured volume path.

- [ ] **Step 1: Declare the volume variable**

In `databricks.yml`, beside `gold_export_schema`:

```yaml
  gold_export_volume:
    description: >-
      UC Volume path the Workday export writes its CSV file set to, for Snaplogic to
      collect. No default: a wrong-but-plausible default is how a file lands somewhere
      nobody is watching, and an unset one fails at configuration time naming the target.
```

Add a value to each target that already sets `gold_export_schema`.

- [ ] **Step 2: Write the failing checks**

Insert into `tests/test_accelerator.py` before the summary block:

```python
print("\n== the export writes a SET of files, joined by two keys ==")
_ie = (ROOT / "checks" / "invoice_export.py").read_text(encoding="utf-8")

check("invoice_export no longer reports itself NOT IMPLEMENTED",
      "NOT IMPLEMENTED" not in _ie,
      "the stub printed a notice and exited 0, which is why the load was green while "
      "writing nothing to gold")

check("the export asserts the join contract before writing",
      "Customer_Invoice_ID" in _ie and "Customer_Invoice_Line_Reference_ID" in _ie,
      "Customer_Invoice_ID appears in every CSV and Customer_Invoice_Line_Reference_ID "
      "in both line files. They are what make the four files a set rather than four "
      "unrelated files, and an orphan on either side fails at Workday, not here")

check("the export takes its destination from a variable, not a literal path",
      "--export-volume" in _ie,
      "a hardcoded path is how a file lands in the wrong place on the wrong target")

_vj = yaml.safe_load((ROOT / "resources" / "vault_job.yml").read_text(encoding="utf-8"))
_vj_params = str((((_vj.get("resources") or {}).get("jobs") or {})
                  .get("vault_load") or {}).get("tasks"))
check("vault_job passes the export volume to invoice_export",
      "${var.gold_export_volume}" in _vj_params,
      "the task cannot write where nobody told it to write")
```

- [ ] **Step 3: Implement the writer**

Replace the stub's `main()`. Read the mapping, project each CSV file's columns from the GIE satellite joined to the issuance ledger, and BEFORE writing:

- assert every `Customer_Invoice_ID` in a line file exists in the header file
- assert every header row has at least one line row
- assert `Customer_Invoice_Line_Reference_ID` is non-null and unique within its invoice

Fail with the offending ids. Only then write the four files. Import pyspark lazily.

- [ ] **Step 4: Run and prove the join contract check can fail**

```bash
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "tests exit=$?"
cp checks/invoice_export.py /tmp/ie.bak
sed -i 's/Customer_Invoice_Line_Reference_ID/Line_Ref_Placeholder/g' checks/invoice_export.py
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -2
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/ie.bak checks/invoice_export.py
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```

- [ ] **Step 5: Commit**

```bash
git add checks/invoice_export.py resources/vault_job.yml databricks.yml \
        tests/test_accelerator.py
git commit -m "Write the customer-invoice CSV set, and assert the join contract first"
```

---

### Task 6: Validation, and the two refusals

**Files:**
- Create: `src/accelerator/dcdd_validation.py`
- Modify: `checks/invoice_export.py`
- Modify: `tests/test_accelerator.py` (insert before the summary block)

**Interfaces:**
- Consumes: `wd_mapping.dcdd_populated_fields` from Task 4.
- Produces: `dcdd_validation.problems(rows, fields) -> list[str]` — every violation, named by field, rule and row.

This task owns Review Focus items 4 and 5.

- [ ] **Step 1: Write the validators**

`src/accelerator/dcdd_validation.py` — pure, no pyspark. Implement the rules the populated set actually uses: `ISNUMERIC` (12 fields), `DATEFORMATCHECK` (6), `MISSINGVALUE` (4), `CHECKBOOLEAN` (4), `MATCHVALUE_COMPNY` (3), `MATCHVALUE_COSTC` (2). `CHECKREFERENCES` (27) is handled in Step 3 because it cannot be satisfied locally.

Each returns a problem string naming the field, the rule and the offending value. Return EVERY violation, not the first: an operator fixing one at a time needs to know how many there are.

- [ ] **Step 2: Write the failing checks for the validators**

```python
print("\n== DCDD validation runs before anything is written ==")
from accelerator import dcdd_validation as _dv  # noqa: E402

check("ISNUMERIC rejects a non-numeric and accepts a numeric",
      _dv.problems([{"Amount": "abc"}], {"Amount": {"validations": ["ISNUMERIC"]}})
      and not _dv.problems([{"Amount": "12.34"}],
                           {"Amount": {"validations": ["ISNUMERIC"]}}),
      "a validator that accepts everything is theatre, and one that rejects everything "
      "blocks every load")

check("MISSINGVALUE rejects blank and whitespace-only",
      _dv.problems([{"F": ""}], {"F": {"validations": ["MISSINGVALUE"]}})
      and _dv.problems([{"F": "   "}], {"F": {"validations": ["MISSINGVALUE"]}}),
      "a whitespace-only required field is missing, not present")

check("problems() reports EVERY violation, not just the first",
      len(_dv.problems([{"A": "x", "B": "y"}],
                       {"A": {"validations": ["ISNUMERIC"]},
                        "B": {"validations": ["ISNUMERIC"]}})) == 2,
      "stopping at the first violation means an operator fixes them one round trip at "
      "a time")
```

- [ ] **Step 3: The two refusals**

Add to `checks/invoice_export.py`, before any file is written:

```python
# PLT-2. The run-as service principal is NOT in
# global_dataplatform_pipeline_job_runners, which every mask function in
# governance/apply_masks.sql admits by name, so every masked money column it reads
# returns NULL. Measured 27 September 2026. A CSV of NULL amounts is indistinguishable
# from a correct one, so this refuses rather than writes.
money_fields = [n for n, e in mapping.items() if e.get("money")]
if money_fields and rows:
    all_null = [f for f in money_fields
                if all(r.get(f) in (None, "") for r in rows)]
    if all_null:
        print(f"EXPORT REFUSED -- {len(all_null)} money field(s) are NULL on every one "
              f"of {len(rows)} row(s): {sorted(all_null)[:6]}. The load identity cannot "
              f"read masked money until PLT-2 lands. Writing this file would ship an "
              f"invoice with no amounts and report success.")
        return 1
```

And for references:

```python
# SUBSYSTEM E. hub_wd_reference exists but is empty: its WORKDAY binding is inactive
# because 01_usnc_bronze_dev has no workday schema. A CHECKREFERENCES pass against an
# empty table is not a pass -- it is the absence of anything to contradict you.
if checkreferences_fields and reference_row_count == 0:
    print(f"EXPORT REFUSED -- {len(checkreferences_fields)} field(s) carry "
          f"CHECKREFERENCES and hub_wd_reference holds 0 rows. Validating against an "
          f"empty reference table passes for the wrong reason.")
    return 1
```

**Neither refusal takes a flag.** A flag is how a temporary suppression becomes permanent.

- [ ] **Step 4: Write the checks that pin the refusals**

```python
_ie2 = (ROOT / "checks" / "invoice_export.py").read_text(encoding="utf-8")

check("the export refuses a file whose money fields are null on every row",
      "EXPORT REFUSED" in _ie2 and "PLT-2" in _ie2,
      "the PLT-2 symptom is a complete, successful-looking CSV with no amounts in it")

check("the export refuses to claim a CHECKREFERENCES pass against an empty table",
      "CHECKREFERENCES" in _ie2 and "0 rows" in _ie2,
      "passing because there is nothing to contradict you is not passing")

check("neither refusal is suppressible by a flag",
      "--allow-null-money" not in _ie2 and "--skip-validation" not in _ie2
      and "--force" not in _ie2,
      "a flag is how a temporary suppression becomes permanent")
```

- [ ] **Step 5: Prove the refusals can fail**

```bash
cp checks/invoice_export.py /tmp/ie.bak
sed -i 's/EXPORT REFUSED/export proceeding/' checks/invoice_export.py
.venv/bin/python tests/test_accelerator.py 2>&1 | grep -E "^  FAIL" | head -3
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "exit=$? (must be 1)"
cp /tmp/ie.bak checks/invoice_export.py
.venv/bin/python tests/test_accelerator.py >/dev/null 2>&1; echo "restored exit=$?"
```

- [ ] **Step 6: Final full run and commit**

```bash
.venv/bin/python tests/test_accelerator.py 2>&1 | tail -2
.venv/bin/python verify_repo.py 2>&1 | tail -2
git add src/accelerator/dcdd_validation.py checks/invoice_export.py tests/test_accelerator.py
git commit -m "Validate before writing, and refuse the two files we cannot stand behind"
```

---

## Deploying and running

Not a task: it touches a live workspace and is the repo owner's call.

```bash
.venv/bin/python checks/preflight_target.py --target usnc_tds --profile hfig-usnc-tds
timeout 600 databricks bundle deploy -t usnc_tds --profile hfig-usnc-tds \
  --var="service_principal=7732b208-8366-4aef-af09-60e9dec9cf86"
```

**Expect the export to REFUSE on its first live run.** PLT-2 is unsent and `hub_wd_reference` is empty. That refusal is the plan working, not failing.

## What this plan does NOT do

- **The other four objects.** Supplier, Customer, Accounting Journal and Supplier Invoice are the same machinery pointed at a different DCDD and mapping file.
- **AME003.** `invoice_amount` needs a join or window and has no Python counterpart. Task 2 Step 6 stops and reports rather than inventing one.
- **Anything that calls Workday.** This produces files. Snaplogic collects them and Logan loads them through DT/DTS.
- **Moving `invoice_export` into `gold_build`.** It stays in `vault_load` until it does real work.
