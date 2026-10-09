#!/usr/bin/env python3
"""HARD GATE: the AME rules agree between Spark SQL and pure Python.

WHY TWO IMPLEMENTATIONS ARE ALLOWED HERE AT ALL. The rules must run in the pipeline,
where only SQL reaches, and must be unit-testable, where only Python reaches. Rather
than pick one and lose the other, both exist and this gate proves they agree over
tests/golden_ame_vectors.json. It is the same bargain checks/hash_parity_check.py
strikes for hashing, and it fails the same way: loudly, naming the rule and the input.

WITHOUT THIS GATE the SQL and the Python drift silently, and the first symptom is an
invoice that disagrees with the rule somebody authorised.

THE TEMPLATES IN `RULES` ARE THE PIPELINE'S OWN SQL, NOT A COPY OF IT. Each one is a
format string over the rule's INPUT NAMES. This gate substitutes SQL literals to compare
one value against invoice_rules.py; metadata/source_unions.yml's fieldglass_us_invoice_gie
profile substitutes nhl_invoice_line's COLUMN NAMES to compute the same rule over rows.
tests/test_accelerator.py asserts that the profile's expressions are exactly these
templates so composed -- so a template edited here and not there fails offline, and a
gate measuring SQL the pipeline does not run cannot exist.

A REFUSAL IS A NULL, AND THAT IS THE DECLARED CORRESPONDENCE. invoice_rules raises where
a value cannot be determined -- an empty task code (AME010 makes it required), an unknown
module (AME012/AME013 refuse to guess which of three modules coded a line). SQL has no
exceptions, so every one of those branches produces NULL, and this gate fails if the SQL
returns a VALUE where the reference refuses one. That is the case a naive parity gate
passes by accident: NULL compared as the string 'None' against a Python exception nobody
caught. The vault then carries NULL, gold's required-field gate sees it, and the line is
routed to exceptions rather than invoiced.
"""
from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define __file__,
# so every Path(__file__) below would raise NameError and the gate would die before
# asserting anything. compile() still records the real path in the code object.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from accelerator import invoice_rules, sql_text  # noqa: E402

VECTORS = ROOT / "tests" / "golden_ame_vectors.json"

#: rule id -> (python callable over the vector's `inputs`, SQL template over the same
#: input names). The template's placeholders are the rule's INPUTS, never column names.
RULES = {
    "AME007_line_type": (
        lambda i: invoice_rules.line_type(i["source_type"]),
        # TRIM IS NOT .strip() AND THE DIFFERENCE INVOICES A TAX LINE AS AN ITEM.
        # Spark's TRIM removes SPACES only; Python's str.strip() removes all
        # whitespace. Measured: line_type('tax\n') is 'TAX' in Python and would be
        # 'ITEM' through TRIM, on a column a CSV-shaped feed can easily deliver with a
        # trailing newline. The golden vectors now carry '\t', '\n', '\r' and ' tax '
        # so this cannot regress silently.
        #
        # THE DOUBLED BACKSLASH IS LOAD-BEARING. Spark SQL string literals process
        # backslash escapes, so '^\s+|\s+$' degrades to the regex ^s+|s+$ -- which
        # strips the LETTER s. Measured: REGEXP_REPLACE('sales', '^\s+|\s+$', '')
        # returns 'ale'. It reads as correct, it changes no vector that has no leading
        # or trailing s, and it is why AME011 carries '|sales'.
        "CASE WHEN UPPER(REGEXP_REPLACE(COALESCE({source_type}, ''), "
        r"'^\\s+|\\s+$', '')) = 'TAX' THEN 'TAX' ELSE 'ITEM' END",
    ),
    "AME011_task_number": (
        lambda i: invoice_rules.task_number(i["task_code"]),
        # THE TWO REGEXES ARE NOT INTERCHANGEABLE WITH THE OBVIOUS SQL, and this is the
        # finding Task 1 measured rather than reasoned about:
        #
        #   task_number('ABC|123')  -> '123'      the value after the FIRST pipe
        #   task_number('A|B|C')    -> 'B|C'      split on the first pipe ONLY
        #   task_number('ABC(123)') -> 'ABC(123'  rstrip(')') strips TRAILING ')' only
        #
        # `^[^|]*[|]` deletes everything up to and including the FIRST pipe, and matches
        # nothing when there is no pipe -- which is what returns the whole code, per the
        # docstring's reading of "parse the value after |". A greedy `.*[|]` would split
        # on the LAST pipe and turn 'A|B|C' into 'C'.
        #
        # `[)]+$` strips only TRAILING parentheses. REPLACE(x, ')', '') and a
        # balanced-pair regex both read as correct in review and both turn 'ABC(123)'
        # into 'ABC(123)' minus the wrong characters -- REPLACE gives 'ABC(123', which
        # happens to agree here, but gives 'AB123' for 'AB)123)' where Python gives
        # 'AB)123'. The character class avoids a backslash, so nothing depends on how
        # YAML and the Spark string literal each treat one.
        #
        # An EMPTY code raises in Python and is NULL here -- see the module docstring.
        # THE FOUR STEPS, IN invoice_rules.task_number's OWN ORDER: strip the whole
        # code, refuse an empty one, take the value after the FIRST pipe, strip
        # trailing ')', strip again. Reordering them changes the answer --
        # task_number('  Task | 42 )  ') is '42', and that one vector exercises all of
        # it. See the AME007 note on why TRIM is not str.strip() and why '\\s' is
        # doubled.
        "CASE WHEN REGEXP_REPLACE(COALESCE({task_code}, ''), "
        r"'^\\s+|\\s+$', '') = '' THEN NULL ELSE "
        "REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("
        r"COALESCE({task_code}, ''), '^\\s+|\\s+$', ''), '^[^|]*[|]', ''), "
        r"'[)]+$', ''), '^\\s+|\\s+$', '') END",
    ),
    "AME012_AME013_module_value": (
        lambda i: invoice_rules.module_value(
            i["line_item_type"], i["ts"], i["es"], i["mi"]),
        # An UNKNOWN module is NULL, never one of the three values. Defaulting to TS
        # would put the timesheet module's expenditure coding on an expense line, which
        # reconciles and is wrong -- the reason the Python raises instead of choosing.
        "CASE UPPER(REGEXP_REPLACE(COALESCE({line_item_type}, ''), "
        r"'^\\s+|\\s+$', '')) "
        "WHEN 'TS' THEN {ts} WHEN 'ES' THEN {es} WHEN 'MI' THEN {mi} ELSE NULL END",
    ),
}

#: rule id -> why it has golden vectors but NO SQL form, and therefore is not compared.
#:
#: DECLARED RATHER THAN SKIPPED. A rule the gate simply does not mention is a rule the
#: gate silently ignores, and the summary line looks identical either way. Naming it
#: here, and failing on any vector rule that is in neither dict, makes the omission a
#: statement somebody wrote down.
NOT_IN_SQL = {
    "AME004_AME005_worker_name": (
        "DEF-58 is only partly closeable. worker_name feeds AME004 description and "
        "AME005 line_description, and nhl_invoice_line carries NO worker name columns "
        "-- it carries worker_hk, a BINARY(32) of the worker id. csat_invoice_line_gie "
        "reads that one table and derived_columns cannot join, so neither payload "
        "column can be computed and no SQL form of this rule exists to measure. "
        "Writing one anyway would assert parity for an implementation the pipeline "
        "never runs. See the underived list in metadata/source_unions.yml."
    ),
}


def sql_literal(v) -> str:
    """A SQL literal, preserving the NULL/blank distinction.

    NULL IS NOT THE STRING 'None', and this is the accident the gate must not have.
    str(None) renders 'None', which Spark reads as a four-character value: AME007 would
    then map it to ITEM and agree with Python for the wrong reason, and AME011 would
    return 'None' where Python raises. Every vector carrying a null input would be
    measuring something other than the null it declares.

    THE ESCAPE COMES FROM accelerator.sql_text, AND IT IS NOT THE ANSI ONE. This line
    doubled the apostrophe until 28 September 2026. Spark does not read `''` as an escape
    in an expression: `SELECT 'O''Brien'` parses as two adjacent literals and returns
    `OBrien`, measured in Spark 3.5.9 -- so the first vector value containing an
    apostrophe would have been compared against a DIFFERENT value than the one the vector
    declares, and this gate would have reported a rule mismatch caused by its own
    rendering. tests/golden_ame_vectors.json already documents parens, pipes and
    whitespace as edge cases; an apostrophe is the obvious next one to add, and whoever
    adds it will not be looking here.
    """
    if v is None:
        return "CAST(NULL AS STRING)"
    return sql_text.literal(str(v))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="print the SQL without a Spark session")
    args = ap.parse_args()

    vectors = json.loads(VECTORS.read_text(encoding="utf-8"))["rules"]
    covered = [r for r in RULES if r in vectors]

    # A RULE WITH A SQL FORM AND NO CASES IS NOT COVERED, IT IS SKIPPED.
    # `covered` used to drop a rule absent from the vector file silently, and the loop
    # below simply never executes for a rule whose case list is empty -- so the gate
    # printed PASSED with a lower comparison count and nothing said which rule had
    # stopped being measured. A floor asserted in a different suite is not this gate
    # defending itself: the gate has to refuse.
    unmeasured = sorted(
        [f"{r} (no such rule in the vector file)" for r in RULES if r not in vectors]
        + [f"{r} (rule present but its case list is empty)"
           for r in RULES if not vectors.get(r)])
    if unmeasured:
        print(f"\nAME PARITY FAILED -- {unmeasured} have a SQL form that is measured "
              f"against nothing. A rule the gate cannot compare must fail, not lower "
              f"the comparison count and still report PASSED.")
        return 1

    # A VECTOR RULE IN NEITHER DICT IS A SILENT SKIP, so it fails here instead.
    unaccounted = sorted(set(vectors) - set(RULES) - set(NOT_IN_SQL))
    for rule in sorted(NOT_IN_SQL):
        print(f"  not compared: {rule} -- {NOT_IN_SQL[rule]}")
    print(f"{len(covered)} rule(s) with both a SQL form and golden vectors")
    if unaccounted:
        print(f"\nAME PARITY FAILED -- {unaccounted} have golden vectors but neither a "
              f"SQL form in RULES nor a recorded reason in NOT_IN_SQL. A rule the gate "
              f"does not mention is a rule the gate does not measure, and the summary "
              f"line reads the same either way.")
        return 1
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
            inputs = case["inputs"]
            expr = tmpl.format(**{k: sql_literal(v) for k, v in inputs.items()})
            spark_val = spark.sql(f"SELECT {expr} AS v").collect()[0]["v"]
            try:
                py_val, py_raised = py(inputs), ""
            except Exception as exc:  # noqa: BLE001
                py_val, py_raised = None, type(exc).__name__
            compared += 1

            if py_raised:
                if spark_val is not None:
                    failures.append(
                        f"{rule}{inputs}: Python raised {py_raised} but Spark returned "
                        f"{spark_val!r} -- the SQL invented a value where the reference "
                        f"refuses one")
            elif spark_val is None or str(spark_val) != str(py_val):
                failures.append(f"{rule}{inputs}: Spark {spark_val!r} != "
                                f"Python {py_val!r}")

            # AND AGAINST THE VECTORS THEMSELVES, not only against each other. Two
            # implementations can agree and both be wrong; the vector file is what
            # says which answer is right.
            if "raises" in case:
                if spark_val is not None:
                    failures.append(
                        f"{rule}{inputs}: the vectors declare {case['raises']}, Spark "
                        f"returned {spark_val!r} rather than NULL")
            elif spark_val is None or str(spark_val) != str(case["expected"]):
                failures.append(f"{rule}{inputs}: Spark {spark_val!r} != "
                                f"vector {case['expected']!r}")

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
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure. Falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
