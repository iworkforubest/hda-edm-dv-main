"""HARD GATE: does a layer's control schema match the published standard?

WHY A CHECK AND NOT GENERATED DDL -- AND WHAT CHANGED FOR BRONZE ON 31 AUG 2026. This said
"Bronze's catalog belongs to another team ... we do not deploy into it". The first half is still
true; the second is not. Measured 31 Aug: the deploying identity holds CREATE_SCHEMA on
01_usnc_bronze_dev through scope_tds_full_scopes_write, so bronze's control DDL is now generated
(governance/control_objects_bronze.sql) and was applied, and this gate verifies OUR objects there
rather than someone else's.

THE DISTINCTION THAT SURVIVES, and it is the important one: we create the CONTAINERS, Bronze
writes the ROWS. control_contracts/bronze.yaml is still the contract, and it is a contract for
the rows -- a delivery manifest records what the delivering system delivered, and only Bronze
knows that. Creating a table is not writing to it. So this gate can report bronze CONFORMANT
while loop-1 stays dormant, and that is not a contradiction.

Gold's DDL is generated because that catalog is ours, and silver's is verified against the
declaration because it already exists and works.

THE DECISION FUNCTIONS ARE PURE AND SPARK-FREE, like offending(), unauthorised_table_readers(),
undeclared_schemas() and misplaced_control_objects() in schema_grant_check.py. That is what
makes the FAILING case testable offline against fabricated rows rather than only against a live
lake -- a gate whose red path can only be seen in production is a gate nobody exercises. That
includes the append-only MARKER decision: append_only_tables_from_properties() takes already-
collected TBLPROPERTIES rows and decides which tables carry delta.appendOnly = 'true', the same
shape as control_object_rows()'s has_marker in schema_grant_check.py -- main() collects the live
rows first and decides after, rather than filtering a live DataFrame, so the decision itself
never touches Spark.

NOT-INSTRUMENTED IS A REPORTED STATE, NOT A FAILURE. Gold has no catalog as of 31 Aug 2026 and
that is expected for some time. Bronze's control schema did NOT exist when this was written and
now does -- created 31 Aug, and reported conformant -- so gold is the only remaining example of
the not-instrumented case. A layer that has built nothing
is reported as absent so an operator sees where to start; it does not fail the build, because
failing on the normal case trains people to ignore the gate.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import control_standard as cs  # noqa: E402

GATE = "control_conformance"

# WHAT TO CITE, PER LAYER. --layer accepts all three (see main()'s argparse choices below),
# but only bronze gets a published CONTRACT: gold's DDL is generated and silver's is
# verified against the declaration rather than published (see this module's own docstring
# and tools/emit_control_contract.py's). Pointing every layer's message at
# control_contracts/{layer}.yaml cited a file that will never exist for two of the three --
# run this against silver or gold and every message named a document nobody will ever write.
# This names what actually exists for each layer instead.
LAYER_REFERENCE: dict[str, str] = {
    # BRONZE NAMES THE DDL NOW, not only the contract. The contract asks Bronze for the ROWS
    # and still stands; but a bronze non-conformance is now OURS to fix by re-applying
    # generated DDL, so the message must send the reader to the thing they would actually
    # change. Updated 31 Aug with the ownership of the containers.
    # The value must stay a BARE PATH: a check asserts every cited reference file exists,
    # and it caught a parenthetical added here on 31 Aug. The contract for the ROWS is
    # control_contracts/bronze.yaml, named in the docstring above rather than here.
    "bronze": "governance/control_objects_bronze.sql",
    "silver": "governance/control_objects.sql",
    "gold": "governance/control_objects_gold.sql",
}


def conformance(layer: str, deployed: dict) -> list:
    """Problems for a layer whose control schema does not match the standard.

    ABSENT and WRONG-COLUMNS are reported differently on purpose: a team that has built nothing
    needs 'start here', and a team that built it slightly wrong needs 'you are close'. Reporting
    both as 'non-conforming' tells neither of them what to do.
    """
    want = cs.tables_for(layer)
    problems = []
    for table in sorted(want):
        if table not in deployed:
            problems.append(
                f"{layer}: table {table} is ABSENT. The standard requires it -- see "
                f"{LAYER_REFERENCE[layer]} for its columns and meaning."
            )
            continue
        missing = sorted(set(want[table]) - set(deployed[table]))
        extra = sorted(set(deployed[table]) - set(want[table]))
        if missing:
            problems.append(
                f"{layer}: {table} is missing column(s) {missing}. The invariant "
                f"staged = accepted + sum(discarded) cannot be evaluated without them."
            )
        if extra:
            problems.append(
                f"{layer}: {table} has column(s) {extra} the standard does not name. Either "
                f"add them to the standard or drop them -- an undeclared column in a control "
                f"schema is governed by accident."
            )
    return problems


def append_only_conformance(layer: str, append_only_tables: set) -> list:
    """Problems where a layer's append-only split disagrees with the standard.

    Both directions are wrong. An audit that can be rewritten cannot be relied on. A config
    table that cannot be rewritten means a mistyped rule or a wrong delivery count can never be
    corrected.
    """
    want = cs.tables_for(layer)
    problems = []
    for table in sorted(want):
        should = table in cs.APPEND_ONLY
        does = table in append_only_tables
        if should and not does:
            problems.append(
                f"{layer}: {table} records what happened and MUST be delta.appendOnly -- an "
                f"audit that can be rewritten is an audit nobody can rely on."
            )
        if not should and does:
            problems.append(
                f"{layer}: {table} records what should happen and must stay MUTABLE -- "
                f"append-only would mean a wrong entry could never be corrected."
            )
    return problems


def append_only_tables_from_properties(rows) -> set:
    """Which tables carry delta.appendOnly = 'true', from already-collected TBLPROPERTIES rows.

    `rows` is an iterable of dicts shaped {"table": ..., "key": ..., "value": ...} -- one entry
    per row SHOW TBLPROPERTIES would produce for some table, already collected. Pure and
    Spark-free, like control_object_rows()'s has_marker decision in schema_grant_check.py, so a
    fabricated row list exercises the same decision the live SHOW TBLPROPERTIES filter made.

    The match is exact-string 'true', matching the live filter this replaces
    (`value = 'true'`): Spark SQL string equality is case-sensitive, so 'True', 'TRUE' or '1'
    must NOT count as append-only, and neither does the property being merely present with any
    other value.
    """
    return {r["table"] for r in rows
            if r["key"] == "delta.appendOnly" and r["value"] == "true"}


# A MISSING CATALOG IS THE SAME "NOT INSTRUMENTED" STATE AS A MISSING CONTROL SCHEMA --
# gold's catalog does not exist at all as of 27 Aug 2026, which is the spec's normal case,
# not an edge case. Before this, main() queried `{catalog}.information_schema.columns`
# unconditionally, and Spark raises resolving that reference BEFORE the `if not deployed`
# branch is ever reached, so gold crashed the gate instead of reporting the state the spec
# requires.
#
# PURE AND SPARK-FREE, like every other decision in this module: catalog_missing() takes an
# already-raised exception (or, for a test fixture, anything whose str() looks like one) and
# decides whether it means "the catalog is absent" as opposed to some other, real failure --
# a permissions error, a typo'd argument, an outage -- that must still raise rather than be
# swallowed and reported as the normal, expected state. main() collects the exception first
# and decides after, the same collect-then-decide shape as append_only_tables_from_properties
# above, so the decision itself is testable offline against a fabricated exception rather
# than only against a live, non-existent catalog.
_CATALOG_MISSING_MARKERS = (
    "CATALOG_NOT_FOUND",
    "SCHEMA_NOT_FOUND",  # information_schema is itself a schema; an absent catalog fails
                         # resolving it the same way an absent schema in an existing
                         # catalog would
)


def catalog_missing(exc: BaseException) -> bool:
    """True when `exc` is Spark/Unity Catalog reporting that the queried CATALOG is absent.

    Matched on the documented Unity Catalog error classes, not on free text: NOT every
    exception querying information_schema means 'not instrumented', and swallowing one that
    does not would hide a real failure -- a permissions error or a genuine outage -- behind
    a reassuring 'expected state' message. Only the specific catalog-absent signal is
    treated as the normal, reportable state; everything else must still raise in main().
    """
    text = str(exc).upper()
    return any(marker in text for marker in _CATALOG_MISSING_MARKERS)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--control-schema", default="control")
    ap.add_argument("--layer", required=True, choices=list(cs.LAYERS))
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # A MISSING CATALOG MUST REPORT NOT-INSTRUMENTED, NOT CRASH. Gold has no catalog at all
    # as of 27 Aug 2026 -- the spec's normal case -- and this query is the first thing that
    # touches it, so Spark raises resolving `{catalog}.information_schema.columns` before
    # `deployed` is ever computed. catalog_missing() decides whether THIS is that case;
    # anything else re-raises rather than being reported as the expected state.
    try:
        rows = spark.sql(
            f"SELECT lower(table_name) t, lower(column_name) c "
            f"FROM `{args.catalog}`.information_schema.columns "
            f"WHERE lower(table_schema) = '{args.control_schema.lower()}'").collect()
    except Exception as exc:  # noqa: BLE001 -- narrowed immediately by catalog_missing()
        if not catalog_missing(exc):
            raise
        rows = []
    deployed: dict = {}
    for r in rows:
        deployed.setdefault(r["t"], set()).add(r["c"])

    if not deployed:
        print(f"GATE SUMMARY :: {GATE} :: status=NOT_EVALUATED asserted=0 not_evaluated=1")
        print(f"{args.layer}: no control schema in {args.catalog}.{args.control_schema}. "
              f"not instrumented -- this is the expected state until the owning team stands "
              f"it up. See {LAYER_REFERENCE[args.layer]}.")
        return 0

    # COLLECT FIRST, THEN DECIDE. SHOW TBLPROPERTIES is queried and .collect()ed per table --
    # TBLPROPERTIES are not exposed through information_schema, so there is no set-based way to
    # read them -- but the has-marker decision itself happens on the collected plain-Python
    # rows, in append_only_tables_from_properties(), not as a live DataFrame filter. That is
    # what makes the decision offline-testable against fabricated rows.
    control_tables = {r["t"] for r in spark.sql(
        f"SELECT lower(table_name) t FROM `{args.catalog}`.information_schema.tables "
        f"WHERE lower(table_schema) = '{args.control_schema.lower()}'").collect()}
    prop_rows = [
        {"table": table, "key": p["key"], "value": p["value"]}
        for table in control_tables
        for p in spark.sql(
            f"SHOW TBLPROPERTIES `{args.catalog}`.`{args.control_schema}`.`{table}`"
        ).collect()
    ]
    ao = append_only_tables_from_properties(prop_rows)

    problems = conformance(args.layer, deployed) + append_only_conformance(args.layer, ao)
    for p in problems:
        print(f"  * {p}")
    status = "FAILED" if problems else "PASSED"
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={len(cs.tables_for(args.layer))} "
          f"not_evaluated=0")
    return 1 if problems else 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for exit code 0. Exit
    # explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
