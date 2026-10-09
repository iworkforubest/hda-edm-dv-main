"""
HARD GATE: did every source key present in `_raw` reach the vault?

For each ACTIVE reconcilable binding:

    distinct dedup keys in the bound `_raw` table
        = landed + (quarantined - superseded)

A source key present upstream and absent from both the vault and its quarantine twin was
dropped silently, which is the failure quarantine exists to make impossible.

THIS IS NOT LOOP-1, AND MUST NEVER BE READ AS LOOP-1
-----------------------------------------------------
The distinction is the whole reason this file exists separately, so it is stated before
anything else.

`loop1_reconciliation.py` compares what we landed against what BRONZE ASSERTED it delivered,
read from a delivery manifest Bronze writes. That is a control over the HANDOFF: it can catch
a delivery that never arrived, a file that was truncated in transit, or a count Bronze believes
and we cannot reproduce.

This gate compares what we landed against WHAT WE OURSELVES READ from `_raw`. Both sides come
from our own read of their table, so it cannot see anything that went wrong before `_raw`. It
is a control over OUR LOADER, not over the handoff.

Measured 1 Sep 2026, that is a real and useful property -- and it was available all along:

    ukg_raw.gl        785 rows ->   525 distinct dedup keys ->   525 landed + 0 quarantined
    great_plains_raw.gl20000
                4,444,172 rows -> 2,453,131 distinct keys  -> 2,453,131 landed + 0 quarantined

Both balance exactly. So this gate can assert something true today, with nothing needed from
Bronze, while loop-1 stays NOT_EVALUATED. What it must not do is let anyone conclude loop-1 is
solved: the gate name, the summary line and the failure text all say LOADER, and BRZ-12 stays
open for the assertion only Bronze can make.

WHY DISTINCT DEDUP KEYS, AND NOT ROWS OR FILES
-----------------------------------------------
Per-FILE counts cannot work. Our loader deduplicates ACROSS deliveries -- gl20000 carries about
1.8 copies of every business row over 7 files -- so the rows landed for one file depend on
what other files contained. Grouping by `input_file_name` would report a variance on every
re-delivery, which is correct behaviour.

Raw ROW counts cannot work either, for the same reason: 4,444,172 rows legitimately become
2,453,131 vault rows.

What survives deduplication is the KEY. `dedup_by` is the measured natural key of the source,
and the loader keeps exactly one row per key, so the count of distinct keys is what the vault
should hold. A binding with no `dedup_by` keeps every row, so its expected count is the row
count -- expected_count() below makes that the same decision, not a special case.

KINDS: THE SAME REFUSAL LOOP-1 MAKES, FOR THE SAME REASON
----------------------------------------------------------
The identity assumes one landed row per source key. A hub deduplicates across bindings and a
satellite stores only changed rows, so it is false for them by design. This reuses
loop1_reconciliation.RECONCILABLE_KINDS rather than restating it -- one definition of which
kinds take one row per source row. That set is nhl, link and hal.

AND "LANDED" IS THE OBJECT THE PIPELINE WROTE, NOT THE VAULT TABLE
-------------------------------------------------------------------
That sentence above used to be the whole story, and it stopped being it on 29 September, when
nhl, link and hal joined `hub` in naming.STAGED_KINDS. Every one of the three kinds this gate
reconciles now deduplicates too -- not in the pipeline, but in checks/load_hubs.py, which
anti-joins the staging log into the vault table one row per hash key. So the vault table is a
DEDUPLICATED PROJECTION of what the pipeline accepted, while the quarantine twin is not
deduplicated at all: counting the vault table would credit fewer rows than the pipeline
accepted and break the identity asymmetrically -- a variance reported on correct behaviour, or
a real variance hidden because the two errors happened to cancel.

The landed side therefore resolves through naming.pipeline_table(entity.kind, table), which is
stg_<table> for a staged kind and <table> for anything else. checks/loop1_reconciliation.py
and checks/supersede_quarantine.py compute the SAME identity over the SAME kinds against the
SAME twin and already did this; this file was the fourth, quiet consumer that did not, and
tests/test_accelerator.py now asserts the three resolve it identically, read from the three
modules rather than typed out.

The quarantine twin is unchanged by any of it: it is named from the LOGICAL table, so it is
qtn_general_journal_line_rev1 either way -- which is why the twin name below is still derived
from `table` and not from the landed name.
"""

from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define __file__, so
# every Path(__file__) below raised NameError and the gate died before asserting anything.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "checks"))

from accelerator import naming, spec  # noqa: E402
import loop1_reconciliation as _l1  # noqa: E402

GATE = "landing_integrity"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def expected_count_sql(bronze_table: str, dedup_by) -> str:
    """The count of source keys the vault should hold for one binding.

    ONE DECISION, NOT A SPECIAL CASE. With dedup_by, the loader keeps one row per key, so the
    expected count is the distinct key count. Without it, it keeps every row, so the expected
    count is the row count. Both are 'how many distinct things did the source offer', which is
    why they are the same function rather than a branch the caller has to remember.

    concat_ws with a separator, NOT string addition: `a || b` makes ('ab', '') and ('a', 'b')
    the same key, which would UNDERCOUNT and let a real drop pass. The separator is the unit
    separator, which cannot appear in a business value.
    """
    if not dedup_by:
        return f"SELECT count(*) AS n FROM {bronze_table}"
    cols = ", ".join(f"`{c}`" for c in dedup_by)
    return (f"SELECT count(DISTINCT concat_ws(char(31), {cols})) AS n "
            f"FROM {bronze_table}")


# THE GHOST ROW IS NOT A LANDED SOURCE ROW, and this is a FUNCTION so that fact is testable.
# Every vault table carries one zero-key ghost at load_dts 1900-01-01 so an unresolved parent has
# something to point at. Counting it makes landed one too high, so variance() returns -1 on every
# correct table -- a gate that is permanently red is a gate nobody reads.
#
# It was inline SQL in main() until a mutation removed the filter and NOTHING offline noticed:
# the string was never returned by anything a test could call. Extracted for exactly that reason.
GHOST_CUTOFF = "TIMESTAMP'1900-01-02'"


# DISTINCT KEYS, NOT ROWS, AND THAT IS THE SECOND CORRECTION THIS SIDE HAS TAKEN.
# The identity is expected - (landed + quarantined - superseded), and `expected` has always
# been count(DISTINCT dedup key) from Bronze. The other two sides counted ROWS, which agreed
# with it only while the pipeline wrote each source row exactly once.
#
# MEASURED 29 September, run 813660406771451: nhl_general_journal_line_rev2's staging log held
# 1,516,175 rows against 758,087 distinct source keys -- EXACTLY DOUBLE -- and the gate reported
# "758087 MORE vault rows than the source offered". Nothing was wrong. The vault held 758,088
# rows over 758,088 keys and load_hubs discarded all 1,516,175 staged rows as already present.
#
# THE CAUSE IS A MECHANISM WORKING AS DESIGNED. SourceBinding.stream_generation exists to
# force a flow onto a fresh checkpoint when Bronze drops and recreates a table, and its whole
# contract is that the flow "re-reads the source and APPENDS, and the duplicate staging rows
# dissolve at the hub anti-join". They do dissolve -- in the vault. The staging log is
# append-only and keeps both copies, so a per-ROW count of it is a count of how many times we
# have read the source, not of what the source offered.
#
# Counting the VAULT instead is still wrong for the reason the section above gives: the vault
# is deduplicated and the twin is not, so the identity would break asymmetrically. Counting
# DISTINCT HASH KEYS keeps both sides on the objects the pipeline wrote AND makes them
# invariant to re-reads, which is what `expected` already was. The twin takes the same change:
# its quarantine flow is renamed by a generation bump exactly as the append flow is, so it
# re-appends its rejects for the same reason.
def landed_count_sql(vault_table: str, hk: str) -> str:
    """Distinct source keys in the object the pipeline wrote, excluding the ghost."""
    return (f"SELECT count(DISTINCT `{hk}`) AS n FROM {vault_table} "
            f"WHERE load_dts > {GHOST_CUTOFF}")


def quarantined_count_sql(qtn_table: str, hk: str) -> str:
    """Distinct source keys in the quarantine twin, excluding its ghost."""
    return (f"SELECT count(DISTINCT `{hk}`) AS n FROM {qtn_table} "
            f"WHERE load_dts > {GHOST_CUTOFF}")


def superseded_count_sql(catalog: str, control_schema: str, qtn_table: str) -> str:
    """Rejects from this twin that a later run legitimately re-landed."""
    return (f"SELECT count(*) AS n FROM "
            f"`{catalog}`.`{control_schema}`.`ctl_quarantine_superseded` "
            f"WHERE table_name = '{qtn_table}'")


def variance(expected: int, landed: int, quarantined: int, superseded: int) -> int:
    """expected - (landed + quarantined - superseded). Zero means every source key is
    accounted for.

    Positive means keys went MISSING -- the silent drop this gate exists to catch. Negative
    means the vault holds more than the source offered, which is a different defect (a
    duplicate the loader failed to collapse) and is reported with its own wording, because
    sending someone to hunt a missing row when the problem is an extra one wastes the trip.
    """
    return expected - (landed + quarantined - superseded)


def finding(entity: str, bronze_table: str, expected: int, landed: int,
            quarantined: int, superseded: int) -> str:
    """One problem line, naming the direction and both sides."""
    v = variance(expected, landed, quarantined, superseded)
    direction = ("source keys MISSING from the vault" if v > 0
                 else "MORE vault rows than the source offered")
    return (f"{entity}: {abs(v)} {direction}. {bronze_table} offers {expected} distinct "
            f"source key(s); vault holds {landed} landed + {quarantined} quarantined "
            f"- {superseded} superseded = {landed + quarantined - superseded}. This is a "
            f"LOADER variance, not a delivery variance -- both counts come from our own read "
            f"of {bronze_table}.")


def gate_status(problems: list, not_evaluated: list) -> str:
    """FAILED / NOT_EVALUATED / PASSED, from those two inputs and nothing else.

    A real problem outranks a skip, and nothing asserted is never PASSED -- the same
    precedence loop-1 and source_conformance apply, stated here rather than assumed so a
    reader of one gate has learned all three.
    """
    if problems:
        return "FAILED"
    if not_evaluated:
        return "NOT_EVALUATED"
    return "PASSED"


def bindings_to_check(model, active) -> list:
    """[(entity_name, vault_table, qtn_table, bronze_table, dedup_by)] for every ACTIVE
    reconcilable binding.

    Reuses loop1_reconciliation.targets() for the kind refusal and
    spec.active_table_bindings for activity -- neither notion is re-invented here. A binding
    that is declared but inactive in this lake yields nothing, so an inactive table can
    neither fail this gate nor pass it silently.
    """
    out = []
    for entity, src, table in _l1.targets(model, []):
        for b in spec.active_table_bindings(entity, src, active):
            bronze = getattr(b, "bronze_table", None)
            if not bronze:
                continue
            # DEF-42: for a STAGED kind, "landed" is the STAGING LOG, not the vault table.
            # Every accepted row lands in the log; the vault table is a deduplicated
            # projection of it, so counting the vault table would credit fewer rows than
            # the pipeline accepted and report a variance on correct behaviour. Same
            # expression, same reasoning, as checks/loop1_reconciliation.py's and
            # checks/supersede_quarantine.py's own `landed_table` -- and the twin below
            # stays derived from `table`, because it is named from the LOGICAL table.
            landed_table = naming.pipeline_table(entity.kind, table)
            out.append((entity.name, landed_table, f"qtn_{table.split('_', 1)[1]}", bronze,
                        list(getattr(b, "dedup_by", ()) or ())))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--active-sources", default="")
    args = ap.parse_args()

    meta = Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta)
    active = spec.resolve_active_sources(model, args.active_sources or None)

    checks = bindings_to_check(model, active)

    # ANTI-VACUITY, FIRST. A gate that finds nothing to check must say so, not report PASSED
    # over an empty loop -- the hollow-gate shape this repo has found more than a dozen times.
    if not checks:
        print("LANDING INTEGRITY NOT EVALUATED -- no active reconcilable binding in this lake, "
              "so there is nothing whose landing could be checked.")
        return finish("NOT_EVALUATED", 0, 1, 0)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    def scalar(sql: str) -> int:
        return int(spark.sql(sql).collect()[0]["n"])

    problems: list = []
    not_evaluated: list = []
    asserted = 0

    for entity, landed_table, qtn, bronze, dedup_by in checks:
        # bindings_to_check has already resolved this through naming.pipeline_table(), so
        # for a staged kind it is stg_<table>. The name says which it is; `vault` did not,
        # and that is how the wrong table came to be counted for four days.
        landed_fq = f"`{args.catalog}`.`{args.schema}`.`{landed_table}`"
        qtn_fq = f"`{args.catalog}`.`{args.schema}`.`{qtn}`"
        try:
            expected = scalar(expected_count_sql(f"`{bronze.split('.')[0]}`."
                                                 f"`{bronze.split('.')[1]}`."
                                                 f"`{bronze.split('.')[2]}`", dedup_by))
            hk = naming.hk(entity)
            landed = scalar(landed_count_sql(landed_fq, hk))
            quarantined = scalar(quarantined_count_sql(qtn_fq, hk))
            superseded = scalar(superseded_count_sql(
                args.catalog, args.control_schema, qtn))
        except Exception as exc:  # noqa: BLE001 -- reported per binding, never swallowed
            not_evaluated.append(f"{entity}: {type(exc).__name__}: {exc}")
            continue

        asserted += 1
        if variance(expected, landed, quarantined, superseded) != 0:
            problems.append(finding(entity, bronze, expected, landed, quarantined, superseded))
        else:
            print(f"  {entity}: {expected} source key(s) all accounted for "
                  f"({landed} landed, {quarantined} quarantined, {superseded} superseded)")

    for line in problems:
        print(f"  * {line}")
    if not_evaluated:
        print(f"  NOT EVALUATED ({len(not_evaluated)} binding(s)):")
        for line in not_evaluated:
            print(f"    {line}")

    if problems:
        print("LANDING INTEGRITY GATE FAILED -- a source key present in _raw is in neither "
              "the vault nor its quarantine twin, or the vault holds rows the source never "
              "offered.")
    elif asserted:
        print(f"LANDING INTEGRITY GATE PASSED -- every source key in {asserted} binding(s) "
              f"reached the vault or its quarantine twin. NOTE: this compares our landed rows "
              f"against OUR OWN READ of _raw, so it says nothing about whether Bronze "
              f"delivered what it believes. That is loop-1, and it needs BRZ-12.")

    return finish(gate_status(problems, not_evaluated), asserted, len(not_evaluated),
                  1 if problems else 0)


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for exit code 0. Exit
    # explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
