"""Create the control schema and its tables, and open the run's audit record.

Nothing ran governance/control_objects.sql before this. apply_governance.py reads only
apply_masks.sql, and no job task referenced the control file at all -- the two control
tables existed because someone applied it by hand once. Four gates are now pointed at this
schema, so the step that creates it cannot be manual.

WHY THIS TASK WRITES THE 'opened' ROW. assert_hash_parity is the first task, but it runs
BEFORE this one, so at that point the control schema may not exist. Gate zero also has no
business writing anything: it proves digests and stops. The task that GUARANTEES the target
exists is the first that can write to it, so it does.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accelerator import audit  # noqa: E402
from apply_governance import render, statements  # noqa: E402

# --------------------------------------------------------------------------------------- #
# DECLARED-VERSUS-DEPLOYED, because this task reported SUCCESS while doing nothing.
#
# Measured 27 Aug 2026: `severity` was added to ref_dq_expectation's declaration when the data
# contract work landed. This task then ran and reported SUCCESS, and the column did not appear
# -- because the table already existed and every statement in control_objects.sql is
# IF NOT EXISTS, which is a no-op on an existing table. It was found a day later, by an INSERT
# failing with "automatic schema migration is not allowed" and the value inferred as `col5`.
#
# ALTER TABLE ... ADD COLUMNS IF NOT EXISTS is not supported, so the file cannot be made
# self-healing in pure SQL and a column addition still needs a manual ALTER per lake. That is a
# nuisance. The DEFECT was that nothing said so: a task that goes green having done nothing is
# worse than one that fails, because the green is taken as evidence.
#
# Both functions are pure over their inputs, like the decision functions in
# schema_grant_check.py, so the failing case is testable offline against fabricated rows.

_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^(]*?\.`?(\w+)`?\s*\((.*?)\)\s*"
    r"(?:COMMENT|CLUSTER|TBLPROPERTIES|USING|;|$)",
    re.S | re.I,
)


def declared_column_types(sql_text: str) -> dict:
    """table -> {column_name: sql_type}, from control_objects.sql's own declarations.

    THE TYPE IS THE SECOND WHITESPACE-SEPARATED TOKEN on a column line -- e.g.
    `staged   BIGINT  NOT NULL COMMENT '...'` -- the same shape control_standard.CORE
    declares its types in, uppercased so a type written in a different case still compares
    equal.

    A declaration this cannot parse yields no entry, and main() refuses to report agreement
    on an empty result -- failing closed, because a regex that quietly stops matching is how
    this check would rot into the green no-op it exists to catch.
    """
    out: dict = {}
    for match in _CREATE_RE.finditer(sql_text):
        table, body = match.group(1), match.group(2)
        cols: dict = {}
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            name = parts[0].strip("`,")
            if name and name.isidentifier():
                cols[name.lower()] = parts[1].strip("`,").upper()
        if cols:
            out[table.lower()] = cols
    return out


def declared_columns(sql_text: str) -> dict:
    """table -> the column NAMES its declaration in control_objects.sql defines.

    A thin view over declared_column_types(): every caller that only ever compared names
    (this file's own column_drift(), and verify_repo.py's table-set check) keeps working
    unchanged, while the type information is available to whoever needs it too.
    """
    return {table: set(cols) for table, cols in declared_column_types(sql_text).items()}


def add_column_statements(declared_types: dict, deployed: dict, catalog: str,
                         schema: str) -> list:
    """ALTER statements for every DECLARED column an existing control table is missing.

    THE DECLARATION IS ALREADY THE AUTHORITY. This task runs control_objects.sql's CREATE
    statements without asking; applying the same file's column list to a table that already
    exists is the same authority, not a new one. What it will NOT do is drop, retype or
    reorder anything -- only add what the reviewed file declares and the lake lacks. A
    column in the lake that no declaration mentions stays a reported drift, because
    deciding to remove it is a decision, not a repair.

    WHY IT IS DONE AT ALL. Every statement in control_objects.sql is IF NOT EXISTS, a no-op
    on an existing table, so a column added to a declaration never lands and this task went
    green having done nothing (measured 27 Aug, ref_dq_expectation.severity, found a day
    later by an INSERT inferring the value as `col5`). The fix then was to REPORT the drift
    and apply the ALTER by hand per lake. That left the repair needing a permission the
    person reading the message may not hold -- measured 25 September: the message said
    exactly what to run, and running it returned PERMISSION_DENIED on MODIFY, while the
    service principal this task runs as owns the table and could.

    Ordered by table then column so the statements, and any diff of them, are stable.
    """
    out = []
    for table in sorted(declared_types):
        if table not in deployed:
            continue
        missing = sorted(set(declared_types[table]) - deployed[table])
        if missing:
            cols = ", ".join(f"{c} {declared_types[table][c]}" for c in missing)
            out.append(f"ALTER TABLE `{catalog}`.`{schema}`.`{table}` ADD COLUMNS ({cols})")
    return out


def column_drift(declared: dict, deployed: dict) -> list:
    """Problems for every table whose deployed columns differ from its declaration.

    MISSING is the case that bit us. EXTRA is reported too: a column in the lake that no
    declaration mentions is drift in the other direction and equally invisible today.

    A declared table absent entirely is NOT reported -- that is what the statements create,
    and a genuine failure to create would have raised.
    """
    problems = []
    for table in sorted(declared):
        if table not in deployed:
            continue
        missing = sorted(declared[table] - deployed[table])
        extra = sorted(deployed[table] - declared[table])
        if missing:
            problems.append(
                f"{table}: declared but NOT DEPLOYED {missing}. Every statement in "
                f"control_objects.sql is IF NOT EXISTS, a no-op on an existing table, so a "
                f"column added after that table's first deploy never lands. Apply it by "
                f"hand: ALTER TABLE <catalog>.<control>.{table} ADD COLUMNS "
                f"({missing[0]} <type>)"
            )
        if extra:
            problems.append(
                f"{table}: deployed but NOT DECLARED {extra}. Add it to control_objects.sql "
                f"or drop it -- an undeclared column in the control schema is governed by "
                f"accident."
            )
    return problems


GATE = "create_control_objects"
SQL_FILE = Path(__file__).resolve().parents[1] / "governance" / "control_objects.sql"


def finish(status: str, asserted: int, not_evaluated: int, code: int) -> int:
    print(f"GATE SUMMARY :: {GATE} :: status={status} asserted={asserted} "
          f"not_evaluated={not_evaluated}")
    return code


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--governance-schema", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--active-sources", default="")
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    stmts = statements(render(SQL_FILE.read_text(encoding="utf-8"), {
        "catalog": args.catalog,
        "governance_schema": args.governance_schema,
        "control_schema": args.control_schema,
    }))

    if args.dry_run:
        for i, s in enumerate(stmts, 1):
            print(f"  {i:2d}. {s.splitlines()[0][:88]}")
        print(f"{len(stmts)} statement(s) rendered -- nothing executed")
        return 0

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for s in stmts:
        spark.sql(s)
    print(f"{len(stmts)} control statement(s) applied")

    # VERIFY WHAT WAS APPLIED, rather than report SUCCESS for having run the statements.
    _declared = declared_columns(render(SQL_FILE.read_text(encoding="utf-8"), {
        "catalog": args.catalog,
        "governance_schema": args.governance_schema,
        "control_schema": args.control_schema,
    }))
    if not _declared:
        print(f"DRIFT CHECK NOT EVALUATED: parsed no table declaration out of "
              f"{SQL_FILE.name} -- refusing to report agreement having compared nothing")
        return finish("FAILED", 0, 1, 1)

    _rows = spark.sql(
        f"SELECT lower(table_name) t, lower(column_name) c "
        f"FROM `{args.catalog}`.information_schema.columns "
        f"WHERE lower(table_schema) = '{args.control_schema.lower()}'").collect()
    _deployed: dict = {}
    for _r in _rows:
        _deployed.setdefault(_r["t"], set()).add(_r["c"])

    # SELF-HEAL FIRST, THEN CHECK. Adding a declared column is applying the same reviewed
    # file the CREATE statements above came from; the drift check below then runs against
    # the lake as it now stands, so it still fails on anything an ALTER cannot fix.
    _declared_types = declared_column_types(render(SQL_FILE.read_text(encoding="utf-8"), {
        "catalog": args.catalog,
        "governance_schema": args.governance_schema,
        "control_schema": args.control_schema,
    }))
    _adds = add_column_statements(_declared_types, _deployed, args.catalog,
                                  args.control_schema)
    for _a in _adds:
        print(f"  ADDING a declared column the lake lacked: {_a}")
        spark.sql(_a)
        _t = _a.split("`")[5]
        _deployed.setdefault(_t, set()).update(
            _c.split()[0].lower() for _c in _a.split("(", 1)[1].rstrip(")").split(", "))

    _drift = column_drift(_declared, _deployed)
    if _drift:
        print(f"DRIFT between control_objects.sql and {args.control_schema}:")
        for _d in _drift:
            print(f"  * {_d}")
        return finish("FAILED", len(_declared), 0, 1)
    print(f"{len(_declared)} control table(s) match their declaration, column for column")

    # The run is opened only after the schema exists. If this write fails the task fails,
    # so a run with no 'opened' row never loaded anything.
    spark.sql(audit.load_run_sql(
        args.catalog, args.control_schema, job_run_id=args.job_run_id,
        phase="opened", target=args.target, active_sources=args.active_sources))
    print(f"run {args.job_run_id} opened")
    return finish("PASSED", len(stmts) + 1, 0, 0)


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
