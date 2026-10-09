#!/usr/bin/env python3
"""
THE SATELLITE LOADER'S BEHAVIOURAL PROBE -- executed, not rendered.

WHY THIS IS A SCRIPT AND NOT A CHECK IN tests/test_accelerator.py.

The offline suite runs with no workspace. It can read `checks/load_satellites.py`'s
rendered SQL as a STRING and assert substrings of it, and it does -- but a substring
cannot distinguish the pre-fix and post-fix shapes of this loader. Both contain
`NOT EXISTS`, both contain `LAG(`, in either order; the two Criticals found in fix round 1
were an argument about WHERE those clauses sit relative to each other, and every one of
the 15 substring checks passed against the broken SQL. THE OFFLINE SUITE CANNOT VERIFY SQL
SEMANTICS. This script is how they are verified: the loader's OWN rendered statements,
executed against real fixtures on a real warehouse, asserting on the rows that come back.

Spec section 6 asked for exactly this and it was represented in the offline suite by a
`Path(...).exists()` check on an evidence markdown file -- a check that can never fail and
can never regress. The evidence document is still good evidence of one run; it is not a
test. This is the runnable form.

WHAT IT PROVES, one key, one satellite, in order:

  1. a first-ever version inserts;
  2. a second run over an unchanged log inserts ZERO (idempotency);
  3. consecutive duplicates WITHIN one batch collapse to nothing new, and the batch's
     first row is SEEDED from the version already stored -- a batch that opens with the
     value the table already ends on is not mistaken for a change;
  4. a genuine change inserts exactly one row, however many copies of it the batch holds;
  5. A -> B -> A stores THREE rows. Compare-to-set-membership would store two, and the
     derived _v1 view would report B as current for ever -- silent, permanent data loss
     that passes every gate this repo has.

THE STATEMENTS UNDER TEST ARE NOT TYPED OUT HERE. They come from
`load_satellites.create_sql()` and `load_satellites.insert_sql()`, called with the column
list `load_satellites.declared_columns()` computes for a REAL satellite in
`metadata/entities` -- so a change to the loader changes what this probe executes, which
is the only arrangement in which the probe can fail when the loader breaks.

SCRATCH TABLES. Two, named below, created in the target schema, dropped in a `finally`,
and their ABSENCE verified against information_schema before this script reports success.
Nothing else in the schema is touched.

RUN IT:

    uv run python checks/probe_satellite_behaviour.py

Read-only against everything except its own two scratch tables. It is NOT part of the
offline suite and NOT part of the vault job: it needs a workspace, and the offline suite
must keep running without one. tests/test_accelerator.py asserts only that this file
exists and is executable.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

sys.path.insert(0, str(Path(__file__).resolve().parent))

from accelerator import naming, spec  # noqa: E402

import load_satellites as ls  # noqa: E402

GATE = "probe_satellite_behaviour"

# The scratch pair. Deliberately fixed names rather than a random suffix: a crashed run
# must leave something a human can find and drop, and the absence check below has to be
# able to name what it is looking for.
LOG = "_probe_behaviour_log"
SAT = "_probe_behaviour_sat"

# Column types for the fixture. The COLUMN LIST is the real satellite's (declared_columns);
# only the types are chosen here, and only three of them are not STRING.
TYPES = {
    naming.COL["load_dts"]: "TIMESTAMP",
    naming.COL["applied_dts"]: "TIMESTAMP",
    naming.COL["sub_seq"]: "INT",
}

KEY = "K1"


class ProbeFailure(Exception):
    pass


def run(sql: str, profile: str) -> list[dict]:
    """Execute one statement through the Databricks CLI and return its rows.

    `databricks experimental aitools tools query` prints JSON rows, or a plain
    "Query executed successfully (no results)" line for a statement that returns none.
    """
    proc = subprocess.run(
        ["databricks", "experimental", "aitools", "tools", "query", sql,
         "--profile", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise ProbeFailure(f"query failed ({proc.returncode}):\n{sql}\n"
                           f"{proc.stdout}\n{proc.stderr}")
    out = proc.stdout.strip()
    if not out.startswith("["):
        return []
    return json.loads(out)


def scalar(sql: str, profile: str, column: str) -> str:
    rows = run(sql, profile)
    if not rows:
        raise ProbeFailure(f"expected one row from:\n{sql}")
    return rows[0][column]


def inserted(sql: str, profile: str) -> int:
    """How many rows the loader's INSERT actually wrote."""
    rows = run(sql, profile)
    if not rows or "num_inserted_rows" not in rows[0]:
        # An INSERT that matched nothing still reports the counter; its absence means the
        # statement was not the INSERT we think it was.
        raise ProbeFailure(f"no num_inserted_rows in the result of:\n{sql}\n{rows}")
    return int(rows[0]["num_inserted_rows"])


def pick_satellite(model) -> tuple[spec.Entity, str, list[str]]:
    """A real plain `sat` from the model, with the loader's own declared column list."""
    for entity, table in ls.staged_satellites(model):
        if entity.kind != "sat":
            continue
        src = next((s for s, t in entity.tables() if t == table), None)
        proj = src if src is not None else (entity.sources[0] if entity.sources else None)
        return entity, table, ls.declared_columns(entity, proj)
    raise ProbeFailure("the model declares no plain `sat` -- nothing to probe")


def log_ddl(catalog: str, schema: str, columns: list[str]) -> str:
    cols = ", ".join(f"`{c}` {TYPES.get(c, 'STRING')}" for c in columns)
    return f"CREATE TABLE {ls.q(catalog, schema, LOG)} ({cols})"


def append(catalog: str, schema: str, columns: list[str], parent_hk: str,
           hashdiff: str, day: int) -> str:
    """One row into the log: same key, given hashdiff, load_dts = 2026-01-<day>."""
    values = []
    for c in columns:
        if c == parent_hk:
            values.append(f"'{KEY}'")
        elif c == naming.COL["hashdiff"]:
            values.append(f"'{hashdiff}'")
        elif c == naming.COL["load_dts"]:
            values.append(f"TIMESTAMP'2026-01-{day:02d} 00:00:00'")
        elif c == naming.COL["applied_dts"]:
            values.append(f"TIMESTAMP'2026-01-{day:02d} 00:00:00'")
        elif c == naming.COL["sub_seq"]:
            values.append("0")
        elif c == naming.COL["cdc_op"]:
            values.append("'I'")
        else:
            values.append(f"'{c}_v'")
    col_list = ", ".join(f"`{c}`" for c in columns)
    return (f"INSERT INTO {ls.q(catalog, schema, LOG)} ({col_list}) "
            f"VALUES ({', '.join(values)})")


def stored(catalog: str, schema: str, profile: str, hashdiff: str) -> list[str]:
    rows = run(
        f"SELECT `{hashdiff}` AS hd FROM {ls.q(catalog, schema, SAT)} "
        f"ORDER BY `{naming.COL['load_dts']}`, `{naming.COL['sub_seq']}`", profile)
    return [r["hd"] for r in rows]


def expect(label: str, actual, wanted, results: list[tuple[str, bool, str]]) -> None:
    ok = actual == wanted
    results.append((label, ok, f"got {actual!r}, expected {wanted!r}"))
    print(f"  {'ok  ' if ok else 'FAIL'} {label}  ({actual!r} vs {wanted!r})")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--catalog", default="02_usnc_silver_edm_dev")
    ap.add_argument("--schema", default="governance",
                    help="scratch schema for this probe's own two tables")
    ap.add_argument("--profile", default="hfig-usnc-tds")
    ap.add_argument("--metadata", default=None)
    ap.add_argument("--keep", action="store_true",
                    help="leave the scratch tables behind for inspection (the absence "
                         "check is then reported as NOT EVALUATED, not passed)")
    args = ap.parse_args()

    meta = Path(args.metadata) if args.metadata else (
        Path(__file__).resolve().parents[1] / "metadata" / "entities")
    model = spec.load_model(meta)
    entity, table, columns = pick_satellite(model)
    parent_hk = naming.hk(entity.parents[0])
    hd = naming.COL["hashdiff"]

    # THE STATEMENT UNDER TEST -- rendered by the loader, not written here.
    insert = ls.insert_sql(args.catalog, args.schema, SAT, LOG, parent_hk,
                           is_msat=False, columns=columns)
    create = ls.create_sql(args.catalog, args.schema, SAT, LOG)

    print(f"probing checks/load_satellites.py against {args.catalog}.{args.schema}")
    print(f"  shape taken from {table} ({len(columns)} declared columns)")
    print(f"  scratch tables: {LOG}, {SAT}")
    print("\n-- the INSERT under test ------------------------------------------")
    print(insert)
    print("-------------------------------------------------------------------\n")

    results: list[tuple[str, bool, str]] = []
    try:
        run(f"DROP TABLE IF EXISTS {ls.q(args.catalog, args.schema, LOG)}", args.profile)
        run(f"DROP TABLE IF EXISTS {ls.q(args.catalog, args.schema, SAT)}", args.profile)
        run(log_ddl(args.catalog, args.schema, columns), args.profile)
        run(create, args.profile)

        # RUN 1 -- the log gains A. Nothing is stored, so it must land.
        run(append(args.catalog, args.schema, columns, parent_hk, "A", 1), args.profile)
        expect("run 1: a first-ever version inserts",
               inserted(insert, args.profile), 1, results)

        # RUN 2 -- nothing new in the log. IDEMPOTENCY: a second run inserts zero.
        expect("run 2: a second run over an unchanged log inserts ZERO",
               inserted(insert, args.profile), 0, results)

        # RUN 3 -- a batch that OPENS with the value already stored (A) and then repeats
        # it, then genuinely changes to B and repeats THAT. Exactly one row may land: the
        # first B. The opening A must be seeded from the stored latest, and both
        # within-batch duplicates must collapse.
        for _hd, _day in (("A", 2), ("A", 3), ("B", 4), ("B", 5)):
            run(append(args.catalog, args.schema, columns, parent_hk, _hd, _day),
                args.profile)
        expect("run 3: within-batch duplicates collapse AND the batch is seeded from the "
               "stored latest -- 4 staged rows, 1 real change",
               inserted(insert, args.profile), 1, results)
        expect("run 3: and what landed is the CHANGE, not a repeat",
               stored(args.catalog, args.schema, args.profile, hd), ["A", "B"], results)

        # RUN 4 -- back to A. Compare-to-latest stores it; compare-to-set-membership
        # would not, and _v1 would report B as current for ever.
        run(append(args.catalog, args.schema, columns, parent_hk, "A", 6), args.profile)
        expect("run 4: A -> B -> A inserts the returning value",
               inserted(insert, args.profile), 1, results)
        expect("run 4: A -> B -> A stores THREE rows, in order",
               stored(args.catalog, args.schema, args.profile, hd), ["A", "B", "A"],
               results)

        # RUN 5 -- idempotent again, now over a log with history in it.
        expect("run 5: re-running over the full log still inserts ZERO",
               inserted(insert, args.profile), 0, results)
        expect("run 5: and the stored history is unchanged",
               stored(args.catalog, args.schema, args.profile, hd), ["A", "B", "A"],
               results)
    finally:
        if args.keep:
            print("\n--keep: scratch tables left in place, absence NOT verified")
        else:
            for t in (LOG, SAT):
                try:
                    run(f"DROP TABLE IF EXISTS {ls.q(args.catalog, args.schema, t)}",
                        args.profile)
                except ProbeFailure as exc:      # noqa: PERF203
                    print(f"  FAIL could not drop {t}: {exc}")

    if not args.keep:
        # THE CLEANUP IS ASSERTED, not assumed. A probe that leaves tables behind in a
        # governed schema is a finding of its own.
        left = scalar(
            f"SELECT count(*) AS n FROM `{args.catalog}`.information_schema.tables "
            f"WHERE table_schema = '{args.schema}' "
            f"AND table_name IN ('{LOG}', '{SAT}')", args.profile, "n")
        expect("cleanup: both scratch tables are gone", int(left), 0, results)

    failed = [label for label, ok, _d in results if not ok]
    print(f"\nGATE SUMMARY :: {GATE} :: status="
          f"{'FAILED' if failed else 'PASSED'} asserted={len(results)} "
          f"not_evaluated={1 if args.keep else 0}")
    if failed:
        print(f"PROBE FAILED -- {len(failed)}: {failed}")
        return 1
    print("PROBE PASSED: the loader's own rendered SQL behaves as specified")
    return 0


if __name__ == "__main__":
    try:
        _rc = main()
    except ProbeFailure as _exc:
        print(f"PROBE ERROR: {_exc}")
        _rc = 1
    if _rc:
        sys.exit(_rc)
