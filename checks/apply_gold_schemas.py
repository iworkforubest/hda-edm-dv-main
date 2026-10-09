#!/usr/bin/env python3
"""Create the gold layer's schemas. Schemas only -- no tables, no permissions, no masks.

WHY A TASK RATHER THAN A HAND-RUN FILE. governance/control_objects_gold.sql has been
generated, committed and gated since 29 August and applied by nobody, which is why the
gold catalog held only information_schema on 27 September. A step that everything else
depends on cannot be manual.

IT ISSUES NO PERMISSIONS. Subsystem A creates the topology; the access model is
subsystem B, and the TDS convenience permission lives in the job that manages Vault
access, where it can be read and removed as one thing. A permission emitted here would
be invisible to that decision.

DEF-12: checks/ is exec()'d by a serverless spark_python_task with no __file__, so the
SQL file is located from a path this module computes at import time the same way its
siblings do.
"""
from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does NOT define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "checks"))

from apply_governance import render, statements  # noqa: E402

SQL_FILE = ROOT / "governance" / "gold_schemas.sql"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold-catalog", required=True)
    ap.add_argument("--dry-run", action="store_true",
                    help="render and split without executing; needs no workspace")
    args = ap.parse_args()

    stmts = statements(render(SQL_FILE.read_text(encoding="utf-8"),
                              {"gold_catalog": args.gold_catalog}))
    print(f"{len(stmts)} statement(s) rendered for gold_catalog={args.gold_catalog}")

    if args.dry_run:
        for i, s in enumerate(stmts, 1):
            print(f"  {i:2d}. {s.splitlines()[0][:88]}")
        return 0

    from pyspark.sql import SparkSession  # noqa: PLC0415 -- not needed for --dry-run
    spark = SparkSession.builder.getOrCreate()
    failed = []
    for i, s in enumerate(stmts, 1):
        first = s.splitlines()[0][:88]
        try:
            spark.sql(s)
            print(f"  ok   {i:2d}. {first}")
        except Exception as exc:  # noqa: BLE001 -- report every failure, not the first
            print(f"  FAIL {i:2d}. {first}\n       {exc}")
            failed.append(first)

    if failed:
        print(f"\nGOLD SCHEMA CREATION FAILED -- {len(failed)} statement(s)")
        return 1
    print(f"\nGOLD SCHEMAS PRESENT: {len(stmts)} statement(s) applied to "
          f"{args.gold_catalog}")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
