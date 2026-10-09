#!/usr/bin/env python3
"""Repoint a vault object's stable view at a new version, and refuse an unproven target.

RUN ONCE, DELIBERATELY, AND READ WHAT IT SAYS FIRST -- the same posture as
checks/drop_drifted_vault_tables.py and checks/migrate_to_versioned_tables.py. It is not
part of the standing job.

THREE REFUSALS, AND THE MIDDLE ONE IS THE POINT. A view flipped to an empty table
replaces real data with nothing and breaks no invariant downstream: append-only holds,
masks hold, reconciliation compares zero against zero. Every gate stays green while the
estate serves an empty answer. Nothing else here would catch it.

WHY GATED-BY-RUN AND NOT A STATUS COLUMN. Gate outcomes are PRINTED as
"GATE SUMMARY :: <gate> :: status=..." and are never persisted anywhere queryable --
`control.aud_table_load` is (job_run_id, pipeline_update_id, table_name, written_by,
staged, accepted, recorded_at), with no status column. A job run that completed is one in
which every gate passed, because the gates ARE tasks and a failed gate fails the run, so
the run id the operator confirms green is a sound proxy. It needs no new control object,
and it keeps a deliberate act deliberate: --gated-by-run must be given by hand, naming a
specific run, or the cutover is refused.
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


def cutover_refusal(target_exists: bool, target_rows: int, target_gated: bool) -> str:
    """Why this cutover must not happen, or "" if it may. PURE.

    THREE REFUSALS, AND THE MIDDLE ONE IS THE POINT. A view flipped to an empty table
    replaces real data with nothing and breaks no invariant downstream: append-only
    holds, masks hold, reconciliation compares zero against zero. Every gate stays green
    while the estate serves an empty answer. Nothing else here would catch it.
    """
    if not target_exists:
        return ("the target version does not exist. Build and load it before "
                "cutting over to it.")
    if target_rows == 0:
        return ("the target version exists but has never loaded -- 0 rows. Flipping "
                "the view to it would replace real data with nothing, and no "
                "downstream gate would fail: append-only holds, masks hold, and "
                "reconciliation would compare zero against zero.")
    if not target_gated:
        return ("the target version has not passed its domain's gates. The new "
                "structure proving itself on real data IS this mechanism; cutting "
                "over before it has is the same as not having one.")
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True,
                     help="the schema holding the physical table and the stable view")
    ap.add_argument("--control-schema", required=True,
                     help="the schema holding control.aud_table_load")
    ap.add_argument("--stable-name", required=True,
                     help="the unversioned view name consumers read, e.g. hub_invoice")
    ap.add_argument("--version", type=int, required=True,
                     help="the version to cut over to")
    ap.add_argument(
        "--gated-by-run", default=None,
        help="a vault_load job_run_id the operator has CONFIRMED GREEN. Without this, "
             "target_gated is False and the cutover is refused -- see the module "
             "docstring for why a run id is a sound proxy for a gate outcome.")
    ap.add_argument("--apply", action="store_true",
                     help="repoint the view. WITHOUT THIS IT ONLY PRINTS THE DECISION, "
                          "which is the default on purpose")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    from accelerator import naming

    spark = SparkSession.builder.getOrCreate()

    target = naming.physical(args.stable_name, args.version)
    fq_target = naming.qualified(args.catalog, args.schema, target)
    fq_view = naming.qualified(args.catalog, args.schema, args.stable_name)

    exists_rows = spark.sql(
        f"SELECT table_name FROM `{args.catalog}`.information_schema.tables "
        f"WHERE table_schema = '{args.schema}' AND table_name = '{target}'"
    ).collect()
    target_exists = bool(exists_rows)

    # Row count is printed BEFORE the decision, whether or not the cutover proceeds, so
    # the cost -- or the reason this is the exact refusal the mechanism exists for -- is
    # on the record either way.
    target_rows = 0
    if target_exists:
        target_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {fq_target}").collect()[0]["n"]
        print(f"  {fq_target}: {target_rows:,} row(s)")
    else:
        print(f"  {fq_target}: does not exist")

    target_gated = False
    if target_exists and args.gated_by_run:
        gated_rows = spark.sql(
            f"SELECT 1 FROM `{args.catalog}`.`{args.control_schema}`.aud_table_load "
            f"WHERE table_name = '{target}' AND job_run_id = '{args.gated_by_run}' "
            f"LIMIT 1"
        ).collect()
        target_gated = bool(gated_rows)
        print(f"  gated by run {args.gated_by_run!r}: {target_gated}")
    else:
        print("  gated by run: not supplied (--gated-by-run) -- treated as ungated")

    reason = cutover_refusal(target_exists, target_rows, target_gated)
    if reason:
        print(f"CUTOVER REFUSED: {reason}")
        return 1

    print(f"cutover proven: {fq_target} exists, holds {target_rows:,} row(s), and is "
          f"gated by run {args.gated_by_run!r}")
    if not args.apply:
        print("DRY RUN -- nothing was changed. Re-run with --apply to repoint the "
              "view above.")
        return 0

    spark.sql(f"CREATE OR REPLACE VIEW {fq_view} AS SELECT * FROM {fq_target}")
    print(f"{fq_view} now points at {fq_target}")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
