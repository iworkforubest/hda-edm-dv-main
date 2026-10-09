#!/usr/bin/env python3
"""Drop a superseded physical version of a vault object, and refuse to drop the wrong one.

RUN ONCE, DELIBERATELY, AND READ WHAT IT SAYS FIRST -- the same posture as
checks/drop_drifted_vault_tables.py. It is not part of the standing job. Row counts are
printed before the drop so the cost is on the record.

TWO REFUSALS.

  * THE LIVE VERSION MAY NEVER BE RETIRED. Dropping the table the stable view currently
    points at destroys the estate's data and leaves a view over nothing.
  * THE DECLARED VERSION MAY NEVER BE RETIRED, even when a view points elsewhere. A
    rolled-back cutover leaves the declared version not-live; retiring it would throw
    away the version the next load is about to rebuild.

WHY "is_live" COMES FROM THE VIEW DEFINITION, NOT FROM NEW STATE (R4). No new state may
be added for this. `information_schema.views` exposes `view_definition`; reading the
stable view's definition and matching the physical name inside it is enough to decide
which version is live, with nothing new to keep in sync.
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

from accelerator import spec  # noqa: E402


def retire_refusal(is_live: bool, version: int, declared_version: int) -> str:
    """Why this version must not be dropped, or "" if it may. PURE.

    RETIREMENT IS NEVER AUTOMATIC AND IS NEVER PART OF THE STANDING JOB. This is the
    same posture as drop_drifted_vault_tables: read what it says it will destroy, run
    it, take it back out. Row counts are printed before the drop so the cost is on the
    record.
    """
    if is_live:
        return (f"v{version} is the live version -- the one the stable view currently "
                f"points at. Dropping it destroys the data the estate is serving and "
                f"leaves a view over nothing. Cut over first, then retire.")
    if version == declared_version:
        return (f"v{version} is the version the model declares. A rolled-back cutover "
                f"leaves the declared version not-live, and retiring it would throw "
                f"away the version the next load is about to rebuild.")
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True,
                     help="the schema holding the physical table and the stable view")
    ap.add_argument("--stable-name", required=True,
                     help="the unversioned view name consumers read, e.g. hub_invoice")
    ap.add_argument("--version", type=int, required=True,
                     help="the version to retire")
    ap.add_argument("--apply", action="store_true",
                     help="drop the table. WITHOUT THIS IT ONLY PRINTS THE DECISION, "
                          "which is the default on purpose")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    from accelerator import naming

    spark = SparkSession.builder.getOrCreate()
    model = spec.load_model(ROOT / "metadata" / "entities")

    declared_version = None
    for e in model.entities:
        if any(s == args.stable_name for _src, s in e.stable_tables()):
            declared_version = e.version
            break
    if declared_version is None:
        print(f"{args.stable_name!r} is not a stable view name any entity in the model "
              f"declares. Refusing to guess which version is 'declared'.")
        return 1

    target = naming.physical(args.stable_name, args.version)
    fq_target = naming.qualified(args.catalog, args.schema, target)
    fq_view = naming.qualified(args.catalog, args.schema, args.stable_name)

    exists_rows = spark.sql(
        f"SELECT table_name FROM `{args.catalog}`.information_schema.tables "
        f"WHERE table_schema = '{args.schema}' AND table_name = '{target}'"
    ).collect()
    if not exists_rows:
        print(f"{fq_target}: does not exist; nothing to retire")
        return 0

    # Printed BEFORE the decision, so the cost is on the record even when retirement is
    # refused.
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {fq_target}").collect()[0]["n"]
    print(f"  {fq_target}: holds {n:,} row(s), which this destroys")

    view_rows = spark.sql(
        f"SELECT view_definition FROM `{args.catalog}`.information_schema.views "
        f"WHERE table_schema = '{args.schema}' AND table_name = '{args.stable_name}'"
    ).collect()
    if not view_rows:
        print(f"{fq_view}: no view found -- cannot determine which version is live, "
              f"refusing to guess")
        return 1
    view_definition = view_rows[0]["view_definition"] or ""
    is_live = target in view_definition
    print(f"  {fq_view} is live over {target!r}: {is_live}")
    print(f"  the model declares version {declared_version}")

    reason = retire_refusal(is_live, args.version, declared_version)
    if reason:
        print(f"RETIREMENT REFUSED: {reason}")
        return 1

    if not args.apply:
        print("DRY RUN -- nothing was changed. Re-run with --apply to drop the table "
              "above.")
        return 0

    spark.sql(f"DROP TABLE {fq_target}")
    print(f"DROPPED {fq_target} ({n:,} row(s))")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
