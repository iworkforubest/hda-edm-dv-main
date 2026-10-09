#!/usr/bin/env python3
"""Rename every unversioned vault table to its _rev1 name and put a view in its place.

RUN ONCE, DELIBERATELY, AND READ WHAT IT SAYS FIRST -- the same posture as
checks/drop_drifted_vault_tables.py. It is not part of the standing job.

WHY A RENAME AND NOT A REBUILD. A vault table carries delta.appendOnly = true and holds
history that cannot be recreated from Bronze. ALTER TABLE ... RENAME TO is a metadata
operation: no data moves, no file is rewritten, and the table's history follows it.

WHY table TYPES MATTER, AND WHY SHOW TABLES CANNOT BE USED. `SHOW TABLES IN ...` returns
only (database, tableName, isTemporary) -- no table type -- so it cannot tell a loader-
created table from a pipeline-owned one. Two object types exist in these schemas and they
behave differently:

  MANAGED          hub_*, sat_* -- created by the batch loaders. ALTER TABLE ... RENAME TO
                   is a metadata operation: no data moves and history follows. These are
                   what this tool renames.
  STREAMING_TABLE  csat_*, stg_* -- owned by their SDP pipeline. Their name comes from the
                   PIPELINE DEFINITION, not from a rename, and ALTER TABLE RENAME on one
                   FAILS.

So this reads `information_schema.tables` (the same source checks/append_only_check.py
already uses) and plans a rename only for MANAGED objects. Every declared table that turns
out to be a STREAMING_TABLE is reported and skipped -- its versioned name comes from the
next deployment of its pipeline, via Entity.tables(), and there is nothing to migrate.

WHAT ELSE IT DOES NOT TOUCH. Any table the model does not declare is left exactly as it is
and reported.
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

GATE = "migrate_to_versioned_tables"


def plan_migration(deployed: set[str],
                    model_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """The renames still outstanding, as (existing_name, new_physical_name).

    PURE, so the interesting states can be proven without a workspace: already done,
    half done, and the ambiguous one. `deployed` is the set of MANAGED table names in the
    schema -- a STREAMING_TABLE must never reach this function's `deployed` set, because
    ALTER TABLE RENAME on one fails; the caller in main() filters those out and reports
    them separately, by type, before calling this.

    `model_pairs` is (stable_name, physical_name) for every table the model declares.
    """
    todo: list[tuple[str, str]] = []
    for stable_name, physical_name in model_pairs:
        has_old = stable_name in deployed
        has_new = physical_name in deployed
        if has_old and has_new:
            raise ValueError(
                f"both {stable_name!r} and {physical_name!r} exist. An earlier run "
                f"renamed this table and something has since recreated the old name. "
                f"Which one holds the real history is not a guess worth making -- "
                f"inspect both and remove the wrong one by hand.")
        if has_old and not has_new:
            todo.append((stable_name, physical_name))
    return todo


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--business-vault-schema", default="business_vault")
    ap.add_argument("--apply", action="store_true",
                     help="perform the renames. WITHOUT THIS IT ONLY PRINTS THEM, which "
                          "is the default on purpose")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    model = spec.load_model(ROOT / "metadata" / "entities")

    from accelerator import naming

    failures: list[str] = []
    for schema in {args.schema, args.business_vault_schema}:
        # DEF-26/R2: MANAGED and STREAMING_TABLE behave differently under RENAME, and
        # SHOW TABLES cannot tell them apart at all -- see the module docstring.
        rows = spark.sql(
            f"SELECT table_name, table_type FROM `{args.catalog}`.information_schema.tables "
            f"WHERE table_schema = '{schema}'"
        ).collect()
        managed = {r["table_name"] for r in rows if r["table_type"] == "MANAGED"}
        streaming = {r["table_name"] for r in rows if r["table_type"] == "STREAMING_TABLE"}

        pairs = [(s, p)
                 for e in model.entities
                 if naming.vault_schema_for(e.kind, args.schema, args.business_vault_schema)
                 == schema
                 for (_s1, s), (_s2, p) in zip(e.stable_tables(), e.tables())]

        # A pipeline-owned object gets its versioned name from the next deployment of its
        # pipeline -- Entity.tables() already drives that -- so there is nothing to
        # migrate, and renaming it would fail outright.
        pipeline_owned = [(s, p) for s, p in pairs if s in streaming]
        for s, p in pipeline_owned:
            print(f"  ~ {schema}.{s}: STREAMING_TABLE, owned by its SDP pipeline -- its "
                  f"versioned name ({p}) comes from the next deployment, not from a "
                  f"rename. Skipped.")

        renameable = [(s, p) for s, p in pairs if s not in streaming]

        try:
            todo = plan_migration(managed, renameable)
        except ValueError as exc:
            failures.append(f"{schema}: {exc}")
            continue

        if not todo:
            print(f"{schema}: nothing to migrate -- every declared MANAGED table already "
                  f"carries a version suffix.")
            continue

        for old, new in todo:
            rows_n = spark.sql(f"SELECT COUNT(*) AS n FROM "
                                f"`{args.catalog}`.`{schema}`.`{old}`").collect()[0]["n"]
            print(f"  {schema}.{old} -> {new}   ({rows_n:,} row(s) follow the rename)")
            if not args.apply:
                continue
            spark.sql(f"ALTER TABLE `{args.catalog}`.`{schema}`.`{old}` "
                      f"RENAME TO `{args.catalog}`.`{schema}`.`{new}`")
            spark.sql(f"CREATE OR REPLACE VIEW `{args.catalog}`.`{schema}`.`{old}` "
                      f"AS SELECT * FROM `{args.catalog}`.`{schema}`.`{new}`")

    if failures:
        for f in failures:
            print(f"  * {f}", file=sys.stderr)
        print(f"GATE SUMMARY :: {GATE} :: status=FAILED")
        return 1
    if not args.apply:
        print("DRY RUN -- nothing was changed. Re-run with --apply to perform the "
              "renames above.")
    print(f"GATE SUMMARY :: {GATE} :: status=PASSED")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. Exit explicitly only on failure; falling off the end is exit 0.
    _rc = main()
    if _rc:
        sys.exit(_rc)
