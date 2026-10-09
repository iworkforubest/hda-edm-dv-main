"""
Publish the committed model metadata to Unity Catalog for lineage and inspection.

READ-ONLY BY CONSTRUCTION: this table is a projection of the repository, refreshed on
every deploy. It is never an input to a load. If it drifts from the repo, the repo wins
and the next deploy corrects it -- which is what keeps structural change reviewable.
"""

from __future__ import annotations

# DEF-12: serverless `spark_python_task` exec()s this file and does NOT define
# __file__, so every Path(__file__) below raised NameError and the gate died before
# asserting anything. compile() still records the real path in the code object.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import Row, SparkSession  # noqa: E402

from accelerator import audit, spec  # noqa: E402
from accelerator import VERSION  # noqa: E402
from accelerator.hashing import RULEBOOK_VERSION  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--control-schema", required=True)
    ap.add_argument("--job-run-id", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--active-sources", default="")
    args = ap.parse_args()

    meta_dir = Path(__file__).resolve().parents[1] / "metadata" / "entities"
    model = spec.load_model(meta_dir)
    spark = SparkSession.builder.getOrCreate()

    rows = [
        Row(
            table_name=e.base_table,
            generated_tables=[name for _s, name in e.tables()],
            entity=e.name,
            kind=e.kind,
            domain=e.domain,
            key_style=e.key_style,
            sensitivity=e.sensitivity,
            business_keys=list(e.business_keys),
            parents=list(e.parents),
            payload=list(e.payload),
            transaction_key=list(e.transaction_key),
            source_count=len(e.sources),
            sources=[s.name for s in e.sources],
            change_detection=e.change_detection,
            rulebook_version=RULEBOOK_VERSION,
            accelerator_version=VERSION,
            grain=e.grain,
            aggregates_from=e.aggregates_from,
            aggregate_drops=list(e.aggregate_drops),
        )
        for e in model.entities
    ]
    target = f"{args.catalog}.{args.schema}.meta_vault_model"
    spark.createDataFrame(rows).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)
    spark.sql(f"COMMENT ON TABLE {target} IS "
              f"'Projection of metadata/entities in the accelerator repo. Read-only.'")
    print(f"published {len(rows)} entities to {target}")

    # The run is closed only once everything before it has succeeded. If this write fails
    # the task fails, so a run with no 'completed' row is a run that did not finish --
    # which is what checks/audit_completeness_check.py asserts.
    spark.sql(audit.load_run_sql(
        args.catalog, args.control_schema, job_run_id=args.job_run_id,
        phase="completed", target=args.target, active_sources=args.active_sources))
    print(f"run {args.job_run_id} completed")
    return 0


if __name__ == "__main__":
    # DEF-14: serverless spark_python_task runs this under an ipykernel wrapper that
    # surfaces SystemExit as an exception and marks the task FAILED -- for exit code 0
    # as readily as for 1. A PASSING gate therefore failed its task and blocked every
    # task behind it. Exit explicitly only on failure; falling off the end is exit 0.
    # Identical behaviour for a shell, correct behaviour on serverless.
    _rc = main()
    if _rc:
        sys.exit(_rc)
