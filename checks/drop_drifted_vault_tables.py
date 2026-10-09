"""Drop vault tables whose DEPLOYED shape no longer matches the model, and only those.

WHY THIS EXISTS. A vault table cannot be altered in place: it carries
delta.appendOnly = true, which is what checks/append_only_check.py enforces, and a full
refresh RESETS a table -- a truncate, which Delta refuses with
DELTA_CANNOT_MODIFY_APPEND_ONLY. docs/RELOADING.md states the consequence plainly: changing
a vault table's shape means dropping and rebuilding it.

MEASURED 25 September. hub_organisation and hub_worker were created on 25 AUGUST, survived
the pipeline deletion that the domain split required (they are loader-created, not
pipeline-managed), and kept a column order no current code writes -- system columns before
the business keys, and in hub_worker no business-key columns at all. load_hubs inserts by
position, so 'Fieldglass_Buyer_Code' landed in load_dts:

    [CAST_INVALID_INPUT] The value 'Fieldglass_Buyer_Code' of the type "STRING" cannot be
    cast to "TIMESTAMP"
    [DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta
    table.

IT DROPS ONLY WHAT IT CAN PROVE IS WRONG. The named table is compared, column for column
and in order, against what the model declares for it. A table that MATCHES is left alone
and reported -- so a mistyped name, a stale invocation, or this task being left in the job
by accident destroys nothing. That is the whole safety argument: the justification for
dropping is computed here, not asserted by the caller.

IT IS NOT PART OF THE STANDING JOB. Run it deliberately, read what it says it will destroy,
and take it back out. Row counts are printed before the drop so the cost is on the record.
"""

from __future__ import annotations

# DEF-12: serverless spark_python_task exec()s this file and does not define __file__.
if "__file__" not in globals():  # noqa: F821
    import inspect as _inspect

    __file__ = _inspect.currentframe().f_code.co_filename

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def deployed_columns(spark, fq: str):
    """(name, type) in ordinal order, or None when the table does not exist."""
    try:
        rows = spark.sql(f"DESCRIBE TABLE {fq}").collect()
    except Exception as exc:  # noqa: BLE001 -- absence is an answer; anything else is not
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            return None
        raise
    out = []
    for r in rows:
        d = r.asDict()
        name = (d.get("col_name") or "").strip()
        if not name or name.startswith("#"):
            break
        out.append((name.lower(), (d.get("data_type") or "").strip().lower()))
    return out


def declared_columns(entity, src):
    """What the model says this table's columns are, in order.

    Through reject_digest rather than factory directly: importing accelerator.factory pulls
    in pyspark.pipelines, which dies at the import hook outside a DLT pipeline, and
    verify_repo refuses any checks/*.py that does it. reject_digest.projected_columns keeps
    that import function-local in the one module allowed to hold it.
    """
    from accelerator import reject_digest  # noqa: PLC0415 -- needs pyspark, so import late

    return [c.lower() for c in reject_digest.projected_columns(entity, src)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--tables", required=True,
                    help="comma-separated table names to CONSIDER; each is dropped only "
                         "if its deployed shape disagrees with the model")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from pyspark.sql import SparkSession  # noqa: E402

    from accelerator import spec  # noqa: E402

    spark = SparkSession.builder.getOrCreate()
    model = spec.load_model(ROOT / "metadata" / "entities")
    # A HUB, LINK, NHL OR HAL YIELDS ONE TABLE AND NO BINDING -- entity.tables() returns
    # (None, table) for them, because the table is conformed across every source. The
    # projection needs a binding to decide whether a business-key position is a literal or a
    # column, but the column NAMES are the entity's either way, and names are all this
    # compares. So any of its bindings serves.
    by_table = {t: (e, s or (e.sources[0] if e.sources else None))
                for e in model.entities for s, t in e.tables()}

    wanted = [t.strip() for t in args.tables.split(",") if t.strip()]
    dropped, kept, absent, unknown = [], [], [], []

    for table in wanted:
        fq = f"`{args.catalog}`.`{args.schema}`.`{table}`"
        if table not in by_table:
            unknown.append(table)
            print(f"  * {table}: the model declares no such table. NOT dropped -- this tool "
                  f"only removes something it can compare against a declaration")
            continue
        entity, src = by_table[table]
        deployed = deployed_columns(spark, fq)
        if deployed is None:
            absent.append(table)
            print(f"  ~ {table}: does not exist; nothing to drop")
            continue
        declared = declared_columns(entity, src)
        got = [c for c, _t in deployed]
        if got == declared:
            kept.append(table)
            print(f"  = {table}: deployed shape MATCHES the model ({len(got)} columns). "
                  f"NOT dropped")
            continue
        n = spark.sql(f"SELECT count(*) AS n FROM {fq}").collect()[0]["n"]
        print(f"  ! {table}: deployed shape DIFFERS from the model")
        print(f"      deployed: {got}")
        print(f"      declared: {declared}")
        print(f"      holds {n:,} row(s), which this destroys")
        if args.dry_run:
            print(f"      --dry-run: not dropped")
            continue
        spark.sql(f"DROP TABLE {fq}")
        dropped.append((table, n))
        print(f"      DROPPED. The next load rebuilds it from Bronze at the declared shape")

    print(f"\nDROP SUMMARY :: dropped={len(dropped)} kept={len(kept)} "
          f"absent={len(absent)} unknown={len(unknown)}")
    for t, n in dropped:
        print(f"  destroyed {n:,} row(s) in {t}")
    return 1 if unknown else 0


if __name__ == "__main__":
    # DEF-14: serverless surfaces SystemExit as a failure even for code 0 -- which is
    # exactly what happened on the first run of this task: it dropped both tables, printed
    # its summary, and the task still reported FAILED.
    _rc = main()
    if _rc:
        sys.exit(_rc)
