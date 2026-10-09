"""
Bronze ingestion -- the ONLY place AUTO CDC is permitted.

Bronze exists to turn whatever a source delivers into one contract for the vault:
keys, payload, cdc_op, applied_dts, manifest_id. Transactional sources already
carry an operation flag; snapshot sources are converted here by AUTO CDC FROM
SNAPSHOT so that the Silver satellite loaders never have to compare against their
own target (see factory.py, decision 3).

Invariants (medallion conformance bar, spike v2 s2):
  * append-only, never overwritten
  * schema evolution tolerated, never rejected
  * duplicates MARKED, never removed -- quarantine belongs at the Silver gate
  * ingestion metadata on every record; partitioned by ingest time, not business time
  * access restricted to data engineering
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.getActiveSession()

SOURCE_ROOT = spark.conf.get("hfig.bronze.source_root")
SOURCE_SYSTEM = spark.conf.get("hfig.bronze.source_system")


@dp.table(
    name="bronze_raw",
    table_properties={"quality": "bronze", "delta.appendOnly": "true"},
    partition_cols=["ingest_date"],
    comment="raw landing -- capture everything, change nothing",
)
def bronze_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # tolerate, never reject
        .load(SOURCE_ROOT)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("_ingest_ts"))
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("_batch_id", F.expr("current_pipeline_update_id()"))
        .withColumn("_dup_flag", F.lit(None).cast("boolean"))  # marked, never removed
    )


# --------------------------------------------------------------------------- #
# Snapshot sources: derive the change stream HERE so Silver stays append-only.
# STUB: wire create_auto_cdc_from_snapshot_flow per source once the Bronze
# contract per feed is agreed. Keys and sequence column come from the feed's
# entry in metadata/sources.yml.
# --------------------------------------------------------------------------- #
