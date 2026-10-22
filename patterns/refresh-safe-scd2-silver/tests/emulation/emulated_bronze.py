"""TEST ONLY. Emulates Lakeflow Connect SCD Type 2 targets from plain Delta source tables.

Each <source>.src_<table> (CDF on) feeds an AUTO CDC SCD Type 2 Bronze table sequenced by a
struct shaped like the connector's (__cdc_internal_value = zero-padded source version as an LSN
stand-in, __cdc_timestamp_value). Rows read from a snapshot (source version <= the value in
<source>.emu_ctl, set by the test driver before a build or a full refresh) get a NULL timestamp,
as connector snapshot rows do. "detail" uses column mapping, like a connector target.
"""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("catalog")
SRC = f"{CAT}.{spark.conf.get('source_schema')}"
TABLES = json.loads(spark.conf.get("tables"))
COLUMN_MAPPED = {"detail"}


def build(t):
    @dp.temporary_view(name=f"src_{t}_changes")
    def _v():
        row = spark.read.table(f"{SRC}.emu_ctl").where(F.col("tbl") == t).first()
        snap = row.snapshot_through_version if row else -1
        return (spark.readStream.option("readChangeFeed", "true").table(f"{SRC}.src_{t}")
                .filter(F.col("_change_type") != "update_preimage")
                .withColumn("_cdc", F.struct(
                    F.lpad(F.col("_commit_version").cast("string"), 20, "0").alias("__cdc_internal_value"),
                    F.when(F.col("_commit_version") <= F.lit(snap), F.lit(None).cast("timestamp"))
                     .otherwise(F.col("_commit_timestamp")).alias("__cdc_timestamp_value"))))

    props = {"delta.columnMapping.mode": "name"} if t in COLUMN_MAPPED else None
    dp.create_streaming_table(name=t, table_properties=props)
    dp.create_auto_cdc_flow(
        target=t, source=f"src_{t}_changes", keys=TABLES[t]["keys"], sequence_by=F.col("_cdc"),
        apply_as_deletes=F.expr("_change_type = 'delete'"),
        except_column_list=["_change_type", "_commit_version", "_commit_timestamp", "_cdc"],
        stored_as_scd_type=2,
    )


for _t in TABLES:
    build(_t)
