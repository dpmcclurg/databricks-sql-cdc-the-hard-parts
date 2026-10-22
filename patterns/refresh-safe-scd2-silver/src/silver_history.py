"""Refresh-safe SCD Type 2 Silver over Lakeflow Connect SCD Type 2 Bronze.

Per table, three AUTO CDC flows write one SCD Type 2 target:
  <table>            main flow from Bronze's change feed
  <table>_tombstones deletes that a Bronze full refresh erased (written by the recovery job)
  <table>_seed       optional ONCE flow replaying legacy history (migration / rebuild only)

Main flow event rules, for an SCD Type 2 Bronze table:
  - every inserted Bronze row is a version open, sequenced by its __START_AT time;
  - a closed Bronze row (__END_AT set) is a DELETE only when no Bronze row for the same key
    starts at the same LSN. An update closes the old version and opens its successor at that
    LSN, so a close with no successor is a source delete. Closed rows arrive as
    update_postimage in normal operation and as insert on a first build or a reset re-read.
Snapshot rows carry a NULL __cdc_timestamp_value. Every such row belongs to Bronze's current
snapshot (a full refresh replaces older ones), so its open time is the snapshot time: the latest
DLT REFRESH in the table history, or the table's creation. Falling back to the change feed's
commit time instead would date a re-read row after its own close and reopen a deleted key.

Silver tables block full refresh (pipelines.reset.allowed=false) unless allow_full_refresh is
"true"; the checkpoint reset the recovery job uses is not affected by that guard.

Pipeline configuration: catalog, bronze_schema, ops_schema, allow_full_refresh, tables (JSON:
{"<table>": {"keys": [...], "columns": [...], "seed": "<catalog.schema.table>" (optional)}}).
"""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("catalog")
BRONZE = f"{CAT}.{spark.conf.get('bronze_schema')}"
OPS = f"{CAT}.{spark.conf.get('ops_schema')}"
TABLES = json.loads(spark.conf.get("tables"))
ALLOW_FULL_REFRESH = spark.conf.get("allow_full_refresh", "false")
EVENT_COLS = ["_seq", "_is_delete"]


def _change_time(validity, fallback):
    return F.coalesce(F.col(validity).getField("__cdc_timestamp_value"), fallback)


def _snapshot_time(table):
    hist = spark.sql(f"DESCRIBE HISTORY {table}")
    marker = hist.where("operation = 'DLT REFRESH'").agg(F.max("timestamp")).first()[0]
    return marker or hist.agg(F.min("timestamp")).first()[0]


def build(table, keys, columns, seed):
    source = f"{BRONZE}.{table}"
    out = keys + columns

    @dp.temporary_view(name=f"{table}_events")
    def _events():
        snapshot_time = F.lit(_snapshot_time(source)).cast("timestamp")
        changes = spark.readStream.option("readChangeFeed", "true").table(source)
        opens = (changes.filter(F.col("_change_type") == "insert")
                 .select(*out, _change_time("__START_AT", snapshot_time).alias("_seq"),
                         F.lit(False).alias("_is_delete")))
        closes = changes.filter(F.col("_change_type").isin("insert", "update_postimage")
                                & F.col("__END_AT").isNotNull())
        successors = spark.read.table(source).select(
            *[F.col(k).alias(f"_s_{k}") for k in keys],
            F.col("__START_AT").getField("__cdc_internal_value").alias("_s_lsn"))
        has_successor = [closes[k] == successors[f"_s_{k}"] for k in keys] + [
            closes["__END_AT"].getField("__cdc_internal_value") == successors["_s_lsn"]]
        deletes = (closes.join(successors, has_successor, "left_anti")
                   .select(*out, _change_time("__END_AT", F.col("_commit_timestamp")).alias("_seq"),
                           F.lit(True).alias("_is_delete")))
        return opens.unionByName(deletes)

    @dp.temporary_view(name=f"{table}_tombstones_v")
    def _tombstones():
        return spark.readStream.table(f"{OPS}.{table}_tombstones")

    dp.create_streaming_table(name=table, comment=f"Durable SCD Type 2 history of {source}",
                              table_properties={"pipelines.reset.allowed": ALLOW_FULL_REFRESH})
    common = dict(target=table, keys=keys, sequence_by=F.col("_seq"),
                  apply_as_deletes=F.expr("_is_delete"), except_column_list=EVENT_COLS,
                  stored_as_scd_type=2)
    dp.create_auto_cdc_flow(source=f"{table}_events", **common)
    dp.create_auto_cdc_flow(flow_name=f"{table}_tombstones", source=f"{table}_tombstones_v", **common)

    if seed:
        @dp.temporary_view(name=f"{table}_seed_v")
        def _seed():
            return spark.readStream.table(seed)

        dp.create_auto_cdc_flow(flow_name=f"{table}_seed", source=f"{table}_seed_v", once=True, **common)


for _table, _cfg in TABLES.items():
    build(_table, _cfg["keys"], _cfg["columns"], _cfg.get("seed"))
