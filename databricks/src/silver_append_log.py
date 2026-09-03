"""Variant E: append-only change log (no key required at all).

When nothing identifies an entity, you cannot build entity-level SCD2. Instead capture every change
operation as an append-only audit log: each insert / update / delete from bronze's change feed becomes
one immutable row, stamped with its operation type and commit timestamp. Full change history, no key,
no SCD2 collapse.

Feasibility: fine for small tables; INFEASIBLE for large ones because the log grows unbounded (one row
per operation forever). For larger tables, partition by an ingest date and apply a retention policy, or
prefer Variant D (content-hash SCD2) if a stable identifying column subset exists.

Config: catalog, bronze_schema (default bronze_nopk), table (default account_nopk).
Publishes to schema silver_log. Requires change data feed on the bronze source table.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
TABLE = spark.conf.get("table", "account_nopk")


# Append-only: every change event as an immutable row (no keys=, no AUTO CDC).
@dp.table(name=f"{TABLE}_log", partition_cols=["op_date"])
def _log():
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{catalog}.{BRONZE}.{TABLE}")
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .withColumnRenamed("_change_type", "op")
        .withColumnRenamed("_commit_timestamp", "op_ts")
        .withColumn("op_date", F.to_date("op_ts"))
        .drop("_commit_version")
    )
