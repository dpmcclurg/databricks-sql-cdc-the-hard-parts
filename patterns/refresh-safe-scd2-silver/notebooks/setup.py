# Databricks notebook source
# One-time setup: Silver and ops schemas, one tombstone table per Silver table (same key and
# column types as Bronze), and the refresh_pins and bronze_identity tables used by the recovery
# job. Idempotent.
import json

for w_ in ("catalog", "bronze_schema", "silver_schema", "ops_schema", "tables"):
    dbutils.widgets.text(w_, "")
CAT = dbutils.widgets.get("catalog")
BRONZE = f"{CAT}.{dbutils.widgets.get('bronze_schema')}"
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
OPS = f"{CAT}.{dbutils.widgets.get('ops_schema')}"
TABLES = json.loads(dbutils.widgets.get("tables"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")
for t, cfg in TABLES.items():
    cols = ", ".join(cfg["keys"] + cfg["columns"])
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.{t}_tombstones AS
                  SELECT {cols}, CAST(NULL AS TIMESTAMP) AS _seq, CAST(NULL AS BOOLEAN) AS _is_delete
                  FROM {BRONZE}.{t} WHERE false""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.refresh_pins (bronze_table STRING, refresh_time TIMESTAMP,
              pin_version BIGINT, detected_by STRING, recorded_at TIMESTAMP, tombstoned BOOLEAN)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.bronze_identity (bronze_table STRING, table_id STRING,
              recorded_at TIMESTAMP)""")
print("setup complete:", list(TABLES))
