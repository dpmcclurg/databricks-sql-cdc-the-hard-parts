"""Silver SCD Type 2 built with AUTO CDC FROM SNAPSHOT reading bronze's current state.

The full-refresh-safe alternative to the readChangeFeed build (silver_scd2.py). Because it
reads bronze's current state as a periodic SNAPSHOT (batch) instead of streaming its change
feed, there is no streaming schema log to break, so a bronze FULL REFRESH does not fail it
(no DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE). Snapshot comparison is idempotent: a
re-delivered unchanged current state opens no new SCD2 version, so history is preserved
across a bronze full refresh. Fidelity is between-snapshot (trigger-interval), the same
ceiling the CDF build already has. Stays fully in LDP.

Reads bronze_schema from pipeline configuration; source is bronze's current-state table
(SCD Type 1), one row per key.
"""
from pyspark import pipelines as dp

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema", "bronze")

# table -> business key columns
TABLES = {
    "account":  ["hmy"],
    "property": ["hmy"],
    "tenant":   ["hmy"],
    "lease":    ["property_hmy", "tenant_hmy"],
}


def build(table, keys):
    dp.create_streaming_table(name=table)
    dp.create_auto_cdc_from_snapshot_flow(
        target=table,
        source=f"{catalog}.{bronze_schema}.{table}",   # current-state snapshot, read each run
        keys=keys,
        stored_as_scd_type=2,
    )


for _t, _k in TABLES.items():
    build(_t, _k)
