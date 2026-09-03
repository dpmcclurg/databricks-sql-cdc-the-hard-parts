"""Variant B (snapshot) on a NO-PRIMARY-KEY source (account_nopk).

Reproduces the keyless-heap case: the source table has no PRIMARY KEY, no IDENTITY, and no
UNIQUE constraint. hmy is a plain unconstrained column. SCD Type 2 is built with AUTO CDC
FROM SNAPSHOT reading bronze_nopk's current state, keyed on hmy (a specified logical key,
not a declared PK). Proves the snapshot flow needs no primary key.
"""
from pyspark import pipelines as dp

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_nopk_schema", "bronze_nopk")

dp.create_streaming_table(name="account_nopk")
dp.create_auto_cdc_from_snapshot_flow(
    target="account_nopk",
    source=f"{catalog}.{bronze_schema}.account_nopk",
    keys=["hmy"],
    stored_as_scd_type=2,
)
