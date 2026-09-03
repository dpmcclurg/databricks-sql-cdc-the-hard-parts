"""Variant D: SCD Type 2 on a content-hash surrogate key (identity manufactured in-lakehouse).

For a true heap where no single column is a trusted key but a stable subset of columns identifies the
entity: land raw rows, derive a deterministic surrogate key from the STABLE identifying columns, and
build SCD2 on it.

Critical design rule: hash only the stable identifying columns (the natural key), NEVER the mutable
attributes. If the mutable attributes were in the hash, every attribute change would produce a new
surrogate key and entity history would be lost. Here the natural key is (hmy, name); status and
balance are mutable attributes that produce new SCD2 versions under a stable `_bk`.

Config: catalog, bronze_schema (default bronze_nopk), table (default account_nopk).
Publishes to schema silver_hash.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
TABLE = spark.conf.get("table", "account_nopk")

# Stable identifying columns only. Adjust per table; do NOT include mutable attributes.
NATURAL_KEY = ["hmy", "name"]


@dp.materialized_view(name="account_hashed")
def _hashed():
    cols = [F.col(c).cast("string") for c in NATURAL_KEY]
    return (
        spark.read.table(f"{catalog}.{BRONZE}.{TABLE}")
        .withColumn("_bk", F.sha2(F.concat_ws("||", *cols), 256))
    )


dp.create_streaming_table(name=TABLE)
dp.create_auto_cdc_from_snapshot_flow(
    target=TABLE,
    source=f"{catalog}.silver_hash.account_hashed",
    keys=["_bk"],
    stored_as_scd_type=2,
)
