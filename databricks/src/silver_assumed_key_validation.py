"""Part A, Tier 2: circuit breaker for the assumed primary key (hmy).

Uniqueness/non-null of an assumed key is a CROSS-ROW check that a row-level expectation cannot
express. This pipeline computes the number of key violations (duplicate hmy, null hmy, or hmy <= 0)
and asserts it is zero with `expect_or_fail` (ON VIOLATION FAIL UPDATE). A violation FAILS this
pipeline's update. Because a validation table does not gate its own downstream, run this pipeline as
the first task of a job whose SCD2-build task depends on it: if the key is breached, the build never
runs (the circuit trips).

Config: catalog, bronze_schema (default bronze_nopk), table (default account_nopk).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
TABLE = spark.conf.get("table", "account_nopk")


# One-row table: total assumed-key violations. FAIL UPDATE if not zero.
@dp.table(name="hmy_key_validation")
@dp.expect_or_fail("assumed_key_holds", "violations = 0")
def _validation():
    src = spark.read.table(f"{catalog}.{BRONZE}.{TABLE}")
    dup = (
        src.groupBy("hmy").agg(F.count("*").alias("c"))
        .filter("c > 1")
        .agg(F.count("*").alias("n")).collect()[0]["n"]
    )
    bad = src.filter("hmy IS NULL OR hmy <= 0").count()
    return spark.createDataFrame(
        [(int(dup) + int(bad), int(dup), int(bad))],
        "violations long, duplicate_keys long, invalid_keys long",
    )
