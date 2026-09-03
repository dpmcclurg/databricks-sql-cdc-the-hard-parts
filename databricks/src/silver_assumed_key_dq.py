"""Part A: DQ-hardened SCD Type 2 on an ASSUMED primary key (hmy) from a keyless source.

The source table has zero database constraints, so hmy is only an *assumed* key. This pipeline
defends it with a Tier-1 quarantine (row-level, non-fatal): rows that fail the row-level key rule
are routed to a persisted quarantine table instead of contaminating history, and only valid rows
build SCD2 (AUTO CDC FROM SNAPSHOT keyed on hmy). The Tier-2 circuit breaker (cross-row uniqueness)
lives in a separate validation pipeline (silver_assumed_key_validation.py) gated by a job, because a
validation table cannot gate its own downstream.

Config: catalog, bronze_schema (default bronze_nopk), table (default account_nopk).
Publishes to schema silver_dq.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
TABLE = spark.conf.get("table", "account_nopk")
KEY = "hmy"

# Row-level DQ rule for the assumed key. Uniqueness is cross-row -> handled by the validation
# pipeline + circuit breaker, not here.
VALID = "hmy IS NOT NULL AND hmy > 0"


# Valid current-state rows (feeds the SCD2 snapshot flow).
@dp.materialized_view(name="account_valid")
@dp.expect_all({"assumed_key_valid": VALID})   # violation counts surface in the event log
def _valid():
    return spark.read.table(f"{catalog}.{BRONZE}.{TABLE}").filter(VALID)


# Tier 1 quarantine: invalid rows preserved for triage, never dropped, never in SCD2.
@dp.table(name="account_quarantine")
def _quarantine():
    return (
        spark.read.table(f"{catalog}.{BRONZE}.{TABLE}")
        .filter(f"NOT({VALID})")
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_reason", F.lit("assumed key hmy null or <= 0"))
    )


# SCD2 history from the valid snapshot, keyed on the assumed key.
dp.create_streaming_table(name=TABLE)
dp.create_auto_cdc_from_snapshot_flow(
    target=TABLE,
    source=f"{catalog}.silver_dq.account_valid",
    keys=[KEY],
    stored_as_scd_type=2,
)
