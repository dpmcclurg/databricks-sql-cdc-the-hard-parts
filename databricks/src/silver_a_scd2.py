"""Variant A downstream: durable SCD Type 2 built by AUTO CDC from the SCD2 connector
target's (bronze_scd2) change feed.

Keyed on the business key, sequenced by the connector's readable change timestamp
(__START_AT.__cdc_timestamp_value), falling back to the bronze commit time for snapshot rows
(which have a NULL cdc timestamp). Reads version-open events (inserts) so it captures every
connector SCD2 version (full fidelity). Because silver_a lives outside the connector's
pipeline, its accumulated history is durable across a connector full refresh; after a refresh
recover the flow with reset_checkpoint_selection (checkpoint reset, keep data), and AUTO CDC
idempotency suppresses spurious versions on the re-delivered current state.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
bronze_scd2_schema = spark.conf.get("bronze_scd2_schema", "bronze_scd2")

TABLES = {
    "account":  ["hmy"],
    "property": ["hmy"],
    "tenant":   ["hmy"],
    "lease":    ["property_hmy", "tenant_hmy"],
}


def build(table, keys):
    view = f"{table}_ver_opens"

    @dp.temporary_view(name=view)
    def _v():
        return (
            spark.readStream
            .option("readChangeFeed", "true")
            .table(f"{catalog}.{bronze_scd2_schema}.{table}")
            # version-open events = inserts of new SCD2 versions in the connector target
            .filter(F.col("_change_type") == "insert")
            # readable sequence: connector change time, fallback to bronze commit time (snapshot rows are NULL)
            .withColumn(
                "_seq",
                F.coalesce(F.col("__START_AT").getField("__cdc_timestamp_value"), F.col("_commit_timestamp")),
            )
        )

    dp.create_streaming_table(name=table)
    dp.create_auto_cdc_flow(
        target=table,
        source=view,
        keys=keys,
        sequence_by=F.col("_seq"),
        stored_as_scd_type=2,
        # exclude the connector's own validity structs and CDF/derived metadata from the target;
        # silver_a builds its OWN __START_AT / __END_AT
        except_column_list=[
            "__START_AT", "__END_AT", "_change_type", "_commit_version", "_commit_timestamp", "_seq",
        ],
    )


for _t, _k in TABLES.items():
    build(_t, _k)
