"""Silver SCD Type 2, built with AUTO CDC from the bronze change feed.

One templated flow per table. Sequenced by the bronze CDF `_commit_timestamp`, so
silver's __START_AT / __END_AT carry real, readable timestamps even for source tables
that have no date column. AUTO CDC is content-comparing and idempotent, so re-delivered
unchanged rows (for example after a full refresh) do not open a new version.

Reads bronze_schema from pipeline configuration: "bronze" (Variant B, SCD1) or
"bronze_scd2" (Variant A). Publishes to the pipeline's default catalog.silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog", "sqlcdc_test")
bronze_schema = spark.conf.get("bronze_schema", "bronze")

# table -> business key columns
TABLES = {
    "account":  ["hmy"],
    "property": ["hmy"],
    "tenant":   ["hmy"],
    "lease":    ["property_hmy", "tenant_hmy"],
}


def build(table, keys):
    view = f"{table}_changes"

    @dp.temporary_view(name=view)
    def _changes():
        # Stream the bronze table's change feed; keep insert / update_postimage / delete.
        # NOTE on full refresh: a bronze FULL REFRESH emits a read-incompatible schema change
        # (SET TBLPROPERTIES / DLT SETUP) that fails the next normal run of this stream with
        # DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE. The standalone-streaming fix
        # (.option("schemaTrackingLocation", ...)) is NOT usable here: LDP requires the schema log to
        # live under the pipeline-managed checkpoint (DELTA_STREAMING_SCHEMA_LOCATION_NOT_UNDER_CHECKPOINT).
        # Recover with reset_checkpoint_selection on this pipeline's flows: it clears the flow
        # checkpoints, KEEPS the silver data, and re-reads bronze; AUTO CDC idempotency opens no
        # spurious versions, so history is preserved. Never full-refresh silver to recover (that
        # clears the table and collapses history to one version per key). See
        # notebooks/01_changefeed_scd2/01b_full_refresh.py and docs/experiments.md (E3, E10).
        return (
            spark.readStream
            .option("readChangeFeed", "true")
            .table(f"{catalog}.{bronze_schema}.{table}")
            .filter(F.col("_change_type") != "update_preimage")
        )

    dp.create_streaming_table(name=table)
    dp.create_auto_cdc_flow(
        target=table,
        source=view,
        keys=keys,
        sequence_by=F.col("_commit_timestamp"),
        apply_as_deletes=F.expr("_change_type = 'delete'"),
        except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
        stored_as_scd_type=2,
    )


for _t, _k in TABLES.items():
    build(_t, _k)
