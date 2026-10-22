"""TEST ONLY. The repository's original silver_scd2 logic (CDF -> AUTO CDC SCD Type 2,
sequenced by _commit_timestamp, deletes only on _change_type = 'delete') pointed at SCD Type 2
Bronze. Used as the legacy history for the migration test."""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("catalog")
BRONZE = f"{CAT}.{spark.conf.get('bronze_schema')}"
TABLES = json.loads(spark.conf.get("tables"))

for _t, _c in TABLES.items():
    def build(t, cfg):
        @dp.temporary_view(name=f"{t}_changes")
        def _v():
            return (spark.readStream.option("readChangeFeed", "true").table(f"{BRONZE}.{t}")
                    .filter(F.col("_change_type") != "update_preimage")
                    .select(*cfg["keys"], *cfg["columns"], "_change_type", "_commit_timestamp"))

        dp.create_streaming_table(name=t)
        dp.create_auto_cdc_flow(
            target=t, source=f"{t}_changes", keys=cfg["keys"], sequence_by=F.col("_commit_timestamp"),
            apply_as_deletes=F.expr("_change_type = 'delete'"),
            except_column_list=["_change_type", "_commit_timestamp"], stored_as_scd_type=2,
        )
    build(_t, _c)
