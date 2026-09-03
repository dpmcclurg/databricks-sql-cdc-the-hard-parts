# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Streaming SCD2 — capture changes (and the executable job source)
# MAGIC ### Standalone Structured Streaming SCD Type 2 off bronze's change feed, OUTSIDE Lakeflow Declarative Pipelines
# MAGIC
# MAGIC **Why this variant exists.** It is the only build that can apply change data **continuously /
# MAGIC always-on** today (sub-minute latency). The LDP variants (`01`–`03`) are triggered-only. It runs the
# MAGIC change-feed SCD2 build *outside* Lakeflow Declarative Pipelines so it can set `schemaTrackingLocation`
# MAGIC under its own checkpoint, and applies SCD2 with a content-comparing `foreachBatch` MERGE. **This
# MAGIC notebook is also the executable source the `silver_stream` jobs run** — what you present here is
# MAGIC exactly what runs.
# MAGIC
# MAGIC **What to expect.** Real change-time validity dates (`__START_AT`/`__END_AT` from the CDF
# MAGIC `_commit_timestamp`). In `continuous` mode, new versions land within the micro-batch interval (the
# MAGIC freshness readout in cell 5 shows seconds-since-newest); in `triggered` mode each run drains available
# MAGIC change data and stops. A re-delivered unchanged row opens no new version (idempotent MERGE).
# MAGIC
# MAGIC **The architectural decision it informs.** Choose this **only if you need genuinely low-latency,
# MAGIC sub-minute SCD2 apply**. On a triggered cadence the in-LDP change-feed build (`01_changefeed_scd2`)
# MAGIC dominates it. The costs: it leaves LDP, uses a hand-written SCD2 MERGE, and a bronze full refresh jams
# MAGIC the stream and needs a checkpoint reset (`04b_full_refresh`).
# MAGIC
# MAGIC ## Run mode
# MAGIC Two modes, chosen by the `mode` widget:
# MAGIC - `triggered` (default): `trigger(availableNow=True)` — drains available change data and stops. Matches
# MAGIC   the triggered cadence of the LDP variants. Run from the `silver_stream_scd2` job:
# MAGIC   `databricks bundle run silver_stream_job -t dev --profile sqlcdc`.
# MAGIC - `continuous`: `trigger(processingTime=<interval>)` — always-on micro-batch streams that never stop,
# MAGIC   for low end-to-end latency. Run from the `silver_stream_continuous` job (unpause it, observe, then
# MAGIC   re-pause). This is the closest OSS equivalent to the internal Realtime/Continuous MI pipelines that
# MAGIC   standard Lakeflow Connect config does not expose.
# MAGIC
# MAGIC Full-refresh caveat (both modes): a bronze full refresh jams the CDF stream and needs a checkpoint
# MAGIC reset — see `04b_full_refresh`. In continuous mode the job auto-restart loops on the jam, so pause the
# MAGIC job around a planned refresh.

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.dropdown("mode", "triggered", ["triggered", "continuous"])
dbutils.widgets.text("processing_time", "10 seconds")
CAT = dbutils.widgets.get("catalog")
BRONZE = dbutils.widgets.get("bronze_schema")
MODE = dbutils.widgets.get("mode")
PROCESSING_TIME = dbutils.widgets.get("processing_time")
SILVER = "silver_stream"
CHK = f"/Volumes/{CAT}/{SILVER}/_chk"

from functools import reduce
from pyspark.sql import functions as F, Window

TABLES = {
    "account":  ["hmy"],
    "property": ["hmy"],
    "tenant":   ["hmy"],
    "lease":    ["property_hmy", "tenant_hmy"],
}
META = {"_change_type", "_commit_version", "_commit_timestamp"}


def make_upsert(table, keys):
    tgt = f"{CAT}.{SILVER}.{table}"

    def upsert(mb, batch_id):
        if len(mb.take(1)) == 0:
            return
        biz = [c for c in mb.columns if c not in META]      # business cols (incl keys)
        content = [c for c in biz if c not in keys]         # non-key business cols

        # Latest change per key within this microbatch.
        w = Window.partitionBy(*keys).orderBy(
            F.col("_commit_version").desc(), F.col("_change_type").desc())
        latest = (mb.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"))

        # Compare against the current open version to classify: new key / content changed / delete.
        cur = (spark.table(tgt).filter("`__END_AT` IS NULL")
               .withColumn("_cur", F.lit(True))
               .select(*keys, "_cur", *[F.col(c).alias(f"_cur_{c}") for c in content]))
        j = latest.join(cur, keys, "left")
        t_exists = F.col("_cur").isNotNull()
        is_delete = F.col("_change_type") == "delete"
        changed = reduce(lambda a, c: a | (~F.col(c).eqNullSafe(F.col(f"_cur_{c}"))),
                         content, F.lit(False))

        keep = biz + list(META)
        to_close = j.filter(t_exists & (changed | is_delete)).select(*keep)
        to_insert = j.filter((~is_delete) & (~t_exists | changed)).select(*keep)

        to_close.createOrReplaceTempView(f"_close_{table}")
        to_insert.createOrReplaceTempView(f"_ins_{table}")
        on_keys = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)

        # 1) close the current open version for changed/deleted keys (idempotent: only matches open rows)
        spark.sql(f"""
          MERGE INTO {tgt} t USING _close_{table} s ON {on_keys} AND t.`__END_AT` IS NULL
          WHEN MATCHED THEN UPDATE SET t.`__END_AT` = s._commit_timestamp
        """)
        # 2) insert the new open version (idempotent: matches only if this exact version already exists)
        ins_cols = ", ".join(f"`{c}`" for c in biz) + ", `__START_AT`, `__END_AT`"
        ins_vals = ", ".join(f"s.`{c}`" for c in biz) + ", s._commit_timestamp, cast(null as timestamp)"
        spark.sql(f"""
          MERGE INTO {tgt} t USING _ins_{table} s
            ON {on_keys} AND t.`__START_AT` = s._commit_timestamp
          WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """)

    return upsert


# COMMAND ----------

# MAGIC %md
# MAGIC ## The build code, and the assumed key
# MAGIC
# MAGIC - **`keys=["hmy"]` (lease: the consolidated `[property_hmy, tenant_hmy]`) is an ASSUMED key, not a
# MAGIC   source primary key.** The source is a keyless heap; SCD Type 2 still needs a logical identity, so the
# MAGIC   `foreachBatch` MERGE keys on `hmy` (`on_keys` above). It is never a declared PK.
# MAGIC - **This variant assumes an already-CLEANED key.** We assume a source we cannot fully control (like a
# MAGIC   vendor ERP) where `hmy` is nullable and can arrive dirty, and this raw `foreachBatch` MERGE does not screen out NULL or duplicate keys — a NULL `hmy`
# MAGIC   would land as its own bogus "entity." So the `05_assumed_key_dq` quarantine + circuit breaker is a
# MAGIC   **prerequisite upstream** of a real streaming deployment; this notebook demonstrates the latency
# MAGIC   mechanism, not the key hardening.
# MAGIC - **`schemaTrackingLocation` under the checkpoint** is what lets the `readChangeFeed` stream exist
# MAGIC   outside LDP (LDP forbids a schema log outside its managed checkpoint). It is also why a bronze full
# MAGIC   refresh jams it, not the LDP `INCOMPATIBLE_SCHEMA_CHANGE` — see `04b`.
# MAGIC - **The MERGE is content-comparing and idempotent:** close the superseded/deleted open version, then
# MAGIC   insert the new version only if it is not already present. A re-delivered unchanged row is a no-op.
# MAGIC
# MAGIC The two cells below are the runnable source (bootstrap, then one CDF stream per table).

# COMMAND ----------

# This notebook owns the silver_stream schema and its checkpoint volume (no LDP to manage them),
# so create them idempotently before bootstrapping the targets. Makes Variant C self-contained.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SILVER}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{SILVER}._chk")

# One-time bootstrap: seed each silver_stream target with bronze's current state as open versions.
for table, keys in TABLES.items():
    tgt = f"{CAT}.{SILVER}.{table}"
    if not spark.catalog.tableExists(tgt):
        spark.sql(f"""
          CREATE TABLE {tgt} AS
          SELECT *, current_timestamp() AS `__START_AT`, CAST(NULL AS TIMESTAMP) AS `__END_AT`
          FROM {CAT}.{BRONZE}.{table}
        """)
        print(f"bootstrapped {tgt}")

# COMMAND ----------

# One CDF stream per table. schemaTrackingLocation lives UNDER the checkpoint (allowed outside LDP).
# Trigger depends on mode: availableNow drains-and-stops (triggered); processingTime is always-on (continuous).
trigger = {"availableNow": True} if MODE == "triggered" else {"processingTime": PROCESSING_TIME}
print(f"mode={MODE}, trigger={trigger}")

queries = []
for table, keys in TABLES.items():
    q = (spark.readStream.format("delta")
         .option("readChangeFeed", "true")
         .option("schemaTrackingLocation", f"{CHK}/{table}/chk/schema")
         .table(f"{CAT}.{BRONZE}.{table}")
         .writeStream
         .queryName(f"{SILVER}_{table}")
         .option("checkpointLocation", f"{CHK}/{table}/chk")
         .foreachBatch(make_upsert(table, keys))
         .trigger(**trigger)
         .start())
    queries.append((table, q))

if MODE == "triggered":
    # Each availableNow query drains its backlog and terminates; wait for all to finish, then the job ends.
    for table, q in queries:
        q.awaitTermination()
        print(f"{table}: stream drained")
else:
    # Continuous: streams never terminate on their own. Block on any termination so a failure in one
    # stream surfaces (and, under the continuous job, triggers the job's auto-restart) instead of being
    # masked while we wait on a healthy stream.
    print(f"continuous: {len(queries)} always-on streams at processingTime={PROCESSING_TIME}")
    spark.streams.awaitAnyTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drive the capture-changes scenario (run in a terminal)
# MAGIC From the repo root, with the venv active and `SQLDB_*` exported:
# MAGIC
# MAGIC ```bash
# MAGIC python sim/changegen.py churn --table account --updates 10
# MAGIC python sim/changegen.py burst --table tenant  --key 42 --updates 8
# MAGIC ```
# MAGIC
# MAGIC Then let the SCD1 bronze ingest, and run this notebook (triggered) or leave the continuous job running:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run bronze_ingestion   -t dev --profile sqlcdc   # SCD1 bronze picks up the DML
# MAGIC databricks bundle run silver_stream_job  -t dev --profile sqlcdc   # triggered: drains and stops
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result: real change-time dates, and continuous-mode freshness
# MAGIC Steady-state SCD2 history with validity dates from the change feed's `_commit_timestamp`.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status, `__START_AT`, `__END_AT`
  FROM {CAT}.{SILVER}.account
  WHERE hmy IN (1, 2, 3, 10, 11)
  ORDER BY hmy, `__START_AT`
""").display()

# COMMAND ----------

# Freshness: how recently the newest version was applied. In continuous mode this stays small
# (sub-minute), illustrating the latency advantage; in triggered mode it grows between runs.
spark.sql(f"""
  SELECT count(*) AS versions,
         max(`__START_AT`) AS newest_version_applied,
         timestampdiff(SECOND, max(`__START_AT`), current_timestamp()) AS seconds_since_newest
  FROM {CAT}.{SILVER}.account
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC Choose this variant only if you need genuinely low-latency, sub-minute SCD2 apply, accepting the
# MAGIC custom-code and per-refresh checkpoint-reset cost. On a triggered cadence the in-LDP change-feed build
# MAGIC (`01_changefeed_scd2`) delivers the same dated SCD2 with no custom code. The full-refresh jam and its
# MAGIC recovery are in `04b_full_refresh`. Cross-variant scoreboard: `08_comparison/08_results.py`.
