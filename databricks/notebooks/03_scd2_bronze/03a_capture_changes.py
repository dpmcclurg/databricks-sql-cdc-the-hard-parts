# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · SCD2-bronze — capture changes
# MAGIC ### SCD Type 2 connector target  ->  AUTO CDC SCD Type 2 silver, off the SCD2 bronze change feed
# MAGIC
# MAGIC **Why this variant exists.** It is the **full per-change fidelity** build. The connector target itself is
# MAGIC SCD Type 2 (`bronze_scd2`), so *every* logged mutation is a distinct version — nothing collapses to a
# MAGIC net-per-interval value. A downstream AUTO CDC flow off that target's change feed makes the history
# MAGIC durable (the connector target itself does not survive a full refresh; see `03b`).
# MAGIC
# MAGIC **What to expect.** Silver carries change-time `__START_AT` / `__END_AT` (from the connector's own change
# MAGIC timestamp, NULL only on the initial snapshot rows — a commonly observed artifact). A burst of 8 updates to
# MAGIC one key keeps ~9 versions here, versus ~1 in `01_changefeed_scd2`. That is the fidelity fork.
# MAGIC
# MAGIC **The architectural decision it informs.** Choose this only when every logged change must be a distinct
# MAGIC version (strict per-change audit). If net-change-per-interval history is enough — the common case —
# MAGIC `01_changefeed_scd2` is simpler and cheaper.
# MAGIC
# MAGIC **Prerequisite (not optional).** We assume a source we cannot fully control (like a vendor ERP), where
# MAGIC the assumed key `hmy` is **nullable** and can arrive dirty. So this build assumes an already-*cleaned* key: a NULL or duplicate `hmy` would corrupt SCD2 (AUTO CDC
# MAGIC misbehaves on a null key). Run the data-quality quarantine + circuit breaker (`05_assumed_key_dq`)
# MAGIC upstream first — it is what makes an assumed key safe to build SCD on, not an add-on.
# MAGIC
# MAGIC ```
# MAGIC Azure SQL (CDC) -> gateway -> bronze_scd2 (SCD2, every version) -> silver_a (SCD2, durable, dated history)
# MAGIC                               connector target                    AUTO CDC off bronze_scd2 CDF (version-opens)
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
print("catalog:", CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build code (this is the entire silver definition)
# MAGIC
# MAGIC From `databricks/src/silver_a_scd2.py`. It reads the SCD2 connector target's change feed, keeps
# MAGIC version-open (`insert`) events so it captures every connector version, and sequences by the connector's
# MAGIC readable change timestamp — falling back to the bronze commit time for snapshot rows (which are NULL).
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC bronze_scd2_schema = spark.conf.get("bronze_scd2_schema", "bronze_scd2")
# MAGIC
# MAGIC TABLES = {"account": ["hmy"], "property": ["hmy"], "tenant": ["hmy"],
# MAGIC           "lease": ["property_hmy", "tenant_hmy"]}
# MAGIC
# MAGIC def build(table, keys):
# MAGIC     view = f"{table}_ver_opens"
# MAGIC
# MAGIC     @dp.temporary_view(name=view)
# MAGIC     def _v():
# MAGIC         return (
# MAGIC             spark.readStream
# MAGIC             .option("readChangeFeed", "true")
# MAGIC             .table(f"{catalog}.{bronze_scd2_schema}.{table}")
# MAGIC             # version-open events = inserts of new SCD2 versions in the connector target
# MAGIC             .filter(F.col("_change_type") == "insert")
# MAGIC             # readable sequence: connector change time, fallback to bronze commit time (snapshot rows NULL)
# MAGIC             .withColumn(
# MAGIC                 "_seq",
# MAGIC                 F.coalesce(F.col("__START_AT").getField("__cdc_timestamp_value"), F.col("_commit_timestamp")),
# MAGIC             )
# MAGIC         )
# MAGIC
# MAGIC     dp.create_streaming_table(name=table)
# MAGIC     dp.create_auto_cdc_flow(
# MAGIC         target=table,
# MAGIC         source=view,
# MAGIC         keys=keys,
# MAGIC         sequence_by=F.col("_seq"),
# MAGIC         stored_as_scd_type=2,
# MAGIC         # exclude the connector's own validity structs and CDF metadata; silver_a builds its OWN dates
# MAGIC         except_column_list=[
# MAGIC             "__START_AT", "__END_AT", "_change_type", "_commit_version", "_commit_timestamp", "_seq",
# MAGIC         ],
# MAGIC     )
# MAGIC
# MAGIC for _t, _k in TABLES.items():
# MAGIC     build(_t, _k)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The mechanism
# MAGIC
# MAGIC - **`keys=["hmy"]` is an ASSUMED key, not a source primary key.** The source is a keyless heap — `hmy` is
# MAGIC   a plain, nullable column. SCD Type 2 still requires a key, so `hmy` is elected as the logical identity
# MAGIC   (defended in `05_assumed_key_dq`); `lease` uses a *consolidated* composite key. This key is specified on
# MAGIC   the connector too (`primary_keys: [hmy]`) — never a declared PK.
# MAGIC - **The connector target is SCD Type 2.** Unlike the SCD1 bronze in variants 01/02, `bronze_scd2` keeps
# MAGIC   every version, so nothing is collapsed before silver sees it — this is where the per-change fidelity
# MAGIC   comes from.
# MAGIC - **`silver_a` captures version-opens off the change feed.** Filtering `_change_type == "insert"` picks up
# MAGIC   each new connector version; AUTO CDC rebuilds a clean SCD2 with `silver_a`'s OWN `__START_AT`/`__END_AT`.
# MAGIC - **Readable change-time dates, NULL only on snapshot rows.** `sequence_by` coalesces the connector's
# MAGIC   `__cdc_timestamp_value` (real on incremental changes) with the bronze commit time (for snapshot rows,
# MAGIC   whose cdc timestamp is NULL — a commonly observed artifact).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bring up the SCD2 bronze (one-time switch), then drive changes
# MAGIC
# MAGIC **Operational constraint (finding A0):** the SCD1 bronze (`sqlserver_to_bronze_scd1`) and this SCD2 bronze
# MAGIC (`sqlserver_to_bronze_scd2`) claim the *same* source tables on one gateway, and a source table can be
# MAGIC registered by only one ingestion pipeline per gateway. They are mutually exclusive — **stopping the SCD1
# MAGIC bronze to IDLE is not enough; you must DELETE it** to free the tables.
# MAGIC
# MAGIC ```bash
# MAGIC # free the source tables (deletes the pipeline definition, not the data):
# MAGIC databricks pipelines delete <sqlserver_to_bronze_scd1 id> --profile sqlcdc
# MAGIC # bring up the SCD2 bronze (first run MUST be a full refresh):
# MAGIC databricks pipelines start-update <sqlserver_to_bronze_scd2 id> --full-refresh --profile sqlcdc
# MAGIC ```
# MAGIC
# MAGIC Then drive changes and build silver_a. The `burst` is the fidelity probe (cell 5):
# MAGIC
# MAGIC ```bash
# MAGIC python sim/changegen.py churn --table account --updates 10
# MAGIC python sim/changegen.py burst --table tenant  --key 42 --updates 8
# MAGIC databricks pipelines start-update <sqlserver_to_bronze_scd2 id> --profile sqlcdc   # incremental
# MAGIC databricks bundle run silver_a -t dev --profile sqlcdc
# MAGIC ```
# MAGIC
# MAGIC (Returning to variants 01/02/04 is symmetric: delete `sqlserver_to_bronze_scd2`, redeploy, and
# MAGIC full-refresh `sqlserver_to_bronze_scd1`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Result: the NULL timestamp is a SNAPSHOT artifact, not a date-less artifact
# MAGIC On the SCD2 connector target, date-less `account` shows the LSN in `__START_AT.__cdc_internal_value`.
# MAGIC `__cdc_timestamp_value` is NULL **only on snapshot rows**; rows from a real incremental change carry a
# MAGIC real commit timestamp even for a date-less table. This corrects a common assumption (Experiment 1).

# COMMAND ----------

spark.sql(f"""
  SELECT hmy,
         `__START_AT`.`__cdc_internal_value`  AS start_lsn,
         `__START_AT`.`__cdc_timestamp_value` AS start_ts,
         `__END_AT`.`__cdc_timestamp_value`   AS end_ts
  FROM {CAT}.bronze_scd2.account
  ORDER BY hmy
  LIMIT 12
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fidelity signature: an intra-interval burst keeps every version
# MAGIC After the `burst` on `tenant` key 42, this variant records **~9** versions (snapshot + 8 updates) —
# MAGIC full per-change fidelity, cadence-independent. `01_changefeed_scd2` collapses the same burst to ~1. This
# MAGIC is the clearest illustration of the A-vs-B fidelity fork (Experiment 5).

# COMMAND ----------

spark.sql(f"""
  SELECT hmy AS tenant_key,
         COUNT(*) AS versions,
         MIN(__START_AT) AS first_seen,
         MAX(__START_AT) AS last_change
  FROM {CAT}.silver_a.tenant
  WHERE hmy = 42
  GROUP BY hmy
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC This variant delivers strict per-change audit history with readable change-time dates on a keyless heap.
# MAGIC Its cost is the SCD2 connector target plus the one-pipeline-per-table switch (A0), and it needs the same
# MAGIC full-refresh recovery as variant 01 (`03b_full_refresh`). Choose it only if you need
# MAGIC every logged mutation as a distinct version; otherwise `01_changefeed_scd2` is the simpler answer.
# MAGIC Cross-variant scoreboard: `08_comparison/08_results.py`.
