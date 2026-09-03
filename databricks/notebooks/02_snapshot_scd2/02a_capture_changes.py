# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Snapshot SCD2 — capture changes
# MAGIC ### SCD Type 1 bronze  ->  AUTO CDC FROM SNAPSHOT SCD Type 2 silver, off bronze's current state
# MAGIC
# MAGIC **Why this variant exists.** It builds the same dated SCD Type 2 history as `01_changefeed_scd2`, but
# MAGIC by reading bronze's *current state* as a periodic batch **snapshot** instead of streaming its change
# MAGIC feed. That one change buys the headline operational property: it survives a connector full refresh with
# MAGIC **zero** recovery step (`02b_full_refresh`), where the change-feed build needs an orchestrated
# MAGIC checkpoint reset.
# MAGIC
# MAGIC **What to expect.** Silver carries real `__START_AT` / `__END_AT`, but the dates are **processing-time**
# MAGIC (when the snapshot ran), not change-time — there is no change feed to read the commit timestamp from.
# MAGIC Fidelity is the same net-change-per-interval ceiling as variant 01.
# MAGIC
# MAGIC **The architectural decision it informs.** Choose this when you want full-refresh survival with no
# MAGIC orchestration and processing-time dates are acceptable. Choose `01_changefeed_scd2` instead when you
# MAGIC want change-time validity dates (accepting the one-call refresh recovery); choose `03_scd2_bronze` when
# MAGIC you need per-change fidelity.
# MAGIC
# MAGIC **Prerequisite (not optional).** We assume a source we cannot fully control (like a vendor ERP), where
# MAGIC the assumed key `hmy` is **nullable** and can arrive dirty. So this build assumes an already-*cleaned* key: a NULL or
# MAGIC duplicate `hmy` would corrupt SCD2 (AUTO CDC misbehaves on a null key). In production, run the
# MAGIC data-quality quarantine + circuit breaker (`05_assumed_key_dq`) upstream first. It is what makes an
# MAGIC assumed key safe to build SCD on — treat it as a prerequisite, not an add-on.
# MAGIC
# MAGIC ```
# MAGIC Azure SQL (CDC)  ->  gateway  ->  bronze  (SCD1, current state)  ->  silver_snap  (SCD2, dated history)
# MAGIC                                   connector target                 AUTO CDC FROM SNAPSHOT (batch)
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
print("catalog:", CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build code (this is the entire silver definition)
# MAGIC
# MAGIC From `databricks/src/silver_snapshot_scd2.py`. One `build()` per table from a `{table: keys}` map — the
# MAGIC same definition templates across all tables. Note how much smaller it is than the change-feed build:
# MAGIC no CDF reader, no `_change_type` filtering, no `apply_as_deletes`. The snapshot flow derives all of
# MAGIC that by comparing successive snapshots.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC bronze_schema = spark.conf.get("bronze_schema", "bronze")
# MAGIC
# MAGIC # table -> business key columns
# MAGIC TABLES = {"account": ["hmy"], "property": ["hmy"], "tenant": ["hmy"],
# MAGIC           "lease": ["property_hmy", "tenant_hmy"]}
# MAGIC
# MAGIC def build(table, keys):
# MAGIC     dp.create_streaming_table(name=table)
# MAGIC     dp.create_auto_cdc_from_snapshot_flow(
# MAGIC         target=table,
# MAGIC         source=f"{catalog}.{bronze_schema}.{table}",   # current-state snapshot, read each run
# MAGIC         keys=keys,
# MAGIC         stored_as_scd_type=2,
# MAGIC     )
# MAGIC
# MAGIC for _t, _k in TABLES.items():
# MAGIC     build(_t, _k)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The mechanism
# MAGIC
# MAGIC - **`keys=["hmy"]` is an ASSUMED key, not a source primary key.** The source `account` is a keyless heap
# MAGIC   — `hmy` is a plain column with no PK, identity, or unique constraint. SCD Type 2 still *requires* a
# MAGIC   key, so we elect `hmy` as the logical identity (a deliberate assumption; `05_assumed_key_dq` shows how
# MAGIC   to defend it). `lease` uses a *consolidated* composite key `[property_hmy, tenant_hmy]`. The snapshot
# MAGIC   flow uses this key to diff successive snapshots — it is never a declared PK.
# MAGIC - **`create_auto_cdc_from_snapshot_flow`** reads bronze's current state as a batch each run and compares
# MAGIC   it to the last snapshot: new/changed keys open a new SCD2 version, absent keys are closed. No change
# MAGIC   feed, no streaming schema log to break.
# MAGIC - **Validity dates are processing-time.** Because there is no CDF commit timestamp, `__START_AT` is when
# MAGIC   the snapshot ran, not when the source changed. Still readable dates on a date-less source — just a
# MAGIC   coarser proxy than variant 01's change-time dates.
# MAGIC - **Idempotent by comparison.** A re-delivered unchanged current state opens no new version — which is
# MAGIC   exactly why a full refresh is a non-event here (`02b`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drive the capture-changes scenario (run in a terminal)
# MAGIC
# MAGIC From the repo root, venv active and `SQLDB_*` exported (see `README.md`):
# MAGIC
# MAGIC ```bash
# MAGIC python sim/changegen.py churn --table account --updates 10
# MAGIC ```
# MAGIC
# MAGIC The snapshot flow reads bronze's *current state*, so refresh bronze first, then run this silver:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run bronze_ingestion -t dev --profile sqlcdc     # SCD1 bronze picks up the DML
# MAGIC databricks bundle run silver_snapshot  -t dev --profile sqlcdc     # AUTO CDC FROM SNAPSHOT builds SCD2
# MAGIC ```
# MAGIC
# MAGIC Then run the result cells below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Result: SCD2 history with processing-time validity dates
# MAGIC `account` has no source date, yet silver_snap carries real `__START_AT` / `__END_AT` — stamped at
# MAGIC snapshot time. Keys changed by the churn show a closed prior version and an open current one.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status,
         __START_AT, __END_AT,
         (__END_AT IS NULL) AS is_current
  FROM {CAT}.silver_snap.account
  ORDER BY hmy, __START_AT
  LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. History depth: extra versions are accumulated history, not duplicates

# COMMAND ----------

spark.sql(f"""
  SELECT COUNT(*) AS versions,
         COUNT(DISTINCT hmy) AS keys,
         COUNT(*) - COUNT(DISTINCT hmy) AS extra_history_versions
  FROM {CAT}.silver_snap.account
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC The snapshot build gives durable, dated SCD Type 2 on a keyless heap, stays fully in Lakeflow
# MAGIC Declarative Pipelines with no custom code, and — its distinguishing property — a connector full refresh
# MAGIC requires **zero** intervention (`02b_full_refresh`). The trade vs `01_changefeed_scd2`: processing-time
# MAGIC dates instead of change-time, and a full re-read of bronze each trigger. Cross-variant scoreboard:
# MAGIC `08_comparison/08_results.py`.
