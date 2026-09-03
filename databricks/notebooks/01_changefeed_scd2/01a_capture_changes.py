# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Change-feed SCD2 — capture changes
# MAGIC ### SCD Type 1 bronze  ->  AUTO CDC SCD Type 2 silver, off bronze's change feed
# MAGIC
# MAGIC **Why this variant exists.** It is the primary, recommended build: durable, *dated* SCD Type 2
# MAGIC history from a source with **no date column**, entirely inside Lakeflow Declarative Pipelines, with
# MAGIC one templated definition and zero custom code. It is the cleanest architecture whenever
# MAGIC trigger-interval history granularity is enough — the common case.
# MAGIC
# MAGIC **What to expect.** Silver carries real `__START_AT` / `__END_AT` even though `account` has no source
# MAGIC date; the dates come from the bronze change-feed commit time. Fidelity is *net-change-per-interval*:
# MAGIC multiple changes to one key inside a trigger window collapse to their final value before silver sees
# MAGIC them (the SCD1 bronze converges them first).
# MAGIC
# MAGIC **The architectural decision it informs.** Choose this when a real-column key exists, CDC is
# MAGIC available, and trigger-interval history is enough. Choose `03_scd2_bronze` instead only when every
# MAGIC logged mutation must be a distinct version (per-change audit); choose `02_snapshot_scd2` when you want
# MAGIC full-refresh survival with zero recovery step. This notebook drives changes and shows the behavior;
# MAGIC `01b_full_refresh` shows what a connector full refresh does and how history survives it.
# MAGIC
# MAGIC ```
# MAGIC Azure SQL (CDC)  ->  gateway  ->  bronze  (SCD1, current state)  ->  silver  (SCD2, dated history)
# MAGIC                                   connector target                 AUTO CDC off bronze CDF
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
print("catalog:", CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build code (this is the entire silver definition)
# MAGIC
# MAGIC From `databricks/src/silver_scd2.py`. One `build()` per table, driven from a `{table: keys}` map, so
# MAGIC the same definition templates across all tables — and would template across hundreds of source tables
# MAGIC unchanged. Shown here so it is on screen during the walkthrough.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC bronze_schema = spark.conf.get("bronze_schema", "bronze")   # "bronze" here (SCD1 target)
# MAGIC
# MAGIC TABLES = {"account": ["hmy"], "property": ["hmy"], "tenant": ["hmy"],
# MAGIC           "lease": ["property_hmy", "tenant_hmy"]}
# MAGIC
# MAGIC def build(table, keys):
# MAGIC     view = f"{table}_changes"
# MAGIC
# MAGIC     @dp.temporary_view(name=view)
# MAGIC     def _changes():
# MAGIC         # Stream bronze's change feed; keep insert / update_postimage / delete.
# MAGIC         return (
# MAGIC             spark.readStream
# MAGIC             .option("readChangeFeed", "true")
# MAGIC             .table(f"{catalog}.{bronze_schema}.{table}")
# MAGIC             .filter(F.col("_change_type") != "update_preimage")
# MAGIC         )
# MAGIC
# MAGIC     dp.create_streaming_table(name=table)
# MAGIC     dp.create_auto_cdc_flow(
# MAGIC         target=table,
# MAGIC         source=view,
# MAGIC         keys=keys,
# MAGIC         sequence_by=F.col("_commit_timestamp"),      # <- dates silver from the bronze commit time
# MAGIC         apply_as_deletes=F.expr("_change_type = 'delete'"),
# MAGIC         except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
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
# MAGIC - **`keys=["hmy"]` is an ASSUMED key, not a source primary key.** The source `account` is a keyless
# MAGIC   heap — `hmy` is a plain column with no PK, identity, or unique constraint. SCD Type 2 still *requires*
# MAGIC   a key, so we elect `hmy` as the logical identity. That is a deliberate assumption; `05_assumed_key_dq`
# MAGIC   shows how to defend it (quarantine + circuit breaker). `lease` uses a *consolidated* composite key
# MAGIC   `[property_hmy, tenant_hmy]`; `06_content_hash` shows a *transformed* surrogate when no column is
# MAGIC   trustworthy. The key is never a declared PK here.
# MAGIC - **`create_auto_cdc_flow` with `stored_as_scd_type=2`** is the managed SCD2 primitive: it opens a new
# MAGIC   version when a tracked column changes and closes the prior one, gap-free, with no hand-written MERGE.
# MAGIC - **`sequence_by=_commit_timestamp`** is why silver has readable validity dates even though `account` /
# MAGIC   `tenant` have no date column: the date comes from the bronze CDF commit time. It is a near-real-time
# MAGIC   proxy for the source change, loose by at most the trigger interval.
# MAGIC - **Idempotent by content.** A re-delivered unchanged row (for example after a full refresh) opens no
# MAGIC   new version — that is what makes the `01b` full-refresh recovery non-destructive.
# MAGIC - **Fidelity is net-per-interval.** Because bronze is SCD Type 1, multiple changes to one key inside a
# MAGIC   trigger interval collapse to the final value before silver ever sees them (cell 5).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drive the capture-changes scenario (run in a terminal)
# MAGIC
# MAGIC From the repo root, with the venv active and `SQLDB_*` exported (see `README.md` §Run it):
# MAGIC
# MAGIC ```bash
# MAGIC # scattered single-row updates across the table
# MAGIC python sim/changegen.py churn --table account --updates 10
# MAGIC
# MAGIC # many rapid updates to ONE key inside a single interval (the fidelity probe, cell 5)
# MAGIC python sim/changegen.py burst --table tenant --key 42 --updates 8
# MAGIC ```
# MAGIC
# MAGIC Then let the change feed flow to silver:
# MAGIC
# MAGIC ```bash
# MAGIC # (first run only: databricks pipelines start-update <bronze_ingestion-id> --full-refresh --profile sqlcdc)
# MAGIC databricks bundle run bronze_ingestion -t dev --profile sqlcdc     # SCD1 bronze picks up the DML
# MAGIC databricks bundle run silver_scd2      -t dev --profile sqlcdc     # AUTO CDC builds SCD2 silver
# MAGIC ```
# MAGIC
# MAGIC CDC delivery is eventual; if a trigger looks behind, run `bronze_ingestion` once more. Then run the
# MAGIC result cells below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Result: readable validity dates on a date-less source
# MAGIC `account` has no date column at the source, yet silver carries real `__START_AT` / `__END_AT`.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status,
         __START_AT, __END_AT,
         (__END_AT IS NULL) AS is_current
  FROM {CAT}.silver.account
  ORDER BY hmy, __START_AT
  LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fidelity signature: an intra-interval burst collapses to one net version
# MAGIC After the `burst` on `tenant` key 42 above, this variant records **~1** new version (the net final
# MAGIC value), because the SCD1 bronze converged the 8 updates first. `03_scd2_bronze` keeps all ~9 — that is
# MAGIC the fidelity fork between the two.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy AS tenant_key,
         COUNT(*) AS versions,
         MIN(__START_AT) AS first_seen,
         MAX(__START_AT) AS last_change
  FROM {CAT}.silver.tenant
  WHERE hmy = 42
  GROUP BY hmy
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC This variant resolves the original NULL-timestamp complaint with readable dates, no LSN, and no
# MAGIC per-table date mapping, in one templated in-LDP definition. Its only ceiling is fidelity:
# MAGIC net-change-per-trigger-interval, not every logged mutation. It also survives a connector full refresh
# MAGIC with history intact — see `01b_full_refresh`. Cross-variant scoreboard: `08_comparison/08_results.py`.
