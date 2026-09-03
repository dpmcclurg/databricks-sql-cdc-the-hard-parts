# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Append-only change log — capture changes
# MAGIC ### The NO-KEY fallback: not SCD, and it cannot be
# MAGIC
# MAGIC **Why this variant exists.** When **no key of any kind can be formed** — not an assumed column, not a
# MAGIC consolidated composite, not a transformed surrogate — you cannot build entity history. SCD Type 1/2 both
# MAGIC require a key to know which entity a change belongs to. This variant abandons that goal and instead
# MAGIC captures every operation as an immutable **append row**: each insert / update / delete from the bronze
# MAGIC change feed becomes one row stamped with its operation type (`op`) and commit timestamp (`op_ts`),
# MAGIC partitioned by `op_date`. No key, no AUTO CDC, no SCD2 collapse.
# MAGIC
# MAGIC **What to expect.** Updates arrive as *new, independent rows* that you **cannot link** back to the row
# MAGIC they changed — that is the whole point and the whole limitation. You get a full, ordered change ledger,
# MAGIC not an entity timeline. Row count grows with every operation, forever.
# MAGIC
# MAGIC **The architectural decision it informs.** This is *correct* when the source rows are inherently events
# MAGIC (an audit / transaction log). For a mutable entity table it is a lossless **"capture now, model once a
# MAGIC key is found"** stopgap, and only for SMALL tables — the log grows unbounded, so large tables need a
# MAGIC retention/partition strategy or one of the keyed variants (05 assumed key, 06 content-hash). It sits at
# MAGIC the very bottom of the decision tree: reach it only when nothing identifies an entity.
# MAGIC
# MAGIC > **Because it needs no key, a NULL `hmy` is a non-issue.** Every row is logged as-is, null key or not.
# MAGIC > This tolerance for anything — including a nullable, uncontrolled source key — is exactly why it is the
# MAGIC > last-resort fallback: it makes no assumption that could be violated.
# MAGIC
# MAGIC Runs on the standalone keyless sandbox `dbo.account_nopk` (bronze schema `bronze_nopk`), a STANDALONE
# MAGIC pipeline reusing the shared gateway (not the main bundle).

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
print("catalog:", CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build code (this is the entire definition)
# MAGIC
# MAGIC From `databricks/src/silver_append_log.py`. A plain append table off the bronze change feed — no
# MAGIC `keys=`, no `create_auto_cdc_flow`.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
# MAGIC TABLE = spark.conf.get("table", "account_nopk")
# MAGIC
# MAGIC # Append-only: every change event as an immutable row (no keys=, no AUTO CDC).
# MAGIC @dp.table(name=f"{TABLE}_log", partition_cols=["op_date"])
# MAGIC def _log():
# MAGIC     return (
# MAGIC         spark.readStream.option("readChangeFeed", "true")
# MAGIC         .table(f"{catalog}.{BRONZE}.{TABLE}")
# MAGIC         .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
# MAGIC         .withColumnRenamed("_change_type", "op")
# MAGIC         .withColumnRenamed("_commit_timestamp", "op_ts")
# MAGIC         .withColumn("op_date", F.to_date("op_ts"))
# MAGIC         .drop("_commit_version")
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The mechanism
# MAGIC
# MAGIC - **No key, so no SCD.** There is no `keys=` and no AUTO CDC — the stream just appends every change
# MAGIC   event. A delete is a row (`op = 'delete'`), not a closed version.
# MAGIC - **Ordering is preserved, linkage is not.** `op_ts` gives you the order of operations, but nothing ties
# MAGIC   an update's row to the row it superseded. You cannot reconstruct "what did entity X look like on date
# MAGIC   D" without a key.
# MAGIC - **Replayable later.** If a key is discovered later (business input, or a stable column subset), you can
# MAGIC   replay this log to build SCD1/SCD2 retroactively — which is why capturing losslessly now has value.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drive the capture-changes scenario (run in a terminal)
# MAGIC
# MAGIC `account_nopk` is the keyless sandbox; inject changes with direct SQL, then run the pipeline:
# MAGIC
# MAGIC ```bash
# MAGIC sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q \
# MAGIC   "UPDATE dbo.account_nopk SET status='closed' WHERE hmy=5;"
# MAGIC
# MAGIC databricks pipelines start-update <bronze_nopk_ingestion-id> --profile sqlcdc     # poll to COMPLETED
# MAGIC databricks pipelines start-update <silver_append_log-id>     --profile sqlcdc     # poll to COMPLETED
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Result: the change ledger and its operation mix
# MAGIC Around 104 rows: 100 initial inserts plus the change operations. Columns are `hmy, name, status,
# MAGIC balance, op, op_date, op_ts`.

# COMMAND ----------

spark.sql(f"""
  SELECT op, COUNT(*) AS rows
  FROM {CAT}.silver_log.account_nopk_log
  GROUP BY op
  ORDER BY op
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. An update is a NEW, unlinked row
# MAGIC After the `UPDATE ... WHERE hmy=5` above, entity 5 has multiple rows in the log — an insert and one or
# MAGIC more `update_postimage` rows — ordered by `op_ts`. They are separate append rows; nothing marks one as
# MAGIC the successor of another. This is the visible limitation: a ledger, not an entity history.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status, balance, op, op_ts
  FROM {CAT}.silver_log.account_nopk_log
  WHERE hmy = 5
  ORDER BY op_ts
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC The append log is the honest answer of last resort: "we can't give you SCD on this table until we can
# MAGIC identify the entity; meanwhile we lose nothing." It tolerates the nullable, uncontrolled key because it
# MAGIC assumes no key at all. The whole thrust of the recommendation is to *avoid* landing here — push the
# MAGIC assumed key + DQ (variant 05) or a content-hash surrogate (variant 06) first. Full-refresh behavior (an
# MAGIC important gotcha): `07b_full_refresh`. Scoreboard: `08_comparison/08_results.py`.
