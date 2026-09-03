# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Content-hash SCD2 — capture changes
# MAGIC ### A TRANSFORMED / created surrogate key, manufactured in-lakehouse
# MAGIC
# MAGIC **Why this variant exists.** For a true heap where **no single source column is a trusted key** but a
# MAGIC *stable subset* of columns identifies the entity: land raw rows, derive a deterministic surrogate key
# MAGIC from the stable columns in Databricks, and build SCD Type 2 on that. The key here is neither a source
# MAGIC PK nor an assumed column — it is **transformed / created**: `_bk = sha2(concat_ws('||', hmy, name), 256)`.
# MAGIC
# MAGIC **What to expect.** SCD2 history keyed on `_bk`. A change to a *mutable attribute* (status, balance)
# MAGIC opens a new version under the **same** `_bk`; the identity is stable because only the stable columns
# MAGIC are hashed. Baseline is 100 versions (one per seeded entity).
# MAGIC
# MAGIC **The architectural decision it informs.** Reach for this only when you cannot trust any single column
# MAGIC as an assumed key (so `05_assumed_key_dq` does not apply) but a stable column *subset* does identify the
# MAGIC entity. If the raw columns are directly usable as a key, a plain assumed/composite key is simpler and a
# MAGIC hash buys nothing. If nothing identifies the entity at all, you cannot do SCD — that is `07_append_log`.
# MAGIC
# MAGIC > **Critical design rule.** Hash ONLY the stable identifying columns (`hmy`, `name`). NEVER include the
# MAGIC > mutable attributes (`status`, `balance`) — if you did, every attribute change would produce a new
# MAGIC > surrogate key and the entity's history would be lost.
# MAGIC
# MAGIC Runs on the standalone keyless sandbox `dbo.account_nopk` (bronze schema `bronze_nopk`), a STANDALONE
# MAGIC pipeline reusing the shared gateway (not the main bundle).

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
print("catalog:", CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build code (this is the entire silver definition)
# MAGIC
# MAGIC From `databricks/src/silver_hash_scd2.py`. An MV computes the transformed key `_bk`; SCD2 is built off
# MAGIC it with `create_auto_cdc_from_snapshot_flow`.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
# MAGIC TABLE = spark.conf.get("table", "account_nopk")
# MAGIC
# MAGIC # Stable identifying columns only. Adjust per table; do NOT include mutable attributes.
# MAGIC NATURAL_KEY = ["hmy", "name"]
# MAGIC
# MAGIC @dp.materialized_view(name="account_hashed")
# MAGIC def _hashed():
# MAGIC     cols = [F.col(c).cast("string") for c in NATURAL_KEY]
# MAGIC     return (
# MAGIC         spark.read.table(f"{catalog}.{BRONZE}.{TABLE}")
# MAGIC         .withColumn("_bk", F.sha2(F.concat_ws("||", *cols), 256))
# MAGIC     )
# MAGIC
# MAGIC dp.create_streaming_table(name=TABLE)
# MAGIC dp.create_auto_cdc_from_snapshot_flow(
# MAGIC     target=TABLE,
# MAGIC     source=f"{catalog}.silver_hash.account_hashed",
# MAGIC     keys=["_bk"],                     # <- the TRANSFORMED surrogate key, not a source column
# MAGIC     stored_as_scd_type=2,
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The mechanism
# MAGIC
# MAGIC - **`_bk` is a deterministic surrogate** over the stable natural key `(hmy, name)`. Same entity -> same
# MAGIC   `_bk`, every run, so it is a durable logical identity even though the source has no key.
# MAGIC - **`create_auto_cdc_from_snapshot_flow` keyed on `_bk`** reads the hashed current state as a snapshot and
# MAGIC   applies SCD2. Because the flow is snapshot-based (not a change-feed stream), it is also full-refresh
# MAGIC   safe (see `06b_full_refresh`).
# MAGIC - **Only stable columns are hashed.** `status` / `balance` are mutable attributes carried on the row but
# MAGIC   NOT in `_bk`, so changing them opens a new SCD2 version under the same key (cell 5) rather than forking
# MAGIC   a new identity.
# MAGIC - **A NULL assumed key still hashes.** `hmy` is nullable (we assume a source we cannot fully control, like a vendor ERP), and
# MAGIC   `concat_ws` skips nulls — so two rows with a NULL `hmy` and the same `name` collide into one `_bk` and
# MAGIC   their histories merge. A manufactured key does not exempt you from null-handling: include enough stable
# MAGIC   identifying columns to disambiguate, and keep the DQ discipline (variant 05) even here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drive the capture-changes scenario (run in a terminal)
# MAGIC
# MAGIC `account_nopk` is the keyless sandbox; the local sim targets only the main tables, so inject changes
# MAGIC with direct SQL, then run the pipeline. A change to a MUTABLE attribute must open a NEW version under
# MAGIC the SAME `_bk`:
# MAGIC
# MAGIC ```bash
# MAGIC # mutable-attribute change on three entities (identity columns hmy/name unchanged)
# MAGIC sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q \
# MAGIC   "UPDATE dbo.account_nopk SET status='hold', balance=balance+1 WHERE hmy IN (5,10,15);"
# MAGIC
# MAGIC # land it: bronze_nopk picks up the change, then rebuild the hashed SCD2
# MAGIC databricks pipelines start-update <bronze_nopk_ingestion-id> --profile sqlcdc     # poll to COMPLETED
# MAGIC databricks pipelines start-update <silver_hash_scd2-id>       --profile sqlcdc     # poll to COMPLETED
# MAGIC ```
# MAGIC
# MAGIC (Resolve the pipeline ids with `databricks pipelines list-pipelines --profile sqlcdc`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Result: the transformed key and the SCD2 target
# MAGIC The MV carries `_bk`; the SCD2 target is keyed on it. Baseline: 100 versions / 100 keys.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status, balance, _bk
  FROM {CAT}.silver_hash.account_hashed
  ORDER BY hmy
  LIMIT 10
""").display()

# COMMAND ----------

spark.sql(f"""
  SELECT COUNT(*) AS versions, COUNT(DISTINCT _bk) AS keys,
         COUNT(*) - COUNT(DISTINCT _bk) AS extra_history_versions
  FROM {CAT}.silver_hash.account_nopk
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. A mutable-attribute change opens a new version under the SAME `_bk`
# MAGIC After the `UPDATE ... WHERE hmy IN (5,10,15)` above, each touched entity should show **2** versions
# MAGIC under one stable `_bk` — the prior version closed (`__END_AT` set), the new one open.
# MAGIC
# MAGIC > **Honest status (experiment E13).** The baseline build on `_bk` is confirmed. The
# MAGIC > change-opens-a-new-version pass on the hash target was **not** separately confirmed in the last run.
# MAGIC > To confirm cleanly: run the `UPDATE` above, run `bronze_nopk_ingestion` then `silver_hash_scd2`, then
# MAGIC > re-run this cell and check that hmy 5/10/15 each show 2 rows sharing one `_bk`.

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status, balance, _bk, `__START_AT`, `__END_AT`,
         (`__END_AT` IS NULL) AS is_current
  FROM {CAT}.silver_hash.account_nopk
  WHERE hmy IN (5, 10, 15)
  ORDER BY hmy, `__START_AT`
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC Content-hash delivers SCD2 on a true heap with zero source constraints — identity manufactured
# MAGIC in-lakehouse from the stable columns. It is the fallback when no single column is a trustworthy assumed
# MAGIC key but a stable subset identifies the entity. The discipline is entirely in choosing the hash inputs:
# MAGIC stable columns only. Full-refresh behavior: `06b_full_refresh`. Scoreboard: `08_comparison/08_results.py`.
