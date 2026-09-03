# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Assumed-key DQ — capture changes (quarantine an unconstrained, nullable key)
# MAGIC ### The RECOMMENDED keyless treatment: trust `hmy` as an assumed key, and defend it with data quality
# MAGIC
# MAGIC **Why this variant exists.** The difficult scenario this variant demonstrates is a keyless heap whose
# MAGIC identifying field is *also nullable* — `hmy` is not just un-keyed, it can arrive NULL or duplicated.
# MAGIC Furthermore, we assume a source we cannot fully control (like a vendor ERP): you cannot add a key or
# MAGIC constraint, or fix bad values, upstream. SCD Type 2 still needs a key, so we elect `hmy` as an *assumed*
# MAGIC key — and because that assumption can be violated by data we do not control, a data-quality layer is
# MAGIC **essential, not optional**. This is the technique to lead with for keyless, unconstrained tables.
# MAGIC
# MAGIC **What to expect.** Rows that violate the row-level key rule (`hmy IS NULL` or `hmy <= 0`) are routed to
# MAGIC a persisted **quarantine** table — preserved for triage, never dropped and never allowed to contaminate
# MAGIC history. Only valid rows build SCD Type 2 (keyed on the assumed key `hmy`). Nothing is silently lost.
# MAGIC
# MAGIC **The architectural decision it informs.** Whenever you build SCD on an assumed key from a source you do
# MAGIC not control, wrap it in this two-tier DQ. Tier 1 (here) quarantines bad *rows*; Tier 2
# MAGIC (`05b_full_refresh`) is a circuit breaker that halts the build if the key assumption breaks *systemically*
# MAGIC (duplicates/nulls at scale). Runs on the standalone `account_nopk` sandbox so a deliberately-injected bad
# MAGIC row never pollutes the `01`–`04` demo tables.
# MAGIC
# MAGIC ```
# MAGIC bronze_nopk.account_nopk  ->  account_valid (MV, @expect_all)  ->  SCD2 silver_dq.account_nopk
# MAGIC   (assumed key hmy,          \-> account_quarantine (nulls / hmy<=0, preserved for triage)
# MAGIC    nullable & uncontrolled)
# MAGIC ```
# MAGIC
# MAGIC These are STANDALONE pipelines (created with `databricks pipelines create`, reusing the shared gateway),
# MAGIC not part of the main bundle. Source `dbo.account_nopk`, bronze schema `bronze_nopk`, silver `silver_dq`.

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
print("catalog:", CAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The build code (this is the entire Tier-1 definition)
# MAGIC
# MAGIC From `databricks/src/silver_assumed_key_dq.py`. A validated MV feeds the SCD2 snapshot flow; a second
# MAGIC table captures whatever the MV rejects. Shown here so it is on screen during the walkthrough.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
# MAGIC TABLE = spark.conf.get("table", "account_nopk")
# MAGIC KEY = "hmy"
# MAGIC
# MAGIC # Row-level DQ rule for the assumed key. Uniqueness is cross-row -> handled by the validation
# MAGIC # pipeline + circuit breaker (05b), not here.
# MAGIC VALID = "hmy IS NOT NULL AND hmy > 0"
# MAGIC
# MAGIC # Valid current-state rows (feeds the SCD2 snapshot flow).
# MAGIC @dp.materialized_view(name="account_valid")
# MAGIC @dp.expect_all({"assumed_key_valid": VALID})   # violation counts surface in the event log
# MAGIC def _valid():
# MAGIC     return spark.read.table(f"{catalog}.{BRONZE}.{TABLE}").filter(VALID)
# MAGIC
# MAGIC # Tier 1 quarantine: invalid rows preserved for triage, never dropped, never in SCD2.
# MAGIC @dp.table(name="account_quarantine")
# MAGIC def _quarantine():
# MAGIC     return (
# MAGIC         spark.read.table(f"{catalog}.{BRONZE}.{TABLE}")
# MAGIC         .filter(f"NOT({VALID})")
# MAGIC         .withColumn("_quarantined_at", F.current_timestamp())
# MAGIC         .withColumn("_reason", F.lit("assumed key hmy null or <= 0"))
# MAGIC     )
# MAGIC
# MAGIC # SCD2 history from the valid snapshot, keyed on the assumed key.
# MAGIC dp.create_streaming_table(name=TABLE)
# MAGIC dp.create_auto_cdc_from_snapshot_flow(
# MAGIC     target=TABLE,
# MAGIC     source=f"{catalog}.silver_dq.account_valid",
# MAGIC     keys=[KEY],
# MAGIC     stored_as_scd_type=2,
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The mechanism
# MAGIC
# MAGIC - **`keys=["hmy"]` is an ASSUMED key on a nullable, unconstrained column** — never a source PK. The
# MAGIC   whole point of this variant is that the assumption is *unsafe by default* and DQ is what makes it safe.
# MAGIC - **DQ is MANDATORY here, precisely because we assume a source we cannot fully control.** You cannot add a PK, a NOT
# MAGIC   NULL, or a unique constraint at the source, so NULL and duplicate `hmy` values *will* arrive. The
# MAGIC   quarantine rule below is therefore load-bearing, not a nicety: it is the only thing standing between a
# MAGIC   bad source row and corrupted history.
# MAGIC - **Tier 1 = quarantine (row-level, non-fatal).** `@expect_all` surfaces violation counts in the event
# MAGIC   log but does not drop rows; the `account_valid` MV filters to valid rows for the SCD2 build, and the
# MAGIC   `account_quarantine` table persists the rest (with a reason + timestamp) so nothing is lost.
# MAGIC - **SCD2 via the snapshot flow.** `create_auto_cdc_from_snapshot_flow` off the valid current state means
# MAGIC   a bronze full refresh is a non-event for history (see `05b`).
# MAGIC - **Uniqueness is NOT here.** A duplicate `hmy` is a cross-row problem a row-level rule cannot catch —
# MAGIC   that is the Tier-2 circuit breaker in `05b`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drive the capture-changes scenario (run in a terminal)
# MAGIC The sim (`sim/changegen.py`) targets only the main tables; `account_nopk` is driven with direct SQL.
# MAGIC With `$SRV` / `$PW` set as in the README:
# MAGIC
# MAGIC ```bash
# MAGIC # a normal change (opens a new SCD2 version for these keys)
# MAGIC sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q \
# MAGIC   "UPDATE dbo.account_nopk SET status='hold', balance=42 WHERE hmy IN (5,10,15);"
# MAGIC
# MAGIC # a genuinely NULL key -> lands in quarantine. This is the exact source hazard, and it is only
# MAGIC # injectable now that hmy is BIGINT NULL at the source (a NOT NULL column previously blocked it).
# MAGIC sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q \
# MAGIC   "INSERT INTO dbo.account_nopk (hmy,name,status,balance) VALUES (NULL,'orphan','active',0);"
# MAGIC
# MAGIC # a non-positive key also fails the rule and is quarantined
# MAGIC sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q \
# MAGIC   "INSERT INTO dbo.account_nopk (hmy,name,status,balance) VALUES (0,'bad key','active',0);"
# MAGIC ```
# MAGIC
# MAGIC Then ingest and build (standalone pipelines — resolve ids from `databricks pipelines list`):
# MAGIC
# MAGIC ```bash
# MAGIC databricks pipelines start-update <bronze_nopk_ingestion-id>  --profile sqlcdc   # bronze_nopk
# MAGIC databricks pipelines start-update <silver_dq_assumed_key-id>  --profile sqlcdc   # Tier-1 build
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Result: valid rows build SCD2; invalid rows are quarantined (not dropped)
# MAGIC The SCD2 history, keyed on the assumed key `hmy`:

# COMMAND ----------

spark.sql(f"""
  SELECT hmy, name, status, `__START_AT`, `__END_AT`,
         (`__END_AT` IS NULL) AS is_current
  FROM {CAT}.silver_dq.account_nopk
  WHERE hmy IN (5, 10, 15)
  ORDER BY hmy, `__START_AT`
""").display()

# COMMAND ----------

# The quarantined rows: the null / non-positive keys the assumption cannot safely build on.
spark.sql(f"""
  SELECT hmy, name, status, _reason, _quarantined_at
  FROM {CAT}.silver_dq.account_quarantine
  ORDER BY _quarantined_at DESC
""").display()

# COMMAND ----------

# Headline count: valid history vs quarantined rows. Nothing was silently dropped.
spark.sql(f"""
  SELECT
    (SELECT count(*) FROM {CAT}.silver_dq.account_nopk)      AS valid_scd2_versions,
    (SELECT count(DISTINCT hmy) FROM {CAT}.silver_dq.account_nopk) AS valid_keys,
    (SELECT count(*) FROM {CAT}.silver_dq.account_quarantine) AS quarantined_rows
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consequence in practice
# MAGIC This is the honest, safe way to build SCD Type 2 on an assumed key you do not control: the nullable/
# MAGIC invalid keys that an uncontrolled source can emit are quarantined for triage rather than corrupting history, and the
# MAGIC valid rows get full dated SCD2. Pair it with the Tier-2 circuit breaker in `05b_full_refresh` for the
# MAGIC systemic (cross-row uniqueness) case. Cross-variant scoreboard: `08_comparison/08_results.py`.
