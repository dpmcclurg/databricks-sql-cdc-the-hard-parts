# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Assumed-key DQ — full refresh & the Tier-2 circuit breaker
# MAGIC
# MAGIC **Why this notebook exists.** Two things this variant must show beyond Tier-1 quarantine: (1) that the
# MAGIC SCD2 build survives a bronze full refresh with zero intervention (it uses the snapshot flow), and (2)
# MAGIC the **Tier-2 circuit breaker** — the systemic guard for when the assumed key breaks at scale. A NULL or
# MAGIC single bad row is a *row-level* problem (Tier 1 quarantines it); a duplicate `hmy` is a *cross-row*
# MAGIC problem a row-level rule cannot see. Because we assume a source we cannot fully control (like a vendor ERP), both can happen, so both tiers
# MAGIC are required.
# MAGIC
# MAGIC **What to expect.** Part 1: a bronze full refresh is a non-event — the snapshot flow runs to COMPLETED
# MAGIC and valid history is preserved (same mechanism as `02_snapshot_scd2`). Part 2: when a duplicate `hmy`
# MAGIC appears, the validation pipeline's `expect_or_fail` fails its update, and because the guarded job runs
# MAGIC *validate -> build*, the SCD2 build is **skipped** — the circuit trips before bad data can build history.
# MAGIC
# MAGIC **The architectural decision it informs.** A validation table cannot gate its own downstream, so
# MAGIC uniqueness enforcement must be a *separate pipeline* gated by a *job dependency*. This is the pattern to
# MAGIC recommend for any assumed key on an uncontrolled source.
# MAGIC
# MAGIC Attach to **serverless**. Standalone pipelines: ingestion `bronze_nopk_ingestion`, Tier-1
# MAGIC `silver_dq_assumed_key`, Tier-2 validation `hmy_key_validation`, guarded job `silver_dq_assumed_key_guarded`.

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def pipeline_id(name_suffix):
    """Resolve a standalone pipeline id by the tail of its name."""
    for p in w.pipelines.list_pipelines():
        if p.name and p.name.endswith(name_suffix):
            return p.pipeline_id
    raise ValueError(f"pipeline not found ending in '{name_suffix}'")


def wait(pid, update_id, label):
    while True:
        st = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update.state.value
        print(f"  {label}: {st}")
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return st
        time.sleep(15)


def valid_counts(label):
    print(f"--- {label} ---")
    r = spark.sql(f"""
      SELECT count(*) v, count(DISTINCT hmy) k
      FROM {CAT}.silver_dq.account_nopk
    """).first()
    print(f"  silver_dq.account_nopk: {r.v} valid versions / {r.k} keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — a bronze full refresh is a non-event (snapshot flow)
# MAGIC The Tier-1 SCD2 build reads the valid current state as a snapshot, so a `bronze_nopk` full refresh does
# MAGIC not break it (no streaming schema log to invalidate) and preserves history — exactly like
# MAGIC `02_snapshot_scd2`.

# COMMAND ----------

BRONZE_NOPK = pipeline_id("bronze_nopk_ingestion")
TIER1 = pipeline_id("silver_dq_assumed_key")
print("bronze_nopk :", BRONZE_NOPK)
print("tier-1 build:", TIER1)

valid_counts("BEFORE full refresh")

# COMMAND ----------

# Full-refresh bronze_nopk, then run the Tier-1 build; history should be preserved with no recovery step.
u = w.pipelines.start_update(pipeline_id=BRONZE_NOPK, full_refresh=True)
wait(BRONZE_NOPK, u.update_id, "bronze_nopk full refresh")

u = w.pipelines.start_update(pipeline_id=TIER1, full_refresh=False)
wait(TIER1, u.update_id, "tier-1 build (snapshot flow)")

valid_counts("AFTER full refresh (expect unchanged)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — the Tier-2 circuit breaker (cross-row uniqueness)
# MAGIC
# MAGIC This is the entire Tier-2 definition, from `databricks/src/silver_assumed_key_validation.py`. It counts
# MAGIC assumed-key violations (duplicate `hmy`, null `hmy`, or `hmy <= 0`) and asserts zero with
# MAGIC `expect_or_fail` (ON VIOLATION FAIL UPDATE). A violation FAILS this pipeline's update.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC catalog = spark.conf.get("catalog")
# MAGIC BRONZE = spark.conf.get("bronze_schema", "bronze_nopk")
# MAGIC TABLE = spark.conf.get("table", "account_nopk")
# MAGIC
# MAGIC @dp.table(name="hmy_key_validation")
# MAGIC @dp.expect_or_fail("assumed_key_holds", "violations = 0")
# MAGIC def _validation():
# MAGIC     src = spark.read.table(f"{catalog}.{BRONZE}.{TABLE}")
# MAGIC     dup = (src.groupBy("hmy").agg(F.count("*").alias("c"))
# MAGIC            .filter("c > 1").agg(F.count("*").alias("n")).collect()[0]["n"])
# MAGIC     bad = src.filter("hmy IS NULL OR hmy <= 0").count()
# MAGIC     return spark.createDataFrame(
# MAGIC         [(int(dup) + int(bad), int(dup), int(bad))],
# MAGIC         "violations long, duplicate_keys long, invalid_keys long")
# MAGIC ```
# MAGIC
# MAGIC Because a validation table cannot gate its own downstream, the guarded job
# MAGIC `silver_dq_assumed_key_guarded` runs it as the FIRST task and makes the SCD2 build DEPEND on it — so a
# MAGIC breach skips the build entirely.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate the trip: inject a duplicate assumed key, then run the guarded job
# MAGIC In a terminal (with `$SRV` / `$PW` set), create a genuine duplicate `hmy` at the source:
# MAGIC
# MAGIC ```bash
# MAGIC sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q \
# MAGIC   "INSERT INTO dbo.account_nopk (hmy,name,status,balance) \
# MAGIC    SELECT hmy,name,status,balance FROM dbo.account_nopk WHERE hmy=5;"
# MAGIC
# MAGIC databricks pipelines start-update <bronze_nopk_ingestion-id> --profile sqlcdc   # land the duplicate
# MAGIC databricks jobs run-now --json '{"job_id": <silver_dq_assumed_key_guarded-id>}' --profile sqlcdc
# MAGIC ```
# MAGIC
# MAGIC **Expected:** the `validate` task FAILS (`expect_or_fail`, `violations > 0`), so the dependent `build`
# MAGIC task never runs — the circuit trips before the duplicate can corrupt history.

# COMMAND ----------

# Read the validation table to see the violation count the breaker keys on.
try:
    spark.sql(f"""
      SELECT violations, duplicate_keys, invalid_keys
      FROM {CAT}.silver_dq.hmy_key_validation
    """).display()
except Exception as e:
    print("hmy_key_validation not present yet (run the validation pipeline / guarded job first):", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPEN ITEM — the definitive trip is UNCONFIRMED (be honest in the demo)
# MAGIC The Tier-2 pipeline and the guarded job are BUILT, but the circuit-breaker's definitive trip on a real
# MAGIC bad key is **not yet confirmed** (experiments E12): the last guarded-job run FAILED, but
# MAGIC `silver_dq.hmy_key_validation` read `violations = 0` at the time, so it is unclear whether the failure
# MAGIC was the `expect_or_fail` tripping or an unrelated internal error.
# MAGIC
# MAGIC To confirm cleanly on resume:
# MAGIC 1. Inject the duplicate above so a re-run of the validation pipeline reports `violations > 0` (verify in
# MAGIC    the cell above).
# MAGIC 2. Run `silver_dq_assumed_key_guarded` and check the run: the `validate` task should FAIL and the
# MAGIC    `build` task should be SKIPPED (never started).
# MAGIC 3. Then clean up the duplicate (`DELETE` the extra row) and re-run to see the job go green again.
# MAGIC
# MAGIC Until step 2 is observed, present Tier 2 as *designed and built*, not as *demonstrated*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The rule to land
# MAGIC An assumed key on a source you do not control needs BOTH tiers: Tier 1 quarantines individual bad rows
# MAGIC (nulls / non-positive) without dropping them; Tier 2 halts the whole build if the key breaks
# MAGIC systemically (duplicates). Uniqueness must live in a separate validation pipeline gated by a job, since
# MAGIC a validation table cannot gate its own downstream. Cross-variant scoreboard: `08_comparison/08_results.py`.
