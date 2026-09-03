# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Content-hash SCD2 — full-refresh behavior
# MAGIC
# MAGIC **Why this notebook exists.** To show that this build treats a connector full refresh as a
# MAGIC **non-event**. Like the in-LDP snapshot build (`02_snapshot_scd2`), it applies SCD2 from a *snapshot*
# MAGIC (`create_auto_cdc_from_snapshot_flow`) rather than a change-feed stream, so there is no streaming schema
# MAGIC log to break — a `bronze_nopk` full refresh runs the silver flow straight to COMPLETED with no recovery
# MAGIC step.
# MAGIC
# MAGIC **What to expect.** After a full refresh, the SCD2 target on `_bk` is unchanged: same version counts,
# MAGIC same `__START_AT` on unchanged entities, zero spurious versions.
# MAGIC
# MAGIC **The architectural decision it informs.** The transformed-key route inherits the snapshot build's
# MAGIC best operational property — zero-touch full-refresh survival — because the surrogate is **deterministic**:
# MAGIC re-hashing the re-delivered rows after a refresh yields identical `_bk` values, so AUTO CDC idempotency
# MAGIC recognizes them as unchanged and opens nothing new. No `reset_checkpoint_selection` is ever needed.
# MAGIC
# MAGIC Attach to **serverless**. Resolves pipelines by name; set `catalog` only if yours differs.

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


BRONZE = pipeline_id("bronze_nopk_ingestion")   # keyless-sandbox ingestion
SILVER = pipeline_id("silver_hash_scd2")        # the content-hash SCD2 build
print("bronze_nopk    :", BRONZE)
print("silver_hash    :", SILVER)


def counts(label):
    r = spark.sql(f"""
      SELECT COUNT(*) v, COUNT(DISTINCT _bk) k,
             COUNT(*) - COUNT(DISTINCT _bk) extra
      FROM {CAT}.silver_hash.account_nopk
    """).first()
    print(f"--- {label} ---")
    print(f"  {r.v} versions / {r.k} keys / {r.extra} extra history versions")


def wait(pid, update_id, label):
    while True:
        st = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update.state.value
        print(f"  {label}: {st}")
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return st
        time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BEFORE: record the accumulated history

# COMMAND ----------

counts("BEFORE full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full-refresh the keyless-sandbox bronze (`bronze_nopk`)
# MAGIC Re-snapshots `account_nopk`. For a change-feed stream this would be a read-incompatible schema change;
# MAGIC for this snapshot-based build it is not.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=BRONZE, full_refresh=True)
wait(BRONZE, u.update_id, "bronze_nopk full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run `silver_hash_scd2` NORMALLY -> it COMPLETES (no recovery needed)
# MAGIC No `reset_checkpoint_selection`, no failure. The MV re-hashes the re-delivered rows to identical `_bk`
# MAGIC values, and the snapshot flow finds them unchanged.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=SILVER, full_refresh=False)
state = wait(SILVER, u.update_id, "silver_hash normal run")
print("\nExpected COMPLETED (no schema-change failure). Got:", state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AFTER: confirm history survived, zero spurious versions

# COMMAND ----------

counts("AFTER full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The one rule to land
# MAGIC A deterministic surrogate key makes the content-hash build a **zero-touch full-refresh survivor**,
# MAGIC exactly like the in-LDP snapshot build. Nothing to reset, no history collapse. Contrast
# MAGIC `07_append_log`, where a full refresh *inflates* the log because an append-only table has no key to
# MAGIC dedup against — the deterministic `_bk` here is precisely what prevents that.
