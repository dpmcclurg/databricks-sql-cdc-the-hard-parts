# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Change-feed SCD2 — full-refresh recovery lab
# MAGIC
# MAGIC **Why this notebook exists.** A connector full refresh re-materializes bronze, which is a
# MAGIC read-incompatible schema change for this downstream change-feed stream: the *next* normal `silver_scd2`
# MAGIC run fails on purpose with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE`. The point to land is that the
# MAGIC accumulated SCD Type 2 history is **not lost** — the correct recovery is `reset_checkpoint_selection`
# MAGIC (reset the flow checkpoint, KEEP the table data), never a full refresh of silver (which clears the
# MAGIC data and rewrites history).
# MAGIC
# MAGIC **What to expect.** After recovery the version counts match BEFORE exactly: no collapse to
# MAGIC one-version-per-key, no timestamps reset. AUTO CDC idempotency recognizes the re-delivered current
# MAGIC state as unchanged, so no spurious versions open.
# MAGIC
# MAGIC **The architectural decision it informs.** A connector full refresh is a handled operational step for
# MAGIC this variant, not a reason to rule it out — it costs one orchestrated API call. If you want a build
# MAGIC where a full refresh needs *zero* recovery, that is `02_snapshot_scd2`.
# MAGIC
# MAGIC **The arc:** record history -> full-refresh bronze -> watch silver break on purpose -> recover with one
# MAGIC checkpoint reset -> confirm history survived intact.
# MAGIC
# MAGIC Attach to **serverless**. It resolves pipelines by name, so it works after a rebuild without editing
# MAGIC IDs; set the `catalog` widget only if yours differs.
# MAGIC
# MAGIC > Tip: to make "history survived" vivid, first create multi-version history via `01a_capture_changes`
# MAGIC > (a `churn` on `account`, then run `bronze_ingestion` + `silver_scd2`). Some keys will then have 2
# MAGIC > versions, and this lab shows they survive the refresh.

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
TABLES = ["account", "property", "tenant", "lease"]
KEYCOL = {"account": "hmy", "property": "hmy", "tenant": "hmy", "lease": "property_hmy"}


def pipeline_id(name_suffix):
    """Resolve a pipeline id by the tail of its name (ignores the [dev <user>] prefix)."""
    for p in w.pipelines.list_pipelines():
        if p.name and p.name.endswith(name_suffix):
            return p.pipeline_id
    raise ValueError(f"pipeline not found ending in '{name_suffix}'")


BRONZE = pipeline_id("sqlserver_to_bronze_scd1")   # the SCD1 connector target (bronze)
SILVER = pipeline_id("silver_scd2")                # the durable downstream SCD2 (silver)
print("bronze  :", BRONZE)
print("silver  :", SILVER)


def counts(label):
    print(f"--- {label} ---")
    for t in TABLES:
        r = spark.sql(
            f"SELECT COUNT(*) v, COUNT(DISTINCT {KEYCOL[t]}) k FROM {CAT}.silver.{t}"
        ).first()
        print(f"  {t}: {r.v} versions / {r.k} keys")


def wait(pid, update_id, label):
    """Poll an update to a terminal state; returns the final state (does not raise on FAILED)."""
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
# MAGIC ## 2. Full-refresh the connector bronze
# MAGIC This re-snapshots the source into bronze. It is correct for bronze, but it re-materializes the backing
# MAGIC table — a read-incompatible schema change for the downstream stream.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=BRONZE, full_refresh=True)
wait(BRONZE, u.update_id, "bronze full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run `silver_scd2` normally -> it FAILS on purpose
# MAGIC The next normal run of the downstream change-feed flow fails with
# MAGIC `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE`. Expected, not data loss: the run applies nothing and
# MAGIC silver's existing data is untouched.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=SILVER, full_refresh=False)
state = wait(SILVER, u.update_id, "silver normal run")
print("\nExpected FAILED (INCOMPATIBLE_SCHEMA_CHANGE). Got:", state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Recover with `reset_checkpoint_selection`
# MAGIC Reset the flow checkpoints and KEEP the streaming-table data. AUTO CDC idempotency then recognizes the
# MAGIC re-delivered current state as unchanged, so no spurious versions open. The CLI has no flag for this, so
# MAGIC call the pipelines REST API directly.

# COMMAND ----------

flows = [f"{CAT}.silver.{t}" for t in TABLES]
resp = w.api_client.do(
    "POST", f"/api/2.0/pipelines/{SILVER}/updates",
    body={"reset_checkpoint_selection": flows},
)
wait(SILVER, resp["update_id"], "silver checkpoint reset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. AFTER: confirm history survived
# MAGIC The counts should match BEFORE: no collapse to one-version-per-key, no timestamps reset. Any
# MAGIC multi-version keys you created keep both versions.

# COMMAND ----------

counts("AFTER recovery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The one rule to land
# MAGIC Recover a broken downstream flow with **`reset_checkpoint_selection`** (checkpoint reset, keep data),
# MAGIC NEVER a full refresh of silver. A full refresh of silver would clear its data and rebuild from bronze's
# MAGIC post-refresh current state, collapsing history to one version per key. The full refresh is therefore a
# MAGIC handled operational step, not a reason to rule out this architecture. Subsequent `silver_scd2` runs pick
# MAGIC up new changes as usual.
