# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Append-only change log — full-refresh behavior (the gotcha)
# MAGIC
# MAGIC **Why this notebook exists.** An append-only log has **no key to dedup against**, so a connector full
# MAGIC refresh does something none of the keyed variants do: it **inflates the log**. Two things happen. (a) The
# MAGIC log reads the bronze change feed inside LDP, so a `bronze_nopk` full refresh breaks the CDF stream on
# MAGIC the next run with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE` (like `01_changefeed_scd2`), needing
# MAGIC `reset_checkpoint_selection`. (b) On recovery the flow re-reads bronze's re-materialized state, and the
# MAGIC **entire current-state snapshot is re-appended as fresh insert rows** — so the row count jumps by about
# MAGIC the size of the table.
# MAGIC
# MAGIC **What to expect.** BEFORE: ~104 rows. AFTER a full refresh + recovery: ~204 rows — the 100-row snapshot
# MAGIC appended again, with no way to tell the duplicates from genuine new inserts (no key).
# MAGIC
# MAGIC **The architectural decision it informs.** For an append log, a full refresh is a **re-baseline you must
# MAGIC plan for**: apply a retention/dedup strategy downstream, or treat every full refresh as a deliberate log
# MAGIC reset. This is the operational cost of the no-key fallback — and another reason to prefer a keyed variant
# MAGIC (05 assumed key, 06 content-hash) whose deterministic identity lets idempotency absorb the refresh.
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
SILVER = pipeline_id("silver_append_log")       # the append-only log build
print("bronze_nopk    :", BRONZE)
print("silver_append  :", SILVER)


def rowcount(label):
    r = spark.sql(f"SELECT COUNT(*) n FROM {CAT}.silver_log.account_nopk_log").first()
    print(f"--- {label} ---  rows = {r.n}")
    return r.n


def wait(pid, update_id, label):
    while True:
        st = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update.state.value
        print(f"  {label}: {st}")
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return st
        time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BEFORE: record the log size

# COMMAND ----------

before = rowcount("BEFORE full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full-refresh the keyless-sandbox bronze (`bronze_nopk`)
# MAGIC Re-materializes `account_nopk`, which is a read-incompatible schema change for the downstream change-feed
# MAGIC stream.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=BRONZE, full_refresh=True)
wait(BRONZE, u.update_id, "bronze_nopk full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run `silver_append_log` normally -> it FAILS on purpose
# MAGIC Same failure mode as the change-feed SCD2 build: `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE`.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=SILVER, full_refresh=False)
state = wait(SILVER, u.update_id, "silver_append normal run")
print("\nExpected FAILED (INCOMPATIBLE_SCHEMA_CHANGE). Got:", state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Recover with `reset_checkpoint_selection`
# MAGIC Reset the flow checkpoint (keep the table data). The flow then re-reads bronze from the start of the
# MAGIC re-materialized feed — which re-appends the full current-state snapshot.

# COMMAND ----------

resp = w.api_client.do(
    "POST", f"/api/2.0/pipelines/{SILVER}/updates",
    body={"reset_checkpoint_selection": [f"{CAT}.silver_log.account_nopk_log"]},
)
wait(SILVER, resp["update_id"], "silver_append checkpoint reset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. AFTER: the log has INFLATED
# MAGIC The row count jumps by ~the table size (the whole snapshot re-appended). With no key, these are
# MAGIC indistinguishable from real inserts.

# COMMAND ----------

after = rowcount("AFTER recovery")
print(f"\nDelta: +{after - before} rows (≈ the current-state snapshot re-appended).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The one rule to land
# MAGIC An append-only log cannot absorb a full refresh — with no key there is nothing to dedup against, so the
# MAGIC current state is re-appended and the log grows. Plan a retention/dedup strategy, or treat a full refresh
# MAGIC as a deliberate log re-baseline. The keyed variants (05, 06) do not have this problem: a deterministic
# MAGIC identity lets AUTO CDC idempotency recognize the re-delivered rows as unchanged.
