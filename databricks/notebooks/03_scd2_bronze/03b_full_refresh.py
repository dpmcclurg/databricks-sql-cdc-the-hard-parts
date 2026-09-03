# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · SCD2-bronze — full-refresh recovery lab
# MAGIC
# MAGIC **Why this notebook exists.** A connector full refresh of the SCD Type 2 bronze target (`bronze_scd2`)
# MAGIC breaks the downstream `silver_a` change-feed flow on its next run, but the accumulated per-change SCD
# MAGIC Type 2 history is **not lost**. The correct recovery is `reset_checkpoint_selection` (reset the flow
# MAGIC checkpoint, KEEP the table data), never a full refresh of `silver_a` (which would clear the data and
# MAGIC rewrite history). This is the same durability story as `01b`, on the full-fidelity build.
# MAGIC
# MAGIC **What to expect.** After recovery the version counts match BEFORE exactly: no collapse, no timestamps
# MAGIC reset. AUTO CDC idempotency recognizes the re-delivered current state as unchanged, so no spurious
# MAGIC versions open — even though the re-snapshot re-delivers rows at a higher sequence.
# MAGIC
# MAGIC **The architectural decision it informs.** A connector full refresh is a handled operational step for
# MAGIC this variant too, not a reason to rule it out — the refresh-durability gap between the change-feed builds
# MAGIC and the SCD2-bronze build has closed (both recover with one `reset_checkpoint_selection`, no custom
# MAGIC code). The remaining differentiator is purely fidelity (`03a`). For zero-recovery refresh, see `02b`.
# MAGIC
# MAGIC **Prerequisite (not optional).** We assume a source we cannot fully control (like a vendor ERP), where
# MAGIC the assumed key `hmy` is nullable and can arrive dirty. So this build assumes an already-cleaned key — run the DQ quarantine (`05_assumed_key_dq`) upstream first.
# MAGIC
# MAGIC **The arc:** record history -> full-refresh bronze_scd2 -> watch silver_a break on purpose -> recover with
# MAGIC one checkpoint reset -> confirm history survived intact.
# MAGIC
# MAGIC Attach to **serverless**. It resolves pipelines by name, so it works after a rebuild without editing IDs;
# MAGIC set the `catalog` widget only if yours differs.
# MAGIC
# MAGIC > Tip: to make "history survived" vivid, first create multi-version history via `03a_capture_changes` (a
# MAGIC > `churn` on `account`, then run the `bronze_scd2` and `silver_a` pipelines). Some `account` keys will
# MAGIC > then have 2 versions, and this lab shows they survive the refresh.

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


BR2 = pipeline_id("sqlserver_to_bronze_scd2")   # the SCD2 connector target (bronze_scd2)
SILVER_A = pipeline_id("silver_a_scd2")          # the durable downstream SCD2 (silver_a)
print("bronze_scd2 :", BR2)
print("silver_a    :", SILVER_A)


def counts(label):
    print(f"--- {label} ---")
    for t in TABLES:
        r = spark.sql(
            f"SELECT COUNT(*) v, COUNT(DISTINCT {KEYCOL[t]}) k FROM {CAT}.silver_a.{t}"
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
# MAGIC ## 2. Full-refresh the connector bronze (`bronze_scd2`)
# MAGIC This re-snapshots the source into bronze. It is correct for bronze, but it re-materializes the backing
# MAGIC table, which is a read-incompatible schema change for the downstream stream.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=BR2, full_refresh=True)
wait(BR2, u.update_id, "bronze_scd2 full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run `silver_a` normally -> it FAILS on purpose
# MAGIC The next normal run of the downstream change-feed flow fails with
# MAGIC `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE`. This is expected, not data loss: the run applies nothing
# MAGIC and `silver_a`'s existing data is untouched.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=SILVER_A, full_refresh=False)
state = wait(SILVER_A, u.update_id, "silver_a normal run")
print("\nExpected FAILED (INCOMPATIBLE_SCHEMA_CHANGE). Got:", state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Recover with `reset_checkpoint_selection`
# MAGIC Reset the flow checkpoints and KEEP the streaming-table data. AUTO CDC idempotency then recognizes the
# MAGIC re-delivered current state as unchanged, so no spurious versions open. The CLI has no flag for this, so
# MAGIC call the pipelines REST API directly.

# COMMAND ----------

flows = [f"{CAT}.silver_a.{t}" for t in TABLES]
resp = w.api_client.do(
    "POST", f"/api/2.0/pipelines/{SILVER_A}/updates",
    body={"reset_checkpoint_selection": flows},
)
wait(SILVER_A, resp["update_id"], "silver_a checkpoint reset")

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
# MAGIC NEVER a full refresh of `silver_a`. A full refresh of `silver_a` would clear its data and rebuild from
# MAGIC bronze's post-refresh current state, collapsing history to one version per key. The full refresh is
# MAGIC therefore a handled operational step, not a reason to rule out this architecture. Subsequent `silver_a`
# MAGIC runs pick up new changes as usual. Evidence: `docs/experiments.md` E4 / E6 / E10.
