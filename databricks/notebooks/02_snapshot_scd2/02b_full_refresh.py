# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Snapshot SCD2 — full-refresh survival (the zero-intervention case)
# MAGIC
# MAGIC **Why this notebook exists.** This is the variant's headline property. Where the change-feed build
# MAGIC (`01b_full_refresh`) breaks on the *next* run after a connector full refresh and needs an orchestrated
# MAGIC `reset_checkpoint_selection`, the snapshot build treats a full refresh as a **true non-event**: the flow
# MAGIC runs straight to COMPLETED, with **no error and no recovery step at all**.
# MAGIC
# MAGIC **What to expect.** After a bronze full refresh, a normal `silver_snapshot` run COMPLETES. History is
# MAGIC preserved, and unchanged keys keep their original `__START_AT` — the snapshot comparison recognizes the
# MAGIC re-delivered current state as unchanged, so no spurious versions open. Version counts match BEFORE.
# MAGIC
# MAGIC **The architectural decision it informs.** If a connector full refresh must require *zero* orchestration
# MAGIC — no fail-first run, no API call — this is the build. It is the reason to accept processing-time dates
# MAGIC (variant 01's only advantage) as the trade.
# MAGIC
# MAGIC **Prerequisite (not optional).** We assume a source we cannot fully control (like a vendor ERP), where
# MAGIC the assumed key `hmy` is nullable and can arrive dirty. So this build — like every keyed variant here — assumes an already-cleaned key. Run the data-quality
# MAGIC quarantine (`05_assumed_key_dq`) upstream first; a NULL or duplicate `hmy` would corrupt SCD2.
# MAGIC
# MAGIC **The arc:** record history -> full-refresh bronze -> run silver_snapshot normally -> it just COMPLETES ->
# MAGIC confirm history survived intact, no reset needed.
# MAGIC
# MAGIC Attach to **serverless**. It resolves pipelines by name, so it works after a rebuild without editing
# MAGIC IDs; set the `catalog` widget only if yours differs.
# MAGIC
# MAGIC > Contrast: run `01b_full_refresh` alongside this one. Same full refresh, but the change-feed flow FAILS
# MAGIC > and needs a checkpoint reset; the snapshot flow here does not. That is the whole difference between the
# MAGIC > two builds.

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
SILVER = pipeline_id("silver_snapshot_scd2")       # AUTO CDC FROM SNAPSHOT silver (silver_snap)
print("bronze       :", BRONZE)
print("silver_snap  :", SILVER)


def counts(label):
    print(f"--- {label} ---")
    for t in TABLES:
        r = spark.sql(
            f"SELECT COUNT(*) v, COUNT(DISTINCT {KEYCOL[t]}) k FROM {CAT}.silver_snap.{t}"
        ).first()
        print(f"  {t}: {r.v} versions / {r.k} keys")


def start_ats(label):
    print(f"--- {label}: __START_AT for account keys 1-3 ---")
    for r in spark.sql(
        f"SELECT hmy, `__START_AT` FROM {CAT}.silver_snap.account "
        f"WHERE hmy IN (1,2,3) AND `__END_AT` IS NULL ORDER BY hmy"
    ).collect():
        print(f"  hmy={r.hmy}: {r['__START_AT']}")


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
# MAGIC ## 1. BEFORE: record history and the current open-version timestamps

# COMMAND ----------

counts("BEFORE full refresh")
start_ats("BEFORE full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full-refresh the connector bronze
# MAGIC This re-snapshots the source into bronze — the same operation that breaks the change-feed flow in
# MAGIC `01b`. Here it is upstream of a batch snapshot reader, which has no streaming schema log to invalidate.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=BRONZE, full_refresh=True)
wait(BRONZE, u.update_id, "bronze full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run `silver_snapshot` NORMALLY -> it just COMPLETES
# MAGIC No fail-first, no incompatible-schema error. This is the whole point of the variant.

# COMMAND ----------

u = w.pipelines.start_update(pipeline_id=SILVER, full_refresh=False)
state = wait(SILVER, u.update_id, "silver_snapshot normal run")
print("\nExpected COMPLETED (no recovery needed). Got:", state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AFTER: confirm history survived with no intervention
# MAGIC Counts match BEFORE, and unchanged keys keep their original `__START_AT` — no timestamp reset, no
# MAGIC spurious versions. No `reset_checkpoint_selection` was called; none was needed.

# COMMAND ----------

counts("AFTER full refresh (no recovery step)")
start_ats("AFTER full refresh (no recovery step)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The one rule to land
# MAGIC For the snapshot build, a connector full refresh is a **non-event**: run silver normally afterward and
# MAGIC it completes with history intact. This is the operational contrast with every change-feed build in this
# MAGIC testbed (01, 03, 04), which fail-first and need a checkpoint reset (`reset_checkpoint_selection`, or a
# MAGIC full checkpoint reset for the standalone stream). The cost you pay for this simplicity is processing-time
# MAGIC validity dates and a full re-read of bronze each trigger. Evidence: `docs/experiments.md` E7.
