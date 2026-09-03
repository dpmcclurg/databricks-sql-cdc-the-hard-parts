# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Streaming SCD2 — full-refresh jam & checkpoint-reset lab
# MAGIC
# MAGIC **Why this notebook exists.** A connector full refresh of bronze **jams** this standalone CDF stream
# MAGIC and, unlike the LDP variants, a plain restart does not fix it. The refresh writes a transient
# MAGIC empty-schema (`DLT SETUP`) commit into bronze; the stream re-traverses it from its stored offset and
# MAGIC fails first with `DELTA_STREAMING_METADATA_EVOLUTION`, then on restart with `DELTA_SCHEMA_NOT_SET`
# MAGIC (experiments E8/E9). The point to land: **history is NOT lost** (the target holds it and the idempotent
# MAGIC MERGE prevents spurious versions), but recovery needs a **full checkpoint reset**.
# MAGIC
# MAGIC **What to expect.** Neither pause-timing nor a schema-log-only reset works (E9): a CDF stream reads
# MAGIC every commit from its offset forward, so it always re-hits the transient commit. Only discarding the
# MAGIC checkpoint (offsets + schema log) and restarting from *latest* clears it — cheap (~40s), no massive
# MAGIC replay, history preserved.
# MAGIC
# MAGIC **The architectural decision it informs.** A full refresh is a per-refresh operational cost for this
# MAGIC variant (a checkpoint reset + replay-from-latest), and in continuous mode the job's auto-restart will
# MAGIC loop on the jam until you reset — so **pause the continuous job around a planned refresh**. If you want
# MAGIC a build that survives a full refresh with *zero* intervention, that is `02_snapshot_scd2`; with one
# MAGIC orchestrated API call and no custom code, `01_changefeed_scd2`.
# MAGIC
# MAGIC **The arc:** record history -> full-refresh bronze -> the stream jams on purpose -> full checkpoint
# MAGIC reset -> restart from latest (re-run `04a`) -> confirm history survived.
# MAGIC
# MAGIC Attach to **serverless**. Set the `catalog` widget only if yours differs.

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CAT = dbutils.widgets.get("catalog")
SILVER = "silver_stream"
CHK = f"/Volumes/{CAT}/{SILVER}/_chk"
TABLES = ["account", "property", "tenant", "lease"]
KEYCOL = {"account": "hmy", "property": "hmy", "tenant": "hmy", "lease": "property_hmy"}


def counts(label):
    print(f"--- {label} ---")
    for t in TABLES:
        r = spark.sql(
            f"SELECT COUNT(*) v, COUNT(DISTINCT {KEYCOL[t]}) k FROM {CAT}.{SILVER}.{t}"
        ).first()
        print(f"  {t}: {r.v} versions / {r.k} keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BEFORE: record the accumulated history

# COMMAND ----------

counts("BEFORE full refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Full-refresh the connector bronze (in a terminal)
# MAGIC Stop the continuous job first if it is running (its auto-restart would loop on the jam). Then:
# MAGIC
# MAGIC ```bash
# MAGIC databricks pipelines start-update <bronze_ingestion-id> --full-refresh --profile sqlcdc   # poll to COMPLETED
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the stream normally -> it JAMS on purpose
# MAGIC Re-run `04a_capture_changes` (or the `silver_stream_scd2` job). The first run fails with
# MAGIC `DELTA_STREAMING_METADATA_EVOLUTION`; a naive restart then fails with `DELTA_SCHEMA_NOT_SET`. Both come
# MAGIC from the refresh's transient empty-schema commit sitting behind the stored offset. This is expected and
# MAGIC is NOT data loss — the `silver_stream` tables are untouched (confirm with the count above).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Recover with a FULL checkpoint reset
# MAGIC Delete each table's checkpoint directory (offsets AND schema log). A fresh `readChangeFeed` stream then
# MAGIC starts from *latest*, skipping the refresh's transient commits. History is preserved because it lives
# MAGIC in the target table, and the idempotent MERGE recognizes the re-delivered current state as unchanged.
# MAGIC
# MAGIC > Partial recovery does NOT work (E9): deleting only the schema-log subdir but keeping offsets still
# MAGIC > re-traverses the empty-schema commit. You must discard the whole checkpoint.

# COMMAND ----------

# Runnable helper: discard the per-table stream checkpoints so the next run starts from latest.
def reset_stream_checkpoints(tables=TABLES):
    for t in tables:
        path = f"{CHK}/{t}/chk"
        try:
            dbutils.fs.rm(path, recurse=True)
            print(f"reset checkpoint: {path}")
        except Exception as e:
            print(f"  ({t}: nothing to remove or already clear — {e})")

reset_stream_checkpoints()
print("\nCheckpoints cleared. Now re-run 04a_capture_changes (or the silver_stream_scd2 job) to "
      "restart the streams from latest.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. AFTER: confirm history survived
# MAGIC Re-run `04a` (drains and rebuilds the streams from latest), then run the count below. It should match
# MAGIC BEFORE: no collapse to one-version-per-key, no timestamps reset, no spurious versions.

# COMMAND ----------

counts("AFTER recovery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The one rule to land
# MAGIC A bronze full refresh jams this stream; recover with a **full checkpoint reset + restart from latest**
# MAGIC (not a pause, not a schema-log-only reset). Because the target holds history and the MERGE is
# MAGIC idempotent, no history is lost and no spurious versions open. In continuous mode, **pause the job
# MAGIC around a planned refresh** so its auto-restart does not loop on the jam. Seamless streaming survival
# MAGIC would require the connector to stop emitting the transient empty-schema commit (product item
# MAGIC ES-2075799); until then, `02_snapshot_scd2` remains the zero-intervention full-refresh survivor.
