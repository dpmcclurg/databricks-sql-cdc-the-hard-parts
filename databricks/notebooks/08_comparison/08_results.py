# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow SQL Server CDC testbed: results and demonstration
# MAGIC
# MAGIC A guided walk through SCD Type 2 history on a hard CDC source (keyless heap, nullable key, no reliable
# MAGIC date), run on live data. Each section
# MAGIC states the point, runs the query, and shows the evidence, then names the **consequence** for the
# MAGIC architecture decision.
# MAGIC
# MAGIC **The realistic setup.** Every source table is a keyless heap: no PRIMARY KEY, no identity, no unique
# MAGIC constraint, and the assumed-key column (`hmy`) is **nullable**. We assume a source we cannot fully
# MAGIC control (like a vendor ERP). Two independent questions drive the choice of build:
# MAGIC 1. **Can you identify the entity?** — an assumed key (`hmy`), a consolidated/composite key (`lease`), a
# MAGIC    transformed surrogate (content-hash), or not at all. Decides whether SCD is even possible.
# MAGIC 2. **How is change captured, and can it run continuously?** — change feed vs. snapshot; triggered vs.
# MAGIC    always-on. Decides the mechanism, not whether SCD is achievable.
# MAGIC
# MAGIC Because the assumed key is nullable and comes from a source we do not fully control, the data-quality layer (variant 05) is a
# MAGIC prerequisite for trusting `hmy`, not an add-on. The four core builds below all key on the assumed
# MAGIC `hmy`; they publish to their own schema and coexist, so the comparison queries them side by side:
# MAGIC
# MAGIC | Variant | Bronze | Silver schema | Executed by |
# MAGIC |---|---|---|---|
# MAGIC | **01 change-feed SCD2** | `bronze` (SCD1) | `silver` | LDP pipeline `silver_scd2` |
# MAGIC | **02 snapshot SCD2** | `bronze` (SCD1) | `silver_snap` | LDP pipeline `silver_snapshot_scd2` |
# MAGIC | **03 SCD2-bronze** | `bronze_scd2` (SCD2) | `silver_a` | LDP pipeline `silver_a_scd2` |
# MAGIC | **04 streaming SCD2** | `bronze` (SCD1) | `silver_stream` | job `silver_stream_scd2` / `silver_stream_continuous` |
# MAGIC
# MAGIC Keyless-heap strategies (variants 05 assumed-key DQ, 06 content-hash, 07 append-log) run on the
# MAGIC separate `account_nopk` sandbox — summarized in the "keyless heap handling" section near the end.
# MAGIC
# MAGIC A section for a variant that has not been run yet reports that its schema is absent and skips — run
# MAGIC that variant (see the next cell) and re-run the section. Per-experiment evidence: repo
# MAGIC `docs/experiments.md`.

# COMMAND ----------

dbutils.widgets.text("catalog", "sqlcdc_testbed_dbx01_7405609436951225")
CATALOG = dbutils.widgets.get("catalog")

# Fixed schema names per variant (all coexist in the catalog).
BRONZE_SCD1 = "bronze"          # variants 01 / 02 / 04 source
BRONZE_SCD2 = "bronze_scd2"     # variant 03 source
SILVER_B    = "silver"          # variant 01, AUTO CDC off the change feed
SILVER_SNAP = "silver_snap"     # variant 02, AUTO CDC FROM SNAPSHOT
SILVER_A    = "silver_a"        # variant 03, durable SCD2 off the SCD2 connector target
SILVER_C    = "silver_stream"   # variant 04, standalone Structured Streaming
print(f"catalog={CATALOG}")


def show(sql):
    display(spark.sql(sql))


def schema_present(schema, table="account"):
    """True if this variant has been materialized (its schema + a table exist)."""
    try:
        return spark.catalog.tableExists(f"{CATALOG}.{schema}.{table}")
    except Exception:
        return False


def require(schema, table="account"):
    if schema_present(schema, table):
        return True
    print(f"[skipped] {CATALOG}.{schema} not present yet. Run this variant (see 'How to run each "
          f"variant'), then re-run this section.")
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## The point (one slide)
# MAGIC The source ERP tables are keyless heaps with no reliable date column, so on the connector's SCD
# MAGIC Type 2 target `__cdc_timestamp_value` is NULL on the snapshot rows first inspected. The question: how to
# MAGIC get durable, readable, **dated** SCD Type 2 history from a keyless source, and how that history behaves
# MAGIC through the operational reality of a connector full refresh. This testbed builds the variants on the
# MAGIC *same* source data and shows what each does for readable dates, per-change fidelity, full-refresh
# MAGIC survival, and latency.
# MAGIC
# MAGIC **Bottom line the sections below prove:**
# MAGIC - The silver key is always an *assumed* key (`hmy`), never a source PK — and because it is nullable and
# MAGIC   uncontrolled, the DQ quarantine + circuit breaker (variant 05) is what makes it safe.
# MAGIC - Readable dates on a date-less source: solved natively by the change feed (variants 01/03/04) — no forward date capture.
# MAGIC - Full-refresh survival: both LDP change-feed builds survive with history intact (`reset_checkpoint_selection`), and the snapshot build survives with zero intervention.
# MAGIC - The remaining real fork is **fidelity** (per-change 03 vs net-per-interval 01) and, separately, **latency** (only 04 runs continuously today).

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to run each variant (execution reference)
# MAGIC Run these from the repo (`databricks/`) with `--profile sqlcdc`. Drive change data with the local
# MAGIC simulator between runs (venv active, `SQLDB_*` exported): e.g.
# MAGIC `python sim/changegen.py churn --table account --updates 10` or
# MAGIC `python sim/changegen.py burst --table tenant --key 42 --updates 8`. Then re-run the sections below.
# MAGIC
# MAGIC ```bash
# MAGIC # 01 change-feed SCD2: SCD1 bronze -> silver
# MAGIC databricks bundle run bronze_ingestion -t dev --profile sqlcdc      # incremental (first run: start-update --full-refresh)
# MAGIC databricks bundle run silver_scd2      -t dev --profile sqlcdc
# MAGIC
# MAGIC # 02 snapshot SCD2: SCD1 bronze -> silver_snap (survives a full refresh with zero intervention)
# MAGIC databricks bundle run silver_snapshot -t dev --profile sqlcdc
# MAGIC
# MAGIC # 03 SCD2-bronze: needs the SCD2 bronze (delete the SCD1 bronze first, finding A0); then silver_a off its change feed
# MAGIC databricks bundle run bronze_ingestion_scd2 -t dev --profile sqlcdc
# MAGIC databricks bundle run silver_a            -t dev --profile sqlcdc
# MAGIC
# MAGIC # 04 streaming SCD2: triggered, or continuous (only variant that runs always-on today)
# MAGIC databricks bundle run silver_stream_job -t dev --profile sqlcdc     # triggered: drains and stops
# MAGIC #   continuous: unpause the silver_stream_continuous job, then re-pause when done (see runbook)
# MAGIC ```
# MAGIC
# MAGIC The full-refresh + `reset_checkpoint_selection` recovery has a runnable helper further down
# MAGIC (Experiment 4/10), and a per-variant lab in each `0Nb_full_refresh` notebook.

# COMMAND ----------

# Which variants are materialized right now?
present = {
    "01 change feed (silver)":  schema_present(SILVER_B),
    "02 snapshot (silver_snap)": schema_present(SILVER_SNAP),
    "03 SCD2-bronze (silver_a)":       schema_present(SILVER_A),
    "04 streaming (silver_stream)": schema_present(SILVER_C),
    "SCD2 bronze (bronze_scd2)": schema_present(BRONZE_SCD2),
}
for name, ok in present.items():
    print(f"  {'present ' if ok else 'absent  '}  {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 2: readable date for a date-less source (the core win)
# MAGIC `account` has no date column, yet silver's validity timestamps are real. They come from the
# MAGIC bronze change feed's `_commit_timestamp`. First the change feed, then variant 01 silver dated from it.

# COMMAND ----------

# Start CDF read from version 1: a full refresh resets version 0, which has no recorded change data.
show(f"""
  SELECT _change_type, _commit_version, _commit_timestamp, hmy, status
  FROM table_changes('{CATALOG}.{BRONZE_SCD1}.account', 1)
  WHERE hmy <= 3
  ORDER BY hmy, _commit_version
""")

# COMMAND ----------

# Silver (variant 01): real __START_AT / __END_AT on a date-less source.
if require(SILVER_B):
    show(f"""
      SELECT hmy, name, status, `__START_AT`, `__END_AT`
      FROM {CATALOG}.{SILVER_B}.account
      WHERE hmy IN (1, 2, 3, 10, 11)
      ORDER BY hmy, `__START_AT`
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** readable dates on a date-less source come for free from the change feed. No forward
# MAGIC date-capture, no date mapping. This is the readable-date solution on a date-less source, native to the change-feed build.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 1 (variant 03): the NULL timestamp on the connector target — and when it is NOT null
# MAGIC On the SCD Type 2 connector target (`bronze_scd2`), the date-less `account` shows the LSN in
# MAGIC `__START_AT.__cdc_internal_value`. `__cdc_timestamp_value` is NULL **only on snapshot rows** (a commonly
# MAGIC observed artifact); rows written by a *real* incremental change carry a real commit timestamp even
# MAGIC for a date-less table. This corrects a common assumption: the all-NULL is a snapshot artifact, not a date-less one.

# COMMAND ----------

if require(BRONZE_SCD2):
    show(f"""
      SELECT hmy,
             `__START_AT`.`__cdc_internal_value`  AS start_lsn,
             `__START_AT`.`__cdc_timestamp_value` AS start_ts,
             `__END_AT`.`__cdc_timestamp_value`   AS end_ts
      FROM {CATALOG}.{BRONZE_SCD2}.account
      ORDER BY hmy
      LIMIT 12
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** the SCD2-bronze build gives readable dates on ongoing changes without forward capture;
# MAGIC only the initial snapshot rows are date-less (stamp them at load if a date is required there).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 5 (headline consequence): fidelity vs cadence — net-change (01) vs every change (03)
# MAGIC Drive a burst inside one trigger interval: `python sim/changegen.py burst --table tenant --key 42 --updates 8`,
# MAGIC then run bronze + both silvers. This is the clearest illustration of the **net-changes** consequence:
# MAGIC - **Variant 01** (SCD1 bronze collapses the burst to net): ~1 new version.
# MAGIC - **Variant 03** (SCD2 bronze keeps every logged change): ~8-9 versions.
# MAGIC
# MAGIC Version count for key 42, side by side:

# COMMAND ----------

frames = []
if schema_present(SILVER_B, "tenant"):
    frames.append(("01 change feed (silver)", f"{CATALOG}.{SILVER_B}.tenant"))
if schema_present(SILVER_A, "tenant"):
    frames.append(("03 SCD2-bronze (silver_a)", f"{CATALOG}.{SILVER_A}.tenant"))

if frames:
    union = " UNION ALL ".join(
        f"SELECT '{label}' AS variant, count(*) AS versions_for_key_42 "
        f"FROM {tbl} WHERE hmy = 42" for label, tbl in frames)
    show(union)
else:
    print("[skipped] Neither silver.tenant nor silver_a.tenant present. Run variant 01 and/or 03 first.")

# COMMAND ----------

# The actual versions for key 42 in each present variant (see the collapse vs the full ladder).
for label, schema in [("01 change feed (silver)", SILVER_B), ("03 SCD2-bronze (silver_a)", SILVER_A)]:
    if schema_present(schema, "tenant"):
        print(f"--- {label} ---")
        show(f"""
          SELECT hmy, credit, `__START_AT`, `__END_AT`
          FROM {CATALOG}.{schema}.tenant
          WHERE hmy = 42
          ORDER BY `__START_AT`
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** this is the real fidelity fork. If you need every logged mutation as a distinct
# MAGIC version (strict per-change audit), only variant 03 delivers it. If net-change-per-interval history is
# MAGIC enough (retention, not granularity, e.g. a long 7-year retention requirement), variant 01 suffices and is simpler.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 3: what a full refresh does to the bronze target
# MAGIC A connector full refresh is a managed re-baseline (`DLT REFRESH -> SETUP -> SET TBLPROPERTIES ->
# MAGIC MERGE -> DELETE -> MERGE`): the destination is retained and current state ends correct, but the
# MAGIC `SET TBLPROPERTIES` / setup step is a read-incompatible schema change to a downstream change-feed
# MAGIC stream. Inspect the history to see the shape.

# COMMAND ----------

show(f"""
  SELECT version, operation, timestamp
  FROM (DESCRIBE HISTORY {CATALOG}.{BRONZE_SCD1}.account)
  ORDER BY version DESC LIMIT 12
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 4 / 10 (headline, CORRECTED): full refresh does NOT have to lose history
# MAGIC A bronze full refresh breaks the *next* run of the downstream variant 01 change-feed flow with
# MAGIC `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE`. The correct recovery is **not** a full refresh of
# MAGIC silver (that clears the table and loses history — the earlier mistaken conclusion). It is
# MAGIC **`reset_checkpoint_selection`** ("reset streaming flow checkpoints"): it clears only the flow
# MAGIC checkpoint and **keeps the streaming table data**, so history is preserved and AUTO CDC idempotency
# MAGIC suppresses spurious versions on the re-delivered current state.
# MAGIC
# MAGIC Lab procedure (variant 01):
# MAGIC 1. Run the count below (BEFORE): keys with history show `extra_history_versions > 0`.
# MAGIC 2. Full-refresh bronze: `databricks pipelines start-update <bronze-id> --full-refresh` (let it complete).
# MAGIC 3. A normal `silver_scd2` run now fails with the incompatible-schema error — expected.
# MAGIC 4. Recover with the helper two cells down (`reset_checkpoint_selection` on the silver flows).
# MAGIC 5. Run the count again (AFTER): identical — history intact, no collapse, no timestamp reset.
# MAGIC
# MAGIC The self-contained runnable version of this lab is `01_changefeed_scd2/01b_full_refresh.py`.

# COMMAND ----------

if require(SILVER_B):
    show(f"""
      SELECT count(*) AS versions, count(DISTINCT hmy) AS keys,
             count(*) - count(DISTINCT hmy) AS extra_history_versions
      FROM {CATALOG}.{SILVER_B}.account
    """)

# COMMAND ----------

# Runnable recovery helper: reset_checkpoint_selection via the pipelines REST API (CLI has no flag).
# Clears the flow checkpoint only; does NOT clear table data (unlike --full-refresh). Fill in the
# silver pipeline id and run AFTER a bronze full refresh has broken a normal silver run.
import json, urllib.request

def reset_checkpoints(pipeline_id, tables, schema=SILVER_B):
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get()
    token = ctx.apiToken().get()
    body = json.dumps({
        "reset_checkpoint_selection": [f"{CATALOG}.{schema}.{t}" for t in tables]
    }).encode()
    req = urllib.request.Request(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
        data=body, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        print(json.load(r))

# Example (uncomment and set the silver pipeline id from `databricks bundle summary`):
# reset_checkpoints("<silver-pipeline-id>", ["account", "property", "tenant", "lease"])
print("reset_checkpoints() defined. Uncomment the call with your silver pipeline id after a full refresh.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** the elegant in-LDP change-feed build (variant 01) survives a full refresh with
# MAGIC history intact. Cost is one orchestration step: the flow fails-first on the post-refresh run, then
# MAGIC the reset clears it (a single API call, automatable by detecting the refresh). This corrects the
# MAGIC earlier "full refresh loses history" finding.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 7: the zero-intervention full-refresh survivor (variant 02 snapshot)
# MAGIC `silver_snap` is built with `create_auto_cdc_from_snapshot_flow` reading bronze's *current state*
# MAGIC (not the change feed). It needs no `reset_checkpoint_selection` at all: a bronze full refresh is a
# MAGIC true non-event — the flow runs to COMPLETED, history is preserved, unchanged keys keep their
# MAGIC original `__START_AT`. The trade vs the change-feed build: full re-read each trigger, and
# MAGIC processing-time (not change-time) validity dates.

# COMMAND ----------

if require(SILVER_SNAP):
    show(f"""
      SELECT count(*) AS versions, count(DISTINCT hmy) AS keys,
             count(*) - count(DISTINCT hmy) AS extra_history_versions
      FROM {CATALOG}.{SILVER_SNAP}.account
    """)
    show(f"""
      SELECT hmy, status, `__START_AT`, `__END_AT`
      FROM {CATALOG}.{SILVER_SNAP}.account
      WHERE hmy IN (1, 2, 3)
      ORDER BY hmy, `__START_AT`
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** if you want a full refresh to require zero orchestration, the snapshot build is
# MAGIC the choice — same trigger-interval fidelity as the change-feed build, stays fully in LDP, no custom code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment 6 (variant 03): is a manual reconcile needed? — ANSWERED: no, redundant
# MAGIC `silver_a` is a durable SCD2 built by AUTO CDC off the SCD2 connector target's change feed, keyed
# MAGIC on the assumed key and sequenced by the connector's readable change timestamp. It was designed to
# MAGIC test whether a content-hash reconcile is needed to close old versions and dedupe after a full
# MAGIC refresh. It is not: after a `bronze_scd2` full refresh, recovering `silver_a` with
# MAGIC `reset_checkpoint_selection` **alone** preserved history with zero spurious versions.

# COMMAND ----------

if require(SILVER_A):
    show(f"""
      SELECT count(*) AS versions, count(DISTINCT hmy) AS keys,
             count(*) - count(DISTINCT hmy) AS extra_history_versions
      FROM {CATALOG}.{SILVER_A}.account
    """)
    # A key with history, showing real change-time validity dates (not snapshot time).
    show(f"""
      SELECT hmy, status, `__START_AT`, `__END_AT`
      FROM {CATALOG}.{SILVER_A}.account
      WHERE hmy IN (1, 2, 3, 20)
      ORDER BY hmy, `__START_AT`
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** the reconciliation-complexity gap between 03 and 01 has closed. Both use the same
# MAGIC in-LDP recovery (`reset_checkpoint_selection` + idempotency), no custom code either way. Variant 03's
# MAGIC only remaining edge is full per-change fidelity — so the 01-vs-03 decision is purely the fidelity
# MAGIC question from Experiment 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variant 04: standalone streaming — steady state, the full-refresh jam, and continuous mode
# MAGIC `silver_stream` is the change-feed SCD2 build run OUTSIDE LDP (so it can set `schemaTrackingLocation`
# MAGIC under its own checkpoint). Its distinctive property is latency: it is the **only** variant that can
# MAGIC run **continuously / always-on** today (the LDP variants 01/02/03 are triggered-only, and the
# MAGIC ingestion pipeline's `continuous: true` is coerced to `false`). Costs: it leaves LDP, uses a
# MAGIC hand-written SCD2 MERGE, and a bronze full refresh still jams it (needs a checkpoint reset to resume —
# MAGIC the lab is `04_streaming_scd2/04b_full_refresh.py`).

# COMMAND ----------

if require(SILVER_C):
    # Steady-state SCD2 history with real change-time dates.
    show(f"""
      SELECT hmy, status, `__START_AT`, `__END_AT`
      FROM {CATALOG}.{SILVER_C}.account
      WHERE hmy IN (1, 2, 3, 10, 11)
      ORDER BY hmy, `__START_AT`
    """)
    # Freshness illustrates the continuous-mode value: how recently the newest version was applied.
    show(f"""
      SELECT count(*) AS versions,
             max(`__START_AT`) AS newest_version_applied,
             timestampdiff(SECOND, max(`__START_AT`), current_timestamp()) AS seconds_since_newest
      FROM {CATALOG}.{SILVER_C}.account
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Consequence:** choose variant 04 only if you need genuinely low-latency, sub-minute SCD2 apply,
# MAGIC accepting the custom-code and per-refresh checkpoint-reset cost. On a triggered cadence the in-LDP
# MAGIC change-feed build dominates it. Continuous mode is a latency capability, not a full-refresh fix:
# MAGIC pause the continuous job around a planned refresh (the auto-restart would otherwise loop on the jam).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keyless heap handling (the assumed key, and when hmy is not usable at all)
# MAGIC The four builds above all key on the assumed `hmy`. Because `hmy` is **nullable** and comes from a source we do not fully control,
# MAGIC that assumption must be defended and, when it cannot hold, replaced. These strategies run on the
# MAGIC `account_nopk` sandbox (schemas `silver_dq`, `silver_hash`, `silver_log`):
# MAGIC
# MAGIC | Variant | Key strategy | When to use |
# MAGIC |---|---|---|
# MAGIC | **05 assumed-key DQ** | assumed key `hmy` + Tier-1 quarantine (null/invalid rows set aside) + Tier-2 circuit breaker (fatal on a uniqueness/null breach) | **The recommended keyless path.** Trust `hmy`, but prove it every run. Essential because the source is uncontrolled. |
# MAGIC | **06 content-hash** | transformed surrogate `_bk = sha2(stable identifying columns)` | No single column is a trusted key, but a stable subset identifies the entity. |
# MAGIC | **07 append-log** | none (not SCD) | Nothing identifies the entity — capture every change as an immutable append row. Small tables / event-shaped data only. |
# MAGIC
# MAGIC **Decision tree, per table:** assumed key holds (with DQ) -> variant 05 feeding 01/02/03. Key must be
# MAGIC *computed* from a stable subset -> variant 06. No key of any kind -> variant 07 (or snapshot-current-only).
# MAGIC The self-contained walkthroughs are `05_assumed_key_dq`, `06_content_hash`, `07_append_log`.

# COMMAND ----------

if schema_present("silver_dq", "account_nopk"):
    print("--- variant 05: valid SCD2 vs quarantined rows ---")
    show(f"SELECT count(*) AS scd2_versions, count(DISTINCT hmy) AS keys FROM {CATALOG}.silver_dq.account_nopk")
    show(f"SELECT count(*) AS quarantined_rows FROM {CATALOG}.silver_dq.account_quarantine")
else:
    print("[skipped] silver_dq.account_nopk not present. Run variant 05 on the account_nopk sandbox.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-variant scoreboard
# MAGIC | Build | Readable dates (date-less src) | Fidelity | Full-refresh survival | Framework | Custom code | Continuous? |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | **01 change feed** (`silver`) | Yes (change-time) | Net per interval | Yes, via `reset_checkpoint_selection` | LDP | None | No |
# MAGIC | **02 snapshot** (`silver_snap`) | Processing-time | Net per interval | Yes, zero intervention | LDP | None | No |
# MAGIC | **03 SCD2-bronze** (`silver_a`) | Yes (change-time) | Every change | Yes, via `reset_checkpoint_selection` | LDP | None | No |
# MAGIC | **04 streaming** (`silver_stream`) | Yes (change-time) | Every change | History kept, but jams; needs checkpoint reset | Structured Streaming | Yes (MERGE) | **Yes (only one)** |
# MAGIC
# MAGIC **Decision:** the assumed key (`hmy`) plus DQ (variant 05) is the foundation for every build — non-negotiable
# MAGIC on an uncontrolled, nullable source. Given that, refresh-durability, custom-code, and reconcile are no
# MAGIC longer differentiators among the LDP builds. The choice reduces to **fidelity** (per-change 03 vs
# MAGIC net-per-interval 01) and, separately, **latency** (only 04 runs continuously today). When the requirement is
# MAGIC long retention (e.g. 7 years) rather than change granularity, a change-feed or snapshot build (01/02) is the
# MAGIC likely recommendation; choose 03 if strict per-change audit is confirmed, or 04 if sub-minute latency is
# MAGIC required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC See `docs/experiments.md` (repo) for the observed result and implication of each experiment
# MAGIC (E1-E14). The per-variant notebooks (`01`-`07`, each with an `a_capture_changes` and `b_full_refresh`)
# MAGIC are the code-forward walkthroughs.
