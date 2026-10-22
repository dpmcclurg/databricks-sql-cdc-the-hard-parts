# Databricks notebook source
# Silver recovery after a Bronze full refresh. Run as two job tasks, in order:
#   mode=reset     Find Bronze tables refreshed since Silver's last successful update, pin each
#                  one's current version, and start Silver with reset_checkpoint_selection on
#                  their main flows. With nothing refreshed, run a normal Silver update.
#   mode=tombstone For each pin not yet processed, close Silver keys that are open but no longer
#                  live in the pinned Bronze version (deleted inside the refresh window), then
#                  run a normal Silver update to apply them.
# A refresh is detected from a DLT REFRESH entry in the Bronze table's history, or, as a
# fallback, from Silver's last update failing with a streaming schema-change error on that flow.
import json
import time

from databricks.sdk import WorkspaceClient

for w_ in ("catalog", "bronze_schema", "silver_schema", "ops_schema", "tables", "silver_pipeline_id", "mode"):
    dbutils.widgets.text(w_, "")
dbutils.widgets.text("detect", "history,failure")  # tests set "failure" to exercise the fallback alone
CAT = dbutils.widgets.get("catalog")
BRONZE = f"{CAT}.{dbutils.widgets.get('bronze_schema')}"
SILVER = f"{CAT}.{dbutils.widgets.get('silver_schema')}"
OPS = f"{CAT}.{dbutils.widgets.get('ops_schema')}"
TABLES = json.loads(dbutils.widgets.get("tables"))
SILVER_ID = dbutils.widgets.get("silver_pipeline_id")
MODE = dbutils.widgets.get("mode")
DETECT = dbutils.widgets.get("detect").split(",")
REFRESH_OPS = ("DLT REFRESH", "DLT SETUP", "CREATE TABLE", "CREATE OR REPLACE TABLE")

w = WorkspaceClient()


def run_silver(body=None):
    uid = (w.api_client.do("POST", f"/api/2.0/pipelines/{SILVER_ID}/updates", body=body)["update_id"]
           if body else w.pipelines.start_update(pipeline_id=SILVER_ID).update_id)
    while (st := w.pipelines.get_update(pipeline_id=SILVER_ID, update_id=uid).update.state.value) \
            not in ("COMPLETED", "FAILED", "CANCELED"):
        time.sleep(20)
    assert st == "COMPLETED", f"Silver update {uid} ended {st}"
    return uid


def schema_change_failures(update):
    """Tables whose main flow failed with a streaming schema-change error in this update."""
    hits = set()
    for e in w.pipelines.list_pipeline_events(pipeline_id=SILVER_ID, max_results=200, filter="level='ERROR'"):
        if not (e.origin and e.origin.update_id == update.update_id and e.origin.flow_name):
            continue
        text = (e.message or "") + json.dumps(e.as_dict().get("error", {}))
        if "INCOMPATIBLE_SCHEMA_CHANGE" in text or "METADATA_EVOLUTION" in text:
            t = e.origin.flow_name.split(".")[-1]
            if t in TABLES:
                hits.add(t)
    return hits


# COMMAND ----------

if MODE == "reset":
    updates = w.pipelines.list_updates(pipeline_id=SILVER_ID, max_results=100).updates or []
    last_ok_ms = max((u.creation_time for u in updates if u.state and u.state.value == "COMPLETED"), default=0)
    failed = [u for u in updates if u.creation_time > last_ok_ms and u.state and u.state.value == "FAILED"]
    by_failure = set().union(*[schema_change_failures(u) for u in failed]) if failed else set()

    refreshed = {}
    for t in TABLES:
        hist = spark.sql(f"DESCRIBE HISTORY {BRONZE}.{t}").where(f"timestamp > timestamp_millis({last_ok_ms})")
        marker = (hist.where("operation = 'DLT REFRESH'").orderBy("version", ascending=False).first()
                  if "history" in DETECT else None)
        if marker:
            refreshed[t] = (marker.timestamp, "DLT REFRESH")
        elif "failure" in DETECT and t in by_failure:
            first = hist.where(f"operation IN {REFRESH_OPS}").orderBy("version").first()
            refreshed[t] = (first.timestamp if first else hist.orderBy("version").first().timestamp,
                            "silver schema-change failure")

    print("refreshed:", {t: str(v) for t, v in refreshed.items()})
    if refreshed:
        for t, (refresh_time, how) in refreshed.items():
            pin = spark.sql(f"DESCRIBE HISTORY {BRONZE}.{t} LIMIT 1").first().version
            spark.sql(f"""INSERT INTO {OPS}.refresh_pins VALUES
                          ('{t}', timestamp'{refresh_time}', {pin}, '{how}', current_timestamp(), false)""")
        run_silver({"reset_checkpoint_selection": [f"{SILVER}.{t}" for t in refreshed]})
    else:
        run_silver()
    dbutils.notebook.exit(json.dumps({t: v[1] for t, v in refreshed.items()}))

# COMMAND ----------

if MODE == "tombstone":
    written = {}
    for p in spark.sql(f"SELECT * FROM {OPS}.refresh_pins WHERE NOT tombstoned").collect():
        t, cfg = p.bronze_table, TABLES[p.bronze_table]
        keys = cfg["keys"]
        on = " AND ".join(f"s.{k} <=> b.{k}" for k in keys)
        nulls = ", ".join(f"CAST(NULL AS {f['type']}) AS {f['name']}" for f in
                          [{"name": c, "type": spark.table(f"{OPS}.{t}_tombstones").schema[c].dataType.simpleString()}
                           for c in cfg["columns"]])
        n = spark.sql(f"""
            INSERT INTO {OPS}.{t}_tombstones
            SELECT {', '.join('s.' + k for k in keys)}, {nulls}, timestamp'{p.refresh_time}' AS _seq, true AS _is_delete
            FROM {SILVER}.{t} s
            LEFT ANTI JOIN (SELECT {', '.join(keys)} FROM {BRONZE}.{t} VERSION AS OF {p.pin_version}
                            WHERE __END_AT IS NULL) b ON {on}
            WHERE s.__END_AT IS NULL AND s.__START_AT <= timestamp'{p.refresh_time}'
        """).first().num_inserted_rows
        written[t] = written.get(t, 0) + n
        spark.sql(f"""UPDATE {OPS}.refresh_pins SET tombstoned = true
                      WHERE bronze_table = '{t}' AND pin_version = {p.pin_version}""")
    print("tombstones written:", written)
    if any(written.values()):
        run_silver()
    dbutils.notebook.exit(json.dumps(written))
