"""Test matrix for the refresh-safe SCD Type 2 Silver pattern, against emulated connector Bronze.

The source tables (<catalog>.source.src_*) are never refreshed, so they are ground truth: after
each scenario, Silver's open versions must match the source exactly, with no duplicate open
versions, no overlapping intervals, and no spurious (unchanged) versions.

Usage (from the bundle root, after `databricks bundle deploy -t emulation`):
  uv run --with databricks-sdk python tests/run_matrix.py <profile> <catalog> [s0,s1,...|all]
Writes tests/report.md.
"""
import json
import subprocess
import sys
import time
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

PROFILE, CAT = sys.argv[1], sys.argv[2]
STEPS = sys.argv[3] if len(sys.argv) > 3 else "all"
w = WorkspaceClient(profile=PROFILE)
TABLES = {"detail": 10000, "gldetail": 5000}
RESULTS = []
STATE = "tests/.state.json"


def state():
    try:
        return json.load(open(STATE))
    except FileNotFoundError:
        return {}


def save(**kw):
    s = state()
    s.update(kw)
    json.dump(s, open(STATE, "w"), indent=2, default=str)


def ids():
    out = subprocess.run(["databricks", "bundle", "summary", "-t", "emulation", "--var", f"catalog={CAT}", "-o", "json", "--profile", PROFILE],
                         capture_output=True, text=True, check=True).stdout
    r = json.loads(out)["resources"]
    return {k: v["id"] for k, v in {**r.get("pipelines", {}), **r.get("jobs", {})}.items()}


ID = ids()
WH = next(x.id for x in w.warehouses.list())


def sql(q):
    r = w.statement_execution.execute_statement(statement=q, warehouse_id=WH, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{q}\n -> {r.status.error}")
    return r.result.data_array if r.result and r.result.data_array else []


def one(q):
    rows = sql(q)
    return rows[0][0] if rows else None


def check(scenario, name, ok, detail=""):
    RESULTS.append((scenario, name, bool(ok), str(detail)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {scenario} {name} {detail}")


def wait_update(pid, uid):
    while True:
        st = w.pipelines.get_update(pipeline_id=pid, update_id=uid).update.state.value
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return st
        time.sleep(10)


def run_pipeline(key, reset=None, **kw):
    pid = ID[key]
    uid = (w.api_client.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={"reset_checkpoint_selection": reset})["update_id"]
           if reset else w.pipelines.start_update(pipeline_id=pid, **kw).update_id)
    st = wait_update(pid, uid)
    print(f"  pipeline {key}{' reset' if reset else ''}{' ' + str(kw) if kw else ''}: {st}")
    return st


def run_job(key, only=None, params=None):
    run = w.jobs.run_now(job_id=int(ID[key]), only=only, notebook_params=params).result(timeout=timedelta(minutes=60))
    outs = {}
    for t in run.tasks:
        o = w.jobs.get_run_output(t.run_id)
        outs[t.task_key] = o.notebook_output.result if o.notebook_output else o.error
    print(f"  job {key}{' ' + str(only) if only else ''}: {run.state.result_state.value} {outs}")
    return run.state.result_state.value, outs


def src_version(t):
    return int(one(f"SELECT max(version) FROM (DESCRIBE HISTORY {CAT}.source.src_{t})"))


def mark_snapshot(*tables):
    for t in tables:
        sql(f"MERGE INTO {CAT}.source.emu_ctl c USING (SELECT '{t}' tbl, {src_version(t)} v) n ON c.tbl = n.tbl "
            "WHEN MATCHED THEN UPDATE SET snapshot_through_version = n.v "
            "WHEN NOT MATCHED THEN INSERT (tbl, snapshot_through_version) VALUES (n.tbl, n.v)")


def versions(table, key, schema="silver"):
    return int(one(f"SELECT count(*) FROM {CAT}.{schema}.{table} WHERE id = {key}"))


def is_open(table, key, schema="silver"):
    return int(one(f"SELECT count(*) FROM {CAT}.{schema}.{table} WHERE id = {key} AND __END_AT IS NULL")) == 1


def total(table, schema="silver"):
    return int(one(f"SELECT count(*) FROM {CAT}.{schema}.{table}"))


def invariants(scenario, table, schema="silver"):
    S, X = f"{CAT}.{schema}.{table}", f"{CAT}.source.src_{table}"
    q = {
        "I1 one open version per key":
            f"SELECT count(*) FROM (SELECT id FROM {S} WHERE __END_AT IS NULL GROUP BY id HAVING count(*) > 1)",
        "I2 open keys = live source keys":
            f"SELECT count(*) FROM (SELECT id FROM {S} WHERE __END_AT IS NULL) s FULL OUTER JOIN {X} x "
            "ON s.id = x.id WHERE s.id IS NULL OR x.id IS NULL",
        "I3 open values = source values":
            f"SELECT count(*) FROM {S} s JOIN {X} x ON s.id = x.id "
            "WHERE s.__END_AT IS NULL AND NOT (s.val <=> x.val AND s.amt <=> x.amt)",
        "I4 intervals ordered, no overlap":
            f"SELECT count(*) FROM (SELECT __START_AT s, __END_AT e, lead(__START_AT) OVER (PARTITION BY id "
            f"ORDER BY __START_AT) n FROM {S}) WHERE e < s OR (n IS NOT NULL AND (e IS NULL OR n < e))",
        "I5 no spurious versions":
            f"SELECT count(*) FROM (SELECT val, amt, __END_AT e, lead(__START_AT) OVER w ns, lead(val) OVER w nv, "
            f"lead(amt) OVER w na FROM {S} WINDOW w AS (PARTITION BY id ORDER BY __START_AT)) "
            "WHERE ns = e AND val <=> nv AND amt <=> na",
    }
    for name, query in q.items():
        n = int(one(query))
        check(scenario, f"{schema}.{table} {name}", n == 0, f"violations={n}")


def bronze_and_silver_normal():
    run_pipeline("emulated_bronze")
    run_job("silver_recovery")
    run_pipeline("legacy_silver")


def reset_legacy(*tables):
    run_pipeline("legacy_silver", reset=[f"{CAT}.legacy.{t}" for t in tables])


# ---------------------------------------------------------------- scenarios

def s0():
    print("\n### S0 initial build (snapshot rows carry NULL __cdc_timestamp_value)")
    for schema in ("source", "bronze", "silver", "legacy"):
        sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{schema}")
    for t, n in TABLES.items():
        sql(f"CREATE OR REPLACE TABLE {CAT}.source.src_{t} (id INT, val STRING, amt DECIMAL(12,2)) "
            "TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        sql(f"INSERT INTO {CAT}.source.src_{t} SELECT CAST(id AS INT), 'v0', CAST(id * 1.5 AS DECIMAL(12,2)) "
            f"FROM range(1, {n + 1})")
    sql(f"CREATE OR REPLACE TABLE {CAT}.source.emu_ctl (tbl STRING, snapshot_through_version BIGINT)")
    mark_snapshot(*TABLES)
    run_pipeline("emulated_bronze")
    run_job("silver_setup")
    bronze_and_silver_normal()
    for t, n in TABLES.items():
        nulls = int(one(f"SELECT count(*) FROM {CAT}.bronze.{t} WHERE __START_AT.__cdc_timestamp_value IS NULL"))
        check("S0", f"bronze.{t} snapshot rows have NULL cdc timestamp", nulls == n, f"{nulls}/{n}")
        check("S0", f"silver.{t} builds one version per key", total(t) == n, f"{total(t)}/{n}")
        check("S0", f"silver.{t} START never NULL",
              int(one(f"SELECT count(*) FROM {CAT}.silver.{t} WHERE __START_AT IS NULL")) == 0)
        invariants("S0", t)


def s1():
    print("\n### S1 steady state: update, 5-update burst, delete, delete+reinsert, bulk update")
    X = f"{CAT}.source.src_detail"
    sql(f"UPDATE {X} SET val = 'b' WHERE id = 1")
    for i in range(1, 6):
        sql(f"UPDATE {X} SET val = 'burst{i}' WHERE id = 2")
    sql(f"DELETE FROM {X} WHERE id = 3")
    sql(f"DELETE FROM {X} WHERE id = 4")
    sql(f"INSERT INTO {X} VALUES (4, 'back', 6.00)")
    sql(f"UPDATE {X} SET amt = amt + 1 WHERE id BETWEEN 1000 AND 1099")
    bronze_and_silver_normal()
    check("S1", "update: key 1 has 2 versions", versions("detail", 1) == 2, versions("detail", 1))
    check("S1", "burst: key 2 has 6 versions (every change kept)", versions("detail", 2) == 6, versions("detail", 2))
    check("S1", "delete: key 3 closed", not is_open("detail", 3))
    gap = one(f"SELECT count(*) FROM (SELECT __END_AT e, lead(__START_AT) OVER (ORDER BY __START_AT) n "
              f"FROM {CAT}.silver.detail WHERE id = 4) WHERE n > e")
    check("S1", "delete + reinsert: key 4 has 2 versions with a gap", versions("detail", 4) == 2 and int(gap) == 1,
          f"versions={versions('detail', 4)} gaps={gap}")
    bulk = one(f"SELECT count(*) FROM {CAT}.silver.detail WHERE id BETWEEN 1000 AND 1099")
    check("S1", "bulk update: 100 keys x 2 versions", int(bulk) == 200, bulk)
    check("S1", "INFO legacy silver_scd2 on SCD2 Bronze leaves deleted key 3 open", is_open("detail", 3, "legacy"),
          "expected True (documents the problem the pattern fixes)")
    invariants("S1", "detail")
    invariants("S1", "gldetail")


def s2():
    print("\n### S2 planned full refresh of all Bronze tables, Silver fully caught up")
    before = {t: total(t) for t in TABLES}
    mark_snapshot(*TABLES)
    run_pipeline("emulated_bronze", full_refresh=True)
    st = run_pipeline("silver_history")
    save(s2_normal_run=st)
    check("S2", "INFO normal Silver run right after the refresh", True, f"state={st} (column-mapped detail expected to fail)")
    status, out = run_job("silver_recovery", params={"detect": "failure"})
    detected = json.loads(out.get("reset") or "{}")
    check("S2", "fallback detection (Silver schema-change failure) finds detail", "detail" in detected or st == "COMPLETED",
          detected)
    status, out = run_job("silver_recovery")
    check("S2", "recovery job completes", status == "SUCCESS", out)
    reset_legacy(*TABLES)
    run_pipeline("legacy_silver")
    for t in TABLES:
        check("S2", f"silver.{t} version count unchanged by refresh", total(t) == before[t], f"{before[t]} -> {total(t)}")
        n = int(one(f"SELECT count(*) FROM {CAT}.silver_ops.{t}_tombstones"))
        check("S2", f"no tombstones for {t} (nothing deleted in window)", n == 0, n)
        invariants("S2", t)


def s3():
    print("\n### S3 table-level refresh (detail only) with changes in the refresh window + changes after the pin")
    D, G = f"{CAT}.source.src_detail", f"{CAT}.source.src_gldetail"
    sql(f"UPDATE {D} SET val = 'window' WHERE id = 5")
    sql(f"DELETE FROM {D} WHERE id IN (6, 7)")
    sql(f"DELETE FROM {G} WHERE id = 1")
    run_pipeline("emulated_bronze")                          # Bronze ingests; Silver does not run
    mark_snapshot("detail")
    run_pipeline("emulated_bronze", full_refresh_selection=["detail"])
    # a snapshot-origin row deleted after the refresh but before the reset's re-read
    sql(f"DELETE FROM {D} WHERE id = 9")
    run_pipeline("emulated_bronze")
    status, out = run_job("silver_recovery", only=["reset"])
    detected = json.loads(out.get("reset") or "{}")
    check("S3", "reset detects only the refreshed table", set(detected) == {"detail"}, detected)
    refresh_time = one(f"SELECT max(refresh_time) FROM {CAT}.silver_ops.refresh_pins WHERE bronze_table = 'detail'")
    check("S3", "window delete still open after reset (expected gap)", is_open("detail", 6) and is_open("detail", 7))
    check("S3", "snapshot row deleted before the reset stays closed after the re-read", not is_open("detail", 9))
    # changes after the pin, ingested and applied before the tombstone step
    sql(f"INSERT INTO {D} VALUES (20001, 'new', 1.00)")
    sql(f"DELETE FROM {D} WHERE id = 8")
    run_pipeline("emulated_bronze")
    run_pipeline("silver_history")
    status, out = run_job("silver_recovery", only=["tombstone"])
    tomb = sorted(int(r[0]) for r in sql(f"SELECT id FROM {CAT}.silver_ops.detail_tombstones"))
    check("S3", "tombstones = exactly the window deletes {6, 7}", tomb == [6, 7], tomb)
    check("S3", "window deletes closed", not is_open("detail", 6) and not is_open("detail", 7))
    end8 = one(f"SELECT max(__END_AT) FROM {CAT}.silver.detail WHERE id = 8")
    check("S3", "delete after the pin closed at its real time, not the refresh time",
          end8 is not None and end8 > refresh_time, f"end={end8} refresh={refresh_time}")
    check("S3", "insert after the pin is open, not tombstoned", is_open("detail", 20001))
    check("S3", "untouched gldetail closed its delete via the normal path", not is_open("gldetail", 1))
    check("S3", "no tombstones for untouched gldetail",
          int(one(f"SELECT count(*) FROM {CAT}.silver_ops.gldetail_tombstones")) == 0)
    reset_legacy("detail")
    run_pipeline("legacy_silver")
    save(s3_refresh_time=refresh_time)
    invariants("S3", "detail")
    invariants("S3", "gldetail")


def s4():
    print("\n### S4 source adds a column; a change touches only the new column")
    D = f"{CAT}.source.src_detail"
    sql(f"ALTER TABLE {D} ADD COLUMNS (note STRING)")
    sql(f"UPDATE {D} SET note = 'n' WHERE id = 10")
    st = run_pipeline("emulated_bronze")
    if st != "COMPLETED":
        st = run_pipeline("emulated_bronze")
    check("S4", "Bronze absorbs the added column", st == "COMPLETED", st)
    status, out = run_job("silver_recovery")
    check("S4", "Silver run completes", status == "SUCCESS", out)
    bv = int(one(f"SELECT count(*) FROM {CAT}.bronze.detail WHERE id = 10"))
    check("S4", "Bronze opened a version for the note-only change", bv == 2, bv)
    check("S4", "Silver (explicit columns) opened no spurious version", versions("detail", 10) == 1, versions("detail", 10))
    run_pipeline("legacy_silver")
    invariants("S4", "detail")


def s5():
    print("\n### S5 recovery job re-run is idempotent")
    before = (total("detail"), total("gldetail"), int(one(f"SELECT count(*) FROM {CAT}.silver_ops.refresh_pins")),
              int(one(f"SELECT count(*) FROM {CAT}.silver_ops.detail_tombstones")))
    run_job("silver_recovery")
    run_job("silver_recovery")
    after = (total("detail"), total("gldetail"), int(one(f"SELECT count(*) FROM {CAT}.silver_ops.refresh_pins")),
             int(one(f"SELECT count(*) FROM {CAT}.silver_ops.detail_tombstones")))
    check("S5", "versions, pins, tombstones unchanged after two re-runs", before == after, f"{before} -> {after}")


def s6():
    print("\n### S6 migrate the legacy Silver onto the pattern through the ONCE seed")
    sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.silver_ops_migrated")
    sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.silver_migrated")
    sql(f"DROP TABLE IF EXISTS {CAT}.silver_ops_migrated.detail_legacy_events_test")
    sql(f"CREATE TABLE IF NOT EXISTS {CAT}.silver_ops_migrated.detail_tombstones AS "
        f"SELECT * FROM {CAT}.silver_ops.detail_tombstones WHERE false")
    w.jobs.run_now(job_id=int(ID["backfill_events"]), job_parameters={
        "table": "detail", "legacy_table": f"{CAT}.legacy.detail",
        "out_table": f"{CAT}.silver_ops_migrated.detail_legacy_events_test"}).result(timeout=timedelta(minutes=60))
    n = one(f"SELECT count(*) FROM {CAT}.silver_ops_migrated.detail_legacy_events_test")
    print(f"  legacy events: {n}")
    st = run_pipeline("silver_history_migrated", full_refresh=True)
    check("S6", "migrated pipeline builds", st == "COMPLETED", st)
    R = one(f"SELECT max(timestamp) FROM (DESCRIBE HISTORY {CAT}.bronze.detail) WHERE operation = 'DLT REFRESH'")
    lost = one(f"""SELECT count(*) FROM {CAT}.legacy.detail l LEFT ANTI JOIN {CAT}.silver_migrated.detail m
                   ON l.id = m.id AND l.__START_AT = m.__START_AT AND l.val <=> m.val AND l.amt <=> m.amt
                   WHERE l.__START_AT < timestamp'{R}'""")
    check("S6", "legacy history from before Bronze's refresh carried over", int(lost) == 0, f"missing={lost}")
    check("S6", "deletes the legacy table missed are closed (keys 3, 6, 7)",
          not any(is_open("detail", k, "silver_migrated") for k in (3, 6, 7)))
    check("S6", "snapshot rows deleted after the refresh stay closed (keys 8, 9)",
          not any(is_open("detail", k, "silver_migrated") for k in (8, 9)))
    invariants("S6", "detail", "silver_migrated")
    save(s6_total=total("detail", "silver_migrated"))


def s7():
    print("\n### S7 a full refresh of the migrated table re-runs the seed and reproduces it exactly")
    before = total("detail", "silver_migrated")
    digest = lambda: one(f"SELECT sum(hash(id, val, amt, __START_AT, __END_AT)) FROM {CAT}.silver_migrated.detail")
    d0 = digest()
    run_pipeline("silver_history_migrated", full_refresh=True)
    check("S7", "identical after re-seed", total("detail", "silver_migrated") == before and digest() == d0,
          f"rows {before} -> {total('detail', 'silver_migrated')}")
    invariants("S7", "detail", "silver_migrated")


ALL = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"]
for step in (ALL if STEPS == "all" else STEPS.split(",")):
    try:
        globals()[step]()
    except Exception as e:
        check(step.upper(), "scenario raised", False, repr(e)[:300])

with open("tests/report.md", "a") as f:
    for r in RESULTS:
        f.write(f"| {r[0]} | {r[1]} | {'PASS' if r[2] else 'FAIL'} | {r[3]} |\n")
print(f"\n{sum(r[2] for r in RESULTS)}/{len(RESULTS)} checks passed")
