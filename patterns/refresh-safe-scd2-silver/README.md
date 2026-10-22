# Refresh-safe SCD Type 2 Silver over Lakeflow Connect SCD Type 2 Bronze

A tested pattern for keeping durable SCD Type 2 history in a decoupled Silver layer when the
Bronze tables are Lakeflow Connect CDC targets with history tracking (SCD Type 2) turned on.

It handles the three things that make this hard:

1. **Deletes.** In an SCD Type 2 Bronze table, a source delete closes the current version
   (`__END_AT` is set) instead of producing a delete event. A Silver flow that waits for
   `_change_type = 'delete'`, or that reads only new versions, never closes deleted keys.
2. **Bronze full refresh.** A refresh re-materializes Bronze and resets its history. The next
   normal Silver run either fails with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE` or, if it does
   not fail, reads the refresh's commits. A Silver full refresh would discard accumulated history.
3. **Deletes during the refresh window.** Rows deleted after Silver's last run and before the
   refresh vanish from Bronze along with their close events, so no stream can see them.

## How it works

```
Lakeflow Connect ──► Bronze (SCD Type 2) ──change feed──► silver_history (SCD Type 2)
                                                             ▲          ▲
                                     ops.<table>_tombstones ─┘          └─ ops.<table>_legacy_events
                                     (written by silver_recovery)          (ONCE seed, rebuilds only)
```

**`src/silver_history.py`**: one Silver table per Bronze table, written by up to three AUTO CDC
flows:

| Flow | Source | Purpose |
|---|---|---|
| `<table>` | Bronze change feed | Every inserted Bronze row is a version open at its `__START_AT` time. A closed Bronze row is a delete **only if no Bronze row for the same key starts at the same LSN**: an update closes the old version and opens its successor at that LSN, so a close with no successor is a source delete. This check is a stream-static anti-join against Bronze |
| `<table>_tombstones` | `ops.<table>_tombstones` | Deletes the refresh erased, written by the recovery job |
| `<table>_seed` | a legacy-events table | Optional `ONCE` flow that replays existing history when migrating or rebuilding |

Silver selects an explicit column list, so a Bronze version change that touches only other
columns opens no Silver version. Validity columns are plain timestamps (the connector's change
time; snapshot rows, which carry a NULL change time, are dated at the Bronze snapshot: its last refresh, or its creation).
Silver tables set `pipelines.reset.allowed = false`; the checkpoint reset still works.

**`silver_recovery` job** (`notebooks/recover.py`): run it in place of triggering Silver directly.

1. **reset**: find Bronze tables refreshed since Silver's last successful update (a `DLT REFRESH`
   entry in the table history; a changed Unity Catalog table ID, meaning the table was deleted and
   rebuilt; or Silver's last update failing with a streaming schema-change or different-table
   error on that table), record a pin (refresh time and current Bronze version), and start Silver
   with `reset_checkpoint_selection` on those tables' main flows. With nothing refreshed, run a
   normal update.
2. **tombstone**: for each unprocessed pin, tombstone every Silver key that is open, started
   before the refresh, and has no live row in the pinned Bronze version. Apply with a normal update.

**`backfill_events` job** (`notebooks/backfill_events.py`): turn an existing SCD Type 2 Silver
table into a dated event table for the seed flow. Bronze holds complete history only from its last
refresh R onward, so the seed carries legacy versions that opened before R, terminal closes before
R, and deletes at R for keys the legacy table still had open but Bronze no longer has at all.

## Deploy

```bash
# add a target with the schemas and tables JSON to databricks.yml (see the comment there), then
databricks bundle deploy -t <target> --var catalog=<catalog>
databricks bundle run silver_setup -t <target>     # tombstone and pin tables
databricks bundle run silver_recovery -t <target>  # schedule this instead of the pipeline
```

`tables` is JSON: `{"<table>": {"keys": ["id"], "columns": ["col1", "col2"]}}`. Add
`"seed": "<catalog.schema.events_table>"` to a table only for a migration or rebuild.

## Test evidence

`tests/run_matrix.py` runs the scenarios below against the `emulation` target, where
`tests/emulation/emulated_bronze.py` stands in for the connector: Delta source tables with change
data feed feed AUTO CDC SCD Type 2 Bronze tables whose `__START_AT`/`__END_AT` structs have the
connector's field names, snapshot rows carry a NULL change time, and one table uses column mapping.
The source tables are never refreshed, so they are ground truth. After every scenario each Silver
table must satisfy five invariants: one open version per key; open keys equal live source keys;
open values equal source values; intervals ordered without overlap; no spurious versions.

| Scenario | What it exercises | Checks passed |
|---|---|---|
| S0 | Initial build over 15,000 snapshot rows with NULL change times | 16/16 |
| S1 | Steady state: update, 5-update burst (every version kept), delete, delete then re-insert, 100-row bulk update | 16/16 |
| S2 | Planned full refresh of all Bronze tables. The normal Silver run fails on the column-mapped table; recovery detects it from the failure and resets; no versions added or lost | 17/17 |
| S3 | Table-level refresh of one table with an update and two deletes in the window, a delete between refresh and reset, and an insert and delete after the pin. Only the refreshed table is reset; tombstones are exactly the two window deletes; later changes keep their real times | 19/19 |
| S4 | Source adds a column; a change touching only that column opens no Silver version | 9/9 |
| S5 | Recovery job re-run twice: nothing changes | 1/1 |
| S6 | Migrate a legacy Silver (the original change-feed flow, which misses deletes) through the ONCE seed: pre-refresh history carried over, missed deletes closed | 9/9 |
| S7 | Full refresh of the migrated table re-runs the seed and reproduces it exactly | 6/6 |
| S8 | Bronze table deleted and rebuilt (the recovery when a source table is dropped and recreated), with an update and a delete while ingestion was down. The table-ID check alone detects it; the delete is tombstoned, the update opens one version, and a later delete closes at its real time | 18/18 |

**93 of 93 checks passed.** Every check, with its measured value, is in `tests/report.md`; the run log is `tests/evidence.log`. Run it yourself with `tests/reset_env.sh <profile> <catalog>` then `uv run --with databricks-sdk python tests/run_matrix.py <profile> <catalog>`.

### Assumptions about the real connector

The emulation reproduces Bronze's output (an AUTO CDC SCD Type 2 table with a change feed). These
connector behaviors are assumed, with the evidence for each:

| Assumption | Evidence | Check on a real pipeline |
|---|---|---|
| A source delete closes the version (`__END_AT` set) and the row stays | Lakeflow Connect SCD Type 2 docs and FAQ; Delta change-feed semantics | `table_changes` on Bronze after a delete shows `update_postimage` with `__END_AT` set and no new version |
| An update's successor starts at the closed version's `__END_AT.__cdc_internal_value` | AUTO CDC SCD Type 2 semantics | Same query after an update |
| A full refresh leaves `DLT REFRESH` in the table history | Observed on a real gateway pipeline (repository testbed, experiment E3) | `DESCRIBE HISTORY` after a refresh the connector starts on its own |
| The next normal Silver run fails with a streaming schema-change error | Observed on the repository testbed (E3) and in the field | The recovery job handles either outcome |

### Known limits

- A key deleted in the refresh window closes at the refresh time; the true delete time is gone.
- Several changes to one key inside the refresh window arrive as the final value only.
- With continuous Bronze, a delete landing between the pin and the reset's first read of Bronze is
  missed by the tombstone step. Running Bronze triggered, in the same job before Silver, removes it.
- The successor check reads Bronze for each micro-batch that contains closes; on very large tables,
  watch its cost and keep Bronze clustered or Z-ordered on the key.
- A source table dropped and recreated under an existing destination is not a supported connector
  operation; the recovery is to delete the Bronze table and let the connector rebuild it, which this
  pattern then handles like a refresh. Keep the outage short: the whole outage is the refresh window.
- Silver should track the same columns Bronze keeps history on (the connector default is all
  columns), or a subset of them.
