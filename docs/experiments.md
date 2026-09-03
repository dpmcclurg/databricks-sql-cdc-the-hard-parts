# Experiment log

Record each run as hypothesis, observed result, and implication for the SCD Approach A vs B decision. Keep raw evidence (query output, DESCRIBE HISTORY snippets) alongside.

Instance: workspace `adb-7405612315185862`, catalog `sqlcdc_testbed_dbx01_7405612315185862`, Central US, run 2026-08-28.

## E1: NULL timestamp reproduction (date-less table)

- Hypothesis: `account` SCD2 target shows LSN in `__cdc_internal_value`, NULL in `__cdc_timestamp_value`; `property` (has modified_at) differs.
- Observed (Variant A, SCD2 target): the NULL is a SNAPSHOT artifact, not a date-less artifact. account (date-less) snapshot version: sentinel LSN + `__cdc_timestamp_value` NULL. Incremental change version (fwd_test): real LSN + `__cdc_timestamp_value`=2026-08-28T13:53:19.733Z (REAL). property (date-bearing) incremental: real ts 13:56:34.040Z. So real CDC changes carry a real commit timestamp even for date-less tables; only snapshot rows are NULL. `sequence_by` sets ordering, not the stored ts value (it is the CDC commit time, not modified_at).
- Implication: corrects a common assumption. The all-NULL was likely observed on snapshot rows. Approach A gives readable dates on ongoing changes without forward capture; snapshot rows still need the load time stamped if required.

## E2: readable date via CDF

- Hypothesis: bronze CDF exposes `_commit_timestamp`; silver sequenced by it gets real `__START_AT`/`__END_AT`.
- Observed: `table_changes('bronze.account')` -> `_change_type=insert, _commit_version=2, _commit_timestamp=2026-08-28T13:01:52Z`. silver.account hmy=1: `closed [13:02:04 -> 13:10:42]`, `demo_changed [13:10:42 -> open]`. Real timestamps on a date-less source.
- Implication: CONFIRMED. This is the readable-date fix, native, no forward capture, no date mapping.

## E3: full-refresh behavior

- Hypothesis: database-connector full refresh is atomic (destination retained during snapshot, single apply).
- Observed (bronze DESCRIBE HISTORY): full refresh = `DLT REFRESH -> DLT SETUP -> SET TBLPROPERTIES -> MERGE -> DELETE -> MERGE` (v9-14). Bronze ends correct (100 rows, updates intact); destination retained. BUT the SET TBLPROPERTIES / DLT SETUP is a read-incompatible schema change for the downstream stream: silver failed with `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG`. Safe failure (silver data intact, 103/100 preserved), but a hard stop.
- Implication: Bronze refresh is correct for bronze but NOT transparent to a downstream CDF stream. Answers approach-doc Open Item #1: the stream does not continue across a bronze full refresh; it halts.

## E4 (headline): AUTO CDC idempotency / history across full refresh

- Hypothesis: unchanged re-delivered rows do NOT create new versions; no manual reconcile needed.
- Observed (Variant B): NOT REACHABLE. The silver stream halts (E3) before applying. Recovery via silver full refresh collapses history: silver.account 103/100 -> 100/100, all `__START_AT` reset to 13:16:43; prior versions gone. `schemaTrackingLocation` workaround rejected in LDP (`DELTA_STREAMING_SCHEMA_LOCATION_NOT_UNDER_CHECKPOINT`).
- Observed (Variant A, SCD2 target): a full refresh WIPES the connector target's history. account 101/100 -> 100/100 (hmy=1: 2 -> 1 version); tenant 108/100 -> 100/100 (hmy=42: 9 -> 1). Re-baselines to one snapshot version per key. Idempotency is moot at the connector target: there is no re-delivery into existing history, the history is discarded and re-snapshotted.
- Implication: NEITHER connector target survives a full refresh with history. Durable refresh-surviving history must live in an owned downstream table outside the refresh scope, in both approaches. Corrects the approach doc's "both survive a full refresh."

## E5: fidelity vs cadence (burst)

- Hypothesis: SCD1 bronze collapses an intra-interval burst to net (~1 version); SCD2 bronze keeps all.
- Observed (Variant B): 8 rapid updates to tenant 42 -> source CDC 16 rows -> bronze SCD1 net credit=705 -> silver 1 new version, not 8.
- Observed (Variant A): same 8-update burst -> bronze_scd2.tenant hmy=42 = 9 versions (snapshot + 8). Full fidelity, cadence-independent.
- Implication: CONFIRMED both. A = every logged change; B = net-change-per-interval. This is the fidelity fork.

## E6: is the manual reconcile needed (Variant A)? -- ANSWERED: NO, redundant

- Design: reconfigured Variant A as SCD2 connector target (bronze_scd2) -> durable downstream SCD2 (silver_a) via create_auto_cdc_flow off bronze_scd2's CDF, keyed on business key, sequenced by COALESCE(__START_AT.__cdc_timestamp_value, _commit_timestamp), capturing version-open (insert) events for full fidelity. src/silver_a_scd2.py + resources/silver_a.pipeline.yml (schema silver_a).
- Observed: silver_a built full-fidelity SCD2 with real change timestamps (key 20 new version at 19:18:18.093 = connector __cdc_timestamp_value). Full-refreshed bronze_scd2, then recovered silver_a with reset_checkpoint_selection ALONE (no hash reconcile): history preserved (101/100 unchanged), key 20 both versions + timestamps intact, ZERO spurious versions despite re-snapshot re-delivering current state at a higher sequence. Continuity: normal run after picked up key 21 (102/100). Since path (a) is already clean, adding a content-hash reconcile has nothing to fix.
- Implication: the content-hash reconcile is REDUNDANT with reset_checkpoint_selection + AUTO CDC idempotency. The reconciliation-complexity gap between Approach A and B disappears; both use the same in-LDP recovery, no custom code. A's remaining edge is full steady-state fidelity; the A-vs-B decision is now purely the fidelity question. (Plus A1: __cdc_timestamp_value populated on real changes, so forward date-capture also not needed.)

## E7: LDP-native full-refresh survival via AUTO CDC FROM SNAPSHOT

- Hypothesis: a silver built with create_auto_cdc_from_snapshot_flow (reading bronze current state) survives a bronze full refresh in LDP and preserves history, unlike the readChangeFeed-stream build.
- Observed: PROVEN. silver_snap (snapshot flow) built SCD2 history (accounts 1,2,3 two versions each; 103/100). After a bronze full refresh, silver_snap ran to COMPLETED (no DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE), stayed 103/100, accounts 1,2,3 kept identical versions/timestamps, unchanged key hmy=50 kept its original __START_AT (no reset). Contrast: the CDF-stream silver halted and lost history on recovery (E3/E4-B).
- Implication: Variant B stays fully in LDP and survives the full-refresh reality by using AUTO CDC FROM SNAPSHOT instead of readChangeFeed. Same trigger-interval fidelity, no structured-streaming step-back, no hand-written reconcile. Continuous mode is not reachable via standard config (continuous:true coerced to false).

## E8: Variant C - standalone Structured Streaming off the change feed + schemaTrackingLocation

- Hypothesis: a change-feed streaming silver OUTSIDE LDP (readChangeFeed + schemaTrackingLocation + foreachBatch SCD2 MERGE) survives a bronze full refresh with history intact, keeping the streaming/push architecture LDP couldn't.
- Observed: PARTIAL. Steady-state works (real _commit_timestamp validity dates; built 103/100). Across a bronze full refresh, history is NOT lost (target held 103/100, no spurious versions, timestamps intact) - better than the LDP CDF build. BUT the stream does not auto-resume: full refresh raises DELTA_STREAMING_METADATA_EVOLUTION (restart), then the restart fails DELTA_SCHEMA_NOT_SET because the refresh's transient DLT-SETUP state logged an empty schema (root) into the schema log. Recovery required resetting the stream checkpoint+schema log (target history retained, idempotency prevents spurious versions - verified success after reset). Bronze healthy throughout.
- Implication: Variant C keeps the change-feed architecture and avoids history loss, but the connector full refresh still jams it and needs a stream reset per refresh, plus it forgoes LDP and uses custom SCD2 MERGE code. AUTO CDC FROM SNAPSHOT (E7) remains the only build that survives a full refresh cleanly AND auto-continues in LDP with no custom code.
- CONTINUOUS-MODE SUPPORT (added 2026-08-28): the notebook now takes a `mode` widget - `triggered` (availableNow, drains+stops) or `continuous` (processingTime always-on streams, spark.streams.awaitAnyTermination). The bundle ships both jobs: silver_stream_scd2 (triggered) and silver_stream_continuous (a Databricks continuous job, PAUSED by default). This is the ONLY variant that can run continuously today (the LDP variants A/B/snapshot are triggered-only; standard config coerces ingestion continuous:true -> false). Distinctive capability, not a full-refresh fix: the jam (E9) is unchanged in continuous mode and the auto-restart loops on it until a checkpoint reset, so pause the continuous job around a planned refresh.
- Files: databricks/notebooks/silver_stream.py + resources/silver_stream.job.yml (schema silver_stream, volume _chk; jobs silver_stream_scd2 + silver_stream_continuous).

## E9: Variant C jam - recovery/prevention approaches

- Hypothesis: (1) orchestrated pausing (never run the stream during a refresh, resume after stabilize) prevents the jam; (2) surgical schema-log-only reset (delete schemaTrackingLocation, keep checkpoint offset) recovers without replay.
- Observed: BOTH fail as stated. (1) Ran the stream only after the refresh completed + 60s stabilization; still DELTA_STREAMING_METADATA_EVOLUTION (empty root schema). A CDF stream reads every commit from its offset forward, so it re-traverses the transient DLT-SETUP empty-schema commit regardless of timing. (2) Deleting only the schema subdir but keeping offsets: run1 METADATA_EVOLUTION, run2 DELTA_SCHEMA_NOT_SET - the retained offset sits behind the empty-schema commit, so restart re-traverses+re-logs it.
- What works: FULL checkpoint reset (discard offset). Fresh readChangeFeed stream starts from latest (not v0; v0 would error on missing CDF post-refresh), skipping the refresh's transient commits; history preserved via idempotency (target holds it). ~43s, no massive replay. Runbook: pause -> refresh -> RESET checkpoint -> resume from latest.
- Implication: pausing and schema-log-only reset are insufficient; the checkpoint reset is the necessary recovery, and it is cheap. Snapshot build (E7) still needs zero intervention. Seamless streaming survival requires the connector to stop emitting the transient empty-schema commit (product fix, ES-2075799).

## E10: reset_checkpoint_selection recovers the LDP AUTO CDC CDF flow (Variant B) - CORRECTS E3/E4

- Context: E3/E4 concluded the LDP CDF stream loses history on a bronze full refresh. That used the WRONG recovery (full refresh of silver, which clears data). LDP also has a checkpoint-only reset that keeps data.
- Hypothesis: reset_checkpoint_selection ("reset streaming flow checkpoints" - clears flow checkpoint, does NOT clear the streaming table) recovers the downstream AUTO-CDC-off-CDF flow with history intact.
- Observed: PROVEN. silver_scd2 (LDP AUTO CDC off bronze CDF) built history (key 7: 2 versions; 101/100). Full-refreshed bronze (breaks a normal downstream run). Then POST /api/2.0/pipelines/{id}/updates with reset_checkpoint_selection=[catalog.silver.account,property,tenant,lease]: update COMPLETED, no INCOMPATIBLE_SCHEMA_CHANGE, history preserved (101/100, key 7 both versions + timestamps intact, no spurious). Continuity: a normal incremental run after picked up key 8 (closed->post_reset, 102/100) - flow fully operational.
- Implication: the ORIGINAL Variant B (LDP AUTO CDC off CDF) SURVIVES a bronze full refresh with history intact, recovered via reset_checkpoint_selection (NOT a full refresh). Cost: a post-refresh orchestration step (the flow fails-first on a normal run; reset clears it). This makes the elegant change-feed in-LDP build viable and dominates OSS Variant C. CLI has no flag; use the REST API.

## E11 (headline): keyless HEAP source - primary_keys:[hmy] is required for updates to apply

- Context: the scenario is SQL Server HEAP tables with no PRIMARY KEY, no identity, no unique constraint. Built a separate source table dbo.account_nopk (hmy is a plain unconstrained column) + standalone ingestion bronze_nopk_ingestion + silver_snap_nopk (AUTO CDC FROM SNAPSHOT keyed on hmy).
- Hypothesis: a no-PK table can be ingested and build SCD2, but only if the logical key is specified.
- Observed: with the connector's DEFAULT (no primary_keys), the initial snapshot ingested 100 rows fine, but source UPDATEs (hmy 5,10,15) were captured by SQL Server CDC (cdc.dbo_account_nopk_CT had change rows, realtime flow RUNNING) yet NEVER applied to bronze_nopk - even a full refresh showed old values. After DELETING and RECREATING bronze_nopk_ingestion with table_configuration.primary_keys:["hmy"], updates propagated: silver_snap_nopk.account_nopk keys 5,10,15,20,21,22 each show 2 SCD2 versions.
- Implication: no declared PRIMARY KEY is required, but you MUST specify primary_keys (the connector's all-columns bundled-key default drops updates). CDC (not CT) for keyless tables. This corrects the earlier assumption that a keyless table forces the snapshot path or blocks SCD2.

## E12: data quality for an assumed key - quarantine + circuit breaker

- Hypothesis: an assumed key from a zero-constraint source can be defended with a Tier-1 quarantine (row-level, non-fatal) and a Tier-2 circuit breaker (cross-row uniqueness, fatal).
- Observed: Tier-1 built (silver_dq_assumed_key): rule hmy IS NOT NULL AND hmy > 0; valid rows -> SCD2 silver_dq.account_nopk (101 versions/100 keys), invalid -> silver_dq.account_quarantine (1 row quarantined, not dropped). Tier-2 built (silver_assumed_key_validation + job silver_dq_assumed_key_guarded): expect_or_fail on a cross-row violations count, gated by a job so the SCD2 build is skipped on a breach. The last job run FAILED but silver_dq.hmy_key_validation reads violations=0, so it is NOT confirmed whether the failure was the expect_or_fail tripping or an unrelated internal error.
- Implication: quarantine works. Circuit-breaker trip on a real bad key is an OPEN ITEM - inject a genuine duplicate/null hmy so violations>0 and confirm the validate task fails and the build task is skipped. Uniqueness is cross-row, so it cannot be a row-level expectation; the validation-table + job-gate pattern is required (a validation table does not gate its own downstream).

## E13: Variant D - content-hash surrogate key (silver_hash_scd2.py)

- Hypothesis: a heap with no single key column but a stable identifying subset can build SCD2 on a hash surrogate.
- Observed: silver_hash_scd2 computes _bk = sha2(concat_ws('||', hmy, name), 256) in an MV (silver_hash.account_hashed) and builds SCD2 keyed on _bk (silver_hash.account_nopk, 100 versions baseline). Design rule enforced in code: hash only stable identifying columns (hmy, name), never mutable attributes (status, balance) - otherwise every attribute change makes a new key and loses entity history.
- Implication: viable for true heaps with a stable identifying column subset. A change-opens-new-version pass on the hash target was not separately confirmed (open item).

## E14: Variant E - append-only change log (silver_append_log.py)

- Hypothesis: when nothing identifies an entity, capture every change as an append-only audit row (no key, no SCD2).
- Observed: silver_append_log reads bronze_nopk CDF, keeps insert/update_postimage/delete, renames to op/op_ts, partitions by op_date -> silver_log.account_nopk_log (104 rows: 100 initial + change ops).
- Implication: full change history with no key. Feasible for small tables; infeasible for large ones (unbounded growth) - partition + retention, or prefer Variant D.

## Decision

- Both connector targets (SCD1 bronze in B, SCD2 bronze in A) lose history on a full refresh; the refresh is destructive to history in both, and durable history must be engineered downstream in both.
- Variant B: solves readable dates natively for all versions (including date-less), clean in steady-state incremental, but a bronze full refresh halts silver and loses history. Fidelity capped at the trigger interval.
- Variant A: full per-change fidelity; readable timestamps on incremental changes even for date-less tables (NULL only on snapshot rows, correcting a common assumption); but the SCD2 target itself is not refresh-durable, so an owned table + reconcile is required for refresh survival.
- Recommendation hinges on two questions: (1) does the source ever full-refresh and must history survive it? (2) is per-change fidelity required or is trigger-interval history enough?
- Open items to confirm: downstream stream halts on bronze full refresh (confirmed); SCD2 target wipes history on full refresh (confirmed); LDP schema-log restriction; DDL objects v1.2 vs expected v1.5; confirm the incremental-change timestamp behavior on the target connector version.

## E15: full rebuild run confirms the keyless open items (2026-09-03)

- Context: full teardown + rebuild (new workspace `adb-7405609436951225`, catalog `sqlcdc_testbed_dbx01_7405609436951225`), all seven variants materialized with a change injected between runs. Closes the E12/E13 open items with live evidence.
- Fidelity fork (same key, identical source): `silver.tenant` hmy=42 = 2 versions (variant 01, net-per-interval) vs `silver_a.tenant` hmy=42 = 7 versions (variant 03, full fidelity). silver.account 110/100, silver_a.account 105/100.
- E12 CONFIRMED (Tier-1 quarantine + Tier-2 circuit breaker): injected invalid keys hmy=0 and hmy=-5 into account_nopk. `silver_dq.account_quarantine` held 2 rows (preserved, not dropped); valid SCD2 `silver_dq.account_nopk` 100/100 uncontaminated. `hmy_key_validation` (expect_or_fail "violations = 0") COMPLETED on clean data and FAILED once the hmy<=0 rows appeared — the circuit trips. This is the definitive trip that was unconfirmed in E12.
- E13 CONFIRMED (content-hash change pass): a status change on hmy 5,10 opened a new SCD2 version under the same `_bk` — `silver_hash.account_nopk` max 2 versions per `_bk` (104/102). Nuance: content-hash has no DQ filter, so the two invalid-key rows hash to their own surrogates (102 `_bk`, not 100).
- Variant 07 append-log: `silver_log.account_nopk_log` 106 rows for 100 entities — grows per operation, no collapse (not SCD, by design).
- Cost (Azure Cost Management, project week): ~$99.62 total; ~$33/active-day, ~$14/day amortized; ~9.1 gateway VM-hours/active-day; Azure Databricks DBU ~87% of spend (activity-driven), Azure SQL DB ~$0.70/day standing.
