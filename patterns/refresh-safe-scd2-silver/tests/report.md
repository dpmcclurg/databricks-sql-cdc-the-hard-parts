| Scenario | Check | Result | Detail |
|---|---|---|---|
| S0 | bronze.detail snapshot rows have NULL cdc timestamp | PASS | 10000/10000 |
| S0 | silver.detail builds one version per key | PASS | 10000/10000 |
| S0 | silver.detail START never NULL | PASS |  |
| S0 | silver.detail I1 one open version per key | PASS | violations=0 |
| S0 | silver.detail I2 open keys = live source keys | PASS | violations=0 |
| S0 | silver.detail I3 open values = source values | PASS | violations=0 |
| S0 | silver.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S0 | silver.detail I5 no spurious versions | PASS | violations=0 |
| S0 | bronze.gldetail snapshot rows have NULL cdc timestamp | PASS | 5000/5000 |
| S0 | silver.gldetail builds one version per key | PASS | 5000/5000 |
| S0 | silver.gldetail START never NULL | PASS |  |
| S0 | silver.gldetail I1 one open version per key | PASS | violations=0 |
| S0 | silver.gldetail I2 open keys = live source keys | PASS | violations=0 |
| S0 | silver.gldetail I3 open values = source values | PASS | violations=0 |
| S0 | silver.gldetail I4 intervals ordered, no overlap | PASS | violations=0 |
| S0 | silver.gldetail I5 no spurious versions | PASS | violations=0 |
| S1 | update: key 1 has 2 versions | PASS | 2 |
| S1 | burst: key 2 has 6 versions (every change kept) | PASS | 6 |
| S1 | delete: key 3 closed | PASS |  |
| S1 | delete + reinsert: key 4 has 2 versions with a gap | PASS | versions=2 gaps=1 |
| S1 | bulk update: 100 keys x 2 versions | PASS | 200 |
| S1 | INFO legacy silver_scd2 on SCD2 Bronze leaves deleted key 3 open | PASS | expected True (documents the problem the pattern fixes) |
| S1 | silver.detail I1 one open version per key | PASS | violations=0 |
| S1 | silver.detail I2 open keys = live source keys | PASS | violations=0 |
| S1 | silver.detail I3 open values = source values | PASS | violations=0 |
| S1 | silver.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S1 | silver.detail I5 no spurious versions | PASS | violations=0 |
| S1 | silver.gldetail I1 one open version per key | PASS | violations=0 |
| S1 | silver.gldetail I2 open keys = live source keys | PASS | violations=0 |
| S1 | silver.gldetail I3 open values = source values | PASS | violations=0 |
| S1 | silver.gldetail I4 intervals ordered, no overlap | PASS | violations=0 |
| S1 | silver.gldetail I5 no spurious versions | PASS | violations=0 |
| S2 | INFO normal Silver run right after the refresh | PASS | state=FAILED (column-mapped detail expected to fail) |
| S2 | fallback detection (Silver schema-change failure) finds detail | PASS | {'detail': 'silver schema-change failure'} |
| S2 | recovery job completes | PASS | {'reset': '{}', 'tombstone': '{}'} |
| S2 | silver.detail version count unchanged by refresh | PASS | 10107 -> 10107 |
| S2 | no tombstones for detail (nothing deleted in window) | PASS | 0 |
| S2 | silver.detail I1 one open version per key | PASS | violations=0 |
| S2 | silver.detail I2 open keys = live source keys | PASS | violations=0 |
| S2 | silver.detail I3 open values = source values | PASS | violations=0 |
| S2 | silver.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S2 | silver.detail I5 no spurious versions | PASS | violations=0 |
| S2 | silver.gldetail version count unchanged by refresh | PASS | 5000 -> 5000 |
| S2 | no tombstones for gldetail (nothing deleted in window) | PASS | 0 |
| S2 | silver.gldetail I1 one open version per key | PASS | violations=0 |
| S2 | silver.gldetail I2 open keys = live source keys | PASS | violations=0 |
| S2 | silver.gldetail I3 open values = source values | PASS | violations=0 |
| S2 | silver.gldetail I4 intervals ordered, no overlap | PASS | violations=0 |
| S2 | silver.gldetail I5 no spurious versions | PASS | violations=0 |
| S3 | reset detects only the refreshed table | PASS | {'detail': 'DLT REFRESH'} |
| S3 | window delete still open after reset (expected gap) | PASS |  |
| S3 | snapshot row deleted before the reset stays closed after the re-read | PASS |  |
| S3 | tombstones = exactly the window deletes {6, 7} | PASS | [6, 7] |
| S3 | window deletes closed | PASS |  |
| S3 | delete after the pin closed at its real time, not the refresh time | PASS | end=2026-10-21T22:03:56.000Z refresh=2026-10-21T22:01:36.000Z |
| S3 | insert after the pin is open, not tombstoned | PASS |  |
| S3 | untouched gldetail closed its delete via the normal path | PASS |  |
| S3 | no tombstones for untouched gldetail | PASS |  |
| S3 | silver.detail I1 one open version per key | PASS | violations=0 |
| S3 | silver.detail I2 open keys = live source keys | PASS | violations=0 |
| S3 | silver.detail I3 open values = source values | PASS | violations=0 |
| S3 | silver.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S3 | silver.detail I5 no spurious versions | PASS | violations=0 |
| S3 | silver.gldetail I1 one open version per key | PASS | violations=0 |
| S3 | silver.gldetail I2 open keys = live source keys | PASS | violations=0 |
| S3 | silver.gldetail I3 open values = source values | PASS | violations=0 |
| S3 | silver.gldetail I4 intervals ordered, no overlap | PASS | violations=0 |
| S3 | silver.gldetail I5 no spurious versions | PASS | violations=0 |
| S4 | Bronze absorbs the added column | PASS | COMPLETED |
| S4 | Silver run completes | PASS | {'reset': '{}', 'tombstone': '{}'} |
| S4 | Bronze opened a version for the note-only change | PASS | 2 |
| S4 | Silver (explicit columns) opened no spurious version | PASS | 1 |
| S4 | silver.detail I1 one open version per key | PASS | violations=0 |
| S4 | silver.detail I2 open keys = live source keys | PASS | violations=0 |
| S4 | silver.detail I3 open values = source values | PASS | violations=0 |
| S4 | silver.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S4 | silver.detail I5 no spurious versions | PASS | violations=0 |
| S5 | versions, pins, tombstones unchanged after two re-runs | PASS | (10109, 5000, 2, 2) -> (10109, 5000, 2, 2) |
| S6 | migrated pipeline builds | PASS | COMPLETED |
| S6 | legacy history from before Bronze's refresh carried over | PASS | missing=0 |
| S6 | deletes the legacy table missed are closed (keys 3, 6, 7) | PASS |  |
| S6 | snapshot rows deleted after the refresh stay closed (keys 8, 9) | PASS |  |
| S6 | silver_migrated.detail I1 one open version per key | PASS | violations=0 |
| S6 | silver_migrated.detail I2 open keys = live source keys | PASS | violations=0 |
| S6 | silver_migrated.detail I3 open values = source values | PASS | violations=0 |
| S6 | silver_migrated.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S6 | silver_migrated.detail I5 no spurious versions | PASS | violations=0 |
| S7 | identical after re-seed | PASS | rows 10105 -> 10105 |
| S7 | silver_migrated.detail I1 one open version per key | PASS | violations=0 |
| S7 | silver_migrated.detail I2 open keys = live source keys | PASS | violations=0 |
| S7 | silver_migrated.detail I3 open values = source values | PASS | violations=0 |
| S7 | silver_migrated.detail I4 intervals ordered, no overlap | PASS | violations=0 |
| S7 | silver_migrated.detail I5 no spurious versions | PASS | violations=0 |
