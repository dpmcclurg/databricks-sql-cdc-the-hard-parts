# Databricks notebook source
# Turn an existing SCD Type 2 Silver table (the legacy history) into a replayable event table for
# the <table>_seed ONCE flow. Used to migrate a table onto silver_history, or to rebuild Silver.
#
# Bronze holds complete history only from its last full refresh R onward, and the main flow
# replays that part. The seed therefore carries what Bronze no longer has:
#   - every legacy version that opened before R (insert at its start);
#   - every legacy version that ended before R with no successor (delete at its end);
#   - every key open in the legacy table at R that has no row in Bronze at all, meaning it was
#     deleted before R and the legacy table missed it (delete at R).
# The output table must not already exist: each rebuild writes a new, dated table.
import json

for w_ in ("catalog", "bronze_schema", "tables", "table", "legacy_table", "out_table"):
    dbutils.widgets.text(w_, "")
CAT = dbutils.widgets.get("catalog")
BRONZE = f"{CAT}.{dbutils.widgets.get('bronze_schema')}"
T = dbutils.widgets.get("table")
CFG = json.loads(dbutils.widgets.get("tables"))[T]
LEGACY = dbutils.widgets.get("legacy_table")
OUT = dbutils.widgets.get("out_table")
keys, cols = CFG["keys"], CFG["columns"]
k, kc = ", ".join(keys), ", ".join(keys + cols)

hist = spark.sql(f"DESCRIBE HISTORY {BRONZE}.{T}")
marker = hist.where("operation = 'DLT REFRESH'").orderBy("version", ascending=False).first() \
    or hist.orderBy("version").first()
R = marker.timestamp
on = " AND ".join(f"l.{x} <=> b.{x}" for x in keys)

spark.sql(f"""
CREATE TABLE {OUT} AS
WITH legacy AS (
  SELECT {kc}, __START_AT AS s, __END_AT AS e,
         lead(__START_AT) OVER (PARTITION BY {k} ORDER BY __START_AT) AS next_s
  FROM {LEGACY})
SELECT {kc}, s AS _seq, false AS _is_delete FROM legacy WHERE s < timestamp'{R}'
UNION ALL
SELECT {kc}, e, true FROM legacy
WHERE e IS NOT NULL AND e < timestamp'{R}' AND (next_s IS NULL OR next_s <> e)
UNION ALL
SELECT {', '.join('l.' + c for c in keys + cols)}, timestamp'{R}', true
FROM legacy l LEFT ANTI JOIN (SELECT DISTINCT {k} FROM {BRONZE}.{T}) b ON {on}
WHERE l.s < timestamp'{R}' AND (l.e IS NULL OR l.e >= timestamp'{R}')
""")
n = spark.table(OUT).count()
spark.sql(f"ALTER TABLE {OUT} SET TBLPROPERTIES ('pattern.expected_rows' = '{n}', 'pattern.bronze_refresh' = '{R}')")
print(f"{OUT}: {n} events (Bronze history starts {R})")
dbutils.notebook.exit(str(n))
