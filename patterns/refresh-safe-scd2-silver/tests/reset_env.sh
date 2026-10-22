#!/bin/bash
# TEST ONLY: destroy the emulation deployment and its schemas, then redeploy clean.
set -e
P=${1:?usage: reset_env.sh <profile> <catalog>}; C=${2:?usage: reset_env.sh <profile> <catalog>}
cd "$(dirname "$0")/.."
databricks bundle destroy -t emulation --var catalog="$C" --auto-approve --profile "$P" | tail -1
for s in source bronze silver silver_ops legacy silver_migrated silver_ops_migrated; do
  uv run -q --with databricks-sdk python - "$P" "$C.$s" <<'PY'
import sys
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile=sys.argv[1]); wh = next(x.id for x in w.warehouses.list())
w.statement_execution.execute_statement(statement=f"DROP SCHEMA IF EXISTS {sys.argv[2]} CASCADE", warehouse_id=wh, wait_timeout="50s")
PY
done
rm -f tests/.state.json
databricks bundle deploy -t emulation --var catalog="$C" --profile "$P" | tail -1
printf '| Scenario | Check | Result | Detail |\n|---|---|---|---|\n' > tests/report.md
