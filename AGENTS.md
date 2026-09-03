# AGENTS.md — end-to-end evaluation guide for AI agents

This file tells an AI coding agent how to evaluate this project end to end: clone the repo, stand up the
Azure + Databricks infrastructure, set the CLI profile, seed the source, run the SCD Type 2 variants,
drive the change scenarios that expose their differences, and produce a final written analysis. It is
self-contained; you do not need any file outside this repository.

The project explores **hard SQL Server CDC scenarios**: keyless heap source tables, a nullable and
uncontrolled assumed key, a date-less source, connector full refresh, and the fidelity/latency trade.
Read `README.md` for the purpose and `docs/experiments.md` for the running evidence log. The
per-command troubleshooting matrix is in this file (see "Troubleshooting").

---

## 0. What you are evaluating and what "done" means

The project ingests SQL Server CDC into Databricks with Lakeflow Connect and builds SCD Type 2 history
several ways, on the *same* keyless-heap source data, so they can be compared. Each silver build
publishes to its own schema and coexists in one catalog.

The source tables are **keyless heaps**: no PRIMARY KEY, no identity, no unique constraint, and the
assumed-key column `hmy` is **nullable**. So the silver key is never a source PK — it is an *assumed*
key (`hmy`), a *consolidated* composite key (`lease`), a *transformed* surrogate (content-hash), or,
when none can be formed, *none at all* (append-log). Because the assumed key is nullable and
uncontrolled, the data-quality layer (variant 05) is essential, not optional.

| Variant | Bronze (connector target) | Silver schema | Bundle run key | What it demonstrates |
|---|---|---|---|---|
| **01 change feed** | `bronze` (SCD Type 1) | `silver` | `silver_scd2` | readable dates on a date-less source; net-change-per-interval fidelity; full-refresh recovery via `reset_checkpoint_selection` |
| **02 snapshot** | `bronze` (SCD Type 1) | `silver_snap` | `silver_snapshot` | zero-intervention full-refresh survival; processing-time dates |
| **03 SCD2-bronze** | `bronze_scd2` (SCD Type 2) | `silver_a` | `silver_a` | full per-change fidelity; the content-hash reconcile is redundant |
| **04 streaming** | `bronze` (SCD Type 1) | `silver_stream` | `silver_stream_job` / `silver_stream_continuous_job` | the only variant that can run continuously / always-on today |
| **05 assumed-key DQ** | `bronze_nopk` (SCD Type 1) | `silver_dq` | standalone (see §7) | quarantine + circuit breaker that make a nullable, uncontrolled assumed key safe |
| **06 content-hash** | `bronze_nopk` (SCD Type 1) | `silver_hash` | standalone (see §7) | SCD2 on a transformed sha2 surrogate over stable identifying columns |
| **07 append-log** | `bronze_nopk` (SCD Type 1) | `silver_log` | standalone (see §7) | append-only audit log (no key); the fallback when nothing identifies an entity |

Variants 05–07 run on a separate keyless sandbox table `dbo.account_nopk` (created by `sql/06`) as
STANDALONE pipelines (§7 "Keyless variants"), so the "break the assumed key" demos never corrupt the
01–04 tables and they never trip the one-pipeline-per-source-table conflict with variant 03.

**Definition of done:** each variant has been run against the same seeded data and at least one change
scenario; the full-refresh labs have been exercised; each run's evidence is appended to
`docs/experiments.md`; and a final synthesis is written to `docs/variant-analysis.md` per §8. Then
compute is stopped (§9).

**The evaluation questions the analysis must answer, per variant:**
1. Readable validity dates on a date-less source (yes/no; change-time vs processing-time)?
2. Fidelity: every logged change kept, or net-change-per-trigger-interval?
3. Does accumulated history survive a connector full refresh, and what recovery does that take?
4. Framework (in Lakeflow Declarative Pipelines vs standalone) and whether custom code is required?
5. Can it run continuously (low latency), or triggered only?
6. Key strategy: assumed / consolidated / transformed / none — and how is it defended?

### Keyless / heap source tables (no primary key, nullable key, no valid date)

Every source table here is a keyless heap, and `account_nopk` is the deliberately worst case: a nullable
`hmy` on a source SQL Server you do not administer. Handling, verified on the testbed:

- **Ingestion needs no declared PRIMARY KEY, but you MUST specify the logical key.** Enable CDC (not
  Change Tracking — CT requires a PK and does not support SCD2) and set
  `table_configuration.primary_keys: ["hmy"]` on the ingestion object. Without it the connector defaults
  to an all-columns bundled key and UPDATEs do not apply (verified: updates never reach bronze). See E11.
- **Data quality for the assumed key (variant 05) is essential.** Because `hmy` is nullable and
  uncontrolled, defend it in two tiers: Tier 1 quarantine (`silver_assumed_key_dq.py`) routes rows
  failing a row-level rule (`hmy IS NOT NULL AND hmy > 0`) to `silver_dq.account_quarantine` instead of
  dropping them; valid rows build SCD2 in `silver_dq.account_nopk`. Tier 2 circuit breaker
  (`silver_assumed_key_validation.py` + a guarded job) uses `expect_or_fail` on a cross-row
  uniqueness/non-null count so the SCD2 build is skipped if the assumed key is breached.
- **True heap with no trustworthy column:** variant 06 (content-hash surrogate over stable identifying
  columns) or, when nothing identifies an entity, variant 07 (append-only change log).
- **No valid date** is handled the same way as the other variants: validity dates come from the commit
  timestamp, not a source column.

---

## 1. Guardrails (read before running anything)

- **Never auto-select a Databricks CLI profile.** Always pass `--profile <name>` (this guide uses
  `sqlcdc`). If multiple profiles exist, ask the user which to use.
- **This spends real cloud money.** The Azure SQL DB bills continuously (a few USD/day; the DTU tier
  cannot be paused, only deleted) and the gateway is classic compute. Confirm the user wants to
  provision before `terraform apply`, and stop compute when done (§9).
- **Never paste secrets into any file.** Read the SQL admin password with
  `terraform -chdir=infra output -raw sql_admin_password`; keep the connector password in a gitignored
  temp file (`/tmp/sqlcdc_connector_pw`). `terraform.tfvars` is gitignored; do not commit it.
- **Use an alphanumeric connector password** (no `!`, `$`, `;`, `{`, `}`) — special characters parse
  inconsistently across shell / `sqlcmd -v` / JSON and silently store mismatched passwords. It must
  also not contain the login name or its parts: Azure SQL rejects a password containing the login name
  as "not complex enough" (Msg 40632).
- **Do not use `databricks bundle validate --strict`** — the required single-node gateway node-pin
  trips a warning that `--strict` treats as an error. Plain `validate` is correct.
- **The first bronze ingestion run must be `--full-refresh`** so it picks up the DDL support objects.
- Everything here is synthetic and disposable. It is safe to tear down and rebuild.

---

## 2. Prerequisites (install once)

An Azure subscription you can create resources in, and a Databricks account (the workspace is created by
Terraform, not by hand). On macOS via Homebrew:

```bash
brew install terraform
brew install azure-cli && az login          # sign in to the Azure subscription
brew install databricks                     # Databricks CLI; verify >= 0.294: databricks --version
brew install python
brew install sqlcmd                          # go-sqlcmd (pure Go, no ODBC) for the SQL scripts
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18   # ODBC driver for the Python simulator
```

Homebrew Python is a PEP 668 "externally managed" environment: a bare `pip install` is refused, so the
simulator deps go in a virtualenv (§5c).

---

## 3. Clone / locate the repo

```bash
git clone <repo-url> databricks-azure-sql-cdc   # or cd into the existing checkout
cd databricks-azure-sql-cdc
```

All commands below run from the repo root unless a `cd` says otherwise.

---

## 4. Stand up the Azure infrastructure (Terraform)

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars     # edit: subscription_id, a globally-unique sql_server_name
terraform init
terraform apply                                   # defaults to eastus; type yes
```

If `terraform apply` fails with `ProvisioningDisabled` ("Provisioning is restricted in this region"),
Azure SQL capacity is gated for the subscription in that region. Re-apply elsewhere (one variable moves
both SQL and the workspace); no destroy is needed between attempts:

```bash
terraform apply -var location=westus3            # or centralus, eastus, westeurope
```

Read the outputs (they will differ from any example values):

```bash
terraform output
terraform output -raw sql_admin_password          # do not paste into any file
cd ..
```

Capture for later steps: the Databricks workspace URL, the SQL server FQDN
(`<sql_server_name>.database.windows.net`), and the database name (`cdctest`).

---

## 5. Configure Databricks, the source, and seed data

### 5a. Authenticate the CLI and set the profile (required first)

Because Terraform created the workspace with your `az login` identity, that identity is already a
workspace admin. Two options:

**Azure CLI auth (no browser; best for agents).** Add to `~/.databrickscfg`:

```ini
[sqlcdc]
host      = https://<workspace-host>.azuredatabricks.net
auth_type = azure-cli
```

**Interactive OAuth (human at a terminal):** `databricks auth login --host https://<host> --profile sqlcdc`
(in Claude Code, prefix with `!` so the browser flow completes in-session).

> Rebuild gotcha: if a `sqlcdc` profile already exists pointing at a torn-down workspace,
> `databricks auth login --host <new>` refuses with "conflicts with --host ... Use --profile only to
> select a profile". Fastest fix (and what an agent should do): edit the `host =` line of the
> `[sqlcdc]` block in `~/.databrickscfg` directly to the new workspace URL. With `auth_type = azure-cli`
> that is the only change needed — no browser, no token.

Verify: `databricks current-user me --profile sqlcdc`. Then set the workspace `host` in
`databricks/databricks.yml` (target `dev`) and the auto-provisioned catalog name in its `catalog`
variable (find it with `databricks catalogs list --profile sqlcdc` — the `sqlcdc*` entry). The notebook
`catalog` widgets default to a catalog id too; refresh them in one pass:
`find databricks/notebooks -name '*.py' | xargs perl -i -pe 's/sqlcdc_testbed_dbx01_[0-9]+/<NEW_CATALOG>/g'`.
**A fresh workspace has a new URL and catalog name, so this profile + `databricks.yml` + widget step is
mandatory on every rebuild.**

### 5b. Create the source tables, CDC, connector auth, and DDL support objects

```bash
PW=$(terraform -chdir=infra output -raw sql_admin_password)
SRV=<sql_server_fqdn>                 # e.g. sqlcdc-testbed-srv-01.database.windows.net
CONNPW=<alphanumeric-password>        # invent one; alphanumeric only (see Guardrails)
echo -n "$CONNPW" > /tmp/sqlcdc_connector_pw   # gitignored temp

sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -i sql/01_create_tables.sql       # keyless heaps, nullable hmy
sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -i sql/02_enable_cdc.sql
# Connector login needs a server LOGIN + a USER in master plus a mapped read user in cdctest:
sqlcmd -S $SRV -d master  -U sqladmin -P "$PW" -C -v CONNECTOR_PW="$CONNPW" -i sql/03_connector_login.sql
sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -i sql/04_connector_user.sql
# Databricks-provided DDL support objects (REQUIRED; @mode='CDC'). Run on cdctest as db_owner.
sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -i sql/05_ddl_support_objects.sql
# Keyless sandbox for variants 05-07 (nullable-hmy heap + CDC). Run after 02.
sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -i sql/06_account_nopk_heap.sql
```

Preflight the connector login against BOTH databases before deploying the gateway:

```bash
sqlcmd -S $SRV -d master  -U lakeflow_connector -P "$CONNPW" -C -Q "SELECT DB_NAME();"
sqlcmd -S $SRV -d cdctest -U lakeflow_connector -P "$CONNPW" -C -Q "SELECT COUNT(*) FROM cdc.change_tables;"
```

### 5c. Seed data with the simulator (the only change-injection path)

There is no in-workspace change job; all source DML is driven from the local `sim/changegen.py` CLI.

```bash
python3 -m venv .venv && source .venv/bin/activate      # required (PEP 668)
pip install -r sim/requirements.txt
export SQLDB_HOST=$SRV SQLDB_NAME=cdctest SQLDB_USER=sqladmin SQLDB_PASSWORD="$PW"
python sim/changegen.py seed --table account  --rows 100
python sim/changegen.py seed --table property --rows 50
python sim/changegen.py seed --table tenant   --rows 100
python sim/changegen.py seed --table lease    --rows 40
```

The simulator targets the four main tables. For the `account_nopk` sandbox, inject changes with direct
SQL via `sqlcmd` (including the NULL / duplicate `hmy` rows the DQ variant quarantines — see the helper
comments at the bottom of `sql/06_account_nopk_heap.sql`).

If `pyodbc` fails to build on a very recent Python, `brew install unixodbc` and retry.

### 5d. Create the UC schemas and the SQL Server connection

Do NOT create a fresh catalog on a Default-Storage workspace (it fails with "Metastore storage root URL
does not exist"). Use the workspace's auto-provisioned managed catalog and set its name in
`databricks/databricks.yml` (`catalog` variable) and the notebook widgets.

```bash
CAT=$(databricks catalogs list --profile sqlcdc -o json | python3 -c "import sys,json;print([c['name'] for c in json.load(sys.stdin) if c['name'].startswith('sqlcdc')][0])")
for s in bronze bronze_scd2 silver silver_snap silver_a ingestion_staging bronze_nopk silver_dq silver_hash silver_log; do
  databricks schemas create $s $CAT --profile sqlcdc
done
# (silver_stream, variant 04's schema, is created by its own notebook.)

# The SQLSERVER connection takes host/port/user/password only; the database is set per-object in the
# ingestion pipeline. Use the same CONNPW as in 5b.
databricks connections create --json "{
  \"name\": \"sqlserver_conn\", \"connection_type\": \"SQLSERVER\",
  \"options\": {\"host\": \"$SRV\", \"port\": \"1433\",
               \"user\": \"lakeflow_connector\", \"password\": \"$(cat /tmp/sqlcdc_connector_pw)\"}
}" --profile sqlcdc
```

The UC connection is metastore-scoped, so it can outlive a `terraform destroy` and reappear in a
rebuilt workspace. If `connections create` fails with "already exists", refresh it in place with
`connections update sqlserver_conn --json "{...options...}"` (the connector password is new on each rebuild).

---

## 6. Deploy and bring up the change-feed baseline (variant 01)

```bash
cd databricks
databricks bundle validate -t dev --profile sqlcdc          # NOT --strict
databricks bundle deploy   -t dev --profile sqlcdc
databricks bundle summary  -t dev --profile sqlcdc          # list every resource key + id/URL

# The gateway is continuous and auto-starts on deploy. Confirm RUNNING before running bronze:
GW=<gateway-pipeline-id>
databricks pipelines get $GW --profile sqlcdc -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['latest_updates'][0]['state'])"

# First bronze run MUST be a full refresh (picks up the DDL support objects); then silver:
BR=<bronze_ingestion-pipeline-id>
databricks pipelines start-update $BR --full-refresh --profile sqlcdc     # poll to COMPLETED
databricks bundle run silver_scd2 -t dev --profile sqlcdc
```

CDC delivery is eventual; if a single trigger looks behind after a source change, run bronze once more.

---

## 7. Run and track the variant scenarios

Work each variant, then record the evidence. The core builds (01–04) coexist and can be materialized in
any order. After each run, query the result (or open the variant's notebooks) and append an entry to
`docs/experiments.md` in its hypothesis / observed / implication format.

**Notebooks:** each variant folder under `databricks/notebooks/` has a code-forward walkthrough pair —
`Na_capture_changes` (build code + change scenario + result) and `Nb_full_refresh` (durability lab). The
side-by-side comparison is `08_comparison/08_results.py`.

**Change scenarios** (venv active, `SQLDB_*` exported):

```bash
python sim/changegen.py churn  --table account --updates 10          # scattered single-row updates
python sim/changegen.py burst  --table tenant  --key 42 --updates 8  # many updates to one key in one interval
python sim/changegen.py reappear --table account --key 5             # delete then re-insert a key (tombstone test)
```

### 01 change feed (`silver`)
Already up from §6. After a `churn`, re-run `bronze_ingestion` then `silver_scd2`. Observe real
`__START_AT`/`__END_AT` on the date-less `account` (E2); a `burst` on `tenant` key 42 collapses to ~1 new
version (net-change; E5). Notebooks: `01_changefeed_scd2/`.

### 02 snapshot (`silver_snap`)
```bash
databricks bundle run silver_snapshot -t dev --profile sqlcdc
```
Builds SCD2 from bronze current state; validity dates are processing-time. Survives a full refresh with
no recovery step (E7). Notebooks: `02_snapshot_scd2/`.

### 03 SCD2-bronze (`silver_a`)
The SCD1 bronze (`bronze_ingestion`) and this SCD2 bronze (`bronze_ingestion_scd2`) claim the same four
source tables on the same gateway, and a source table can be registered by only one ingestion pipeline
per gateway (finding A0). They are mutually exclusive: before running the SCD2 bronze you must DELETE the
SCD1 bronze — stopping it to IDLE does not release the registration.

```bash
databricks pipelines delete <bronze_ingestion-pipeline-id> --profile sqlcdc   # free the source tables
databricks pipelines start-update <bronze_ingestion_scd2-pipeline-id> --full-refresh --profile sqlcdc
databricks bundle run silver_a -t dev --profile sqlcdc
```
Returning to 01/02/04 is symmetric: delete `bronze_ingestion_scd2`, redeploy, full-refresh
`bronze_ingestion`. Observe: NULL `__cdc_timestamp_value` ONLY on snapshot rows (E1); a `burst` on
`tenant` key 42 keeps ~9 versions — full fidelity vs 01's ~1 (E5). Notebooks: `03_scd2_bronze/`.

### 04 streaming (`silver_stream`), triggered or continuous
```bash
databricks bundle run silver_stream_job -t dev --profile sqlcdc       # triggered: drains and stops

# continuous (only variant that runs always-on). The job is PAUSED by definition; unpause, observe, RE-PAUSE:
CJOB=$(databricks bundle summary -t dev --profile sqlcdc | grep -A2 silver_stream_continuous_job | grep -oE '/jobs/[0-9]+' | grep -oE '[0-9]+')
databricks jobs update --job-id $CJOB --json '{"new_settings":{"continuous":{"pause_status":"UNPAUSED"}}}' --profile sqlcdc
# ... watch low-latency apply, then re-pause:
databricks jobs update --job-id $CJOB --json '{"new_settings":{"continuous":{"pause_status":"PAUSED"}}}' --profile sqlcdc
```
Continuous mode keeps a serverless meter running; treat it as a bounded demo, and pause it around a full
refresh (the refresh jams the change-feed stream). Notebooks: `04_streaming_scd2/`. This notebook is also
the executable source the `silver_stream` jobs run.

### Keyless variants (05 / 06 / 07) — standalone pipelines on `account_nopk`
These run on the `account_nopk` sandbox as STANDALONE pipelines (not in the main bundle), reusing the
shared gateway. They coexist with either bronze family (a different source table, so no A0 conflict).
`bundle deploy` already uploaded the src files to the workspace; reference them by that path. Set
`CAT=<catalog>`, `GW=<gateway-pipeline-id>`, and
`SRC=/Workspace/Users/<you>/.bundle/databricks-azure-sql-cdc/dev/files/src`, then create and run each
(these exact commands are verified working):

```bash
# Seed the sandbox first (the sim targets only the four main tables), then create bronze_nopk:
sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q "INSERT INTO dbo.account_nopk (hmy,name,status,balance) SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), CONCAT('Account ',ROW_NUMBER() OVER (ORDER BY (SELECT NULL))),'active',1000 FROM sys.all_objects;"

# bronze_nopk ingestion (SCD1, assumed key hmy) — first run --full-refresh
databricks pipelines create --profile sqlcdc --json "{\"name\":\"bronze_nopk_ingestion\",\"catalog\":\"$CAT\",\"schema\":\"bronze_nopk\",\"continuous\":false,\"ingestion_definition\":{\"ingestion_gateway_id\":\"$GW\",\"objects\":[{\"table\":{\"source_catalog\":\"cdctest\",\"source_schema\":\"dbo\",\"source_table\":\"account_nopk\",\"destination_catalog\":\"$CAT\",\"destination_schema\":\"bronze_nopk\",\"table_configuration\":{\"scd_type\":\"SCD_TYPE_1\",\"primary_keys\":[\"hmy\"]}}}]}}"

# 05 assumed-key DQ (schema silver_dq): quarantine + SCD2
databricks pipelines create --profile sqlcdc --json "{\"name\":\"silver_dq_assumed_key\",\"serverless\":true,\"catalog\":\"$CAT\",\"schema\":\"silver_dq\",\"configuration\":{\"catalog\":\"$CAT\",\"bronze_schema\":\"bronze_nopk\",\"table\":\"account_nopk\"},\"libraries\":[{\"file\":{\"path\":\"$SRC/silver_assumed_key_dq.py\"}}]}"
# 05 Tier-2 validation (same schema); run it as the first task of a job whose build task depends on it
databricks pipelines create --profile sqlcdc --json "{\"name\":\"hmy_key_validation\",\"serverless\":true,\"catalog\":\"$CAT\",\"schema\":\"silver_dq\",\"configuration\":{\"catalog\":\"$CAT\",\"bronze_schema\":\"bronze_nopk\",\"table\":\"account_nopk\"},\"libraries\":[{\"file\":{\"path\":\"$SRC/silver_assumed_key_validation.py\"}}]}"
# 06 content-hash (schema silver_hash)
databricks pipelines create --profile sqlcdc --json "{\"name\":\"silver_hash_scd2\",\"serverless\":true,\"catalog\":\"$CAT\",\"schema\":\"silver_hash\",\"configuration\":{\"catalog\":\"$CAT\",\"bronze_schema\":\"bronze_nopk\",\"table\":\"account_nopk\"},\"libraries\":[{\"file\":{\"path\":\"$SRC/silver_hash_scd2.py\"}}]}"
# 07 append-log (schema silver_log)
databricks pipelines create --profile sqlcdc --json "{\"name\":\"silver_append_log\",\"serverless\":true,\"catalog\":\"$CAT\",\"schema\":\"silver_log\",\"configuration\":{\"catalog\":\"$CAT\",\"bronze_schema\":\"bronze_nopk\",\"table\":\"account_nopk\"},\"libraries\":[{\"file\":{\"path\":\"$SRC/silver_append_log.py\"}}]}"

# Run: start-update --full-refresh bronze_nopk, then start-update each silver pipeline (poll to COMPLETED).
```

**Fire the DQ tiers (verified):** run `hmy_key_validation` on clean data (COMPLETED, circuit closed).
Then inject invalid keys and re-run — the Tier-1 quarantine sets them aside and the Tier-2 breaker
trips:
```bash
sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -C -Q "INSERT INTO dbo.account_nopk (hmy,name,status,balance) VALUES (0,'zero-key','active',0),(-5,'neg-key','active',0);"
# start-update bronze_nopk, then silver_dq_assumed_key -> silver_dq.account_quarantine now holds 2 rows;
# start-update hmy_key_validation -> FAILS (expect_or_fail on hmy<=0) = the circuit trips.
```

> Reproducibility note: these keyless pipelines are created standalone (by design, so the "break the
> key" demos are isolated from the 01–04 tables). They are not yet expressed as bundle resources; a
> future pass could add them as `resources/*.pipeline.yml` on the `account_nopk` source (safe, since it
> is a different source table from the A0 conflict set).

### The full-refresh recovery lab (headline finding)
A connector full refresh breaks the *next* run of a downstream change-feed flow. The correct recovery is
`reset_checkpoint_selection` (reset the flow checkpoint, KEEP the table data), NOT a full refresh of
silver (which clears data and loses history). This preserves history for both 01 and 03; variant 02
(snapshot) needs no recovery at all.

```bash
SILVER=<silver_scd2-pipeline-id>
databricks pipelines start-update $BR --full-refresh --profile sqlcdc          # 1. full-refresh bronze
databricks bundle run silver_scd2 -t dev --profile sqlcdc                      # 2. normal silver run FAILS on purpose
databricks api post /api/2.0/pipelines/$SILVER/updates --json "{               # 3. recover (CLI has no flag)
  \"reset_checkpoint_selection\": [
    \"$CAT.silver.account\", \"$CAT.silver.property\", \"$CAT.silver.tenant\", \"$CAT.silver.lease\"
  ]
}" --profile sqlcdc
```
Each variant's `Nb_full_refresh` notebook is a self-contained, runnable version of the relevant lab.

### Run the notebooks and emit review links

After the data is materialized, validate the notebooks and give the user clickable links to review them.
`bundle deploy` uploaded them under
`/Workspace/Users/<you>/.bundle/databricks-azure-sql-cdc/dev/files/notebooks/<variant>/`.

- **Validate each cell runs** (optional but recommended before publishing): submit each notebook as a
  serverless run and confirm SUCCESS, e.g.
  `databricks jobs submit --profile sqlcdc --json '{"run_name":"nb-check","tasks":[{"task_key":"nb","notebook_task":{"notebook_path":"<workspace-path>"},"environment_key":"default"}],"environments":[{"environment_key":"default","spec":{"client":"3"}}]}'`,
  then poll the run. The `Nb_full_refresh` notebooks *perform* full refreshes / checkpoint resets, so run
  them deliberately; the `Na_capture_changes` notebooks are safe read/query walkthroughs.
- **Emit clickable links** for the user (cells render output only when opened and run interactively):
  print one workspace URL per notebook in the form
  `https://<workspace-host>/#workspace/Users/<you>/.bundle/databricks-azure-sql-cdc/dev/files/notebooks/<variant>/<file>.py`.
  List all 15 (each variant's `a`/`b` plus `08_comparison/08_results.py`) so the reviewer can open them
  directly.

---

## 8. Output the final analysis

Write `docs/variant-analysis.md`, grounded in what you actually observed and logged in
`docs/experiments.md` — cite version counts, timestamps, and error codes; do not assert from this guide
alone. Structure:

1. **Setup summary** — instance region, seed sizes, which scenarios you ran.
2. **Per-variant section** (01 change-feed, 02 snapshot, 03 SCD2-bronze, 04 streaming, plus the keyless
   05/06/07), each answering the six evaluation questions from §0 with the evidence you captured.
3. **Cross-variant scoreboard** — this table, filled from your runs:

   | Build | Readable dates (date-less src) | Fidelity | Full-refresh survival + recovery | Framework | Custom code | Continuous? | Key strategy |
   |---|---|---|---|---|---|---|---|
   | 01 change feed (`silver`) | | | | LDP | | | assumed |
   | 02 snapshot (`silver_snap`) | | | | LDP | | | assumed |
   | 03 SCD2-bronze (`silver_a`) | | | | LDP | | | assumed |
   | 04 streaming (`silver_stream`) | | | | Structured Streaming | | | assumed |
   | 05 assumed-key DQ (`silver_dq`) | | | | LDP | | | assumed + DQ |
   | 06 content-hash (`silver_hash`) | | | | LDP | | | transformed |
   | 07 append-log (`silver_log`) | | | | LDP | | | none |

4. **Recommendation** — the assumed key plus DQ (variant 05) is the non-negotiable foundation on an
   uncontrolled, nullable source. Given that, the choice among the LDP builds reduces to **fidelity**
   (per-change = 03, net-per-interval = 01/02) and **latency** (continuous/always-on is only 04 today).
   Refresh-durability and custom code are not differentiators among the LDP builds. State which variant
   you would recommend and under what requirement.

Keep the writing plain: no emojis, no em dashes, no AI-authored attribution.

---

## 9. Stop compute / teardown (do not skip)

**Pause between sessions (keeps the instance, stops the meters):** the scheduled/continuous jobs are
`PAUSED` by definition, so you only stop the gateway. Re-pause the continuous job if you unpaused it.

```bash
databricks pipelines stop <gateway-pipeline-id> --profile sqlcdc
```

**Full teardown (zero cost):**

```bash
cd infra && terraform destroy      # type yes; the RG-delete flag clears nested resources
# optional, if you keep the workspace: databricks bundle destroy -t dev --profile sqlcdc
```

A rebuild starts from §4; the new workspace URL and catalog name change, so update `databricks.yml`
(`host` + `catalog`) and re-auth (§5a). The notebook `catalog` widget defaults also change.

---

## 10. Troubleshooting

| Symptom | Fix |
|---|---|
| `terraform apply`: `ProvisioningDisabled` | Re-apply with `-var location=<other>` (westus3, centralus, eastus, westeurope). No destroy needed. |
| `terraform destroy`: "Resource Group still contains Resources" | `main.tf` sets `prevent_deletion_if_contains_resources = false`; re-run. Fallback: `az group delete -n rg-sqlcdc-testbed --yes`, then `terraform state rm` the workspace + RG resources. |
| `brew install msodbcsql18`: "No available formula" | Add the Microsoft tap first (§2). |
| `error: externally-managed-environment` (PEP 668) | Use a venv (§5c); do not `--break-system-packages`. |
| `pip install` fails building `pyodbc` | `brew install unixodbc` and retry. |
| `sqlcmd`: certificate / SSL error | Add `-C` to trust the server cert (already in the commands). |
| `databricks catalogs create`: "Metastore storage root URL does not exist" | Use the auto-provisioned managed catalog (§5d); do not create a new catalog. |
| `connections create`: "does not support the following option(s): database" | Drop `database`; it is set per-object in the pipeline. |
| Gateway `SOURCE_AUTHENTICATION_FAILURE` on `master` | The login has no USER in master; `sql/03` creates both. Preflight with a `-d master` connect. |
| Gateway `SOURCE_AUTHENTICATION_FAILURE`, password looks right | SQL-side and UC-connection passwords differ due to a special char; use an alphanumeric connector password and recreate the connection. |
| Ingestion pipeline fails analysis: `INGESTION_GATEWAY_DDL_OBJECTS_MISSING` | Run `sql/05_ddl_support_objects.sql` on cdctest, then `start-update --full-refresh` bronze. |
| Keyless bronze ingests the snapshot but never applies UPDATEs | The ingestion object is missing `table_configuration.primary_keys: ["hmy"]`; the all-columns bundled-key default drops updates. Recreate the pipeline with the key specified (E11). |
| `bundle validate --strict`: single-node warning fails | Drop `--strict`; the gateway node-pin is single-node by design. |
| First bronze run fails analysis even after `05` ran | Trigger with `start-update --full-refresh` (a plain run reuses the failed analysis graph). |
| Gateway stuck `WAITING_FOR_RESOURCES` 15+ min | The gateway node type is in a VM family with 0 vCPU quota; pin `clusters.node_type_id` in `gateway.pipeline.yml` to a family that has quota (e.g. `Standard_D4ds_v4`) and redeploy. |
| Downstream silver run fails `DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE` after a bronze full refresh | Expected. Recover with `reset_checkpoint_selection` (§7 lab); do NOT full-refresh silver. |
| `bronze_ingestion_scd2` fails analysis with `Failed to resolve flow: '...bronze_scd2..._snapshot_flow / _cdc_flow'` | The SCD1 `bronze_ingestion` pipeline still owns the source tables (one pipeline per source table per gateway, A0). DELETE it; stopping to IDLE is not enough. Then re-run the SCD2 bronze full-refresh. Symmetric when switching back. |
| `sql/03`: `Msg 40632 ... not complex enough` on the connector login | The password contains the login name. Choose an alphanumeric password that does not include the login substrings. |
| `connections create`: "Connection 'sqlserver_conn' already exists" | The UC connection is metastore-scoped and survived a prior teardown. Use `connections update sqlserver_conn` to refresh host/user/password (§5d). |

---

## Repository map

```
infra/        Terraform: resource group, Azure SQL (S3, CDC-capable), Databricks workspace
sql/          01 create heaps (nullable hmy), 02 enable CDC, 03 connector login+master user,
              04 cdctest user, 05 DDL support objects, 06 account_nopk keyless sandbox
sim/          changegen.py: seed / churn / burst / reappear against the Azure SQL source (CLI)
databricks/   the DAB — gateway + bronze ingestion (SCD1 and SCD2) + silver builds + jobs + notebooks
  src/          LDP source: silver_scd2.py (01), silver_snapshot_scd2.py (02), silver_a_scd2.py (03);
                keyless: silver_assumed_key_dq.py (05 quarantine), silver_assumed_key_validation.py
                (05 circuit breaker), silver_hash_scd2.py (06), silver_append_log.py (07),
                silver_snap_nopk_scd2.py (bare no-PK snapshot proof, E11)
  notebooks/    numbered by variant; each folder has a capture-changes + full-refresh notebook:
    01_changefeed_scd2/   02_snapshot_scd2/   03_scd2_bronze/   04_streaming_scd2/
    05_assumed_key_dq/    06_content_hash/    07_append_log/    08_comparison/ (side-by-side)
  resources/    pipeline and job YAML (bundle resource keys live here); variant 04 runs the
                04_streaming_scd2/04a_capture_changes.py notebook as its job source
docs/         experiments.md (running log) and variant-analysis.md (your final deliverable)
```
