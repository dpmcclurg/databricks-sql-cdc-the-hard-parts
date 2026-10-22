# databricks-azure-sql-cdc

A disposable Azure testbed for exploring **hard SQL Server CDC scenarios** with Lakeflow Connect and Lakeflow Declarative Pipelines. It stands up an Azure SQL Database (S3, the lowest DTU tier that supports CDC) and a Databricks workspace, seeds deliberately awkward source tables, enables CDC, and runs a set of candidate SCD Type 2 architectures on the *same* data so their behavior can be compared side by side.

The scenarios are the ones that break naive CDC pipelines:

- **Keyless heap source tables** — no PRIMARY KEY, no identity, no unique constraint.
- **A nullable, uncontrolled assumed key** — the field you would key on (`hmy`) can arrive NULL or duplicated, and you do not own the source to fix it.
- **No reliable date column** — reproduce the NULL `__cdc_timestamp_value` that a date-less SCD2 target shows on snapshot rows, and recover readable validity dates anyway.
- **Connector full refresh** — what a full refresh does to accumulated history downstream, and how to survive it.
- **Fidelity vs. latency** — net-change-per-interval vs. every logged change; triggered vs. continuous/always-on.

Everything here is synthetic and disposable. Per-experiment evidence lives in [`docs/experiments.md`](docs/experiments.md); the end-to-end agent runbook is [`AGENTS.md`](AGENTS.md).

## Run the full evaluation with an AI agent

The fastest way to exercise this project is to point an AI coding agent at **[`AGENTS.md`](AGENTS.md)**. It is a self-contained, end-to-end guide: clone the repo, stand up the Azure and Databricks infrastructure, set the CLI profile, seed the source, run and track each variant (including the full-refresh labs), and write a final per-variant analysis. Because the flow provisions real cloud resources that cost money, review "Resources deployed and cost profile" below first, and let the agent stop compute when it finishes.

## The key insight (why the silver code says `keys=[...]`)

Ingestion can run against a source with **no declared primary key** — but you must still *specify* the logical key (`table_configuration.primary_keys: [hmy]`), or the connector falls back to an all-columns bundled key and silently drops UPDATEs.

SCD Type 1/2 in silver **always** needs a key, and on these sources it is **never a database primary key**. It is one of:

- an **assumed key** — a column you elect to trust as identity (`hmy`), defended with data quality;
- a **consolidated / composite key** — several columns combined (`lease = [property_hmy, tenant_hmy]`);
- a **transformed / created key** — a surrogate manufactured in-lakehouse (a content-hash over stable identifying columns);
- or, when none of those can be formed, **no key at all** — in which case SCD is impossible and only an append-only log remains.

Because the assumed key is nullable and uncontrolled, the **data-quality layer (variant 05) is essential, not optional** — it is what makes an assumed key safe to build history on.

## Architectures under test

Two independent questions decide the approach, and the variants only make sense if you keep them separate:

1. **Can you identify the entity?** — with an assumed key, a composite key, a derived key, or not at all. This decides whether SCD is even possible: **no key means no SCD**.
2. **How is change captured, and can it run continuously?** — change feed vs. periodic snapshot; triggered vs. always-on. This decides the *mechanism*, not whether SCD is achievable.

**The default, whenever the assumed key holds (with DQ) and CDC is available: keyed AUTO CDC → SCD Type 2 inside LDP.** Everything else is a fallback for a *missing* capability — no usable change feed, a key that must be computed, or no key at all.

### Core silver-build comparisons (the assumed key holds)

Variants 01–04 all key on the assumed `hmy`, on the same seeded source, each in its own schema so they coexist:

- **01 · change-feed SCD2 (primary):** SCD Type 1 bronze (connector target) plus a decoupled SCD Type 2 silver built with AUTO CDC from bronze's change feed, sequenced by `_commit_timestamp`. Readable dates on a date-less source; net-change-per-interval fidelity; full-refresh recovery via `reset_checkpoint_selection`.
- **02 · snapshot SCD2:** the same SCD Type 1 bronze, but silver built with AUTO CDC *from snapshot* off bronze's current state. Survives a connector full refresh with **zero** intervention; processing-time validity dates.
- **03 · SCD2-bronze:** SCD Type 2 connector target plus a decoupled SCD Type 2 silver off its change feed. Full per-change fidelity; the content-hash refresh reconcile turns out to be redundant. On SCD Type 2 bronze a source delete is a version close, not a delete event, so this silver misses deletes; see [`patterns/refresh-safe-scd2-silver`](patterns/refresh-safe-scd2-silver) for a delete-aware, refresh-safe version.
- **04 · streaming SCD2:** a standalone Structured Streaming silver (outside LDP). The only variant that can run **continuously / always-on** today; costs a hand-written MERGE and a per-refresh checkpoint reset.

### Keyless-heap strategies (the assumed key may not hold)

Variants 05–07 run on a separate `account_nopk` sandbox, so the "break the key" demos never corrupt the 01–04 tables. Here the question is *identity*:

- **05 · assumed-key DQ (recommended keyless path):** treat `hmy` as the key and *prove it every run* — a **Tier-1 quarantine** (null/invalid rows set aside, never dropped) plus a **Tier-2 circuit breaker** (`expect_or_fail` on a cross-row uniqueness/null check, which halts the build on a breach). This is what makes a nullable, uncontrolled key safe.
- **06 · content-hash:** SCD Type 2 on a *transformed* surrogate `_bk = sha2(...)` over the stable identifying columns. Choose it only when the key must be *derived* rather than named; hash only stable columns, never mutable attributes.
- **07 · append-log:** **not SCD, and cannot be.** A raw, ordered change ledger with no entity linkage — the fallback when no key of any kind can be formed. Correct for event-shaped data; otherwise a lossless "capture now, model once a key is found" last resort for small tables.

**Decision tree, per table:** assumed key holds (with DQ) → 05 feeding 01/02/03; key must be *computed* from a stable subset → 06; no key at all → 07 (or snapshot-current-only).

### Coexistence constraint

The core four silver builds coexist (each in its own schema), but the two bronze targets do not: they share one gateway and the same source tables, and a source table can be registered by only one ingestion pipeline per gateway. So only one bronze family is live at a time — variant 03 (SCD2 bronze) or variants 01/02/04 (SCD1 bronze) — and switching means deleting the other bronze pipeline first (see `AGENTS.md`). The keyless variants run as standalone pipelines on `account_nopk`, so they never trip this.

## Repository layout

```
infra/        Terraform: resource group, Azure SQL (S3, CDC-capable), Databricks workspace
sql/          01 create heaps, 02 enable CDC, 03 connector login, 04 connector user,
              05 DDL support objects, 06 account_nopk keyless sandbox
sim/          changegen.py — a local CLI to drive inserts/updates/deletes at the command line
databricks/   the DAB: gateway + bronze ingestion (SCD1 and SCD2) + silver builds + jobs
  src/          LDP source per variant (silver_scd2.py, silver_snapshot_scd2.py, silver_a_scd2.py,
                silver_assumed_key_dq.py, silver_assumed_key_validation.py, silver_hash_scd2.py,
                silver_append_log.py, silver_snap_nopk_scd2.py)
  notebooks/    numbered by variant; each has a capture-changes + full-refresh notebook:
                01_changefeed_scd2  02_snapshot_scd2  03_scd2_bronze  04_streaming_scd2
                05_assumed_key_dq   06_content_hash   07_append_log   08_comparison
docs/         experiments.md — the running experiment log
patterns/     refresh-safe-scd2-silver — tested SCD2 silver over SCD2 bronze: deletes, full refresh, migration
```

## Prerequisites

Install these once. Links go to each tool's official install instructions (they age better than pasted commands):

- **Homebrew** — macOS package manager — https://brew.sh
- **Terraform** (≥ 1.5) — https://developer.hashicorp.com/terraform/install
- **Azure CLI** (`az`), signed in via `az login` to a subscription you can create resources in — https://learn.microsoft.com/cli/azure/install-azure-cli
- **Databricks CLI** (≥ 0.294) — https://docs.databricks.com/aws/en/dev-tools/cli/install
- **go-sqlcmd** (`sqlcmd`) — for the SQL setup scripts — https://learn.microsoft.com/sql/tools/sqlcmd/sqlcmd-utility
- **Python 3** — https://www.python.org/downloads/
- **ODBC Driver 18 for SQL Server** — for the local simulator (`pyodbc`) — https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server

On macOS, Homebrew covers most of these in one line — `brew install terraform azure-cli databricks python sqlcmd` — plus `msodbcsql18` from the `microsoft/mssql-release` tap. `AGENTS.md` §2 has the exact commands.

## Run it (from a fresh clone)

1. **Provision:** `cd infra && cp terraform.tfvars.example terraform.tfvars` (edit `subscription_id`, a globally-unique `sql_server_name`), then `terraform init && terraform apply`. Note the outputs.
2. **Set your CLI profile** to the new workspace (required before anything Databricks-side):
   ```bash
   databricks auth login --host <workspace-url> --profile sqlcdc
   ```
   Since Terraform created the workspace under your `az login` identity, you can skip the browser entirely by using Azure CLI auth instead — put this block in `~/.databrickscfg`:
   ```ini
   [sqlcdc]
   host      = <workspace-url>
   auth_type = azure-cli
   ```
   **Rebuilding into an existing `sqlcdc` profile?** `auth login --host` refuses to re-point a profile whose host differs; just edit the `host =` line in `~/.databrickscfg` directly (the Azure CLI auth block above makes this a one-line change). Then set the new `host` **and** the auto-provisioned catalog name (`databricks catalogs list --profile sqlcdc`, the `sqlcdc*` one) in `databricks/databricks.yml`, and in the notebook `catalog` widgets if you run them interactively.
3. **Create the source:** run `sql/01`–`sql/06` against the Azure SQL DB via `sqlcmd` (see `AGENTS.md` §5b; `06` adds the keyless sandbox).
4. **Seed and drive change** — the local simulator is how you inject DML (there is no in-workspace change job):
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r sim/requirements.txt
   export SQLDB_HOST=<sql_server_fqdn> SQLDB_NAME=cdctest SQLDB_USER=sqladmin SQLDB_PASSWORD='...'
   python sim/changegen.py seed  --table account --rows 100          # repeat per table
   python sim/changegen.py churn --table account --updates 10        # scattered updates
   python sim/changegen.py burst --table tenant  --key 42 --updates 8   # intra-interval burst (fidelity probe)
   ```
   For the `account_nopk` sandbox, inject changes with direct SQL (`sqlcmd`) — including the NULL/duplicate `hmy` rows the DQ variant quarantines.
5. **Deploy:** create the UC schemas + SQL Server connection (`AGENTS.md` §5d), then `cd databricks && databricks bundle validate -t dev --profile sqlcdc && databricks bundle deploy -t dev --profile sqlcdc`. The gateway auto-starts; wait for it to reach RUNNING (`databricks pipelines get <gateway-id> --profile sqlcdc`) before running bronze.
6. **Bring up a variant and create history.** The core builds (01/02/04) all run off the SCD1 bronze:
   - Run bronze `--full-refresh` (first run), then a silver pipeline (`silver_scd2` / `silver_snapshot` / `silver_stream_job`).
   - Inject a change (the `churn`/`burst` above), then re-run bronze and silver — now silver shows real multi-version SCD2 history (and the `burst` is the fidelity probe: variant 01 nets ~1 version for the key, variant 03 keeps them all).
   - **Variant 03 (SCD2-bronze)** needs the *other* bronze: delete the SCD1 bronze first (one ingestion pipeline per source table per gateway), then full-refresh `bronze_ingestion_scd2` and run `silver_a` (`AGENTS.md` §7).
   - **Keyless variants 05/06/07** run as standalone pipelines on the `account_nopk` sandbox — `AGENTS.md` §7 has the `databricks pipelines create` commands; inject a NULL/duplicate `hmy` to watch the DQ quarantine and circuit breaker fire.
7. **Walk / review** each variant in `databricks/notebooks/<variant>/` (`a_capture_changes` = code + result, `b_full_refresh` = durability lab), or the side-by-side `08_comparison/08_results.py`. Cells show output once you run them top-to-bottom. Log outcomes in `docs/experiments.md`.

## Resources deployed and cost profile

Standing up the testbed creates an Azure resource group, an Azure SQL Database (Standard **S3**), and an Azure Databricks workspace; on the Databricks side, an ingestion **gateway** on classic compute (a single pinned VM that runs while up), serverless **ingestion and Declarative Pipelines**, and a few **jobs** (the scheduled and continuous jobs ship paused).

**Measured cost** (from Azure Cost Management, over one week of intermittent development with teardown between sessions):

- **~$33/day on days you actively run pipelines**, and **~$14/day amortized** across a calendar week that included idle/torn-down days. On days the environment was destroyed, cost was **$0**.
- **Cost is activity-driven, not idle standing cost.** Azure Databricks DBUs (serverless silver builds + the gateway) were **~87%** of spend — you pay for how much you *run*, and the gateway averaged **~9 compute-hours per active day**.
- **The standing Azure SQL DB is trivial: ~$0.70/day.** Its DTU tier cannot be paused, only deleted, so it is the one meter that bills 24/7 while deployed — but it is a rounding error next to compute.
- **To control cost:** stop the gateway between sessions (`databricks pipelines stop <gateway-id>`) and `terraform destroy` when done. An idle-but-deployed environment costs cents/day; a torn-down one costs nothing.

> **Cost disclaimer:** these figures are rough, unofficial, order-of-magnitude estimates with **no guarantee of accuracy**. Actual charges depend on region, subscription, discounts, runtime, and current list prices. Confirm against the official [Azure pricing](https://azure.microsoft.com/pricing/) and [Databricks pricing](https://www.databricks.com/product/pricing) pages before relying on any number here. You are responsible for the resources you deploy and the costs they incur.

## Cost and teardown

Pause between sessions: stop the gateway (`databricks pipelines stop <gateway-id> --profile sqlcdc`) and keep the scheduled/continuous jobs paused. Full teardown (zero cost): `cd infra && terraform destroy`. A rebuild starts from step 1; the new workspace URL and catalog name change, so update `databricks.yml` and re-auth. The notebooks resolve pipelines by name, so they work against a fresh deploy without editing IDs.
