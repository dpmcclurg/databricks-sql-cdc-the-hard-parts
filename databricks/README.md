# databricks (DAB)

The Databricks side of the testbed: a UC SQL Server connection, the continuous gateway, a triggered ingestion pipeline landing bronze, a silver LDP pipeline that builds SCD Type 2 from bronze's change feed, and a 5-minute Jobs schedule.

## One-time setup before deploy

1. **Auth.** `databricks auth login --host <databricks_workspace_url> --profile sqlcdc`, then set that `host` in `databricks.yml` (target `dev`).
2. **Catalog and schemas.** On a Default-Storage workspace, do NOT create a fresh catalog via CLI (it fails with "Metastore storage root URL does not exist" because the metastore has no storage root). Use the workspace's auto-provisioned managed catalog instead, and create the schemas under it. Find the catalog name and set it as the `catalog` variable default in `databricks.yml`:
   ```bash
   databricks catalogs list --profile sqlcdc          # e.g. sqlcdc_testbed_dbx01
   CAT=<that-catalog>
   for s in bronze bronze_scd2 silver ingestion_staging; do
     databricks schemas create $s $CAT --profile sqlcdc
   done
   ```
   (To use a dedicated named catalog instead, create it in the UI, which uses Default Storage, or provide a `MANAGED LOCATION`.)
3. **UC connection to Azure SQL** (basic auth; not OAuth, so CLI is fine). The SQLSERVER connection does NOT accept a `database` option; the database is set per-object in the ingestion pipeline (`source_catalog`). Use a server-level login (see `sql/03_connector_login.sql`), not a contained user:
   ```bash
   databricks connections create --json '{
     "name": "sqlserver_conn",
     "connection_type": "SQLSERVER",
     "options": {
       "host": "<sql_server_fqdn>",
       "port": "1433",
       "user": "lakeflow_connector",
       "password": "<connector-password>"
     }
   }' --profile sqlcdc
   ```
   Confirm the exact `connection_type` and `options` keys with `databricks connections create --help` on the installed CLI version.
4. **Firewall.** Make sure the Azure SQL server allows the gateway egress (the open firewall rule from Terraform, `allow_all_ips = true`, covers this).

## Deploy and run

```bash
databricks bundle validate --strict -t dev --profile sqlcdc
databricks bundle deploy -t dev --profile sqlcdc

# gateway must be running (continuous) before the ingestion pipeline lands data
databricks bundle run sqlserver_gateway -t dev --profile sqlcdc --no-wait
databricks bundle run bronze_ingestion  -t dev --profile sqlcdc
databricks bundle run silver_scd2        -t dev --profile sqlcdc
```

The `testbed_5min` job triggers bronze then silver every 5 minutes. Pause it between sessions to control cost.

## Switching to Variant A (SCD Type 2 bronze)

1. Deploy the `bronze_ingestion_scd2` pipeline (it writes to the `bronze_scd2` schema).
2. Redeploy with `--var bronze_schema=bronze_scd2` so silver reads the SCD2 bronze:
   ```bash
   databricks bundle deploy -t dev --var bronze_schema=bronze_scd2 --profile sqlcdc
   ```
3. Point the schedule at `bronze_ingestion_scd2` (edit `resources/schedule.job.yml`).

## Teardown

```bash
databricks bundle destroy -t dev --profile sqlcdc
```
