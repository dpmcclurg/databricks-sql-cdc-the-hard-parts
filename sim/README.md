# sim: changegen

A local CLI that drives inserts, updates, and deletes against the testbed tables so the CDC feed has something to carry. Runnable over and over.

## Setup

Needs the Microsoft ODBC Driver 18 for SQL Server installed locally (`brew install msodbcsql18` on macOS, or the platform equivalent), then:

```bash
pip install -r requirements.txt
export SQLDB_HOST=sqlcdc-testbed-srv.database.windows.net
export SQLDB_NAME=cdctest
export SQLDB_USER=sqladmin
export SQLDB_PASSWORD='...'          # do not commit
```

## Commands

- `seed --table <t> --rows N` : initial load with app-assigned keys (no IDENTITY).
- `churn --table <t> --updates N [--inserts N] [--deletes N]` : random DML across the table.
- `burst --table <t> --key <hmy> --updates N` : N rapid updates to ONE key with no delay.
- `reappear --table <t> --key <hmy>` : delete then re-insert the same key with identical content.

Use `--seed <int>` for reproducible content and `--dry-run` to print the DML without executing.

## What the two diagnostic commands prove

- **burst** exercises the fidelity question. Fire it inside a single 5-minute ingestion interval. Expected: an SCD Type 1 bronze (Variant B) collapses the N updates to the net value, so silver records one version; an SCD Type 2 bronze (Variant A) keeps every intermediate version. Compare the silver version counts.
- **reappear** exercises unchanged-record handling. A delete then an identical re-insert should not manufacture a spurious "changed" version in silver once AUTO CDC's content comparison is in play.

Composite keys (lease) take comma-separated values, e.g. `--key 5,1005`.
