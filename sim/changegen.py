#!/usr/bin/env python3
"""changegen: drive inserts/updates/deletes against the CDC testbed tables.

Runnable repeatedly. Business keys are app-assigned (no IDENTITY), mirroring a real vendor source.
Connection comes from env vars; nothing is hardcoded:

    SQLDB_HOST      e.g. sqlcdc-testbed-srv.database.windows.net
    SQLDB_NAME      e.g. cdctest
    SQLDB_USER      e.g. sqladmin  (or the connector user)
    SQLDB_PASSWORD  the password
    SQLDB_DRIVER    optional, default "ODBC Driver 18 for SQL Server"

Examples:
    python changegen.py seed    --table account --rows 100
    python changegen.py churn   --table account --updates 20 --inserts 5 --deletes 2
    python changegen.py burst   --table tenant  --key 42 --updates 8
    python changegen.py reappear --table account --key 7
"""
import argparse
import os
import random
import sys
from datetime import datetime, timezone

import pyodbc

# table -> (key columns, has modified_at, business-column generator)
TABLES = {
    "account":  (["hmy"], False),
    "property": (["hmy"], True),
    "tenant":   (["hmy"], False),
    "lease":    (["property_hmy", "tenant_hmy"], True),
}


def connect():
    driver = os.environ.get("SQLDB_DRIVER", "ODBC Driver 18 for SQL Server")
    host = os.environ["SQLDB_HOST"]
    db = os.environ["SQLDB_NAME"]
    user = os.environ["SQLDB_USER"]
    pwd = os.environ["SQLDB_PASSWORD"]
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={host},1433;DATABASE={db};"
        f"UID={user};PWD={pwd};Encrypt=yes;TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def now():
    return datetime.now(timezone.utc)


def gen_row(table, key_vals):
    """Return a dict of column -> value for INSERT/UPDATE of the given table."""
    if table == "account":
        return {"name": f"Account {key_vals[0]}",
                "status": random.choice(["active", "hold", "closed"]),
                "balance": round(random.uniform(0, 1_000_000), 2)}
    if table == "property":
        return {"name": f"Property {key_vals[0]}",
                "city": random.choice(["Austin", "Denver", "Miami", "Reno"]),
                "sq_ft": random.randint(500, 500_000),
                "modified_at": now()}
    if table == "tenant":
        return {"name": f"Tenant {key_vals[0]}",
                "email": f"tenant{key_vals[0]}@example.com",
                "credit": random.randint(300, 850)}
    if table == "lease":
        return {"rent": round(random.uniform(500, 50_000), 2),
                "term_months": random.choice([6, 12, 24, 36]),
                "modified_at": now()}
    raise ValueError(table)


def existing_keys(cur, table):
    keys, _ = TABLES[table]
    cur.execute(f"SELECT {', '.join(keys)} FROM dbo.{table}")
    return [tuple(r) for r in cur.fetchall()]


def max_hmy(cur, table, col="hmy"):
    cur.execute(f"SELECT ISNULL(MAX({col}), 0) FROM dbo.{table}")
    return int(cur.fetchone()[0])


def insert_row(cur, table, key_vals, dry):
    keys, _ = TABLES[table]
    row = gen_row(table, key_vals)
    cols = keys + list(row.keys())
    vals = list(key_vals) + list(row.values())
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO dbo.{table} ({', '.join(cols)}) VALUES ({placeholders})"
    if dry:
        print("DRY", sql, vals)
        return
    cur.execute(sql, vals)


def update_row(cur, table, key_vals, dry):
    keys, _ = TABLES[table]
    row = gen_row(table, key_vals)          # regenerate business columns (a real change)
    set_clause = ", ".join(f"{c} = ?" for c in row)
    where = " AND ".join(f"{k} = ?" for k in keys)
    sql = f"UPDATE dbo.{table} SET {set_clause} WHERE {where}"
    vals = list(row.values()) + list(key_vals)
    if dry:
        print("DRY", sql, vals)
        return
    cur.execute(sql, vals)


def delete_row(cur, table, key_vals, dry):
    keys, _ = TABLES[table]
    where = " AND ".join(f"{k} = ?" for k in keys)
    sql = f"DELETE FROM dbo.{table} WHERE {where}"
    if dry:
        print("DRY", sql, list(key_vals))
        return
    cur.execute(sql, list(key_vals))


def next_keys(cur, table, n):
    """Generate n new app-assigned key tuples that do not collide with existing rows."""
    keys, _ = TABLES[table]
    if len(keys) == 1:
        start = max_hmy(cur, table, keys[0]) + 1
        return [(start + i,) for i in range(n)]
    # composite (lease): pair fresh property/tenant ids
    base = max(max_hmy(cur, table, keys[0]), max_hmy(cur, table, keys[1])) + 1
    return [(base + i, base + 1000 + i) for i in range(n)]


def cmd_seed(cur, args):
    new = next_keys(cur, args.table, args.rows)
    for k in new:
        insert_row(cur, args.table, k, args.dry_run)
    print(f"seed: inserted {len(new)} rows into dbo.{args.table}")


def cmd_churn(cur, args):
    ks = existing_keys(cur, args.table)
    if not ks and (args.updates or args.deletes):
        sys.exit(f"no rows in dbo.{args.table}; run seed first")
    for _ in range(args.updates):
        update_row(cur, args.table, random.choice(ks), args.dry_run)
    for _ in range(args.deletes):
        if not ks:
            break
        k = random.choice(ks)
        ks.remove(k)
        delete_row(cur, args.table, k, args.dry_run)
    for k in next_keys(cur, args.table, args.inserts):
        insert_row(cur, args.table, k, args.dry_run)
    print(f"churn: {args.updates} updates, {args.deletes} deletes, "
          f"{args.inserts} inserts on dbo.{args.table}")


def cmd_burst(cur, args):
    """N rapid updates to ONE key, no delay. Tests intra-interval collapse:
    an SCD1 bronze collapses these to net within a trigger window; an SCD2 bronze
    keeps each version."""
    key_vals = tuple(int(x) for x in args.key.split(","))
    for i in range(args.updates):
        update_row(cur, args.table, key_vals, args.dry_run)
    print(f"burst: {args.updates} rapid updates to dbo.{args.table} key={key_vals}")


def cmd_reappear(cur, args):
    """Delete then re-insert the SAME key with identical content, to probe
    unchanged-record handling across a delete/insert cycle."""
    key_vals = tuple(int(x) for x in args.key.split(","))
    random.seed(hash(key_vals))            # regenerate identical content
    delete_row(cur, args.table, key_vals, args.dry_run)
    insert_row(cur, args.table, key_vals, args.dry_run)
    print(f"reappear: deleted and re-inserted dbo.{args.table} key={key_vals}")


def main():
    p = argparse.ArgumentParser(description="Drive DML against the CDC testbed tables.")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed"); s.add_argument("--table", required=True, choices=TABLES)
    s.add_argument("--rows", type=int, default=100); s.add_argument("--dry-run", action="store_true")
    s.set_defaults(func=cmd_seed)

    c = sub.add_parser("churn"); c.add_argument("--table", required=True, choices=TABLES)
    c.add_argument("--updates", type=int, default=10); c.add_argument("--deletes", type=int, default=0)
    c.add_argument("--inserts", type=int, default=0); c.add_argument("--dry-run", action="store_true")
    c.set_defaults(func=cmd_churn)

    b = sub.add_parser("burst"); b.add_argument("--table", required=True, choices=TABLES)
    b.add_argument("--key", required=True, help="key value(s), comma-separated for composite")
    b.add_argument("--updates", type=int, default=8); b.add_argument("--dry-run", action="store_true")
    b.set_defaults(func=cmd_burst)

    r = sub.add_parser("reappear"); r.add_argument("--table", required=True, choices=TABLES)
    r.add_argument("--key", required=True, help="key value(s), comma-separated for composite")
    r.add_argument("--dry-run", action="store_true")
    r.set_defaults(func=cmd_reappear)

    args = p.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    conn = connect()
    try:
        cur = conn.cursor()
        args.func(cur, args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
