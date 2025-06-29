#!/usr/bin/env python3
import os
import psycopg2
import csv

DB_PARAMS = {
    "dbname":   os.getenv("POSTGRES_DB", "company_db"),
    "user":     os.getenv("POSTGRES_USER", "etl_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "securepassword"),
    "host":     "localhost",            # or "postgres" if run from container
    "port":     5432,
}

TABLES = [
    "staging_commoncrawl",
    "staging_abr",
    "unified_companies",
    "processed_files",
]

def dump_table(cur, table):
    cur.execute(f"SELECT * FROM {table};")
    cols = [d[0] for d in cur.description]
    with open(f"{table}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(cur.fetchall())
    print(f"→ Wrote {table}.csv ({cur.rowcount} rows)")

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    with conn:
        with conn.cursor() as cur:
            for table in TABLES:
                dump_table(cur, table)
    conn.close()

if __name__ == "__main__":
    main()

