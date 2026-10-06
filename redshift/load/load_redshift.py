"""Load curated star-schema datasets into Amazon Redshift (PRD §10.3, FR-06).

Reads the CSV exports written by the PySpark pipeline (data/curated/csv/),
creates the warehouse schema via redshift/ddl/*.sql, and bulk-loads with
batched INSERTs. Credentials come ONLY from the .env file (see .env.example).

Usage:
    python redshift/load/load_redshift.py            # full load
    python redshift/load/load_redshift.py --dry-run  # validate files, no DB writes
    python redshift/load/load_redshift.py --tables dim_customer fact_sales
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings  # noqa: E402

DDL_FILES = ["01_dimensions.sql", "02_facts.sql"]

# table -> curated csv dir (written by etl/etl_pipeline.py)
TABLES = ["dim_customer", "dim_product", "dim_store", "dim_supplier",
          "dim_promotion", "dim_date", "fact_sales", "fact_returns"]


def find_csv(table: str) -> Path:
    d = settings.CURATED_DATA_DIR / "csv" / table
    parts = sorted(d.glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(f"No curated CSV found for {table} in {d} "
                                f"(run the PySpark pipeline first)")
    return parts[0]


def clean_value(v: str):
    v = v.strip()
    if v == "":
        return None
    low = v.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    return v


def load_table(cur, table: str, page_size: int = 1000) -> int:
    import psycopg2.extras
    path = find_csv(table)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        rows = [[clean_value(v) for v in row.values()] for row in reader]
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f'INSERT INTO retailiq.{table} ({", ".join(cols)}) VALUES ({placeholders})'
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=page_size)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="validate files and config without touching Redshift")
    ap.add_argument("--tables", nargs="*", default=TABLES, choices=TABLES)
    args = ap.parse_args()

    print("Checking curated files ...")
    for t in args.tables:
        p = find_csv(t)
        print(f"  {t:18s} -> {p}")

    if args.dry_run:
        print("DRY RUN: files OK, no database writes.")
        return

    if not settings.redshift_configured():
        sys.exit("Redshift is not configured. Copy .env.example to .env and "
                 "fill in REDSHIFT_HOST / REDSHIFT_USER / REDSHIFT_PASSWORD.")

    import psycopg2
    conn = psycopg2.connect(host=settings.REDSHIFT_HOST, port=settings.REDSHIFT_PORT,
                            dbname=settings.REDSHIFT_DATABASE, user=settings.REDSHIFT_USER,
                            password=settings.REDSHIFT_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        ddl_dir = PROJECT_ROOT / "redshift" / "ddl"
        for fname in DDL_FILES:
            sql = (ddl_dir / fname).read_text(encoding="utf-8")
            print(f"Applying {fname} ...")
            cur.execute(sql)

        for t in args.tables:
            n = load_table(cur, t)
            print(f"Loaded {t:18s} {n:>8,} rows")

        print("Row counts in warehouse:")
        for t in args.tables:
            cur.execute(f"SELECT COUNT(*) FROM retailiq.{t}")
            print(f"  {t:18s} {cur.fetchone()[0]:>8,}")
        print("LOAD COMPLETE")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
