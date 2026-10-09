"""Load curated star-schema datasets into Amazon Redshift (PRD §10.3, FR-06).

Two loading modes (controlled by .env):
  - S3 COPY (recommended): uploads curated CSV to S3, then runs Redshift COPY
    — leverages MPP parallelism, handles millions of rows efficiently
  - Direct INSERT (fallback): batched multi-row INSERTs via psycopg2
    — works without S3/IAM, suitable for small datasets or development

Credentials come ONLY from the .env file (see .env.example).

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


# --------------------------------------------------- S3 COPY (production) --
def upload_to_s3(table: str) -> str:
    """Upload curated CSV to S3 and return the S3 URI."""
    import boto3
    path = find_csv(table)
    s3_key = f"{settings.S3_PREFIX}/{table}/{path.name}"
    s3 = boto3.client("s3")
    print(f"  uploading {path.name} -> s3://{settings.S3_BUCKET}/{s3_key}")
    s3.upload_file(str(path), settings.S3_BUCKET, s3_key)
    return f"s3://{settings.S3_BUCKET}/{settings.S3_PREFIX}/{table}/"


def load_table_s3_copy(cur, table: str) -> int:
    """Upload CSV to S3, then COPY into Redshift (MPP-optimized)."""
    s3_uri = upload_to_s3(table)
    copy_sql = f"""
        COPY retailiq.{table}
        FROM '{s3_uri}'
        IAM_ROLE '{settings.REDSHIFT_IAM_ROLE}'
        CSV
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        BLANKSASNULL
        EMPTYASNULL
        REGION 'ap-south-1';
    """
    cur.execute(copy_sql)
    cur.execute(f"SELECT COUNT(*) FROM retailiq.{table}")
    return cur.fetchone()[0]


# ------------------------------------------- Direct INSERT (fallback) ------
def load_table_insert(cur, table: str, page_size: int = 1000) -> int:
    """Fallback: batched multi-row INSERT via psycopg2 execute_values."""
    from psycopg2.extras import execute_values
    path = find_csv(table)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        rows = [[clean_value(v) for v in row.values()] for row in reader]
    sql = f'INSERT INTO retailiq.{table} ({", ".join(cols)}) VALUES %s'
    execute_values(cur, sql, rows, page_size=page_size)
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

    use_s3 = settings.s3_configured()
    if use_s3:
        print(f"Loading mode: S3 COPY (bucket={settings.S3_BUCKET})")
    else:
        print("Loading mode: Direct INSERT (S3 not configured, using fallback)")
        print("  TIP: Set S3_BUCKET and REDSHIFT_IAM_ROLE in .env for production loads")

    import psycopg2
    # sslmode=require: Redshift Serverless enforces SSL by default.
    conn = psycopg2.connect(host=settings.REDSHIFT_HOST, port=settings.REDSHIFT_PORT,
                            dbname=settings.REDSHIFT_DATABASE, user=settings.REDSHIFT_USER,
                            password=settings.REDSHIFT_PASSWORD, sslmode="require")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        ddl_dir = PROJECT_ROOT / "redshift" / "ddl"
        for fname in DDL_FILES:
            sql = (ddl_dir / fname).read_text(encoding="utf-8")
            print(f"Applying {fname} ...")
            cur.execute(sql)

        for t in args.tables:
            if use_s3:
                n = load_table_s3_copy(cur, t)
            else:
                n = load_table_insert(cur, t)
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
