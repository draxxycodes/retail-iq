"""RetailIQ PySpark ETL pipeline (PRD §10, FR-01..FR-06, FR-09, FR-10).

    Extract   read sources via JDBC (Oracle/MySQL) with CSV fallback
    Validate  run every PRD §11 quality check; quarantine violators with reasons
    Transform standardize, join to masters, derive business measures
    Load      write curated star-schema datasets (parquet + CSV for Redshift)

Ingestion modes (controlled by .env):
  - JDBC: reads directly from Oracle CRM / MySQL databases when configured
  - CSV fallback: reads from data/raw/*.csv when DB credentials are absent

Run from the project root:
    python etl/etl_pipeline.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import settings
from data_quality import checks
from ingestion import schemas

SOURCE_FILES = {
    "sales": "sales.csv",
    "returns": "returns.csv",
    "customers": "customers_oracle_extract.csv",
    "products": "products_mysql_extract.csv",
    "stores": "stores_mysql_extract.csv",
    "suppliers": "suppliers_mysql_extract.csv",
    "promotions": "promotions_mysql_extract.csv",
}

# JDBC table mapping: source name -> (db_type, table_name)
JDBC_TABLES = {
    "customers": ("oracle", "customers"),
    "products":  ("mysql",  "products"),
    "stores":    ("mysql",  "stores"),
    "suppliers": ("mysql",  "suppliers"),
    "promotions":("mysql",  "promotions"),
}


def spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("RetailIQ-ETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
    )
    # Windows compatibility: suppress winutils warnings when HADOOP_HOME is unset
    if os.name == "nt":
        builder = builder.config("spark.sql.warehouse.dir",
                                str(PROJECT_ROOT / "spark-warehouse"))
    # Add JDBC driver JARs if available on the classpath
    jdbc_jars = []
    jars_dir = PROJECT_ROOT / "jars"
    if jars_dir.is_dir():
        for jar in jars_dir.glob("*.jar"):
            jdbc_jars.append(str(jar))
    if jdbc_jars:
        builder = builder.config("spark.jars", ",".join(jdbc_jars))
    return builder.getOrCreate()


# -------------------------------------------------------------- JDBC -----
def _read_jdbc(spark: SparkSession, db_type: str, table: str,
               target_schema) -> DataFrame:
    """Read a table via JDBC (Oracle or MySQL)."""
    if db_type == "oracle":
        url = settings.ORACLE_JDBC_URL
        props = {
            "user": settings.ORACLE_USER,
            "password": settings.ORACLE_PASSWORD,
            "driver": "oracle.jdbc.OracleDriver",
        }
    else:  # mysql
        url = settings.MYSQL_JDBC_URL
        props = {
            "user": settings.MYSQL_USER,
            "password": settings.MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver",
        }
    df = spark.read.jdbc(url=url, table=table, properties=props)
    # Cast to target schema for consistency with CSV path
    for f in target_schema.fields:
        if f.name in df.columns:
            df = df.withColumn(f.name, F.trim(F.col(f.name).cast("string")).cast(f.dataType))
    return df


def _read_csv(spark: SparkSession, fname: str, schema) -> DataFrame:
    """Read a source CSV file with explicit schema (fail fast on drift)."""
    path = str(settings.RAW_DATA_DIR / fname)
    df = spark.read.format("csv").option("header", "true").load(path)
    checks.check_required_columns(df, schema.fieldNames(), fname)
    for f in schema.fields:
        df = df.withColumn(f.name, F.trim(F.col(f.name)).cast(f.dataType))
    return df


# ------------------------------------------------------------ extract -----
def extract(spark: SparkSession) -> dict[str, DataFrame]:
    """Extract all sources. Uses JDBC when configured, CSV fallback otherwise."""
    raw = {}
    mysql_ok = settings.mysql_configured()
    oracle_ok = settings.oracle_configured()

    if mysql_ok:
        print("  [JDBC] MySQL connection configured — reading master tables via JDBC")
    if oracle_ok:
        print("  [JDBC] Oracle connection configured — reading customer CRM via JDBC")
    if not mysql_ok and not oracle_ok:
        print("  [CSV]  No JDBC credentials — using CSV fallback for all sources")

    for name, fname in SOURCE_FILES.items():
        schema = schemas.ALL[fname]

        # Decide ingestion mode
        if name in JDBC_TABLES:
            db_type, table_name = JDBC_TABLES[name]
            use_jdbc = (db_type == "oracle" and oracle_ok) or \
                       (db_type == "mysql" and mysql_ok)
        else:
            use_jdbc = False

        if use_jdbc:
            db_type, table_name = JDBC_TABLES[name]
            try:
                df = _read_jdbc(spark, db_type, table_name, schema)
                mode = f"JDBC/{db_type}"
            except Exception as e:
                print(f"  WARN  JDBC read failed for {name}: {e}")
                print(f"         Falling back to CSV: {fname}")
                df = _read_csv(spark, fname, schema)
                mode = "CSV/fallback"
        else:
            df = _read_csv(spark, fname, schema)
            mode = "CSV"

        df = (df.withColumn("_row_id", F.monotonically_increasing_id())
                .withColumn("_source_file", F.lit(fname)))
        raw[name] = df
        print(f"extract  {fname:32s} {df.count():>8,} rows  [{mode}]")
    return raw


# ------------------------------------------------------------ helpers -----
def _trim_strings(df: DataFrame) -> DataFrame:
    for f in df.schema.fields:
        if str(f.dataType) == "StringType()":
            df = df.withColumn(f.name, F.trim(F.col(f.name)))
    return df


def _dedup_master(df: DataFrame, key: str, label: str) -> tuple[DataFrame, int]:
    dup = checks.check_duplicates(df, key)
    n_dup = dup.count()
    if n_dup:
        print(f"  master {label}: dropping {n_dup:,} duplicate {key} rows")
    clean = df.join(dup.select("_row_id"), "_row_id", "left_anti")
    return clean, n_dup


# ----------------------------------------------------------- validate -----
def validate(raw: dict[str, DataFrame]) -> tuple[dict[str, DataFrame], dict]:
    """Run PRD §11 checks. Returns (clean_frames, dq_summary)."""
    from pyspark.storagelevel import StorageLevel

    dq: dict = {"rule_violations": {}, "sources": {}}
    # Materialize once: _row_id from monotonically_increasing_id() must be
    # stable across the many downstream actions that reference it.
    sales = raw["sales"].persist(StorageLevel.MEMORY_AND_DISK)
    returns = raw["returns"].persist(StorageLevel.MEMORY_AND_DISK)
    sales.count(); returns.count()

    # clean masters first (dedup) so FK checks are meaningful
    masters = {}
    for name, key in [("customers", "customer_id"), ("products", "product_id"),
                      ("stores", "store_id"), ("suppliers", "supplier_id"),
                      ("promotions", "promotion_id")]:
        clean, n_dup = _dedup_master(_trim_strings(raw[name]), key, name)
        masters[name] = clean
        received = raw[name].count()
        dq["sources"][SOURCE_FILES[name]] = {
            "received": received, "valid": clean.count(),
            "rejected": received - clean.count(), "duplicates": n_dup,
        }

    sales_v = [
        checks.check_nulls(sales, schemas.SALES_REQUIRED),
        checks.check_duplicates(sales, "transaction_id"),
        checks.check_referential(sales, masters["products"], "product_id", "product_id", "product"),
        checks.check_referential(sales, masters["customers"], "customer_id", "customer_id", "customer"),
        checks.check_referential(sales, masters["stores"], "store_id", "store_id", "store"),
        checks.check_numerics_sales(sales),
        checks.check_dates_sales(sales),
    ]
    sales_keys = sales.select("transaction_id", "transaction_date")
    returns_v = [
        checks.check_nulls(returns, schemas.RETURNS_REQUIRED),
        checks.check_duplicates(returns, "return_id"),
        checks.check_returns_consistency(returns, sales_keys),
    ]

    dq["rule_violations"] = checks.summarize_violations(*(sales_v + returns_v))

    # Rejected output = the violation rows themselves (one row per rule hit).
    # Clean = anti-join on the small rejected-id sets (broadcast, no shuffle).
    rej_sales_union = _union_all(sales_v)
    rej_ret_union = _union_all(returns_v)
    rejected_sales = rej_sales_union.drop("_row_id")
    rejected_returns = rej_ret_union.drop("_row_id")
    rej_sales_ids = F.broadcast(rej_sales_union.select("_row_id").distinct())
    rej_ret_ids = F.broadcast(rej_ret_union.select("_row_id").distinct())
    clean_sales = sales.join(rej_sales_ids, "_row_id", "left_anti").drop("_row_id")
    clean_returns = returns.join(rej_ret_ids, "_row_id", "left_anti").drop("_row_id")

    dq["sources"]["sales.csv"] = _src_stats(sales, clean_sales)
    dq["sources"]["returns.csv"] = _src_stats(returns, clean_returns)

    rejected = {"rejected_sales": rejected_sales, "rejected_returns": rejected_returns}
    clean = {**{k: v.drop("_row_id", "_source_file") for k, v in masters.items()},
             "sales": clean_sales, "returns": clean_returns}
    sales.unpersist(); returns.unpersist()
    return clean, dq, rejected


def _union_all(dfs: list[DataFrame]) -> DataFrame:
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out


def _src_stats(raw: DataFrame, clean: DataFrame) -> dict:
    received, valid = raw.count(), clean.count()
    return {"received": received, "valid": valid, "rejected": received - valid,
            "duplicates": 0}


# ---------------------------------------------------------- transform -----
def build_dimensions(clean: dict[str, DataFrame]) -> dict[str, DataFrame]:
    dims = {}
    cust = clean["customers"]
    dims["dim_customer"] = (
        cust.withColumn("age_years",
                        F.floor(F.months_between(F.current_date(),
                                                 F.to_date("date_of_birth")) / 12).cast("int"))
            .withColumn("gender", F.when(F.upper(F.col("gender")).isin("M", "MALE"), "Male")
                        .when(F.upper(F.col("gender")).isin("F", "FEMALE"), "Female")
                        .otherwise(F.coalesce(F.initcap(F.col("gender")), F.lit("Unknown"))))
            .withColumn("email", F.lower(F.col("email")))
            .select("customer_id", "name", "gender", "age_years", "email", "phone",
                    "city", "state", "customer_segment", "registration_date")
    )
    dims["dim_product"] = (
        clean["products"].join(clean["suppliers"], "supplier_id", "left")
        .select("product_id", "product_name", "category", "subcategory", "brand",
                "supplier_id", "supplier_name", "unit_cost", "selling_price")
    )
    dims["dim_store"] = clean["stores"].select(
        "store_id", "store_name", "store_type", "city", "state", "region",
        "manager", "opening_date")
    dims["dim_supplier"] = clean["suppliers"].select(
        "supplier_id", "supplier_name", "supplier_region", "supplier_rating")
    dims["dim_promotion"] = clean["promotions"].select(
        "promotion_id", "promotion_name", "promotion_type", "discount_percentage",
        "start_date", "end_date", "campaign_channel")

    dates = (clean["sales"].select(F.to_date("transaction_date").alias("d"))
             .union(clean["returns"].select(F.to_date("return_date").alias("d")))
             .filter(F.col("d").isNotNull()).distinct())
    dims["dim_date"] = (
        dates.withColumn("date_key", F.date_format("d", "yyyyMMdd").cast("int"))
        .withColumnRenamed("d", "full_date")
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_festive_season", F.month("full_date").isin(10, 11))
        .select("date_key", "full_date", "day", "month", "month_name", "year",
                "quarter", "day_of_week", "day_name", "is_weekend", "is_festive_season")
    )
    return dims


def build_facts(clean: dict[str, DataFrame], dims: dict[str, DataFrame]) -> dict[str, DataFrame]:
    s = clean["sales"].alias("s")
    p = clean["products"].alias("p")
    enriched = (
        s.join(p, "product_id", "inner")
        .withColumn("gross_sales", F.col("quantity") * F.col("unit_price"))
        .withColumn("discount_amount", F.coalesce(F.col("discount"), F.lit(0.0)))
        .withColumn("net_sales", F.col("gross_sales") - F.col("discount_amount"))
        .withColumn("cost_of_goods", F.col("quantity") * F.col("unit_cost"))
    )
    enriched = (enriched
                .withColumn("profit", F.col("net_sales") - F.col("cost_of_goods"))
                .withColumn("profit_margin",
                            F.when(F.col("net_sales") != 0,
                                   F.col("profit") / F.col("net_sales")).otherwise(0.0))
                .withColumn("date_key", F.date_format(F.to_date("transaction_date"), "yyyyMMdd").cast("int"))
                .withColumn("processed_ts", F.current_timestamp()))

    fact_sales = enriched.select(
        "transaction_id", "date_key", "customer_id", "product_id", "store_id",
        "promotion_id", "quantity", "unit_price", "discount_amount", "gross_sales",
        "net_sales", "cost_of_goods", "profit", "profit_margin",
        F.col("_source_file").alias("source_file"), "processed_ts")

    r = clean["returns"]
    fact_returns = (
        r.withColumn("date_key", F.date_format(F.to_date("return_date"), "yyyyMMdd").cast("int"))
         .withColumn("processed_ts", F.current_timestamp())
         .select("return_id", F.col("date_key").alias("return_date_key"), "transaction_id",
                 "customer_id", "product_id", "store_id", "return_quantity",
                 "return_reason", "refund_amount",
                 F.col("_source_file").alias("source_file"), "processed_ts")
    )
    return {"fact_sales": fact_sales, "fact_returns": fact_returns}


# --------------------------------------------------------------- load -----
def write_curated(tables: dict[str, DataFrame]) -> None:
    out = settings.CURATED_DATA_DIR
    csv_out = out / "csv"
    csv_out.mkdir(exist_ok=True)
    for name, df in tables.items():
        n = df.count()
        df.write.mode("overwrite").parquet(str(out / f"{name}.parquet"))
        (df.coalesce(1).write.mode("overwrite").option("header", "true")
           .csv(str(csv_out / name)))
        print(f"curated  {name:20s} {n:>8,} rows  -> parquet + csv")


def write_rejected(rejected: dict) -> None:
    out = settings.REJECTED_DATA_DIR
    for name in ("rejected_sales", "rejected_returns"):
        df = rejected[name]
        n = df.count()
        if n:
            (df.coalesce(1).write.mode("overwrite").option("header", "true")
               .csv(str(out / name)))
        print(f"rejected {name:20s} {n:>8,} rows")


def write_dq_log(dq: dict, tables: dict[str, DataFrame]) -> None:
    dq["run_id"] = datetime.now().strftime("%Y%m%d_%H%M%S")
    dq["processed_at"] = datetime.now().isoformat(timespec="seconds")
    dq["outputs"] = {name: df.count() for name, df in tables.items()}
    dq["status"] = "SUCCESS"
    path = settings.DQ_LOG_DIR / f"dq_log_{dq['run_id']}.json"
    path.write_text(json.dumps(dq, indent=2, default=str))
    print(f"dq log  -> {path}")
    print(json.dumps(dq["rule_violations"], indent=2))


# ----------------------------------------------------------------- main ---
def main() -> None:
    t0 = datetime.now()
    ss = spark()
    ss.sparkContext.setLogLevel("WARN")

    print("== 1. EXTRACT ==")
    raw = extract(ss)
    print("== 2. VALIDATE ==")
    clean, dq, rejected = validate(raw)
    print("== 3. TRANSFORM ==")
    dims = build_dimensions(clean)
    facts = build_facts(clean, dims)
    tables = {**dims, **facts}
    print("== 4. LOAD (curated) ==")
    write_curated(tables)
    write_rejected(rejected)
    write_dq_log(dq, tables)

    print(f"DONE in {(datetime.now() - t0).total_seconds():.1f}s")
    ss.stop()


if __name__ == "__main__":
    main()
