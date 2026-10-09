"""Unit tests for the data-quality check functions (FR-09 support).

Run: pytest tests/ -q
These run on tiny in-memory Spark DataFrames - no source data needed.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

from data_quality import checks

SALES_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("promotion_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
])


@pytest.fixture(scope="module")
def spark():
    # Fix: ensure PySpark workers use the same Python that launched pytest,
    # preventing version mismatch crashes on Windows (Python 3.14 vs 3.12).
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    ss = (SparkSession.builder.appName("RetailIQ-DQ-Tests")
          .master("local[1]")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.python.worker.reuse", "true")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.driver.bindAddress", "127.0.0.1")
          .getOrCreate())
    ss.sparkContext.setLogLevel("ERROR")
    yield ss
    ss.stop()


def _sales(spark):
    rows = [
        ("T1", "2024-05-01", "C1", "P1", "ST1", None, 2, 100.0, 10.0),   # clean
        ("T2", "2024-05-02", "C1", "P1", "ST1", None, 1, 50.0, 0.0),     # clean
        (None, "2024-05-03", "C1", "P1", "ST1", None, 1, 50.0, 0.0),     # null key
        ("T2", "2024-05-04", "C1", "P1", "ST1", None, 1, 50.0, 0.0),     # duplicate
        ("T5", "2024-05-05", "C1", "P_NOPE", "ST1", None, 1, 50.0, 0.0), # orphan product
        ("T6", "2024-05-06", "C1", "P1", "ST1", None, 0, 50.0, 0.0),     # bad qty
        ("T7", "2024-05-07", "C1", "P1", "ST1", None, 1, 50.0, -5.0),    # neg discount
        ("T8", "2999-01-01", "C1", "P1", "ST1", None, 1, 50.0, 0.0),     # future date
    ]
    cols = ["transaction_id", "transaction_date", "customer_id", "product_id",
            "store_id", "promotion_id", "quantity", "unit_price", "discount"]
    return spark.createDataFrame(rows, SALES_SCHEMA)


def _masters(spark):
    key_schema = StructType([StructField("key", StringType(), True)])
    prod = spark.createDataFrame([("P1",)], StructType([StructField("product_id", StringType(), True)]))
    cust = spark.createDataFrame([("C1",)], StructType([StructField("customer_id", StringType(), True)]))
    store = spark.createDataFrame([("ST1",)], StructType([StructField("store_id", StringType(), True)]))
    return prod, cust, store


def test_check_nulls(spark):
    bad = checks.check_nulls(_sales(spark), ["transaction_id", "product_id"])
    assert bad.count() == 1
    assert bad.first()["dq_reason"] == "null_critical_field"


def test_check_duplicates(spark):
    bad = checks.check_duplicates(_sales(spark), "transaction_id")
    assert bad.count() == 1  # second T2 only
    assert bad.first()["transaction_id"] == "T2"


def test_check_referential(spark):
    prod, _, _ = _masters(spark)
    bad = checks.check_referential(_sales(spark), prod, "product_id", "product_id", "product")
    assert bad.count() == 1
    assert bad.first()["dq_reason"] == "orphan_product"


def test_check_numerics(spark):
    bad = checks.check_numerics_sales(_sales(spark))
    reasons = {r["dq_reason"] for r in bad.select("dq_reason").distinct().collect()}
    assert reasons == {"invalid_quantity", "invalid_discount"}
    assert bad.count() == 2


def test_check_dates(spark):
    bad = checks.check_dates_sales(_sales(spark))
    assert bad.count() == 1
    assert bad.first()["transaction_id"] == "T8"


def test_check_required_columns_raises(spark):
    df = spark.createDataFrame(
        [("x",)],
        StructType([StructField("wrong_col", StringType(), True)]))
    with pytest.raises(ValueError, match="missing columns"):
        checks.check_required_columns(df, ["transaction_id"], "sales.csv")
