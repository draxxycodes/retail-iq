"""Data-quality checks (PRD §11, FR-03/FR-04/FR-09).

Every check takes a Spark DataFrame and returns a DataFrame of *violating*
rows tagged with a `dq_reason` column. The pipeline unions the violations,
writes them to data/rejected/ with their reasons, and keeps only the clean
rows for transformation. Pure functions -> unit-testable (see tests/).
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def tag(df: DataFrame, reason: str) -> DataFrame:
    return df.withColumn("dq_reason", F.lit(reason))


def check_required_columns(df: DataFrame, expected: list[str], source: str) -> None:
    """FR-03: fail fast when the source schema drifts."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"[{source}] schema validation failed - missing columns: {missing}")


def check_nulls(df: DataFrame, required: list[str], reason_prefix: str = "null") -> DataFrame:
    """PRD §11 null validation: critical fields must be populated."""
    cond = None
    for c in required:
        is_null = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        cond = is_null if cond is None else (cond | is_null)
    return tag(df.filter(cond), f"{reason_prefix}_critical_field")


def check_duplicates(df: DataFrame, key: str) -> DataFrame:
    """Repeated business keys -> keep first, flag the rest."""
    rn = F.row_number().over(Window.partitionBy(key).orderBy(F.lit(1)))
    return tag(df.withColumn("_rn", rn).filter(F.col("_rn") > 1).drop("_rn"),
               f"duplicate_{key}")


def check_referential(df: DataFrame, master: DataFrame, fk: str, master_key: str,
                      label: str) -> DataFrame:
    """Orphan records: FK value with no matching master row."""
    valid_keys = master.select(master_key).distinct()
    orphans = df.join(valid_keys, df[fk] == valid_keys[master_key], "left_anti")
    return tag(orphans, f"orphan_{label}")


def check_numerics_sales(df: DataFrame) -> DataFrame:
    """quantity > 0, unit_price >= 0, discount >= 0 (and discount <= gross)"""
    bad_qty = df.filter((F.col("quantity").isNull()) | (F.col("quantity") <= 0))
    bad_price = df.filter((F.col("unit_price").isNull()) | (F.col("unit_price") < 0))
    bad_disc = df.filter(
        (F.col("discount").isNull())
        | (F.col("discount") < 0)
        | (F.col("discount") > F.col("quantity") * F.col("unit_price"))
    )
    return tag(bad_qty, "invalid_quantity").unionByName(
        tag(bad_price, "invalid_unit_price"), allowMissingColumns=True).unionByName(
        tag(bad_disc, "invalid_discount"), allowMissingColumns=True)


def check_dates_sales(df: DataFrame, today: str | None = None) -> DataFrame:
    """Transaction date must parse and must not be in the future."""
    today = today or date.today().isoformat()
    parsed = df.withColumn("_d", F.to_date("transaction_date", "yyyy-MM-dd"))
    bad = parsed.filter(F.col("_d").isNull() | (F.col("_d") > F.lit(today).cast("date")))
    return tag(bad.drop("_d"), "invalid_transaction_date")


def check_returns_consistency(returns: DataFrame, sales_keys: DataFrame) -> DataFrame:
    """Return must reference a real transaction and not predate it."""
    orphan = check_referential(returns, sales_keys, "transaction_id", "transaction_id",
                               "return_transaction")
    sales_dates = sales_keys.select("transaction_id", "transaction_date")
    joined = returns.join(sales_dates, "transaction_id", "left")
    bad_date = joined.filter(
        F.to_date("return_date", "yyyy-MM-dd") < F.to_date("transaction_date", "yyyy-MM-dd")
    ).drop("transaction_date")
    return tag(orphan, "orphan_return_transaction").unionByName(
        tag(bad_date, "return_before_transaction"), allowMissingColumns=True)


def summarize_violations(*violation_dfs: DataFrame) -> dict:
    """Rule -> violating row count, for the DQ log (FR-09)."""
    out: dict[str, int] = {}
    for v in violation_dfs:
        for row in v.groupBy("dq_reason").count().collect():
            out[row["dq_reason"]] = out.get(row["dq_reason"], 0) + row["count"]
    return out
