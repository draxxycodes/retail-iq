"""Source-system schemas (PRD §9).

Every raw extract is validated against its schema before any transformation
runs (FR-03). Keeping schemas explicit (instead of inferSchema) means a
renamed/missing column fails fast with a clear error instead of silently
producing wrong joins downstream.
"""
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

SALES = StructType([
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

RETURNS = StructType([
    StructField("return_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("return_quantity", IntegerType(), True),
    StructField("return_reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
])

CUSTOMERS = StructType([  # Oracle CRM extract
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("registration_date", StringType(), True),
])

PRODUCTS = StructType([  # MySQL product master extract
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("selling_price", DoubleType(), True),
])

STORES = StructType([  # MySQL store master extract
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
    StructField("manager", StringType(), True),
    StructField("opening_date", StringType(), True),
])

SUPPLIERS = StructType([  # MySQL supplier master extract
    StructField("supplier_id", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("supplier_region", StringType(), True),
    StructField("supplier_rating", DoubleType(), True),
])

PROMOTIONS = StructType([  # MySQL promotion master extract
    StructField("promotion_id", StringType(), True),
    StructField("promotion_name", StringType(), True),
    StructField("promotion_type", StringType(), True),
    StructField("discount_percentage", DoubleType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("campaign_channel", StringType(), True),
])

ALL = {
    "sales.csv": SALES,
    "returns.csv": RETURNS,
    "customers_oracle_extract.csv": CUSTOMERS,
    "products_mysql_extract.csv": PRODUCTS,
    "stores_mysql_extract.csv": STORES,
    "suppliers_mysql_extract.csv": SUPPLIERS,
    "promotions_mysql_extract.csv": PROMOTIONS,
}

# Fields that must be populated for a sales row to be processable (PRD §11)
SALES_REQUIRED = ["transaction_id", "transaction_date", "product_id", "store_id", "quantity"]
RETURNS_REQUIRED = ["return_id", "return_date", "transaction_id", "product_id", "return_quantity"]
