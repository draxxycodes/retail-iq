from pyspark.sql.types import DoubleType, IntegerType, StringType

from ingestion.schemas import (
    ALL,
    CUSTOMERS,
    PRODUCTS,
    PROMOTIONS,
    RETURNS,
    SALES,
    STORES,
    SUPPLIERS,
    SALES_REQUIRED,
    RETURNS_REQUIRED,
)


def test_all_source_files_have_registered_schemas():
    expected_files = {
        "sales.csv",
        "returns.csv",
        "customers_oracle_extract.csv",
        "products_mysql_extract.csv",
        "stores_mysql_extract.csv",
        "suppliers_mysql_extract.csv",
        "promotions_mysql_extract.csv",
    }

    assert set(ALL.keys()) == expected_files
    assert all(schema is not None for schema in ALL.values())


def test_sales_schema_has_expected_column_types():
    fields = {field.name: field.dataType for field in SALES.fields}

    assert isinstance(fields["transaction_id"], StringType)
    assert isinstance(fields["transaction_date"], StringType)
    assert isinstance(fields["quantity"], IntegerType)
    assert isinstance(fields["unit_price"], DoubleType)
    assert isinstance(fields["discount"], DoubleType)


def test_returns_schema_has_expected_column_types():
    fields = {field.name: field.dataType for field in RETURNS.fields}

    assert isinstance(fields["return_id"], StringType)
    assert isinstance(fields["return_quantity"], IntegerType)
    assert isinstance(fields["refund_amount"], DoubleType)


def test_master_data_schemas_are_registered_correctly():
    expected_columns = {
        CUSTOMERS: {"customer_id", "email", "customer_segment"},
        PRODUCTS: {"product_id", "product_name", "selling_price"},
        STORES: {"store_id", "store_name", "region"},
        SUPPLIERS: {"supplier_id", "supplier_name", "supplier_rating"},
        PROMOTIONS: {"promotion_id", "promotion_name", "discount_percentage"},
    }

    for schema, required_columns in expected_columns.items():
        actual_columns = {field.name for field in schema.fields}
        assert required_columns.issubset(actual_columns)


def test_required_field_lists_are_defined():
    assert set(SALES_REQUIRED) == {
        "transaction_id",
        "transaction_date",
        "product_id",
        "store_id",
        "quantity",
    }

    assert set(RETURNS_REQUIRED) == {
        "return_id",
        "return_date",
        "transaction_id",
        "product_id",
        "return_quantity",
    }


def test_registered_schemas_match_source_definitions():
    assert ALL["sales.csv"] == SALES
    assert ALL["returns.csv"] == RETURNS
    assert ALL["customers_oracle_extract.csv"] == CUSTOMERS
    assert ALL["products_mysql_extract.csv"] == PRODUCTS
    assert ALL["stores_mysql_extract.csv"] == STORES
    assert ALL["suppliers_mysql_extract.csv"] == SUPPLIERS
    assert ALL["promotions_mysql_extract.csv"] == PROMOTIONS