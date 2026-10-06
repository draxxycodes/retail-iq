-- RetailIQ Redshift DDL: fact tables (PRD §12)
-- Dimensions use DISTSTYLE ALL (small, replicated); facts are distributed
-- on their most-joined key and sorted by date for fast range scans.

SET search_path TO retailiq;

-- ---------------------------------------------------------------- sales --
DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
    transaction_id   VARCHAR(20)   NOT NULL PRIMARY KEY,
    date_key         INTEGER       NOT NULL REFERENCES dim_date(date_key),
    customer_id      VARCHAR(20)   REFERENCES dim_customer(customer_id),
    product_id       VARCHAR(20)   NOT NULL REFERENCES dim_product(product_id),
    store_id         VARCHAR(20)   NOT NULL REFERENCES dim_store(store_id),
    promotion_id     VARCHAR(20)   REFERENCES dim_promotion(promotion_id),
    quantity         INTEGER       NOT NULL,
    unit_price       DECIMAL(12,2) NOT NULL,
    discount_amount  DECIMAL(12,2) NOT NULL DEFAULT 0,
    gross_sales      DECIMAL(14,2) NOT NULL,
    net_sales        DECIMAL(14,2) NOT NULL,
    cost_of_goods    DECIMAL(14,2) NOT NULL,
    profit           DECIMAL(14,2) NOT NULL,
    profit_margin    DECIMAL(8,4)  NOT NULL,
    source_file      VARCHAR(60),
    processed_ts     TIMESTAMP
)
DISTKEY (customer_id)
SORTKEY (date_key);

-- -------------------------------------------------------------- returns --
DROP TABLE IF EXISTS fact_returns;
CREATE TABLE fact_returns (
    return_id         VARCHAR(20)   NOT NULL PRIMARY KEY,
    return_date_key   INTEGER       NOT NULL REFERENCES dim_date(date_key),
    transaction_id    VARCHAR(20)   REFERENCES fact_sales(transaction_id),
    customer_id       VARCHAR(20)   REFERENCES dim_customer(customer_id),
    product_id        VARCHAR(20)   NOT NULL REFERENCES dim_product(product_id),
    store_id          VARCHAR(20)   NOT NULL REFERENCES dim_store(store_id),
    return_quantity   INTEGER       NOT NULL,
    return_reason     VARCHAR(50),
    refund_amount     DECIMAL(14,2) NOT NULL,
    source_file       VARCHAR(60),
    processed_ts      TIMESTAMP
)
DISTKEY (transaction_id)
SORTKEY (return_date_key);
