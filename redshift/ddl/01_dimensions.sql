-- RetailIQ Redshift DDL: schema + dimensions (PRD §12)
-- Run order: 00_create_schema.sql -> 01_dimensions.sql -> 02_facts.sql

CREATE SCHEMA IF NOT EXISTS retailiq;
SET search_path TO retailiq;

-- ------------------------------------------------------------ customers --
DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_id        VARCHAR(20)  NOT NULL PRIMARY KEY,
    name               VARCHAR(100),
    gender             VARCHAR(10),
    age_years          INTEGER,
    email              VARCHAR(120),
    phone              VARCHAR(20),
    city               VARCHAR(50),
    state              VARCHAR(50),
    customer_segment   VARCHAR(20),
    registration_date  DATE
)
DISTSTYLE ALL;

-- ------------------------------------------------------------- products --
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_id     VARCHAR(20)  NOT NULL PRIMARY KEY,
    product_name   VARCHAR(120),
    category       VARCHAR(50),
    subcategory    VARCHAR(50),
    brand          VARCHAR(50),
    supplier_id    VARCHAR(20),
    supplier_name  VARCHAR(100),
    unit_cost      DECIMAL(12,2),
    selling_price  DECIMAL(12,2)
)
DISTSTYLE ALL;

-- --------------------------------------------------------------- stores --
DROP TABLE IF EXISTS dim_store;
CREATE TABLE dim_store (
    store_id     VARCHAR(20)  NOT NULL PRIMARY KEY,
    store_name   VARCHAR(120),
    store_type   VARCHAR(20),
    city         VARCHAR(50),
    state        VARCHAR(50),
    region       VARCHAR(20),
    manager      VARCHAR(100),
    opening_date DATE
)
DISTSTYLE ALL;

-- ------------------------------------------------------------ suppliers --
DROP TABLE IF EXISTS dim_supplier;
CREATE TABLE dim_supplier (
    supplier_id     VARCHAR(20)  NOT NULL PRIMARY KEY,
    supplier_name   VARCHAR(100),
    supplier_region VARCHAR(20),
    supplier_rating DECIMAL(3,1)
)
DISTSTYLE ALL;

-- ----------------------------------------------------------- promotions --
DROP TABLE IF EXISTS dim_promotion;
CREATE TABLE dim_promotion (
    promotion_id        VARCHAR(20)  NOT NULL PRIMARY KEY,
    promotion_name      VARCHAR(120),
    promotion_type      VARCHAR(50),
    discount_percentage DECIMAL(5,2),
    start_date          DATE,
    end_date            DATE,
    campaign_channel    VARCHAR(30)
)
DISTSTYLE ALL;

-- ----------------------------------------------------------------- date --
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key          INTEGER      NOT NULL PRIMARY KEY,
    full_date         DATE         NOT NULL,
    day               SMALLINT,
    month             SMALLINT,
    month_name        VARCHAR(15),
    year              SMALLINT,
    quarter           SMALLINT,
    day_of_week       SMALLINT,
    day_name          VARCHAR(15),
    is_weekend        BOOLEAN,
    is_festive_season BOOLEAN
)
DISTSTYLE ALL;
