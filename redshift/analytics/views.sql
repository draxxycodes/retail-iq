-- RetailIQ analytical views (PRD §13/§14, FR-07)
-- Run after the warehouse load. These views power both the SQL KPI library
-- and the Power BI dashboards (import the views, not the raw tables).

SET search_path TO retailiq;

-- ------------------------------------------------- enriched sales base --
CREATE OR REPLACE VIEW v_sales_enriched AS
SELECT
    f.transaction_id,
    d.full_date            AS transaction_date,
    d.year, d.quarter, d.month, d.month_name, d.day_name,
    d.is_weekend, d.is_festive_season,
    c.customer_id, c.name AS customer_name, c.gender, c.age_years,
    c.city AS customer_city, c.state AS customer_state, c.customer_segment,
    p.product_id, p.product_name, p.category, p.subcategory, p.brand,
    p.supplier_id, p.supplier_name,
    s.store_id, s.store_name, s.store_type, s.city AS store_city,
    s.state AS store_state, s.region,
    pr.promotion_id, pr.promotion_name, pr.promotion_type,
    pr.discount_percentage, pr.campaign_channel,
    f.quantity, f.unit_price, f.discount_amount,
    f.gross_sales, f.net_sales, f.cost_of_goods, f.profit, f.profit_margin
FROM fact_sales f
JOIN dim_date d       ON d.date_key = f.date_key
LEFT JOIN dim_customer c  ON c.customer_id = f.customer_id
JOIN dim_product p   ON p.product_id = f.product_id
JOIN dim_store s     ON s.store_id = f.store_id
LEFT JOIN dim_promotion pr ON pr.promotion_id = f.promotion_id;

-- ------------------------------------------------------- daily KPIs ----
CREATE OR REPLACE VIEW v_daily_kpi AS
SELECT
    d.full_date,
    d.year, d.quarter, d.month, d.month_name,
    d.is_weekend, d.is_festive_season,
    COUNT(DISTINCT f.transaction_id) AS orders,
    SUM(f.quantity)                  AS units_sold,
    SUM(f.gross_sales)               AS gross_revenue,
    SUM(f.discount_amount)           AS total_discount,
    SUM(f.net_sales)                 AS revenue,
    SUM(f.cost_of_goods)             AS total_cost,
    SUM(f.profit)                    AS profit,
    CASE WHEN SUM(f.net_sales) = 0 THEN 0
         ELSE SUM(f.profit) / SUM(f.net_sales) END AS profit_margin,
    SUM(f.net_sales) / NULLIF(COUNT(DISTINCT f.transaction_id), 0) AS avg_order_value,
    COUNT(DISTINCT f.customer_id)    AS active_customers
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- ------------------------------------------------- RFM segmentation ----
CREATE OR REPLACE VIEW v_customer_rfm AS
WITH agg AS (
    SELECT
        f.customer_id,
        MAX(d.full_date)          AS last_purchase,
        COUNT(DISTINCT f.transaction_id) AS frequency,
        SUM(f.net_sales)          AS monetary
    FROM fact_sales f
    JOIN dim_date d ON d.date_key = f.date_key
    WHERE f.customer_id IS NOT NULL
    GROUP BY 1
),
based AS (
    SELECT
        a.*,
        (SELECT MAX(full_date) FROM dim_date) AS max_date
    FROM agg a
),
scored AS (
    SELECT
        b.customer_id, b.last_purchase, b.frequency, b.monetary,
        (b.max_date - b.last_purchase) AS recency_days,
        NTILE(5) OVER (ORDER BY (b.max_date - b.last_purchase) DESC) AS r_score,
        NTILE(5) OVER (ORDER BY b.frequency) AS f_score,
        NTILE(5) OVER (ORDER BY b.monetary)   AS m_score
    FROM based b
)
SELECT
    s.customer_id, c.name, c.customer_segment, c.city, c.state,
    s.recency_days, s.frequency, s.monetary,
    s.r_score, s.f_score, s.m_score,
    (s.r_score::VARCHAR || s.f_score::VARCHAR || s.m_score::VARCHAR) AS rfm_code,
    CASE
        WHEN s.r_score >= 4 AND s.f_score >= 4 THEN 'Champions'
        WHEN s.r_score >= 4 AND s.f_score >= 3 THEN 'Loyal Customers'
        WHEN s.r_score >= 3 AND s.f_score >= 3 THEN 'Potential Loyalists'
        WHEN s.r_score >= 4 AND s.f_score <= 2 THEN 'New Customers'
        WHEN s.r_score = 3 AND s.f_score <= 2 THEN 'About to Sleep'
        WHEN s.r_score = 2 THEN 'At Risk'
        ELSE 'Lost'
    END AS rfm_segment
FROM scored s
JOIN dim_customer c ON c.customer_id = s.customer_id;

-- ---------------------------------------------- product performance ----
CREATE OR REPLACE VIEW v_product_performance AS
SELECT
    p.product_id, p.product_name, p.category, p.subcategory, p.brand,
    p.supplier_name,
    COUNT(DISTINCT f.transaction_id) AS orders,
    SUM(f.quantity)   AS units_sold,
    SUM(f.net_sales)  AS revenue,
    SUM(f.profit)     AS profit,
    CASE WHEN SUM(f.net_sales) = 0 THEN 0
         ELSE SUM(f.profit) / SUM(f.net_sales) END AS profit_margin,
    COALESCE(MAX(r.ret_qty), 0) AS units_returned,
    CASE WHEN SUM(f.quantity) = 0 THEN 0
         ELSE COALESCE(MAX(r.ret_qty), 0)::DECIMAL / SUM(f.quantity) END AS return_rate
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
LEFT JOIN (
    SELECT product_id, SUM(return_quantity) AS ret_qty
    FROM fact_returns GROUP BY 1
) r ON r.product_id = p.product_id
GROUP BY 1, 2, 3, 4, 5, 6;

-- ------------------------------------------------ store performance ----
CREATE OR REPLACE VIEW v_store_performance AS
SELECT
    s.store_id, s.store_name, s.store_type, s.city, s.state, s.region,
    COUNT(DISTINCT f.transaction_id) AS orders,
    SUM(f.quantity)   AS units_sold,
    SUM(f.net_sales)  AS revenue,
    SUM(f.profit)     AS profit,
    SUM(f.net_sales) / NULLIF(COUNT(DISTINCT f.transaction_id), 0) AS avg_order_value,
    COUNT(DISTINCT f.customer_id) AS customers_served,
    COALESCE(MAX(r.refunds), 0) AS refund_amount,
    CASE WHEN SUM(f.net_sales) = 0 THEN 0
         ELSE COALESCE(MAX(r.refunds), 0) / SUM(f.net_sales) END AS refund_rate
FROM fact_sales f
JOIN dim_store s ON s.store_id = f.store_id
LEFT JOIN (
    SELECT store_id, SUM(refund_amount) AS refunds
    FROM fact_returns GROUP BY 1
) r ON r.store_id = s.store_id
GROUP BY 1, 2, 3, 4, 5, 6;

-- -------------------------------------------- promotion performance ----
CREATE OR REPLACE VIEW v_promotion_performance AS
SELECT
    pr.promotion_id, pr.promotion_name, pr.promotion_type,
    pr.discount_percentage, pr.campaign_channel,
    pr.start_date, pr.end_date,
    COUNT(DISTINCT f.transaction_id) AS promo_orders,
    SUM(f.quantity)   AS units_sold,
    SUM(f.gross_sales)    AS gross_sales,
    SUM(f.discount_amount) AS discount_given,
    SUM(f.net_sales)  AS promo_revenue,
    SUM(f.profit)     AS promo_profit,
    CASE WHEN SUM(f.net_sales) = 0 THEN 0
         ELSE SUM(f.profit) / SUM(f.net_sales) END AS profit_margin,
    CASE WHEN SUM(f.discount_amount) = 0 THEN NULL
         ELSE SUM(f.profit) / SUM(f.discount_amount) END AS profit_per_discount_rupee
FROM fact_sales f
JOIN dim_promotion pr ON pr.promotion_id = f.promotion_id
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- ------------------------------------------------- returns analysis ----
CREATE OR REPLACE VIEW v_returns_analysis AS
SELECT
    r.return_id, d.full_date AS return_date,
    r.transaction_id, r.customer_id, c.name AS customer_name,
    r.product_id, p.product_name, p.category, p.brand,
    r.store_id, s.store_name, s.region,
    r.return_quantity, r.return_reason, r.refund_amount
FROM fact_returns r
JOIN dim_date d     ON d.date_key = r.return_date_key
LEFT JOIN dim_customer c ON c.customer_id = r.customer_id
JOIN dim_product p  ON p.product_id = r.product_id
JOIN dim_store s    ON s.store_id = r.store_id;
