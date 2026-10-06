-- RetailIQ KPI query library (PRD §4 business questions, §13 KPIs, FR-07)
-- Run in Redshift Query Editor after views.sql. Each query is self-contained
-- and numbered for the demo script (docs/demo_script.md).

SET search_path TO retailiq;

-- ============================================================ SALES ====
-- Q01: Headline KPIs (Executive Overview page)
SELECT
    COUNT(DISTINCT transaction_id) AS orders,
    SUM(quantity)                  AS units_sold,
    SUM(net_sales)                 AS revenue,
    SUM(profit)                    AS profit,
    SUM(profit) / NULLIF(SUM(net_sales), 0) AS profit_margin,
    SUM(net_sales) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS avg_order_value
FROM fact_sales;

-- Q02: Monthly revenue trend with year-over-year growth
SELECT
    year, month, month_name,
    SUM(revenue) AS revenue,
    SUM(revenue) - LAG(SUM(revenue)) OVER (ORDER BY year, month) AS mom_change,
    ROUND(100.0 * (SUM(revenue) - LAG(SUM(revenue)) OVER (ORDER BY year, month))
          / NULLIF(LAG(SUM(revenue)) OVER (ORDER BY year, month), 0), 1) AS mom_pct
FROM v_daily_kpi
GROUP BY 1, 2, 3
ORDER BY 1, 2;

-- Q03: Festive-season vs normal-day performance
SELECT
    is_festive_season,
    COUNT(*) AS days,
    SUM(revenue) AS revenue,
    SUM(profit)  AS profit,
    SUM(revenue) / COUNT(*) AS revenue_per_day
FROM v_daily_kpi
GROUP BY 1;

-- ========================================================= PRODUCTS ====
-- Q04: Top 10 products by revenue
SELECT product_name, category, brand, units_sold, revenue, profit, profit_margin
FROM v_product_performance
ORDER BY revenue DESC
LIMIT 10;

-- Q05: Bottom 10 products by profit (candidates for delisting)
SELECT product_name, category, brand, units_sold, revenue, profit, profit_margin
FROM v_product_performance
ORDER BY profit ASC
LIMIT 10;

-- Q06: Category performance with return rate
SELECT category,
       SUM(units_sold) AS units, SUM(revenue) AS revenue, SUM(profit) AS profit,
       SUM(units_returned)::DECIMAL / NULLIF(SUM(units_sold), 0) AS return_rate
FROM v_product_performance
GROUP BY 1
ORDER BY revenue DESC;

-- =========================================================== STORES ====
-- Q07: Store ranking by revenue
SELECT store_name, region, store_type, orders, revenue, profit,
       avg_order_value, refund_rate
FROM v_store_performance
ORDER BY revenue DESC;

-- Q08: Region performance comparison
SELECT region,
       COUNT(DISTINCT store_id) AS stores,
       SUM(orders)  AS orders,
       SUM(revenue) AS revenue,
       SUM(profit)  AS profit,
       SUM(revenue) / NULLIF(COUNT(DISTINCT store_id), 0) AS revenue_per_store
FROM v_store_performance
GROUP BY 1
ORDER BY revenue DESC;

-- Q09: Stores below 80% of average regional revenue (underperformers)
WITH reg AS (
    SELECT region, AVG(revenue) AS avg_rev
    FROM v_store_performance GROUP BY 1
)
SELECT s.store_name, s.region, s.revenue, r.avg_rev,
       ROUND(100.0 * s.revenue / r.avg_rev, 1) AS pct_of_regional_avg
FROM v_store_performance s
JOIN reg r ON r.region = s.region
WHERE s.revenue < 0.8 * r.avg_rev
ORDER BY pct_of_regional_avg;

-- ======================================================== CUSTOMERS ====
-- Q10: Customer KPI headline
SELECT
    COUNT(*) AS customers,
    SUM(monetary) AS total_spend,
    AVG(monetary) AS avg_spend_per_customer,
    AVG(frequency) AS avg_purchase_frequency
FROM v_customer_rfm;

-- Q11: RFM segment distribution (who drives revenue?)
SELECT rfm_segment,
       COUNT(*) AS customers,
       SUM(monetary) AS revenue,
       ROUND(100.0 * SUM(monetary) / SUM(SUM(monetary)) OVER (), 1) AS revenue_share_pct,
       AVG(frequency) AS avg_frequency
FROM v_customer_rfm
GROUP BY 1
ORDER BY revenue DESC;

-- Q12: New vs returning customers (first purchase in last 90 days = new)
WITH first_buy AS (
    SELECT customer_id, MIN(d.full_date) AS first_date
    FROM fact_sales f
    JOIN dim_date d ON d.date_key = f.date_key
    WHERE customer_id IS NOT NULL
    GROUP BY 1
),
maxd AS (SELECT MAX(full_date) AS m FROM dim_date)
SELECT
    CASE WHEN (SELECT m FROM maxd) - first_date <= 90 THEN 'New' ELSE 'Returning' END AS cohort,
    COUNT(*) AS customers
FROM first_buy
GROUP BY 1;

-- Q13: Top 20 high-value customers
SELECT customer_id, name, customer_segment, city, rfm_segment,
       frequency, monetary, recency_days
FROM v_customer_rfm
ORDER BY monetary DESC
LIMIT 20;

-- Q14: At-risk customers (bought before, silent for 180+ days)
SELECT customer_id, name, city, frequency, monetary, recency_days
FROM v_customer_rfm
WHERE rfm_segment IN ('At Risk', 'Lost')
ORDER BY monetary DESC
LIMIT 20;

-- ======================================================= PROMOTIONS ====
-- Q15: Promotion effectiveness (sales vs profitability)
SELECT promotion_name, promotion_type, campaign_channel,
       promo_orders, promo_revenue, discount_given, promo_profit,
       profit_margin, profit_per_discount_rupee
FROM v_promotion_performance
ORDER BY promo_revenue DESC;

-- Q16: Promo vs non-promo order economics
SELECT
    CASE WHEN promotion_id IS NULL THEN 'No Promotion' ELSE 'Promotion' END AS grp,
    COUNT(DISTINCT transaction_id) AS orders,
    SUM(net_sales)  AS revenue,
    SUM(profit)     AS profit,
    SUM(net_sales) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS avg_order_value,
    SUM(profit) / NULLIF(SUM(net_sales), 0) AS profit_margin
FROM fact_sales
GROUP BY 1;

-- ========================================================== RETURNS ====
-- Q17: Overall return KPIs
SELECT
    COUNT(*) AS return_events,
    SUM(return_quantity) AS units_returned,
    SUM(refund_amount)   AS total_refunds,
    SUM(refund_amount) / NULLIF((SELECT SUM(net_sales) FROM fact_sales), 0) AS refund_to_revenue_ratio
FROM fact_returns;

-- Q18: Return reasons breakdown
SELECT return_reason,
       COUNT(*) AS events,
       SUM(return_quantity) AS units,
       SUM(refund_amount)   AS refunds
FROM v_returns_analysis
GROUP BY 1
ORDER BY refunds DESC;

-- Q19: Highest return-rate categories
SELECT category,
       SUM(units_sold) AS units_sold,
       SUM(units_returned) AS units_returned,
       ROUND(100.0 * SUM(units_returned) / NULLIF(SUM(units_sold), 0), 2) AS return_rate_pct
FROM v_product_performance
GROUP BY 1
ORDER BY return_rate_pct DESC;

-- Q20: Highest return-rate stores
SELECT store_name, region, orders, refund_amount, refund_rate
FROM v_store_performance
ORDER BY refund_rate DESC
LIMIT 10;
