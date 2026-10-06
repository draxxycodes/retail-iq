# RetailIQ Power BI Dashboard — Build Guide (PRD §15, FR-08)

Five pages, ~45 minutes to build. Every visual below maps to a view in
`redshift/analytics/views.sql` or a measure in `measures.dax`.

## 0. Connect & model (do once)

1. **Get Data → Amazon Redshift** → server = your `REDSHIFT_HOST`, database =
   your `REDSHIFT_DATABASE`. Import (not DirectQuery) these views:
   `v_sales_enriched`, `v_daily_kpi`, `v_customer_rfm`, `v_product_performance`,
   `v_store_performance`, `v_promotion_performance`, `v_returns_analysis`,
   plus `dim_date`.
2. **Model view relationships** (all single-direction, many-to-one):
   - `dim_date[date_key]` → `v_sales_enriched` via `transaction_date` → mark
     `dim_date` as the date table (Table tools → Mark as date table).
   - `v_sales_enriched[customer_id]` → `v_customer_rfm[customer_id]`
   - `v_sales_enriched[product_id]` → `v_product_performance[product_id]`
   - `v_sales_enriched[store_id]` → `v_store_performance[store_id]`
   - `v_sales_enriched[promotion_id]` → `v_promotion_performance[promotion_id]`
3. Create the measures from `measures.dax` (Modeling → New measure).
4. Add **slicers** to every page: Year, Month, Region, Category
   (use the Sync slicers pane so they apply across pages).

## Page 1 — Executive Overview
Purpose: one-screen business summary.
- **Cards (top row):** Revenue, Profit, Profit Margin %, Orders, Units Sold,
  Avg Order Value, Return Rate %.
- **Line chart:** Revenue by month (`dim_date[month_name]`, sort by month number).
- **Bar chart:** Top 10 products by Revenue (`v_product_performance`).
- **Bar chart:** Top 10 stores by Revenue (`v_store_performance`).
- **KPI visual:** Revenue vs Revenue LY with YoY % callout.
- **Drill-through:** right-click a month → drill to Page 2 filtered.

## Page 2 — Sales & Store Performance
Purpose: where is the business winning/losing geographically.
- **Map (filled):** Revenue by `region` / `store_state`.
- **Table:** Store ranking — store_name, region, orders, revenue, profit,
  avg_order_value, refund_rate. Conditional formatting on refund_rate (red > 8%).
- **Line chart:** Revenue trend by month, legend = region.
- **Bar chart:** Profit by store_type.
- **Card:** Revenue per store (region average) for the underperformer story (Q09).

## Page 3 — Customer Insights
Purpose: who buys, how often, who is drifting away.
- **Cards:** Customers, Avg Spend / Customer, New vs Returning split (Q12 as donut).
- **Donut:** Customers by `rfm_segment` (`v_customer_rfm`).
- **Bar chart:** Revenue by rfm_segment (shows Champions/Loyal concentration — Q11).
- **Table:** Top 20 high-value customers (Q13): name, city, rfm_segment,
  frequency, monetary, recency_days.
- **Table:** At-risk customers (Q14) with red data bars on recency_days.

## Page 4 — Product Analytics
Purpose: assortment decisions — what to push, what to fix, what to delist.
- **Bar chart:** Top 10 products by revenue (Q04).
- **Bar chart:** Bottom 10 products by profit (Q05) — the delist candidates.
- **Matrix:** Category × Brand with Revenue, Profit, Return Rate % (Q06).
- **Bar chart:** Return rate % by category (Q19) — quality story.
- **Scatter:** Profit margin % (x) vs Revenue (y), bubble = units, legend = category.

## Page 5 — Promotions & Returns
Purpose: did campaigns pay, and where is money leaking to returns.
- **Cards:** Promo orders, Promo revenue, Discount given, Profit per discount ₹.
- **Table:** Promotion effectiveness (Q15): promotion_name, type, channel,
  promo_orders, promo_revenue, discount_given, promo_profit, profit_margin.
- **Bar chart:** Promo vs non-promo avg order value & margin (Q16).
- **Donut:** Returns by return_reason (Q18).
- **Bar chart:** Refund rate by store (Q20) and by category.

## Demo tips (for the evaluation)
- Start on Page 1, apply the festive-season filter (Oct–Nov) and show the
  revenue jump — it traces back to `_seasonality` in the generator and the
  `is_festive_season` flag in `dim_date`.
- Drill: Page 1 total revenue → Page 2 region → store → Page 4 category →
  product. This is demo step 7 in `docs/demo_script.md`.
- Keep one "bad data" story ready: the DQ log shows ~4–5% rejected rows;
  Page 5's return reasons tie data trust to business action.
