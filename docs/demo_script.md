# RetailIQ End-to-End Demo Script (PRD §21)

One continuous business story, ~15 minutes. Each step names the file or
screen to show.

## 1. Raw source record (2 min)
Open `data/raw/sales.csv`, pick one `transaction_id` (e.g. the first row).
Show its customer, product, store, and promotion rows in the four master
extracts. Point: the same business event is scattered across systems.

## 2. Run the pipeline (3 min)
```bash
python etl/etl_pipeline.py
```
Narrate the four stages as they print: EXTRACT (row counts per source) →
VALIDATE (rule-violation counts) → TRANSFORM → LOAD. Emphasize: the run
fails fast on schema drift and never silently corrupts curated data.

## 3. Data-quality evidence (2 min)
- Open the latest `data/curated/_dq_log/dq_log_*.json`: received vs valid vs
  rejected per source, per-rule violation counts.
- Open `data/rejected/rejected_sales/part-*.csv`: show 2–3 rejected rows and
  read their `dq_reason` aloud (e.g. `orphan_product`, `invalid_quantity`).
- Cross-check one category against `data/raw/_injection_manifest.json`
  (the answer key) — the counts match.

## 4. Warehouse (2 min)
In Redshift Query Editor: `SELECT * FROM retailiq.fact_sales LIMIT 5;`
Show the derived measures (`net_sales`, `profit`, `profit_margin`) and the
audit columns (`source_file`, `processed_ts`). Trace the demo transaction_id
from step 1: it is here, enriched.

## 5. KPI query (2 min)
Run Q01 (headline KPIs) and Q11 (RFM segment revenue share) from
`redshift/analytics/kpi_queries.sql`. Read the numbers that will anchor the
recommendation in step 8.

## 6. Power BI drill-down (3 min)
Page 1 Executive Overview → note total Revenue → drill to Page 2, filter
Region = West → pick the top store → Page 4, filter Category → show the
worst-margin product and its return rate (Q19 visual).

## 7. The recommendation (1 min)
Template: *"The [segment/category/store] driving [X]% of revenue has a
[Y]% return rate vs [Z]% average, costing ₹[N] in refunds. Recommend
[delist / quality review with supplier / targeted promotion pause], which
protects ₹[M] margin."* Fill X/Y/Z/N/M from the queries you just ran —
never invent numbers.

## If something breaks live
- Pipeline error → show the explicit schema-validation message (it names the
  file and missing column); this is a feature, not a bug.
- Redshift unreachable → fall back to the curated parquet: the same KPI
  logic runs in Spark SQL against `data/curated/*.parquet`.
