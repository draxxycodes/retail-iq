# RetailIQ Architecture

```
 sales.csv ──┐
 returns.csv ─┤
 customers (Oracle CRM extract) ──────┐
 products / stores / suppliers /      │
   promotions (MySQL extracts) ───────┤
                                      ▼
                        ┌─────────────────────────┐
                        │  Python + PySpark       │
                        │  1. Extract (explicit   │
                        │     schemas, fail fast) │
                        │  2. Validate (PRD §11   │
                        │     checks → quarantine)│
                        │  3. Transform (clean,   │
                        │     join, derive)       │
                        └────────────┬────────────┘
                                     │ curated parquet + csv
                                     │ rejected rows + dq log (JSON)
                                     ▼
                        ┌─────────────────────────┐
                        │  Amazon Redshift        │
                        │  star schema:           │
                        │  dim_customer/product/  │
                        │  store/supplier/        │
                        │  promotion/date (ALL)   │
                        │  fact_sales (KEY cust)  │
                        │  fact_returns (KEY txn) │
                        └────────────┬────────────┘
                                     │ SQL views + KPI queries
                                     ▼
                        ┌─────────────────────────┐
                        │  Power BI               │
                        │  5 pages, DAX measures, │
                        │  slicers + drill-down   │
                        └─────────────────────────┘
```

## Design decisions (and why)

1. **Explicit schemas, not inferSchema.** A renamed column in a source
   extract fails the run with a named error instead of silently producing
   wrong joins. (FR-03)
2. **Quarantine, don't delete.** Every rejected row is kept in
   `data/rejected/` with its `dq_reason`; the DQ log records per-rule counts.
   The demo can point at real rejected rows and explain each one. (FR-09)
3. **Dimensions `DISTSTYLE ALL`.** All six dimensions are small (≤ 8k rows);
   replicating them to every node makes every fact join local. Facts are
   distributed on their most-joined key and sorted by date for range scans.
4. **Audit columns on facts** (`source_file`, `processed_ts`) so any
   warehouse row traces back to its source file and pipeline run.
5. **Views, not raw tables, feed Power BI.** `v_sales_enriched` pre-joins the
   star so dashboard authors never hand-write joins; KPI queries double as
   the view test-suite (they must reconcile with the warehouse — PRD §22).
6. **No streaming, no orchestrator, no S3** (PRD §23). The pipeline is one
   `python` command; Redshift loads via batched INSERTs. Everything a team
   member needs to explain fits on one whiteboard.
