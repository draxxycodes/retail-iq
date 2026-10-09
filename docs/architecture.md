# RetailIQ Architecture

## Data Flow

```
 sales.csv (daily batch) ──────────────────┐
 returns.csv (daily batch) ────────────────┤
 Oracle CRM (JDBC) ─── customers ─────────┤
 MySQL Masters (JDBC) ─ products / stores /│
                        suppliers /        │
                        promotions ────────┤
                                           ▼
                          ┌──────────────────────────────┐
                          │  PySpark ETL (local[*])      │
                          │  1. Extract                  │
                          │     JDBC (Oracle/MySQL)      │
                          │     with CSV fallback        │
                          │  2. Validate (PRD §11        │
                          │     checks → quarantine)     │
                          │  3. Transform (star schema,  │
                          │     derived measures)        │
                          └──────────┬───────────────────┘
                                     │ curated parquet + csv
                                     │ rejected rows + dq log (JSON)
                                     ▼
                          ┌──────────────────────────────┐
                          │  AWS S3 Staging               │
                          │  s3://<bucket>/retailiq/      │
                          │  curated/<table>/part-*.csv   │
                          └──────────┬───────────────────┘
                                     │ Redshift COPY command
                                     ▼
                          ┌──────────────────────────────┐
                          │  Amazon Redshift Serverless   │
                          │  retailiq schema:             │
                          │  dim_customer / product /     │
                          │  store / supplier /           │
                          │  promotion / date (DIST ALL)  │
                          │  fact_sales (KEY customer)    │
                          │  fact_returns (KEY txn)       │
                          │                               │
                          │  7 analytical views           │
                          │  20 KPI queries (Q01-Q20)     │
                          └──────────┬───────────────────┘
                                     │ views as data source
                                     ▼
                          ┌──────────────────────────────┐
                          │  Power BI                     │
                          │  5 pages, DAX measures,       │
                          │  slicers + drill-down         │
                          └──────────────────────────────┘
```

## Orchestration (Apache Airflow)

```
 generate_source_data → run_pyspark_etl → dq_quality_gate ──┐
                                                             ├── upload_to_s3 → load_redshift → apply_views → verify → done
                                                             └── dq_alert (halt if rejection > 10%)
```

See `airflow/dags/retailiq_etl_dag.py` for the full DAG definition.

## Ingestion modes

| Source | Primary Mode | Fallback | Config |
|--------|-------------|----------|--------|
| Sales & Returns (CSV) | File read | — | `RAW_DATA_DIR` |
| Customers (Oracle CRM) | JDBC (`oracle.jdbc.OracleDriver`) | CSV extract | `ORACLE_*` in `.env` |
| Products, Stores, Suppliers, Promotions (MySQL) | JDBC (`com.mysql.cj.jdbc.Driver`) | CSV extract | `MYSQL_*` in `.env` |

## Loading modes

| Mode | When | How |
|------|------|-----|
| **S3 COPY** (production) | `S3_BUCKET` + `REDSHIFT_IAM_ROLE` set | Upload CSV → S3 → `COPY` command (MPP-optimized) |
| **Direct INSERT** (dev/fallback) | S3 not configured | `psycopg2.extras.execute_values` batched inserts |

## Design decisions (and why)

1. **Explicit schemas, not inferSchema.** A renamed column in a source
   extract fails the run with a named error instead of silently producing
   wrong joins. (FR-03)
2. **JDBC with CSV fallback.** The pipeline reads directly from Oracle CRM
   and MySQL databases when JDBC credentials are configured. When they're
   absent (dev environment, demo), it falls back to CSV extracts — same
   schema validation, same downstream logic. (FR-01, FR-02)
3. **Quarantine, don't delete.** Every rejected row is kept in
   `data/rejected/` with its `dq_reason`; the DQ log records per-rule counts.
   The demo can point at real rejected rows and explain each one. (FR-09)
4. **S3 staging for Redshift COPY.** Production loads use the MPP-native
   `COPY` command from S3 (parallel ingestion across slices). The fallback
   `INSERT` mode supports development without AWS infrastructure. (FR-06)
5. **Dimensions `DISTSTYLE ALL`.** All six dimensions are small (≤ 8k rows);
   replicating them to every node makes every fact join local. Facts are
   distributed on their most-joined key and sorted by date for range scans.
6. **Audit columns on facts** (`source_file`, `processed_ts`) so any
   warehouse row traces back to its source file and pipeline run.
7. **Views, not raw tables, feed Power BI.** `v_sales_enriched` pre-joins the
   star so dashboard authors never hand-write joins; KPI queries double as
   the view test-suite (they must reconcile with the warehouse).
8. **Airflow orchestration.** A single DAG with a data-quality gate ensures
   bad batches never reach the warehouse. The pipeline is idempotent:
   re-running on the same day replaces data cleanly.
