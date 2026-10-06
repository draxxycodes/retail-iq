# RetailIQ — Retail Sales Analytics & Customer Insights Platform

End-to-end retail analytics platform for the Cognizant NPN program
(Use Case 5). Fragmented source data → PySpark ETL → data quality →
Amazon Redshift warehouse → SQL analytics → Power BI dashboards.

**Pipeline in one line:** CSV + Oracle/MySQL extracts → PySpark →
validated star schema in Redshift → SQL KPIs → Power BI.

## Repository layout

```
retail-iq/
├── scripts/generate_source_data.py  # reproducible synthetic source data (+DQ answer key)
├── ingestion/schemas.py             # explicit source schemas (fail fast on drift)
├── etl/etl_pipeline.py          # extract → validate → transform → load (curated)
├── data_quality/checks.py           # PRD §11 quality checks (unit-tested)
├── redshift/ddl/                    # star-schema DDL (dimensions + facts)
├── redshift/load/load_redshift.py   # bulk load curated data into Redshift
├── redshift/analytics/              # views.sql, kpi_queries.sql (Q01–Q20)
├── powerbi/                         # measures.dax + 5-page dashboard build guide
├── docs/                            # dictionary, architecture, demo script
├── tests/                           # pytest unit tests for DQ checks
├── data/                            # raw / curated / rejected (generated, gitignored)
├── requirements.txt
└── .env.example                     # copy to .env — credentials live ONLY here
```

## Quickstart

```bash
# 1. Install (Java 17+ required for PySpark)
pip install -r requirements.txt

# 2. Generate source data (~60k sales, masters, deliberate DQ issues)
python scripts/generate_source_data.py

# 3. Run the PySpark pipeline (extract → validate → transform → curated)
python etl/etl_pipeline.py

# 4. Configure Redshift (only step that needs secrets)
cp .env.example .env   # then fill in REDSHIFT_HOST / USER / PASSWORD

# 5. Create schema + load warehouse
python redshift/load/load_redshift.py

# 6. In Redshift Query Editor, run redshift/analytics/views.sql,
#    then explore redshift/analytics/kpi_queries.sql (Q01–Q20)

# 7. Build the dashboards following powerbi/dashboard_build_guide.md
```

Run the unit tests any time: `pytest tests/ -q`

## Demo in 8 steps

See `docs/demo_script.md` — it walks one transaction from a raw CSV row
through validation, transformation, the warehouse, a KPI query, and a
Power BI drill-down, ending in a business recommendation.

## Troubleshooting

- `ModuleNotFoundError: No module named 'config'` — run scripts from the
  project root (`python scripts/...`, `python etl/...`), not from inside
  the subdirectories.
- PySpark `Broken pipe` / `Task lost` on startup behind a corporate proxy —
  force IPv4: `export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"`
  (Linux/macOS) or set the `JAVA_TOOL_OPTIONS` environment variable on
  Windows before running.
- `Host key verification failed` / Redshift connection errors — check `.env`
  values; never hardcode credentials, the loader reads only from `.env`.

## Team split (7 members, PRD §22)

| # | Area | Owns |
|---|------|------|
| 1 | Sources & ingestion | `scripts/`, `ingestion/`, data dictionary |
| 2 | PySpark transformations | `etl/etl_pipeline.py` (transform stage) |
| 3 | Data quality | `data_quality/`, DQ log, rejected-records story |
| 4 | Warehouse modeling | `redshift/ddl/`, star schema rationale |
| 5 | Warehouse loading | `redshift/load/`, `.env` config |
| 6 | SQL analytics | `redshift/analytics/`, KPI walkthrough |
| 7 | Power BI | `powerbi/`, dashboard demo |
