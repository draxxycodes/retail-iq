# 🛒 RetailIQ — Enterprise Retail Analytics & Data Platform 🚀

**Cognizant NPN Hackathon — Use Case 5: Retail Sales Analytics & Customer Insights Platform**

*A Zero-Data-Loss, Enterprise-Grade Hybrid ELT Architecture leveraging PySpark, AWS S3, Amazon Redshift Serverless, Apache Airflow, and Power BI.*

---

## 📊 Executive KPI Summary

| 💰 Total Revenue | 📈 Gross Profit | 🎯 Profit Margin % | 📦 Units Sold | 🔄 Return Rate % | 💵 Total Refunds |
|---|---|---|---|---|---|
| **₹2.76B** | **₹768.06M** | **27.86%** | **94,000+** | **5.48%** | **₹149.84M** |

| 👥 Unique Customers | 💳 Avg Spend / Customer | 🏬 Avg Revenue / Store | 🚀 YoY Growth (Monthly) | 🛡️ Data Audit Precision | ⚡ Processing Quality |
|---|---|---|---|---|---|
| **8,000** | **₹344.92K** | **₹137.83M** | **+108.33%** | **100% Audit Precision** | **95.2% Valid / 4.8% Quarantined** |

---

## 🌟 Architectural Highlights & Enterprise Patterns

1. **Hybrid Ingestion Engine (Failover Protection):**
   - Pulls customer master data from **Oracle CRM** and catalog inventories from **MySQL** via production JDBC drivers.
   - Automatically switches to local CSV snapshots if live databases are offline or credentials are missing, ensuring demo resilience without code modifications.

2. **In-Memory PySpark ETL & Quality Gates:**
   - Binds incoming tables to explicit `StructType` contracts, eliminating runtime `inferSchema` performance bottlenecks and preventing silent type-casting errors.
   - Evaluates 9 strict schema and domain validation rules per row. Corrupt records are routed to a **Dead-Letter Quarantine** (`data/rejected/`) alongside structured JSON audit logs instead of being silently dropped.

3. **Massively Parallel Cloud Warehousing (Amazon Redshift):**
   - Stages curated, compressed Parquet files in **AWS S3** (`s3://retailiq-curated/`).
   - Utilizes the native Redshift parallel `COPY` command to load data directly from S3, avoiding slow row-by-row `INSERT` operations.
   - Uses a Kimball Star Schema featuring 6 Dimension Tables (`DISTSTYLE ALL`) and 2 Fact Tables sorted by `transaction_date`.

4. **Automated Pipeline Orchestration & Circuit Breakers (Apache Airflow):**
   - Nightly scheduled DAG (`retailiq_daily_etl.py`) at 02:00 AM IST, managing multi-source task dependencies and idempotent retries.
   - **Automated Circuit Breaker:** If data rejection exceeds 10% of total volume, the DAG aborts execution, halting warehouse loads and dispatching alerts before reporting layers are contaminated.

5. **Analytical SQL Semantic Tier (Power BI Integration):**
   - Exposes 7 pre-aggregated analytical SQL views directly to Power BI.
   - Pushes joins, window functions, and aggregations upstream into Redshift to improve dashboard performance and reduce DAX calculation overhead.

---

## 🏗️ End-to-End Architecture Flow

**Stage 1: Multi-Source Ingestion**
- Daily POS flat feeds (`sales.csv`, `returns.csv`)
- Oracle CRM customer profiles via JDBC
- MySQL store and catalog tables via JDBC

⬇️

**Stage 2: PySpark ETL & In-Memory Validation Engine**
- Explicit `StructType` contracts without `inferSchema`
- 9 strict production validation rules evaluated per row
- Clean records pass through (95.2%)
- Corrupted rows isolated to `data/rejected/` (4.8%)

⬇️

**Stage 3: AWS S3 Staging Lake**
- Curated, columnar Parquet files
- Snappy compression format
- Partitioned S3 staging paths: `s3://retailiq-curated/`

⬇️

**Stage 4: Amazon Redshift Serverless Warehouse**
- Native parallel S3 `COPY` command
- Kimball Star Schema: 6 Dimensions (`DISTSTYLE ALL`) and 2 Fact Tables
- Compound Sort Keys based on `transaction_date`

⬇️

**Stage 5: Semantic Warehouse Views (Gold Tier)**
- `v_sales_enriched`
- `v_customer_rfm`
- `v_product_performance`
- Additional curated analytical SQL views

⬇️

**Stage 6: Power BI Business Intelligence**
- 5 interactive executive report pages
- Centralized DAX measures and business KPIs

---

## 🖥️ Power BI Interactive Analytics Suite (5 Pages)

- **Page 1: Executive Overview** — High-level business performance displaying revenue, profit margins, units sold, and year-over-year monthly benchmark comparisons (+108.33% growth).

- **Page 2: Sales & Stores** — State-by-state Indian revenue geospatial map, store-tier profitability (Flagship vs. Standard vs. Express), and a store-ranking matrix featuring automated **Red Alerts** for branches where `refund_rate > 8%`.

- **Page 3: Customer Insights (RFM Analytics)** — Behavioral segmentation into 7 tiers: *Champions, Loyal, Potential Loyalists, New Customers, About to Sleep, At Risk,* and *Lost*. Includes an At-Risk Retention Table with in-cell red data bars mapped to `recency_days`.

- **Page 4: Products & Assortment** — Ranks the Top 10 Revenue Generators against the Bottom 10 Profit Bleeders (potential delist candidates), includes category return matrices, and features a four-quadrant Margin vs. Revenue scatter plot.

- **Page 5: Promotions & Returns** — Audits markdown revenue uplift, identifies reverse logistics leakage (₹149.84M in refunds), and classifies return drivers: *Changed Mind, Damaged in Transit, Defective, Wrong Size,* and *Not as Described*.

---

## 📂 Project Structure

```text
retail-iq/
├── airflow/
│   └── dags/
│       └── retailiq_daily_etl.py
├── config/
│   ├── settings.py
│   └── .env.example
├── data/
│   ├── raw/
│   ├── curated/
│   └── rejected/
├── data_quality/
│   └── checks.py
├── docs/
│   ├── architecture_spec.md
│   └── RetailIQ_Pitch_Deck.pptx
├── etl/
│   ├── schemas.py
│   └── etl_pipeline.py
├── powerbi/
│   ├── RetailIQ_Dashboard.pbix
│   └── measures.dax
├── redshift/
│   ├── ddl/
│   │   ├── 01_dimensions.sql
│   │   └── 02_facts.sql
│   ├── analytics/
│   │   └── views.sql
│   └── load/
│       └── load_redshift.py
├── scripts/
│   └── generate_source_data.py
├── tests/
│   └── test_data_quality.py
└── requirements.txt
```

---

## 🚀 How to Run the Project Locally

### 1. Prerequisites & Environment Setup

Clone the repository and install the required Python packages:

```bash
git clone https://github.com/your-username/retail-iq.git
cd retail-iq
pip install -r requirements.txt
```

For cloud execution, copy `.env.example` to `.env` and configure your AWS, Redshift, Oracle, and MySQL credentials:

```bash
cp config/.env.example config/.env
```

### 2. Generate Synthetic Stress-Test Data

Generate 60,000+ realistic retail transaction rows with 2,200+ injected data-quality anomalies, including negative prices, missing foreign keys, and future dates.

```bash
python scripts/generate_source_data.py
```

### 3. Execute PySpark ETL & Quality Gates

Extract data from the configured sources, validate schemas and business rules, export quarantined records to `data/rejected/`, and write curated Snappy-compressed Parquet tables to `data/curated/`.

```bash
python etl/etl_pipeline.py
```

### 4. Deploy Redshift DDLs & Execute Parallel Loading

Apply the Star Schema DDLs and trigger bulk ingestion. In configured AWS environments, the loader uses S3-based Redshift `COPY`; local database loading depends on the implementation and configured connection.

```bash
python redshift/load/load_redshift.py
```

### 5. Compile Analytical Views & Verify Integrity

Compile the analytical SQL views and execute automated tests for data quality and table integrity.

```bash
pytest tests/ -v
```

---

## 👥 Project Team & Academic Credentials

**Use Case 5 — Retail Analytics Platform**

**Hackathon:** Cognizant NPN Hackathon / Industry Track

- **Chinmaya Kumar Rout (Chinu)** — Data Architect & Pipeline Lead  
  Responsible for overall pipeline design, stage decoupling, and hybrid architecture execution.
- **Punyasila** — Project Lead & Strategy
- **Sairush** — Data Ingestion Engineer
- **Priyabrata Lenka** — ETL & Data Quality Engineer
- **Om Priyadarshi** — Cloud Data Warehouse & DevOps Engineer
- **Aurokalyan** — BI Analyst & Visualization Lead

**Mentor:** Satyabrata Tripathy

**Institution:** Institute of Technical Education and Research (ITER)

**University:** Siksha 'O' Anusandhan (Deemed to be University), Bhubaneswar, Odisha

**Department:** Department of Computer Science & Engineering
