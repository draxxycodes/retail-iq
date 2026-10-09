# RetailIQ - Enterprise Data Pipeline 🚀

**Cognizant NPN Hackathon - Use Case 5: Retail Sales Analytics & Customer Insights Platform**

RetailIQ is an end-to-end, production-ready data engineering pipeline designed to ingest, validate, transform, and load retail data into a cloud data warehouse for Business Intelligence. 

Unlike basic data-movement scripts, this project implements **enterprise-grade data engineering patterns** including Data Quality quarantines, hybrid JDBC/CSV ingestion, Massively Parallel Processing (MPP) loads via AWS S3, and Apache Airflow orchestration.

---

## 🌟 Key Features & Architecture

1. **Hybrid Ingestion Engine**: 
   * Reads customer data from **Oracle CRM** and master data from **MySQL** via JDBC.
   * Automatically falls back to local CSV extraction if database credentials are not present (perfect for local dev/demo).
2. **PySpark ETL & Data Quality Gate**: 
   * Transforms raw data into a Star Schema in-memory using Apache Spark.
   * Validates data against business rules (e.g., negative discounts, orphaned keys). Bad records are not silently dropped—they are **quarantined** in `data/rejected/` with a logged JSON reason code.
3. **Cloud-Native Redshift Loading**:
   * Stages curated Parquet/CSV files in **AWS S3**.
   * Uses the Redshift `COPY` command for massively parallel ingestion, rather than slow row-by-row `INSERT` statements.
4. **Apache Airflow Orchestration**:
   * Fully orchestrated DAG (`retailiq_etl_dag.py`) that handles dependencies, scheduling, and automatically halts the pipeline if the Data Quality rejection rate exceeds 10%.
5. **Analytics-Ready Views (Power BI)**:
   * Optimized SQL Views (using CTEs to avoid data duplication/Cartesian explosions) feed directly into a Power BI dashboard.

---

## 📂 Project Structure

```text
retail-iq/
├── airflow/dags/          # Apache Airflow orchestration DAG (retailiq_etl_dag.py)
├── config/                # Environment variables and pipeline settings (.env)
├── data/                  # Local data storage (raw, curated, rejected)
├── data_quality/          # PySpark Data Quality check rules
├── docs/                  # Architecture diagrams & Data Dictionaries
├── etl/                   # Core PySpark pipeline script (etl_pipeline.py)
├── powerbi/               # DAX measures and Dashboard Build Guide
├── redshift/              # Redshift DDLs, Views, and Python S3 Loader
├── scripts/               # Synthetic source data generator (for demo mode)
├── tests/                 # Pytest suite for Data Quality rules
└── requirements.txt       # Python dependencies
```

---

## 🚀 How to Run the Project Locally

### 1. Prerequisites & Setup
Make sure you have Python installed. Then install dependencies:
```bash
pip install -r requirements.txt
```
*Optional but recommended:* Copy `.env.example` to `.env` and fill in your AWS Redshift, S3, Oracle, and MySQL credentials.

### 2. Generate Demo Data (If DBs are offline)
If you don't have active Oracle/MySQL databases configured, generate synthetic source CSVs to use the pipeline's fallback mode:
```bash
python scripts/generate_source_data.py
```

### 3. Run the PySpark ETL
This step extracts data (via JDBC or CSV), runs data quality checks, quarantines bad rows, and writes curated files to `data/curated/`.
```bash
python etl/etl_pipeline.py
```

### 4. Load into Amazon Redshift
Applies DDLs and loads the curated data into Redshift. (This automatically uses S3 `COPY` if configured in `.env`, otherwise it safely falls back to direct `INSERT`).
```bash
python redshift/load/load_redshift.py
```

### 5. Verify Row Counts & Run Tests (Optional)
```bash
pytest tests/ -v
```

---

## 👥 Team & Acknowledgements
*Built for the Cognizant NPN Hackathon under the mentorship of Satyabrata Tripathy.*
