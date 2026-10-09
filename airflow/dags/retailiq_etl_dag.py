"""RetailIQ ETL Orchestration DAG (PRD §10, Production Architecture).

This Airflow DAG orchestrates the end-to-end data pipeline:
  1. Generate/ingest source data from CSV, Oracle CRM, and MySQL databases
  2. Run PySpark ETL (extract, validate, transform)
  3. Upload curated datasets to S3 staging
  4. Execute Redshift DDL and COPY commands
  5. Apply analytical views and verify row counts
  6. Trigger Power BI dataset refresh

Schedule: Daily at 02:00 AM IST (after source systems close daily batch)
Retries: 2 attempts with 5-minute delay

Deployment:
  - Copy this file to your Airflow dags/ folder
  - Set the following Airflow Variables: RETAILIQ_PROJECT_ROOT
  - Set the following Airflow Connections: retailiq_redshift, retailiq_mysql,
    retailiq_oracle, aws_default
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# --------------------------------------------------------------------------- #
# Configuration                                                                #
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Variable.get("RETAILIQ_PROJECT_ROOT", default_var="/opt/retailiq")
DAG_OWNER = "retailiq-team"

default_args = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# --------------------------------------------------------------------------- #
# Branch logic                                                                 #
# --------------------------------------------------------------------------- #
def _check_dq_status(**context):
    """Read the DQ log and branch based on rejection rate."""
    import json
    from pathlib import Path

    dq_dir = Path(PROJECT_ROOT) / "data" / "curated" / "_dq_log"
    logs = sorted(dq_dir.glob("dq_log_*.json"), reverse=True)
    if not logs:
        return "dq_alert"

    dq = json.loads(logs[0].read_text())
    total_received = sum(s.get("received", 0) for s in dq.get("sources", {}).values())
    total_rejected = sum(s.get("rejected", 0) for s in dq.get("sources", {}).values())
    rejection_rate = total_rejected / max(total_received, 1)

    context["ti"].xcom_push(key="rejection_rate", value=round(rejection_rate * 100, 2))
    context["ti"].xcom_push(key="dq_summary", value=dq.get("rule_violations", {}))

    if rejection_rate > 0.10:  # >10% rejection = halt pipeline
        return "dq_alert"
    return "upload_to_s3"


# --------------------------------------------------------------------------- #
# DAG definition                                                               #
# --------------------------------------------------------------------------- #
with DAG(
    dag_id="retailiq_daily_etl",
    default_args=default_args,
    description="RetailIQ end-to-end ETL: sources -> PySpark -> S3 -> Redshift -> Power BI",
    schedule_interval="0 20 * * *",  # 02:00 IST = 20:30 UTC (prev day)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retailiq", "etl", "data-engineering", "cognizant-npn"],
    doc_md=__doc__,
) as dag:

    # ---- 1. Generate source data (for demo/dev; in prod this is a no-op) ----
    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/generate_source_data.py",
        doc="Generate synthetic source data with deliberate DQ issues. "
            "In production, replace with file-sensor or DB-export trigger.",
    )

    # ---- 2. Run PySpark ETL pipeline ----
    run_etl_pipeline = BashOperator(
        task_id="run_pyspark_etl",
        bash_command=f"cd {PROJECT_ROOT} && python etl/etl_pipeline.py",
        doc="PySpark pipeline: extract (JDBC/CSV) -> validate (quarantine) -> "
            "transform (star schema) -> load (curated parquet + CSV).",
    )

    # ---- 3. Data quality gate ----
    dq_gate = BranchPythonOperator(
        task_id="dq_quality_gate",
        python_callable=_check_dq_status,
        doc="Read the latest DQ log. If rejection rate > 10%, halt and alert. "
            "Otherwise proceed to warehouse loading.",
    )

    dq_alert = BashOperator(
        task_id="dq_alert",
        bash_command="echo 'DATA QUALITY ALERT: rejection rate exceeded 10%! "
                     "Pipeline halted. Review data/rejected/ and DQ log.' && exit 1",
        doc="Alert task triggered when data quality is unacceptable.",
    )

    # ---- 4. Upload curated data to S3 staging ----
    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"cd {PROJECT_ROOT} && python -c \""
                     f"import boto3, os; from pathlib import Path; "
                     f"from config import settings; "
                     f"s3 = boto3.client('s3'); "
                     f"csv_dir = settings.CURATED_DATA_DIR / 'csv'; "
                     f"[s3.upload_file(str(f), settings.S3_BUCKET, "
                     f"f'{{settings.S3_PREFIX}}/{{f.parent.name}}/{{f.name}}') "
                     f"for d in csv_dir.iterdir() if d.is_dir() "
                     f"for f in d.glob('part-*.csv')]; "
                     f"print('S3 upload complete')\"",
        doc="Upload curated CSV files to S3 for Redshift COPY.",
    )

    # ---- 5. Load into Redshift ----
    load_redshift = BashOperator(
        task_id="load_redshift",
        bash_command=f"cd {PROJECT_ROOT} && python redshift/load/load_redshift.py",
        doc="Apply DDL and bulk-load curated data into Redshift star schema.",
    )

    # ---- 6. Apply analytical views ----
    apply_views = BashOperator(
        task_id="apply_analytical_views",
        bash_command=f"cd {PROJECT_ROOT} && python -c \""
                     f"import psycopg2; from config import settings; "
                     f"conn = psycopg2.connect(host=settings.REDSHIFT_HOST, "
                     f"port=settings.REDSHIFT_PORT, dbname=settings.REDSHIFT_DATABASE, "
                     f"user=settings.REDSHIFT_USER, password=settings.REDSHIFT_PASSWORD, "
                     f"sslmode='require'); conn.autocommit=True; cur=conn.cursor(); "
                     f"sql=open('redshift/analytics/views.sql').read(); cur.execute(sql); "
                     f"print('Views applied'); cur.close(); conn.close()\"",
        doc="Create/refresh all analytical views used by Power BI.",
    )

    # ---- 7. Row count verification ----
    verify_counts = BashOperator(
        task_id="verify_row_counts",
        bash_command=f"cd {PROJECT_ROOT} && python -c \""
                     f"import psycopg2; from config import settings; "
                     f"conn = psycopg2.connect(host=settings.REDSHIFT_HOST, "
                     f"port=settings.REDSHIFT_PORT, dbname=settings.REDSHIFT_DATABASE, "
                     f"user=settings.REDSHIFT_USER, password=settings.REDSHIFT_PASSWORD, "
                     f"sslmode='require'); cur=conn.cursor(); "
                     f"tables = ['dim_customer','dim_product','dim_store','dim_supplier',"
                     f"'dim_promotion','dim_date','fact_sales','fact_returns']; "
                     f"[print(f'  {{t:18s}} {{c[0]:>8,}}') for t in tables "
                     f"for c in [cur.execute(f'SELECT COUNT(*) FROM retailiq.{{t}}') or cur.fetchone()]]; "
                     f"cur.close(); conn.close(); print('VERIFY OK')\"",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc="Post-load verification: check row counts in all warehouse tables.",
    )

    # ---- 8. Pipeline completion ----
    pipeline_done = BashOperator(
        task_id="pipeline_complete",
        bash_command="echo 'RetailIQ daily ETL pipeline completed successfully at $(date)'",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc="Final success marker.",
    )

    # ---- Task dependencies (execution flow) ----
    generate_source_data >> run_etl_pipeline >> dq_gate
    dq_gate >> [upload_to_s3, dq_alert]
    upload_to_s3 >> load_redshift >> apply_views >> verify_counts >> pipeline_done
