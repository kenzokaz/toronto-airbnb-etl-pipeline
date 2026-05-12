"""
airbnb_etl_dag.py
-----------------
Orchestrates the full Toronto Airbnb ETL pipeline:
  extract → transform → validate → notify

Schedule: weekly (every Sunday at 02:00)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "kenzo",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="airbnb_etl_pipeline",
    description="End-to-end ETL for Toronto Airbnb data → PostgreSQL warehouse",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * 0",   # weekly, Sunday 02:00
    catchup=False,
    tags=["airbnb", "etl", "cosc4606"],
) as dag:

    # ── Task 1: Extract ────────────────────────────────────────────────────────
    def extract_task(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/services")
        from extractor.extractor import run
        run()
        context["ti"].xcom_push(key="extract_status", value="success")

    extract = PythonOperator(
        task_id="extract_listings",
        python_callable=extract_task,
        provide_context=True,
    )

    # ── Task 2: Transform ──────────────────────────────────────────────────────
    def transform_task(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/services")
        from transformer.transformer import run
        run()
        context["ti"].xcom_push(key="transform_status", value="success")

    transform = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_task,
        provide_context=True,
    )

    # ── Task 3: Validate row counts ────────────────────────────────────────────
    def validate_task(**context):
        import os, psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            dbname=os.getenv("DB_NAME", "airbnb_dw"),
            user=os.getenv("DB_USER", "airflow"),
            password=os.getenv("DB_PASSWORD", "airflow"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM staging.listings_raw")
            raw_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM warehouse.fact_listings")
            fact_count = cur.fetchone()[0]
        conn.close()

        retention = fact_count / raw_count if raw_count else 0
        print(f"Raw rows: {raw_count:,} | Warehouse rows: {fact_count:,} | Retention: {retention:.1%}")

        if retention < 0.70:
            raise ValueError(f"Data quality check FAILED — retention {retention:.1%} < 70%")

        return "validation_passed" if retention >= 0.70 else "validation_failed"

    validate = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_task,
        provide_context=True,
    )

    # ── Task 4a / 4b: Branch on validation ────────────────────────────────────
    passed  = EmptyOperator(task_id="pipeline_success")
    failed  = EmptyOperator(task_id="pipeline_failed")

    def branch_on_validation(**context):
        ti = context["ti"]
        result = ti.xcom_pull(task_ids="validate_quality")
        return "pipeline_success" if result == "validation_passed" else "pipeline_failed"

    branch = BranchPythonOperator(
        task_id="branch_validation",
        python_callable=branch_on_validation,
        provide_context=True,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    extract >> transform >> validate >> branch >> [passed, failed]
