"""
Starter DAG — DataOps & MLOps Training (Module 2, Sessions 3-5)

A minimal working pipeline to confirm the Airflow environment is set up
correctly, and to give participants a template to extend during labs.

Once this file is saved into the mounted `dags/` folder, it appears in
the Airflow UI at http://localhost:8085 within ~30 seconds (scheduler
polling interval) — no restart needed.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    print("Pretending to pull data from a source system...")
    return {"rows": 100}


def validate(**context):
    data = context["ti"].xcom_pull(task_ids="extract")
    print(f"Validating {data['rows']} rows...")
    assert data["rows"] > 0, "No rows found!"


def load(**context):
    print("Loading validated data into the warehouse...")


with DAG(
    dag_id="starter_pipeline",
    description="Day-1 verification pipeline for the DataOps & MLOps course",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # manual trigger only — no auto-scheduling
    catchup=False,
    tags=["dataops-mlops", "module2", "starter"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    validate_task = PythonOperator(task_id="validate", python_callable=validate)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> validate_task >> load_task
