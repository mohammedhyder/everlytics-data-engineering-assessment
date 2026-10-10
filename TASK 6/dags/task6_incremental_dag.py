from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from mohammedhyder.assessment.task6_incremental_functions import (
    run_incremental_ingestion,
)

with DAG(
    dag_id="mohammedhyder_task6_incremental_etl",
    start_date=datetime(2026, 10, 10),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["task6", "incremental", "minio", "doris"],
    description="Process only new MinIO CSV objects and track each file in Doris.",
) as dag:
    ingest_new_files = PythonOperator(
        task_id="process_new_files",
        python_callable=run_incremental_ingestion,
    )
