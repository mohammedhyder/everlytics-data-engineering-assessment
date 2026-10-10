from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from mohammedhyder.assessment.task1_tasks import (
    check_input_task,
    load_staging_task,
    transform_task,
    load_target_task,
    validate_task,
)


# --------------------------------------------------
# TASK 1 ETL DAG
# --------------------------------------------------

with DAG(
    dag_id="mohammedhyder_task1_etl",

    start_date=datetime(
        2026,
        10,
        8
    ),

    schedule=None,

    catchup=False,

    tags=[
        "task1",
        "etl",
        "minio",
        "doris",
    ],
) as dag:

    # --------------------------------------------------
    # CHECK INPUT
    # --------------------------------------------------

    check_input = PythonOperator(
        task_id="check_input",
        python_callable=check_input_task,
    )

    # --------------------------------------------------
    # LOAD STAGING
    # --------------------------------------------------

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging_task,
    )

    # --------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    # --------------------------------------------------
    # LOAD TARGET
    # --------------------------------------------------

    load_target = PythonOperator(
        task_id="load_target",
        python_callable=load_target_task,
    )

    # --------------------------------------------------
    # VALIDATE
    # --------------------------------------------------

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
    )

    # --------------------------------------------------
    # TASK DEPENDENCIES
    # --------------------------------------------------

    check_input >> load_staging >> transform >> load_target >> validate