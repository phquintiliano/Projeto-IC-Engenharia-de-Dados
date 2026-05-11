from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime

from ingestion.ingestionZikaVirus import run_zika_year
from silver.zika.pipeline import run_zika_silver


def execute_live(**context):
    ano = context["logical_date"].year
    return run_zika_year(ano)


def has_new_data(**context):
    result = context["ti"].xcom_pull(task_ids="ingest_zika_live")
    return bool(result and result.get("has_new_data"))


def execute_silver(**context):
    result = context["ti"].xcom_pull(task_ids="ingest_zika_live") or {}
    new_keys = result.get("new_keys", [])
    return run_zika_silver(keys=new_keys)


with DAG(
    dag_id="zikavirus_live",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    ingest_zika_live = PythonOperator(
        task_id="ingest_zika_live",
        python_callable=execute_live,
    )

    check_has_new_data = ShortCircuitOperator(
        task_id="check_has_new_data",
        python_callable=has_new_data,
    )

    run_silver_zika = PythonOperator(
        task_id="run_silver_zika",
        python_callable=execute_silver,
    )

    ingest_zika_live >> check_has_new_data >> run_silver_zika
