from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime

from ingestion.ingestionZikaVirus import run_zika_range
from silver.zika.pipeline import run_zika_silver


def executar_backfill():
    return run_zika_range(2020, 2024)


def has_new_data(**context):
    results = context["ti"].xcom_pull(task_ids="ingest_zika_backfill") or []
    return any(result.get("has_new_data") for result in results)


def executar_silver_backfill(**context):
    results = context["ti"].xcom_pull(task_ids="ingest_zika_backfill") or []
    new_keys: list[str] = []

    for result in results:
        new_keys.extend(result.get("new_keys", []))

    unique_keys = list(dict.fromkeys(new_keys))
    return run_zika_silver(keys=unique_keys)


with DAG(
    dag_id="zikavirus_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    task_backfill = PythonOperator(
        task_id="ingest_zika_backfill",
        python_callable=executar_backfill,
    )

    check_has_new_data = ShortCircuitOperator(
        task_id="check_has_new_data",
        python_callable=has_new_data,
    )

    run_silver_zika = PythonOperator(
        task_id="run_silver_zika",
        python_callable=executar_silver_backfill,
    )

    task_backfill >> check_has_new_data >> run_silver_zika
