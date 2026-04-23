from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ingestion.ingestionZikaVirus import run_zika_range


def executar_backfill():
    run_zika_range(2015, 2024)


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