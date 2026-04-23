from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ingestion.ingestionZikaVirus import run_zika_year


def executar_live(**context):
    ano = context["logical_date"].year
    run_zika_year(ano)


with DAG(
    dag_id="zikavirus_live",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task_live = PythonOperator(
        task_id="ingest_zika_live",
        python_callable=executar_live,
    )