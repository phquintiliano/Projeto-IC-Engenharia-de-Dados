from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion import ingestionChikungunya


def extract_backfill(**context):
    year = context["data_interval_start"].year
    ingestionChikungunya.run(year)


current_year = datetime.now().year

with DAG(
    dag_id="chikungunya_backfill",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(current_year, 1, 1),
    schedule_interval="@yearly",
    catchup=True,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="extract_backfill_year",
        python_callable=extract_backfill,
    )
