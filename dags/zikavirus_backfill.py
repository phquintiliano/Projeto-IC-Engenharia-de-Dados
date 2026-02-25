from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion import ingestionZikaVirus


def extract_backfill(**context):
    year = context["data_interval_start"].year
    ingestionZikaVirus.run(year)


current_year = datetime.now().year

with DAG(
    dag_id="zikavirus_backfill",
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
