from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion import ingestionDengue


def extract_live(**context):
    year = context["data_interval_start"].year
    if year != datetime.now().year:
        print("[SKIP] Live não roda para ano diferente do atual.")
        return
    ingestionDengue.run_ingestion_year(year)


with DAG(
    dag_id="dengue_live",
    start_date=datetime(2025, 11, 15, 9, 0),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="extract_live_current_year",
        python_callable=extract_live,
    )
