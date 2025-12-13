from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion import ingestionChikungunya


def extract_data_chikungunya():
    ano_inicio = 2020
    ano_fim = 2025
    ingestionChikungunya.run_ingestion_range(start_year=ano_inicio, end_year=ano_fim)


with DAG(
    dag_id="chikungunya",
    start_date=datetime(year=2025, month=11, day=15, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_chikungunya",
        python_callable=extract_data_chikungunya,
    )
