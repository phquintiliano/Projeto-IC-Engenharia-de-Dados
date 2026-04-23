from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingestion import ingestionFebreAmarela


def extract_data_febre_amarela():
    ingestionFebreAmarela.run_ingestion()


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}

with DAG(
    dag_id="febreAmarela",
    start_date=datetime(2025, 11, 15, 9, 0),
    schedule_interval="0 0 * * 0",  # domingo 00:00
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    render_template_as_native_obj=True,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_febre_amarela",
        python_callable=extract_data_febre_amarela,
    )
