from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.ingestionZikaVirus import run_zika_pipeline


def _run_live():
    return run_zika_pipeline(
        raw_prefix="raw/zika/live/",
        silver_prefix="silver/zika/live/",
        record_source="minio_raw_zika_live",
    )


with DAG(
    dag_id="zikavirus_live",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["zika", "silver", "dv"],
) as dag:
    PythonOperator(
        task_id="process_zika_live",
        python_callable=_run_live,
    )
