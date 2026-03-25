from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.ingestionZikaVirus import run_zika_pipeline


def _run_backfill():
    return run_zika_pipeline(
        raw_prefix="raw/zika/backfill/",
        silver_prefix="silver/zika/backfill/",
        record_source="minio_raw_zika_backfill",
    )


with DAG(
    dag_id="zikavirus_backfill",
    start_date=datetime(2020, 1, 1),
    schedule="@yearly",
    catchup=False,
    tags=["zika", "backfill", "silver", "dv"],
) as dag:
    PythonOperator(
        task_id="process_zika_backfill",
        python_callable=_run_backfill,
    )
