from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Importa o módulo de ingestão
from ingestion import ingestionZikaVirus


def extract_data_zikavirus():
    # aqui você decide o ano (fixo, ou pegar de variável, etc.)
    ano = 2020
    ingestionZikaVirus.run_ingestion(ano)
    # se não quiser criar run_ingestion, você pode chamar as funções direto:
    # df = ingestionZikaVirus.fetch_ano(ano)
    # ingestionZikaVirus.save_parquet_to_minio(
    #     df,
    #     bucket="datalake",
    #     key=f"raw/zikavirus/ano={ano}/zikavirus_{ano}.parquet",
    # )


with DAG(
    dag_id="zikaVirus",
    start_date=datetime(year=2025, month=11, day=15, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_zikavirus",
        python_callable=extract_data_zikavirus,
    )
