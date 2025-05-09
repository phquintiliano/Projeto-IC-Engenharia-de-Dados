from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_spark_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Spark"],
) as dag:

    rodar_spark = BashOperator(
        task_id="executar_hello_spark",
        bash_command="spark-submit --master spark://spark-master:7077 /opt/airflow/data/hello_spark.py",
    )
