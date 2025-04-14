from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_world():
    print("👋 Olá, mundo! Aqui é o Airflow!")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # executa manualmente
    catchup=False,
    tags=["teste"],
) as dag:

    tarefa_hello = PythonOperator(
        task_id="imprimir_hello",
        python_callable=hello_world,
    )
