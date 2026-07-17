from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="hello_airflow_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    hello_bash = BashOperator(
        task_id="say_hello_bash",
        bash_command='echo "Hello Airflow from my Docker container"'
    )

    hello_python = PythonOperator(
        task_id="say_hello_python",
        python_callable=lambda: print("Hello Airflow from my Docker container")
    )

    hello_bash >> hello_python