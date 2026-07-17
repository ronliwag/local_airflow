from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def _task_a():  
    print("Task A is running...")

@dag(
    schedule="@daily", 
    start_date=datetime(2024, 1, 1), 
    description="This dag does...", 
    tags=["team_a", "source_a"], 
    max_consecutive_failed_dag_runs=3, 
    catchup=False
    )

def my_dag():
    
    task_a = PythonOperator(
        task_id="task_a",
        python_callable=_task_a
    )

    @task
    def task_b():
        print("Task B is running...")

    task_a >> task_b()


my_dag()