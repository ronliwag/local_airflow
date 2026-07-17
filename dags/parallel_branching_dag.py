from datetime import datetime
import random
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Define python functions for our tasks
def extract_data():
    print("Extracting raw dataset...")

# Our 2 Parallel Processing Functions
def process_movies():
    print("Processing movie datasets in parallel...")

def process_shows():
    print("Processing TV show datasets in parallel...")

def choose_branch():
    chosen = random.choice(["path_a", "path_b"])
    print(f"Branching decision made. Proceeding with: {chosen}")

with DAG(
    dag_id="parallel_tasks",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["chapter3", "task4"]
) as dag:

    # 1. Start with sequential Extraction
    task_extract = PythonOperator(
        task_id="extract_source_data",
        python_callable=extract_data
    )

    # 2. The 2 Parallel Tasks
    task_movies = PythonOperator(
        task_id="process_movies_parallel",
        python_callable=process_movies
    )

    task_shows = PythonOperator(
        task_id="process_shows_parallel",
        python_callable=process_shows
    )

    # 3. Branching Decision
    task_branch_decision = PythonOperator(
        task_id="decide_branch_path",
        python_callable=choose_branch
    )

    end = EmptyOperator(task_id="end")

    # Define Pipeline Flow
    # Extract runs first.
    # Then, Movies and Shows run concurrently in parallel.
    # Once both finish, it proceeds to the branching decision.
    task_extract >> [task_movies, task_shows] >> task_branch_decision >> end