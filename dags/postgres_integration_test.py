from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'ronliwag',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='postgres_blank_db_demo',
    default_args=default_args,
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['postgres', 'demo'],
) as dag:

    # One simple task: sets up the table and injects 1 record right after
    setup_and_ingest = SQLExecuteQueryOperator(
        task_id='create_and_insert',
        conn_id='postgres_local',
        sql="""
            -- 1. Create a minimal table
            CREATE TABLE IF NOT EXISTS demo_table (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- 2. Inject exactly 1 record
            INSERT INTO demo_table (message) 
            VALUES ('Hello from Airflow to a blank database!');
        """
    )