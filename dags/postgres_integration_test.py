from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'ronliwag',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='postgres_integration_demo',
    default_args=default_args,
    description='A DAG to test connection and run queries on local PostgreSQL',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['bootcamp', 'postgres'],
) as dag:

    # Task 1: Create the demo table in the public schema
    create_table = SQLExecuteQueryOperator(
        task_id='create_bootcamp_table',
        conn_id='postgres_local',
        sql="""
            CREATE TABLE IF NOT EXISTS public.bootcamp_progress (
                task_id VARCHAR(50) PRIMARY KEY,
                task_name VARCHAR(100),
                status VARCHAR(20),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # Task 2: Insert a success record
    insert_record = SQLExecuteQueryOperator(
        task_id='insert_sample_record',
        conn_id='postgres_local',
        sql="""
            INSERT INTO public.bootcamp_progress (task_id, task_name, status)
            VALUES ('task_5', 'Configure Local PostgreSQL & Airflow Connection', 'Success')
            ON CONFLICT (task_id) 
            DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP;
        """
    )

    create_table >> insert_record