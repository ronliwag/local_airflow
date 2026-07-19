from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLValueCheckOperator

default_args = {
    'owner': 'ronliwag',
}

with DAG(
    dag_id='postgres_quality_check_demo',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['postgres', 'demo'],
) as dag:
    # Task 1: Basic row count or null constraint check
    check_null_titles_raw = SQLValueCheckOperator(
        task_id="check_null_titles_raw",
        conn_id="postgres_netflix",
        pass_value=0, # The DAG will pass ONLY if the query returns 0
        retries=3,
        retry_delay=timedelta(seconds=5),
        sql="""
            SELECT COUNT(*) 
            FROM public.staging_raw 
            WHERE title IS NULL;
        """,
    )

    check_null_titles_cleaned = SQLValueCheckOperator(
        task_id="check_null_titles_cleaned",
        conn_id="postgres_netflix",
        pass_value=0, # The DAG will pass ONLY if the query returns 0
        retries=3,
        retry_delay=timedelta(seconds=5),
        sql="""
            SELECT COUNT(*) 
            FROM public.clean_data 
            WHERE title IS NULL;
        """,
    )

    # Task 2: Primary key uniqueness check (no duplicates)
    check_duplicates_raw = SQLValueCheckOperator(
    task_id="check_duplicate_shows_raw",
        conn_id="postgres_netflix",
        pass_value=0, # The DAG will pass ONLY if the query returns 0
        retries=3,
        retry_delay=timedelta(seconds=5),
        sql="""
            SELECT COUNT(show_id) - COUNT(DISTINCT show_id) 
            FROM public.staging_raw;
    """,
    )

    check_duplicates_cleaned = SQLValueCheckOperator(
    task_id="check_duplicate_shows_cleaned",
        conn_id="postgres_netflix",
        pass_value=0, # The DAG will pass ONLY if the query returns 0
        retries=3,
        retry_delay=timedelta(seconds=5),
        sql="""
            SELECT COUNT(show_id) - COUNT(DISTINCT show_id) 
            FROM public.clean_data;
    """,
    )

    check_null_titles_raw, check_duplicates_raw, check_null_titles_cleaned, check_duplicates_cleaned