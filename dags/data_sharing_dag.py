from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

# Data to be shared between tasks
payload_template = {"token_id": "Token123", "source": "", "data": "Airflow Bootcamp"}

# MODERN WAY: TaskFlow API (@dag and @task decorators)
@dag(
    dag_id="xcom_demo_taskflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootcamp", "xcom"],
)
def taskflow_dag():
    # Task A: Explicitly returns a value, which Airflow automatically pushes to XCom
    @task(task_id="taskflow_sender")
    def push_metadata():
        payload = payload_template.copy()
        payload["source"] = "taskflow_sender"
        return payload

    # Task B: Automatically pulls the value by accepting it as a function argument
    @task(task_id="taskflow_receiver")
    def pull_metadata(received_data):
        print("Received data in TaskFlow API:")
        print(f"Source: {received_data.get('source')}")
        print(f"Token ID: {received_data.get('token_id')}")
        print(f"Data: {received_data.get('data')}")

    # Establish the implicit data flow and task dependency
    data_flow = push_metadata()
    pull_metadata(data_flow)

taskflow_dag_instance = taskflow_dag()



# CLASSIC WAY: Traditional Operators & Task Instance (ti)
def classic_push_callable(ti):
    payload = payload_template.copy()
    payload["source"] = "classic_sender"
    # Pushing explicitly with a custom key
    ti.xcom_push(key="bootcamp_custom_key", value=payload)

def classic_pull_callable(ti):
    # Pulling explicitly matching the task_id and key from above
    fetched_payload = ti.xcom_pull(task_ids="classic_sender", key="bootcamp_custom_key")
    print("Received data in TI way:")
    print(f"Source: {fetched_payload.get('source')}")
    print(f"Token ID: {fetched_payload.get('token_id')}")
    print(f"Data: {fetched_payload.get('data')}")

with DAG(
    dag_id="xcom_demo_classic",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootcamp", "xcom"],
) as classic_dag:

    # Task A: Pushes data via the context variable
    classic_sender = PythonOperator(
        task_id="classic_sender",
        python_callable=classic_push_callable,
    )

    # Task B: Pulls data via the context variable
    classic_receiver = PythonOperator(
        task_id="classic_receiver",
        python_callable=classic_pull_callable,
    )

    # Establish explicit task dependency
    classic_sender >> classic_receiver