from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standards.operators.bash import BashOperator


def say_hello(**kwargs):
    print(f"Keyword arguments passed to the task: {kwargs} ")

    print("Hello Airflow! Your DAG is running successfully 😊")

def welcome_dag(**kwargs):
    print(f"Keyword arguments passed to the task: {kwargs} ")
    print("Welcome to your first DAG! 🎉")

# Default DAG settings
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id="my_first_dag",
    default_args=default_args,
    start_date=datetime(2024, 4, 2),
    catchup=False
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=say_hello
    )

    welcome_task = PythonOperator(
        task_id="welcome_message", 
        python_callable=welcome_dag
    )

    hello_task >> welcome_task   # task Dependency: hello_task runs before welcome_task
