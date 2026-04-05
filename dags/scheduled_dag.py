from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def my_task():
    print("DAG running every 2 minutes!")

with DAG(
    dag_id="run_every_2_minutes",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",   # Airflow 3.x syntax
    catchup=False,
    description="A DAG that runs every 2 minutes",
) as dag:

    run_task = PythonOperator(
        task_id="print_message",
        python_callable=my_task
    )
