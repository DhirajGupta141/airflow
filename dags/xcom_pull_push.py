from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='xcom_pull_push_demo',
    description='A simple DAG to demonstrate XCom pull and push',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['xcom', 'demo'],
) as dag:

    def push_to_xcom(**kwargs):
        # Push a value to XCom
        kwargs['ti'].xcom_push(key='my_key', value='Hello from the first task!')

    def pull_from_xcom(**kwargs):
        # Pull the value from XCom
        print(f"Pulling value from XCom... : {kwargs}")
        value = kwargs['ti'].xcom_pull(key='my_key', task_ids='push_task')
        print(f"Pulled value from XCom: {value}")

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_to_xcom,
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_from_xcom,
    )

    push_task >> pull_task
