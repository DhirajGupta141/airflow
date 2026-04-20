from airflow.decorators import dag , task
from datetime import datetime, timedelta
import logging


@dag(
    dag_id='taskflow_api_demo',
    description='A simple DAG to demonstrate TaskFlow API',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['taskflow', 'demo'],
)
def taskflow_api_demo():
    @task
    def extract():
        logging.info("Extracting data...")
        return {"data": [1, 2, 3, 4, 5]}

    @task
    def transform(extracted_data):
        logging.info(f"Transforming data: {extracted_data}")
        transformed = [x * 2 for x in extracted_data['data']]
        return transformed
    @task
    def load(transformed_data):
        logging.info(f"Loading data: {transformed_data}")
        # Here you would typically load the data to a database or another destination

    extracted = extract()
    transformed = transform(extracted)
    load(transformed)

taskflow_api_demo_dag = taskflow_api_demo()






