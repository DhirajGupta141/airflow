from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_logic(**kwargs):
    """
    Using kwargs to access the Airflow Context.
    In 3.0, 'data_interval_start' is the most reliable way to 
    identify the beginning of your incremental window.
    """
    # Pulling the window bounds from the context
    start_time = kwargs['data_interval_start']
    end_time = kwargs['data_interval_end']
    
    logging.info(f"Extracting data created between {start_time} and {end_time}")
    
    # Example SQL: 
    # SELECT * FROM orders WHERE updated_at >= '{{ data_interval_start }}'
    
    # Logic to push data to XCom for the next task
    return [{"id": 101, "status": "shipped"}]

def load_logic(**kwargs):
    # Pulling data from the previous task via XCom
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='extract_incremental_task')
    
    if not records:
        logging.info("No records found to load.")
        return

    logging.info(f"Loading {len(records)} records into the destination.")

# Defining the DAG
with DAG(
    dag_id='traditional_incremental_load_v3',
    default_args=default_args,
    description='Incremental load using traditional PythonOperators',
    schedule='@hourly',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['legacy_style', 'incremental'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_incremental_task',
        python_callable=extract_logic,
        # op_kwargs can be used to pass extra static data
    )

    load_task = PythonOperator(
        task_id='load_incremental_task',
        python_callable=load_logic,
    )

    # Old fashion dependency setting
    extract_task >> load_task