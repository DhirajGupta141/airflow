from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging

# Configuration
API_URL = "http://fastapi-app:5000/getAll"
OUTPUT_DIR = "/opt/airflow/output_files"

default_args = {
    'owner': 'Dhiraj Gupta Data Engineer',
    'retries': 1, # Number of times to retry if the task fails
    'retry_delay': timedelta(minutes=1),  # if the daf fails, retry after 5 minutes
}

def fetch_and_save_api_data(**kwargs):

    logging.info("Starting API data fetch task")
    logging.info(f"Received kwargs: {kwargs}")


    # 1. Extract the interval markers from kwargs (The "Incremental" Logic)
    # We use these to tell the API what slice of data we want
    start_ts = kwargs['data_interval_start']
    end_ts = kwargs['data_interval_end']
    
    # 2. Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logging.info(f"Created directory: {OUTPUT_DIR}")

    # 2. Format them to YYYY-MM-DD as required by your API
    formatted_start = start_ts.strftime('%Y-%m-%d')
    formatted_end = end_ts.strftime('%Y-%m-%d')
    
    # 3. Prepare the request payload
    # Note: If your API expects JSON in the body, use json=payload
    # If it expects URL parameters, use params=payload
    payload = {
        "start_date": formatted_start,
        "end_date": formatted_end
    }

    headers = {
    'accept': 'application/json',
    'Authorization': 'Basic YWRtaW46bWFuaXNo',
    'Content-Type': 'application/json'
    }

    logging.info(f"Fetching data from {API_URL} for interval {formatted_start} to {formatted_end}")
    logging.info(f"Request payload: {payload}")

    try:
        response = requests.request("POST", API_URL, headers=headers, json=payload)
        response.raise_for_status() # Raise error for 4xx/5xx responses
        data = response.json()
        logging.info(f"Received {len(data)} records from API")

    except Exception as e:
        logging.error(f"API Request failed: {e}")
        raise

    # 4. Save to a unique file based on the logical date
    # Using 'ds_nodash' (YYYYMMDD) or a timestamp ensures files don't collide
    file_timestamp = start_ts.strftime('%Y%m%d')
    file_path = os.path.join(OUTPUT_DIR, f"data_{file_timestamp}.json")

    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    logging.info(f"Successfully saved {len(data)} records to {file_path}")

# Define the DAG
with DAG(
    dag_id='incremental_load_using_api_backfill',
    default_args=default_args,
    schedule='@daily', # Or '@daily' depending on your needs
    start_date=datetime(2026, 4, 1),
    catchup=True, # Set to True to backfill from the start date
    tags=['api', 'incremental', 'local_storage']
) as dag:

    incremental_api_task = PythonOperator(
        task_id='fetch_api_data_task',
        python_callable=fetch_and_save_api_data,
        # Airflow 3.0+ provides the context to kwargs automatically
    )