from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import requests
import json
import logging
from datetime import datetime

# ==============================================================================
# Airflow DAG Configuration
# ==============================================================================

# Fill in your GCP parameters
GCP_PROJECT_ID = '{GCP_PROJECT_ID}'
GCS_BUCKET = '{BUCKET_NAME}'
BQ_DATASET = '{BQ_DATASET}'
BQ_TABLE = '{TABLE_NAME}'
BQ_LOCATION = '{BQ_LOCATION}'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# ==============================================================================
# Python Function to be called by PythonOperator
# ==============================================================================

def _fetch_and_upload_to_gcs(bucket_name, **context):
    """
    Fetches product data from a public API, formats it as newline-delimited JSON,
    and uploads it to Google Cloud Storage.
    """
    logging.info("Fetching data from public API...")
    # Updated to a different, freely available API
    url = 'https://fakestoreapi.com/products'
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    # Prepare data as newline-delimited JSON for BigQuery
    nl_json_lines = '\n'.join(json.dumps(row) for row in data)
    
    # Generate a unique filename based on the execution date
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"product_data_{timestamp}.json"
    
    logging.info(f"Uploading data to gs://{bucket_name}/{filename}")
    
    # Initialize GCS client and upload the data
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(nl_json_lines, content_type='application/json')
    
    gcs_uri = f"gs://{bucket_name}/{filename}"
    logging.info(f"Successfully uploaded data. GCS URI: {gcs_uri}")
    
    # Return the GCS URI. Airflow will automatically push this to XCom.
    return gcs_uri

# ==============================================================================
# DAG Definition
# ==============================================================================

with DAG(
    dag_id='api_to_bigquery_dag',
    default_args=default_args,
    description='Extracts data from a public API and loads it into BigQuery.',
    schedule_interval=None, # This DAG runs on a manual trigger
    tags=['gcp', 'bigquery', 'api', 'etl'],
) as dag:
    
    # Task 1: Fetch data from the API and upload it to GCS
    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload_to_gcs',
        python_callable=_fetch_and_upload_to_gcs,
        op_kwargs={'bucket_name': GCS_BUCKET},
    )
    
    # Task 2: Load the data from GCS into BigQuery
    load_to_bq_task = BigQueryInsertJobOperator(
        task_id='load_to_bigquery'  ,
        configuration={
            "load": {
                # Use Jinja to pull the GCS URI from the previous task's XCom
                "sourceUris": ["{{ task_instance.xcom_pull(task_ids='fetch_and_upload_to_gcs') }}"],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": True,
                "writeDisposition": "WRITE_APPEND",
            }
        },
        location=BQ_LOCATION,
    )
    
    # Define the task dependencies
    fetch_and_upload_task >> load_to_bq_task
