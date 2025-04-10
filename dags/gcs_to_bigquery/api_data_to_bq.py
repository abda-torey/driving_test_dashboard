from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import datetime
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("DATASET_ID")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = os.getenv("DATASET_ID")
TABLE_NAME = os.getenv("TABLE_NAME")
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
    tags=['driving_tests'],
)

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=BUCKET_NAME,
    source_objects=['driving_tests/api/*'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}',
    schema_fields=[
        {'name': 'Statistic', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Month', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Driving_Test_Categories', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Driving_Test_Centre', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'UNIT', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'VALUE', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,  # Skip header with empty column name
    source_format='CSV',
    field_delimiter='\t',  # Set to ',' if using comma delimiter
    autodetect=False,
    dag=dag,
)

load_csv