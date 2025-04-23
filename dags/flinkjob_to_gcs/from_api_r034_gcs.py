from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'api_r034_to_gcs',
    default_args=default_args,
    description='Move driving test data from api to GCS using Flink job',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['flink_job'],
)

run_flink_command = """
docker exec -it flink-jobmanager ./bin/flink run -py /opt/src/job/ingest_r034_api_gcs.py
"""

run_flink_api_data_task = BashOperator(
    task_id='run_r034_api_flink_job',
    bash_command=run_flink_command,
    dag=dag,
)

run_flink_api_data_task
