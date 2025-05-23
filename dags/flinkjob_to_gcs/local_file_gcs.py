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
    'csv_to_gcs',
    default_args=default_args,
    description='Move driving test file to GCS using Flink job',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['flink_job'],
)

run_flink_command = """
docker exec -it flink-jobmanager ./bin/flink run -py /opt/src/job/ingest_to_gcs.py

"""

run_flink_localfile_task = BashOperator(
    task_id='run_localfile_flink_job',
    bash_command=run_flink_command,
    dag=dag,
)

run_flink_localfile_task
