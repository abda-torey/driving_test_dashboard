from pyflink.table import EnvironmentSettings, TableEnvironment
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")  # e.g., fake-ecommerce-taxi-data-447320

def create_driving_test_sink_gcs(t_env):
    table_name = 'driving_test_sink'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            `Statistic Label` STRING,
            `Month` STRING,
            `County` STRING,
            `Driving Test Categories` STRING,
            `UNIT` STRING,
            `VALUE` INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'gs://{BUCKET_NAME}/driving_tests/output/',
            'format' = 'csv',
            'csv.include-header' = 'true',
            'sink.parallelism' = '1'
        )
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_driving_test_source_local(t_env):
    table_name = "driving_test_source"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            `Statistic Label` STRING,
            `Month` STRING,
            `County` STRING,
            `Driving Test Categories` STRING,
            `UNIT` STRING,
            `VALUE` INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/data/driving_test.csv',  -- Adjust to actual local path on VM
            'format' = 'csv',
            'csv.include-header' = 'true',
            'csv.ignore-parse-errors' = 'true'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing_driving_tests():
    # Set up the table environment for batch mode
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(settings)

    try:
        # Create source and sink tables
        source_table = create_driving_test_source_local(t_env)
        gcs_sink_table = create_driving_test_sink_gcs(t_env)

        # Insert records into the GCS sink
        t_env.execute_sql(
            f"""
            INSERT INTO {gcs_sink_table}
            SELECT
                `Statistic Label`,
                `Month`,
                `County`,
                `Driving Test Categories`,
                `UNIT`,
                `VALUE`
            FROM {source_table}
            """
        ).wait()

    except Exception as e:
        print("Writing records to GCS failed:", str(e))


if __name__ == '__main__':
    log_processing_driving_tests()
