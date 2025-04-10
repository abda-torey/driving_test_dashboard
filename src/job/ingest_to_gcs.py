# from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")


def create_driving_test_sink_gcs(t_env):
    table_name = 'drivingtest_sink'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
        `Statistic Label` STRING,
        `Month` STRING,
        `County` STRING,
        `Driving Test Categories` STRING,
        `UNIT` STRING,
        `VALUE` DOUBLE     
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'gs://{BUCKET_NAME}/driving_tests/from_files',  -- Output GCS path for driving tests
            'format' = 'csv',
            'csv.include-header' = 'true',  -- Include header row in CSV output
            'sink.parallelism' = '1'
        )
    """
    t_env.execute_sql(sink_ddl)
    
    return table_name


def create_drivingtest_source_local(t_env):
    table_name = "driving_test_source"
    
    source_ddl = f"""
        CREATE TABLE {table_name} (
            `Statistic Label` STRING,
            `Month` STRING,
            `County` STRING,
            `Driving Test Categories` STRING,
            `UNIT` STRING,
            `VALUE` DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/data/RAO31.csv',  -- Local CSV file path for driversTests
            'format' = 'csv',
            'csv.include-header' = 'true',  -- Include header row in CSV output
            'csv.ignore-parse-errors' = 'true'
        );
    """
    t_env.execute_sql(source_ddl)
    
    return table_name


def log_processing_tests():
    # Set up the table environment for batch mode using the unified TableEnvironment
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(settings)
    
    try:
        # Create source and sink tables
        source_table = create_drivingtest_source_local(t_env)
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
                CAST(
                    CASE 
                        WHEN `VALUE` IS NULL THEN 0.0
                        ELSE `VALUE`
                    END
                AS DOUBLE) AS `VALUE`
            FROM {source_table}
            WHERE `Statistic Label` <> 'Statistic Label'
            """
        ).wait()
        preview_result = t_env.sql_query(f"""
                    SELECT
                        `Statistic Label`,
                        `Month`,
                        `County`,
                        `Driving Test Categories`,
                        `UNIT`,
                        CAST(
                            CASE 
                                WHEN`VALUE` IS NULL THEN 0.0
                                ELSE `VALUE`
                            END AS DOUBLE
                        ) AS `VALUE`
                    FROM {source_table}
                    WHERE `Statistic Label` <> 'Statistic Label'
                    LIMIT 10
                """)

        # Print to terminal
        preview_result.execute().print()

    except Exception as e:
        print("Writing records to GCS failed:", str(e))


if __name__ == '__main__':
    log_processing_tests()
