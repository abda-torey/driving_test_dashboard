#!/usr/bin/env python3

from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.common.typeinfo import Types
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import storage
import pandas as pd
import numpy as np

# Load environment variables from .env file
load_dotenv()

# GCP bucket configuration
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

# CSO API endpoint
API_URL = "https://ws.cso.ie/public/api.jsonrpc?data=%7B%22jsonrpc%22:%222.0%22,%22method%22:%22PxStat.Data.Cube_API.ReadDataset%22,%22params%22:%7B%22class%22:%22query%22,%22id%22:%5B%5D,%22dimension%22:%7B%7D,%22extension%22:%7B%22pivot%22:null,%22codes%22:false,%22language%22:%7B%22code%22:%22en%22%7D,%22format%22:%7B%22type%22:%22JSON-stat%22,%22version%22:%222.0%22%7D,%22matrix%22:%22ROA32%22%7D,%22version%22:%222.0%22%7D%7D"

def fetch_cso_api_data():
    try:
        print(f"Fetching data from CSO API...")
        response = requests.get(API_URL)

        if response.status_code != 200:
            print(f"API request failed with status code: {response.status_code}")
            return None

        json_data = response.json()

        if 'result' not in json_data:
            print("Invalid API response format: 'result' field missing")
            return None

        dataset = json_data['result']

        if 'dimension' not in dataset or 'value' not in dataset:
            print("Invalid JSON-stat format")
            return None

        dimensions = dataset['dimension']
        values = dataset['value']

        dim_order = dataset.get('id', [])
        size = dataset.get('size', [])

        if not dim_order or not size:
            print("Missing dimension order or size information")
            return None

        dim_info = {}
        for dim_id in dim_order:
            dim_data = dimensions[dim_id]
            dim_labels = dim_data['category']['label']
            dim_info[dim_id] = list(dim_labels.values())

        def get_index_combinations(sizes):
            if not sizes:
                return [[]]
            result = []
            for i in range(sizes[0]):
                for rest in get_index_combinations(sizes[1:]):
                    result.append([i] + rest)
            return result

        idx_combinations = get_index_combinations(size)

        records = []
        for idx, combo in enumerate(idx_combinations):
            if idx >= len(values):
                break

            value = values[idx]
            if value is None or (isinstance(value, float) and np.isnan(value)):
                value = 0  # or `None` if you want to keep it null for dbt to handle

            record = {}
            for i, dim_id in enumerate(dim_order):
                dim_labels = dim_info[dim_id]
                dim_index = combo[i]
                label = dim_labels[dim_index] if dim_index < len(dim_labels) else "Unknown"

                # Clean column names
                if dim_id.upper() == "STATISTIC":
                    col_name = "Statistic"
                elif dim_id.lower().startswith("c"):
                    if "centre" in dimensions[dim_id]['label'].lower():
                        col_name = "Driving Test Centre"
                    elif "categorie" in dimensions[dim_id]['label'].lower():
                        col_name = "Driving Test Categories"
                    else:
                        col_name = dimensions[dim_id]['label']
                else:
                    col_name = dimensions[dim_id]['label']

                record[col_name] = label

            record['UNIT'] = 'Number'
            # Fix: Change 'VALUE' to 'Value' to match BigQuery schema
            record['Value'] = int(value)  # Changed from 'VALUE' to 'Value'
            records.append(record)

        print(f"Extracted {len(records)} records from the API response")
        return records

    except Exception as e:
        print(f"Error fetching or processing API data: {e}")
        return None

def main():
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(settings)

    cso_data = fetch_cso_api_data()
    if not cso_data:
        print("No data available from CSO API")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_csv = f"/opt/data/driving_test_{timestamp}.csv"

    # Create DataFrame with the correct column names
    df = pd.DataFrame(cso_data)
    
    # Verify column names before saving
    print(f"DataFrame columns before saving: {df.columns.tolist()}")
    
    # Ensure column names match BigQuery schema (case-sensitive)
    column_mapping = {
        'Statistic': 'Statistic',
        'Month': 'Month',
        'Driving Test Categories': 'Driving Test Categories',
        'Driving Test Centre': 'Driving Test Centre',
        'UNIT': 'UNIT',
        'Value': 'VALUE'  # Ensure this matches what's used in the record creation
    }
    
    df = df.rename(columns=column_mapping)
    print(f"DataFrame columns after mapping: {df.columns.tolist()}")
    
    # Save to CSV
    df.to_csv(temp_csv, index=False, header=False)
    print(f"Saved {len(df)} records to temporary CSV: {temp_csv}")
    print(df.head(10))

    # Now defines source and sink tables using the Table API
    try:
        # Create source table from the temporary CSV
        # Ensure column names here match what's in the CSV
        source_table = "driving_test_source"
        source_ddl = f"""
            CREATE TABLE {source_table} (
                `Statistic` STRING,
                `Month` STRING,
                `Driving Test Categories` STRING,
                `Driving Test Centre` STRING,
                `UNIT` STRING,
                `VALUE` INT
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'file://{temp_csv}',
                'format' = 'csv',
                'csv.ignore-parse-errors' = 'true'
            );
        """
        t_env.execute_sql(source_ddl)
        
        # Create GCS sink table
        sink_table = "driving_test_sink"  
        sink_ddl = f"""
            CREATE TABLE {sink_table} (
                `Statistic` STRING,
                `Month` STRING,
                `Driving Test Categories` STRING,
                `Driving Test Centre` STRING,
                `UNIT` STRING,
                `VALUE` INT
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'gs://{BUCKET_NAME}/driving_tests/api/old/',
                'format' = 'csv',
                'csv.write-header' = 'true',
                'sink.parallelism' = '1'
            )
        """
        t_env.execute_sql(sink_ddl)
        
        # Insert data from source to sink
        print("Transferring data from temporary CSV to GCS bucket...")
        result = t_env.execute_sql(
            f"""
            INSERT INTO {sink_table}
            SELECT
                `Statistic`,
                `Month`,
                `Driving Test Categories`,
                `Driving Test Centre`,
                `UNIT`,
                `VALUE`
            FROM {source_table}
            """
        )
        
        # Wait for job completion
        result.wait()
        preview_result = t_env.sql_query(f"""
                    SELECT
                        `Statistic`,
                        `Month`,
                        `Driving Test Categories`,
                        `Driving Test Centre`,
                        `UNIT`,
                        `VALUE`
                    FROM {source_table}
                    LIMIT 10
                """)

        # Print to terminal
        preview_result.execute().print()
        print(f"Successfully wrote driving test data to gs://{BUCKET_NAME}/driving_tests/api/")
        
        # Clean up temporary file
        os.remove(temp_csv)
        
    except Exception as e:
        print(f"Error executing Flink job: {e}")

if __name__ == "__main__":
    main()