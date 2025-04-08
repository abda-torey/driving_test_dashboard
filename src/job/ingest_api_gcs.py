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
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")  # e.g., fake-ecommerce-taxi-data-447320

# CSO API endpoint
API_URL = "https://ws.cso.ie/public/api.jsonrpc?data=%7B%22jsonrpc%22:%222.0%22,%22method%22:%22PxStat.Data.Cube_API.ReadDataset%22,%22params%22:%7B%22class%22:%22query%22,%22id%22:%5B%5D,%22dimension%22:%7B%7D,%22extension%22:%7B%22pivot%22:null,%22codes%22:false,%22language%22:%7B%22code%22:%22en%22%7D,%22format%22:%7B%22type%22:%22JSON-stat%22,%22version%22:%222.0%22%7D,%22matrix%22:%22ROA32%22%7D,%22version%22:%222.0%22%7D%7D"

def fetch_cso_api_data():
    """Fetch data from CSO API and transform it into a tabular format"""
    try:
        print(f"Fetching data from CSO API...")
        response = requests.get(API_URL)
        
        if response.status_code != 200:
            print(f"API request failed with status code: {response.status_code}")
            return None
            
        # Parse the JSON response
        json_data = response.json()
        
        # Extract the dataset from the result
        if 'result' not in json_data:
            print("Invalid API response format: 'result' field missing")
            return None
            
        dataset = json_data['result']
        
        # Process JSON-stat format (version 2.0)
        # JSON-stat is a specific format for statistical data
        if 'dimension' not in dataset or 'value' not in dataset:
            print("Invalid JSON-stat format")
            return None
        
        dimensions = dataset['dimension']
        values = dataset['value']
        
        # Extract dimension information
        dim_info = {}
        for dim_id, dim_data in dimensions.items():
            if 'category' in dim_data:
                category = dim_data['category']
                if 'label' in category:
                    labels = category['label']
                    dim_info[dim_id] = labels
        
        # Convert to pandas DataFrame for easier manipulation
        # This is a common approach for working with JSON-stat data
        print("Converting JSON-stat data to tabular format...")
        
        # Extract the specific dimensions we need
        stat_labels = list(dim_info.get('statistic', {}).values())
        months = list(dim_info.get('Month', {}).values())
        test_categories = list(dim_info.get('Driving Test Categories', {}).values())
        test_centres = list(dim_info.get('Driving Test Centre', {}).values())
        
        # Create a list to store our records
        records = []
        
        # Extract size information
        size = dataset.get('size', [])
        if not size:
            print("Missing size information in dataset")
            return None
            
        # Create a mapping function to convert indices to dimension values
        def get_index_combinations(sizes):
            if not sizes:
                return [[]]
            result = []
            for i in range(sizes[0]):
                for rest in get_index_combinations(sizes[1:]):
                    result.append([i] + rest)
            return result
        
        # Get all index combinations
        idx_combinations = get_index_combinations(size)
        
        # Map each value to its corresponding dimension values
        for idx, combo in enumerate(idx_combinations):
            if idx >= len(values):
                break
                
            value = values[idx]
            
            # Skip missing values
            if value is None or np.isnan(value):
                continue
                
            # Map indices to dimension values
            # This might need adjustment based on the exact order of dimensions in the API response
            stat_idx, month_idx, category_idx, centre_idx = combo
            
            # Get the actual dimension values (with bounds checking)
            stat_label = stat_labels[stat_idx] if stat_idx < len(stat_labels) else "Unknown"
            month = months[month_idx] if month_idx < len(months) else "Unknown"
            test_category = test_categories[category_idx] if category_idx < len(test_categories) else "Unknown"
            test_centre = test_centres[centre_idx] if centre_idx < len(test_centres) else "Unknown"
            
            records.append({
                'Statistic Label': stat_label,
                'Month': month,
                'Driving Test Categories': test_category,
                'Driving Test Centre': test_centre,
                'UNIT': 'Number',  # This appears to be constant based on your schema
                'VALUE': int(value) if not np.isnan(value) else 0
            })
        
        print(f"Extracted {len(records)} records from the API response")
        return records
        
    except Exception as e:
        print(f"Error fetching or processing API data: {e}")
        return None

def main():
  
   
    
    # Create Table environment with batch settings
    settings = EnvironmentSettings.new_instance() \
        .in_batch_mode() \
        .build()
    
    t_env = TableEnvironment.create(settings)
    
    # Fetch data from CSO API
    cso_data = fetch_cso_api_data()
    
    if not cso_data:
        print("No data available from CSO API")
        return
    
    # Write to a temporary CSV file first
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_csv = f"/opt/data/driving_test_{timestamp}.csv"
    
    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(cso_data)
    df.to_csv(temp_csv, index=False)
    
    print(f"Saved {len(df)} records to temporary CSV: {temp_csv}")
  
    # Now defines source and sink tables using the Table API
    try:
        # Create source table from the temporary CSV
        source_table = "driving_test_source"
        source_ddl = f"""
            CREATE TABLE {source_table} (
                `Statistic ` STRING,
                `Month` STRING,
                `Driving Test Categories` STRING,
                `Driving Test Centre` STRING,
                `UNIT` STRING,
                `VALUE` INT
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'file://{temp_csv}',
                'format' = 'csv',
                'csv.include-header' = 'true',
                'csv.ignore-parse-errors' = 'true'
            );
        """
        t_env.execute_sql(source_ddl)
        
        # Create GCS sink table
        sink_table = "driving_test_sink"  
        sink_ddl = f"""
            CREATE TABLE {sink_table} (
                `Statistic ` STRING,
                `Month` STRING,
                `Driving Test Categories` STRING,
                `Driving Test Centre` STRING,
                `UNIT` STRING,
                `VALUE` INT
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'gs://{BUCKET_NAME}/driving_tests/output/api/',
                'format' = 'csv',
                'csv.include-header' = 'true',
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
                `Statistic `,
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
        print(f"Successfully wrote driving test data to gs://{BUCKET_NAME}/driving_tests/output/")
        
        # Clean up temporary file
        os.remove(temp_csv)
        
    except Exception as e:
        print(f"Error executing Flink job: {e}")

if __name__ == "__main__":
    main()