# RSA Driving Tests Analytics - dbt Setup

This directory contains the dbt models used for transforming the RSA driving test data. The data is sourced from the CSO API and local CSV files, then staged and transformed using dbt into fact tables for county-level and test centre-level analysis. The final output is used in Looker Studio dashboards for reporting.

## Directory Structure

```
├── README.md
├── analyses
├── dbt_project.yml
├── macros
├── models
│   ├── core
│   │   ├── county_fact_driving_test_statistics.sql
│   │   ├── fact_driving_test_statistics.sql
│   │   ├── schema.yml
│   │   └── test_centre_fact_driving_stats.sql
│   └── staging
│       ├── schema.yml
│       ├── stg_staging__driving_test_api_table.sql
│       ├── stg_staging__driving_test_localfile_table.sql
│       ├── stg_staging__driving_test_ro30_table.sql
│       └── stg_staging__driving_test_ro34_table.sql
├── snapshots
└── tests
```

## Setup Instructions

### 1. Install dbt and Dependencies

Ensure you have dbt installed along with the necessary dependencies. If not, you can install dbt using the following command:

```bash
pip install dbt
```

### 2. dbt Configuration

#### dbt_project.yml

The `dbt_project.yml` file defines the dbt project settings, including configurations for models, snapshots, and more. Here's a brief overview of the configurations:

- **Models**: Defines how models are structured and which schemas to use.
- **Materializations**: Defines how models are built (e.g., tables, views).
- **Macros**: Defines reusable SQL snippets that can be invoked in your models.

#### profiles.yml

The `profiles.yml` file should be located in the `~/.dbt/` directory and contain the necessary database connection details. You can configure it to connect to BigQuery, as follows:

```yaml
your_profile:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: YOUR_PROJECT_ID
      dataset: YOUR_DATASET
      threads: 1
      location: YOUR_LOCATION
```

### 3. Running dbt Models

To run the dbt models, execute the following command in your dbt project directory:

```bash
dbt run
```

This will compile and execute the models, applying transformations to your data and creating the necessary tables in BigQuery.

### 4. Testing dbt Models

You can test your models using the following command:

```bash
dbt test
```

This will run the tests defined in the `tests` directory and ensure that your models are working as expected.

## dbt Models Breakdown

### Staging Models

The staging models clean and structure raw data from the API and local CSV files:

- `stg_staging__driving_test_api_table.sql`: Transforms data from the driving test API.
- `stg_staging__driving_test_localfile_table.sql`: Transforms data from the local CSV file.
- `stg_staging__driving_test_ro30_table.sql`: Transforms a specific dataset (RO30).
- `stg_staging__driving_test_ro34_table.sql`: Transforms another specific dataset (RO34).

### Core Models

These models create the fact tables used for analysis:

- `county_fact_driving_test_statistics.sql`: A fact table for county-level statistics.
- `test_centre_fact_driving_stats.sql`: A fact table for test centre-level statistics.

#### Schema Configuration

Each model has an associated `schema.yml` file that contains the schema definition, including column names, types, and tests. For example:

- `county_fact_driving_test_statistics.sql`: Defines the schema for the county-level statistics table.
- `test_centre_fact_driving_stats.sql`: Defines the schema for the test centre-level statistics table.

### Macros

Macros are reusable SQL functions or snippets that can be used across models. The macros folder contains custom macros used to streamline SQL transformations in the dbt models.

## Testing and Validation

To ensure data quality, you can define custom tests in dbt. For example:

- **Unique Key Test**: Ensures that no duplicates exist in key columns.
- **Not Null Test**: Ensures that important columns do not contain null values.

### Running Tests

You can run the tests with the following command:

```bash
dbt test
```

