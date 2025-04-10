provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id  = var.dataset_id
  location    = var.region
  description = "Dataset for driving test data"
}

# Table for API Data
resource "google_bigquery_table" "api_data_table" {
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  table_id   = "api_driving_tests"


  schema = jsonencode([
    {
      "name": "statistic",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "month",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "driving_test_categories",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "driving_test_centre",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "unit",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "value",
      "type": "INT64",
      "mode": "NULLABLE"
    }
  ])
}

# Table for CSV File Data
resource "google_bigquery_table" "csv_data_table" {
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  table_id   = "csv_driving_tests"

  schema = jsonencode([
    {
      "name": "statistic_label",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "month",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "county",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "driving_test_categories",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "unit",
      "type": "STRING",
      "mode": "NULLABLE"
    },
    {
      "name": "value",
      "type": "FLOAT64",
      "mode": "NULLABLE"
    }
  ])
}
