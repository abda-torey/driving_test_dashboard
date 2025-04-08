provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_bucket" {
  name     = var.bucket_name
  location = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
  description = "Dataset for driving test data"
}
