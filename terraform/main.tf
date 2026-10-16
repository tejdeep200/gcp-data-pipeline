terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Storage buckets for raw/staging/temp
resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-raw"
  location = var.region
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "staging" {
  name     = "${var.project_id}-staging"
  location = var.region
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "temp" {
  name     = "${var.project_id}-temp"
  location = var.region
  uniform_bucket_level_access = true
}

# Pub/Sub topic
resource "google_pubsub_topic" "yelp_topic" {
  name = "yelp-json-topic"
}

# BigQuery dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "yelp_dataset"
  location   = "US"
}

# Service account for Dataflow
resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-sa"
  display_name = "Dataflow service account"
}

# Minimal IAM roles for the service account (adjust for least privilege in prod)
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "bigquery_dataeditor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

