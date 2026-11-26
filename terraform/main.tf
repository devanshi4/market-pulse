# GCP resources for the Market Pulse pipeline: the lakehouse bucket, BigQuery
# datasets for each layer, bronze external tables, and the pipeline service account.

# ── Provider ───────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Credentials come from Application Default Credentials or GOOGLE_APPLICATION_CREDENTIALS.
provider "google" {
  project     = var.project_id
  region      = var.region
}

# ── Lakehouse bucket ───────────────────────────────────────────────────────────
# Holds the bronze Iceberg data files.

resource "google_storage_bucket" "lakehouse" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false    # refuse to delete a bucket that still holds data

  # Access is managed through IAM only, not per-object ACLs.
  uniform_bucket_level_access = true

  # Versioning allows recovery of overwritten or deleted Parquet files.
  versioning {
    enabled = true
  }

  # Keep the three most recent archived versions of each object; delete older ones.
  lifecycle_rule {
    condition {
      num_newer_versions = 3
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}

# ── Layer prefixes ─────────────────────────────────────────────────────────────
# Marker objects so the bronze/silver/gold prefixes exist before the first write.

resource "google_storage_bucket_object" "bronze_folder" {
  name    = "bronze/.keep"
  bucket  = google_storage_bucket.lakehouse.name
  content = "bronze layer placeholder"
}

resource "google_storage_bucket_object" "silver_folder" {
  name    = "silver/.keep"
  bucket  = google_storage_bucket.lakehouse.name
  content = "silver layer placeholder"
}

resource "google_storage_bucket_object" "gold_folder" {
  name    = "gold/.keep"
  bucket  = google_storage_bucket.lakehouse.name
  content = "gold layer placeholder"
}

# ── BigQuery datasets (one per layer) ──────────────────────────────────────────

resource "google_bigquery_dataset" "bronze" {
  dataset_id  = var.bq_dataset_bronze
  location    = var.region
  description = "Bronze layer — raw data from Kafka via Iceberg on GCS"

  delete_contents_on_destroy = false   # never drop tables on terraform destroy
}

resource "google_bigquery_dataset" "silver" {
  dataset_id  = var.bq_dataset_silver
  location    = var.region
  description = "Silver layer — cleaned and typed dbt models"

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = var.bq_dataset_gold
  location    = var.region
  description = "Gold layer — business metrics and analytics dbt models"

  delete_contents_on_destroy = false
}

# ── Pipeline service account ───────────────────────────────────────────────────
# Used by the consumer, dbt and Dagster.

resource "google_service_account" "pipeline" {
  account_id   = "market-pulse-pipeline"
  display_name = "Market Pulse Pipeline"
  description  = "Service account for the Market Pulse data pipeline"
}

# ── IAM ────────────────────────────────────────────────────────────────────────
# Granted at project level. Scoping them to the bucket and the three datasets is
# the next hardening step.

# Read and write Iceberg data files in GCS.
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Create and replace dbt tables and views.
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Run query jobs.
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ── Bronze external tables ─────────────────────────────────────────────────────
# BigQuery reads the Parquet files on GCS in place; dbt sources point at these.

resource "google_bigquery_table" "bronze_trades" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "trades"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = [
      "gs://${var.bucket_name}/bronze/trades/data/*.parquet"
    ]

    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${var.bucket_name}/bronze/trades/data/"
    }
  }

  depends_on = [google_bigquery_dataset.bronze]
}

resource "google_bigquery_table" "bronze_positions" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "positions"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris = [
      "gs://${var.bucket_name}/bronze/positions/data/*.parquet"
    ]
    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${var.bucket_name}/bronze/positions/data/"
    }
  }

  depends_on = [google_bigquery_dataset.bronze]
}

resource "google_bigquery_table" "bronze_accounts" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "accounts"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris = [
      "gs://${var.bucket_name}/bronze/accounts/data/*.parquet"
    ]
  }

  depends_on = [google_bigquery_dataset.bronze]
}

resource "google_bigquery_table" "bronze_market_prices" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "market_prices"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris = [
      "gs://${var.bucket_name}/bronze/market_prices/data/*.parquet"
    ]
    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${var.bucket_name}/bronze/market_prices/data/"
    }
  }

  depends_on = [google_bigquery_dataset.bronze]
}
