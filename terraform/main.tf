# main.tf
# Defines every GCP resource the Market Pulse pipeline needs.
# Running "terraform apply" creates all of this from scratch.

# ── Provider configuration ─────────────────────────────────────────────────────
# A provider is the plugin that knows how to talk to a specific
# cloud. We use the "google" provider to manage GCP resources.
# Terraform downloads this plugin when you run "terraform init".

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  #credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ── GCS Bucket ─────────────────────────────────────────────────────────────────
# The main storage bucket for our lakehouse.
# All bronze Iceberg data files live here.

resource "google_storage_bucket" "lakehouse" {
  # resource type = "google_storage_bucket"
  # resource name = "lakehouse" (used to reference this resource elsewhere)

  name          = var.bucket_name
  location      = var.region
  force_destroy = false    # prevents accidental deletion if bucket has data

  # Uniform access control — simpler and more secure than
  # per-object ACLs. All access managed through IAM.
  uniform_bucket_level_access = true

  # Versioning keeps previous versions of files.
  # Useful for recovering accidentally deleted Parquet files.
  versioning {
    enabled = true
  }

  # Lifecycle rule — automatically delete old file versions
  # after 30 days to control storage costs.
  lifecycle_rule {
    condition {
      num_newer_versions = 3    # keep 3 most recent versions
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}

# ── GCS Folder structure ────────────────────────────────────────────────────────
# GCS doesn't have real folders — just objects with "/" in their names.
# Creating empty placeholder objects gives you the visual folder
# structure in the GCP Console.

resource "google_storage_bucket_object" "bronze_folder" {
  name    = "bronze/.keep"     # the .keep file is a convention
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

# ── BigQuery Datasets ───────────────────────────────────────────────────────────
# A BigQuery dataset is like a database schema —
# a container that holds related tables.

resource "google_bigquery_dataset" "bronze" {
  dataset_id  = var.bq_dataset_bronze
  location    = var.region
  description = "Bronze layer — raw data from Kafka via Iceberg on GCS"

  # Delete the dataset even if it has tables inside.
  # Set to false in production to prevent accidental data loss.
  delete_contents_on_destroy = false
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

# ── Service Account ─────────────────────────────────────────────────────────────
# The identity our pipeline uses to authenticate with GCP.
# All pipeline scripts (Python, dbt, Dagster) use this account.

resource "google_service_account" "pipeline" {
  account_id   = "market-pulse-pipeline"
  display_name = "Market Pulse Pipeline"
  description  = "Service account for the Market Pulse data pipeline"
}

# ── IAM Bindings ────────────────────────────────────────────────────────────────
# IAM (Identity and Access Management) controls what the
# service account is allowed to do.
# Each binding grants one role on one resource.

# Storage Admin — full access to GCS
# Needed to read and write Iceberg Parquet files
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery Admin — full access to BigQuery
# Needed for dbt to create and update tables
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery Job User — allows running BigQuery jobs (queries)
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ── BigQuery External Tables ────────────────────────────────────────────────────
# External tables point at GCS Parquet files.
# BigQuery queries them without copying data.
# Depends on the dataset existing first —
# Terraform handles this ordering automatically because we
# reference google_bigquery_dataset.bronze.dataset_id below.

resource "google_bigquery_table" "bronze_trades" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "trades"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    # Wildcard matches all partition folders and parquet files
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
