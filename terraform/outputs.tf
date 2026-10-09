# outputs.tf
# Values printed after "terraform apply" completes.
# Useful for confirming what was created and getting
# connection details you need elsewhere.

output "gcs_bucket_name" {
  description = "Name of the GCS lakehouse bucket"
  value       = google_storage_bucket.lakehouse.name
}

output "gcs_bucket_url" {
  description = "Full URL of the GCS bucket"
  value       = "gs://${google_storage_bucket.lakehouse.name}"
}

output "service_account_email" {
  description = "Email of the pipeline service account"
  value       = google_service_account.pipeline.email
}

output "bigquery_bronze_dataset" {
  description = "BigQuery bronze dataset ID"
  value       = google_bigquery_dataset.bronze.dataset_id
}

output "bigquery_silver_dataset" {
  description = "BigQuery silver dataset ID"
  value       = google_bigquery_dataset.silver.dataset_id
}

output "bigquery_gold_dataset" {
  description = "BigQuery gold dataset ID"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "bronze_trades_table" {
  description = "Full BigQuery path to bronze trades external table"
  value       = "${var.project_id}.${var.bq_dataset_bronze}.trades"
}