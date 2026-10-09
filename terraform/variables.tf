# variables.tf
# Declares every variable this Terraform configuration needs.
# Think of this as the function signature — what inputs does
# this infrastructure definition require?

variable "project_id" {
  description = "Your GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-east1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for the lakehouse"
  type        = string
}

variable "credentials_file" {
  description = "Path to your GCP service account JSON key file"
  type        = string
}

variable "bq_dataset_bronze" {
  description = "BigQuery dataset name for bronze layer"
  type        = string
  default     = "bronze"
}

variable "bq_dataset_silver" {
  description = "BigQuery dataset name for silver layer"
  type        = string
  default     = "silver"
}

variable "bq_dataset_gold" {
  description = "BigQuery dataset name for gold layer"
  type        = string
  default     = "gold"
}