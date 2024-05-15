variable "credentials" {
  description = "My Credentials"
  default     = "~/.google/credentials/google_credentials.json"
}

variable "project" {
  description = "Project"
  default     = "ny-rides-joshua"
}

variable "region" {
  description = "Region"
  default     = "asia-northeast3-a"
}

variable "location" {
  description = "Project Location"
  default     = "ASIA-NORTHEAST3"
}

variable "bigquery_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "ny-rides-joshua-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}