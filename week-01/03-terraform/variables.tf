variable "project" {
  description = "Project"
  default     = "de-zoomcamp-joshua"
}

variable "region" {
  description = "Region"
  default     = "asia-northeast3-a"
}

variable "location" {
  description = "Location"
  default     = "ASIA-NORTHEAST3"
}

variable "bigquery_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "bucket_name" {
  description = "Storage Bucket Name"
  default     = "de-zoomcamp-joshua-terra-bucket"
}

variable "storage_class" {
  description = "Storage Class"
  default     = "STANDARD"
}