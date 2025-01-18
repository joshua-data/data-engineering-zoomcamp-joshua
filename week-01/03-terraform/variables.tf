variable "project" {
  description = "Project"
  default     = "terraform-demo-421007"
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
  default     = "terraform-demo-421007-terra-bucket"
}

variable "storage_class" {
  description = "Storage Class"
  default     = "STANDARD"
}