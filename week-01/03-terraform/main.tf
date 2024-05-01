terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.26.0"
    }
  }
}

provider "google" {
  project = "terraform-demo-421007"
  region  = "asia-northeast3-a"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-demo-421007-terra-bucket"
  location      = "ASIA-NORTHEAST3"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}