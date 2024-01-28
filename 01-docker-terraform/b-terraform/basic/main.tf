terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = "de-zoomcamp-412515"
  region  = "ASIA-SOUTHEAST2"
}

resource "google_storage_bucket" "demo-bucket" {
  name     = "de-zoomcamp-412515-demo-bucket"
  location = "ASIA-SOUTHEAST2"

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = "demo-dataset"
  location   = "ASIA-SOUTHEAST2"
}
