terraform {
  required_version = ">= 1.0"
  backend "local" {} # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.gcp_credentials) # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

provider "aws" {
  shared_config_files      = [var.aws_conf]
  shared_credentials_files = [var.aws_credentials]
  profile                  = "default"
}

resource "google_storage_bucket" "steam-dataset" {
  name     =  var.gcp_bucket_dataset
  location = var.region

  # Optional, but recommended settings:
  storage_class               = var.storage_class
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

resource "google_storage_bucket" "steam-reviews" {
  name     =  var.gcp_bucket_reviews
  location = var.region

  # Optional, but recommended settings:
  storage_class               = var.storage_class
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

resource "google_bigquery_dataset" "steam-raw" {
  dataset_id                 = var.gcp_bq_dataset_steam_raw
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}



# TRANSFER AWS -> GCP

data "google_storage_transfer_project_service_account" "default" {
  project = var.project
}

resource "google_storage_bucket_iam_member" "s3-transfer-dataset" {
  bucket     = google_storage_bucket.steam-dataset.name
  role       = "roles/storage.admin"
  member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_bucket_iam_member" "s3-transfer-reviews" {
  bucket     = google_storage_bucket.steam-reviews.name
  role       = "roles/storage.admin"
  member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_transfer_job" "steam-dataset" {
  description = "Nightly backup of S3 bucket"
  project     = var.project

  transfer_spec {
    
    transfer_options {
      delete_objects_unique_in_sink = false
    }
    aws_s3_data_source {
      bucket_name = var.aws_s3_bucket_dataset
      aws_access_key {
      access_key_id     = "AKIAUTEF27PZPSBFSKFA"                     # !! 
      secret_access_key = "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" # !!
      }

    }
    gcs_data_sink {
      bucket_name = var.gcp_bucket_dataset
      path = "raw/"
    }
  }

  schedule {
    schedule_start_date {
      year  = 2022
      month = 9
      day   = 21
    }
  }

  depends_on = [google_storage_bucket_iam_member.s3-transfer-dataset]
}


resource "google_storage_transfer_job" "steam-reviews" {
  description = "Nightly backup of S3 bucket"
  project     = var.project

  transfer_spec {
    
    transfer_options {
      delete_objects_unique_in_sink = false
    }
    aws_s3_data_source {
      bucket_name = var.aws_s3_bucket_reviews
      aws_access_key {
      access_key_id     = "AKIAUTEF27PZPSBFSKFA"                     # !! 
      secret_access_key = "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" # !!
      }

    }
    gcs_data_sink {
      bucket_name = var.gcp_bucket_reviews
      path = "raw/"
    }
  }

  schedule {
    schedule_start_date {
      year  = 2022
      month = 9
      day   = 21
    }
  }

  depends_on = [google_storage_bucket_iam_member.s3-transfer-reviews]
}