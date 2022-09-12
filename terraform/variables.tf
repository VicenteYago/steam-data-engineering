

locals {
  data_lake_bucket = "steam-bucket"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "steam-data-engineering-gcp"
}

variable "credentials" {
  description = "JSON service account credentials"
  default     = "/home/vyago/.google/credentials/google_credentials.json"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-southwest1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}