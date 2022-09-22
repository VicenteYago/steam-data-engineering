

#locals {
#  data_lake_bucket = "steam-bucket"
#}

variable "project" {
  description = "Your GCP Project ID"
  default = "steam-data-engineering-gcp"
}

variable "gcp_credentials" {
  description = "JSON service account credentials"
  default     = "/home/vyago/.google/credentials/google_credentials.json"
  type        = string
}

variable "aws_credentials" {
  description = "JSON service account credentials"
  default     = "/home/vyago/.aws/credentials"
  type        = string
}

variable "aws_conf" {
  description = "JSON service account credentials"
  default     = "/home/vyago/.aws/conf"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-southwest1"
  type        = string
}

variable "gcp_bucket_dataset" {
  description = "Bucket name for storage steam appdata, steamspy & scraped"
  default     = "steam-datalake-dataset"
  type        = string
}

variable "gcp_bucket_reviews" {
  description = "Bucket name for storage steam appdata, steamspy & scraped"
  default     = "steam-datalake-reviews"
  type        = string
}

variable "aws_s3_bucket_dataset"{
  description = "S3 Bucket steam dataset"
  default     = "steam-dataset"
  type        = string
}

variable "aws_s3_bucket_reviews"{
  description = "S3 Bucket steam reviews"
  default     = "steam-reviews"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "gcp_bq_dataset_steam_raw" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "steam_raw"
  type        = string
}




#variable "aws_access_key"{
#  description = ""
#  default     = "AKIAUTEF27PZPSBFSKFA"
#  type        = string
#}

#variable "aws_secret_key"{ # TODO
#  description = ""
#  default     = "???"
#  type        = string
#}