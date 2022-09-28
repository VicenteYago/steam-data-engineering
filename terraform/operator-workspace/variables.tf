

#locals {
#  data_lake_bucket = "steam-bucket"
#}

variable "project" {
  description = "Your GCP Project ID"
  default = "steam-data-engineering-gcp"
}

variable "gcp_credentials" {
  description = "JSON service account credentials"
  default     = "/home/vyago-gcp/.google/credentials/google_credentials.json"
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

#--------------------------ADMIN VAULT----------------------#
variable "name" { 
  default = "dynamic-aws-creds-operator" 
}

variable "aws_vault_region" { 
  default = "us-east-1" # change
}

variable "terraform_admin_path" { 
  default = "../vault-admin-workspace/terraform.tfstate" 
}

variable "ttl" { 
  default = "1" 
}
#------------------------------------------------#
variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-southwest1"
  type        = string
}

variable "gcp_bucket_dataset" {
  description = "Bucket name for storage steam appdata, steamspy & scraped"
  default     = "steam-datalake-store-alldata"
  type        = string
}

variable "gcp_bucket_reviews" {
  description = "Bucket name for storage steam appdata, steamspy & scraped"
  default     = "steam-datalake-reviews"
  type        = string
}

variable "aws_s3_bucket_dataset"{
  description = "S3 Bucket steam dataset"
  default     = "steam-store-alldata"
  type        = string
}

variable "aws_s3_bucket_reviews"{
  description = "S3 Bucket steam reviews"
  default     = "steam-reviews-lite" # !!!! Reduced version, "steam-reviews" full
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
