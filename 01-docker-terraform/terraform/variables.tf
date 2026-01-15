variable "credentials" {
  description = "My Credentials"
  type        = string
  default     = "./key.json"
}

variable "project_id" {
  description = "Project ID"
  type        = string
  default     = "steel-shine-484409-g6"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  type        = string
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
  default     = "steel-shine-484409-g6-terra-bucket"
}

variable "location" {
  description = "Project Location"
  type        = string
  default     = "EU"
}

variable "region" {
  description = "Project Region"
  type        = string
  default     = "europe-north1"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}