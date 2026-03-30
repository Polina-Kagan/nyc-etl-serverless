# I am defining the exact name for my Raw Zone S3 bucket.
# This bucket will store the incoming, unprocessed Parquet and CSV files.

variable "raw_bucket_name" {
  description = "The name of the S3 bucket for raw data"
  type        = string
  default     = "nyc-raw-zone-2026-polina"
}

# I am defining the exact name for my Curated Zone S3 bucket.
# This bucket will store the cleaned, analytics-ready data after the Transform Lambda runs.

variable "curated_bucket_name" {
  description = "The name of the S3 bucket for curated data"
  type        = string
  default     = "nyc-curated-zone-2026-polina"
}