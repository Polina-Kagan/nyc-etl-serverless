# I am creating the Raw Zone S3 bucket.

resource "aws_s3_bucket" "raw_zone" {
  bucket = var.raw_bucket_name
  
  # I use force_destroy so Terraform can automatically delete this bucket 
  # during teardown (terraform destroy), even if it contains leftover files.

  force_destroy = true 
}

# I am creating the Curated Zone S3 bucket.

resource "aws_s3_bucket" "curated_zone" {
  bucket = var.curated_bucket_name
  
  force_destroy = true
}