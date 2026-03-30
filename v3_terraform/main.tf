# I am configuring Terraform to use the official AWS provider.
# Pinning the version to ~> 5.0 ensures my infrastructure won't break 
# if a major, backwards-incompatible update is released in the future.


terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# I am setting my primary deployment region.
# This dictates where all my physical resources (Buckets, Lambdas) will be hosted.


provider "aws" {
  region = "us-east-1"
}

# I am fetching the current AWS account ID and ARN dynamically.
# This is a best practice for writing secure, least-privilege IAM policies later,
# without having to hardcode my sensitive Account ID in the Git repository.


data "aws_caller_identity" "current" {}


# I am fetching the current AWS region dynamically

data "aws_region" "current" {}