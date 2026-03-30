# ==========================================
# 1. ZIP ARCHIVES (Auto-packaging)
# ==========================================

# I use Terraform to automatically zip my Python scripts. 
# This eliminates manual steps and ensures consistent deployments.
data "archive_file" "extract_zip" {
  type        = "zip"
  # Pointing to the file we wrote earlier
  source_file = "../v2_aws_serverless/lambda_extract.py"
  output_path = "extract_payload.zip"
}

data "archive_file" "transform_zip" {
  type        = "zip"
  source_file = "../v2_aws_serverless/lambda_transform.py"
  output_path = "transform_payload.zip"
}

# ==========================================
# 2. EXTRACT LAMBDA FUNCTION
# ==========================================

resource "aws_lambda_function" "extract_lambda" {
  function_name    = "nyc-extract-pipeline"
  role             = aws_iam_role.extract_lambda_role.arn
  
  # Injecting the zipped code
  filename         = data.archive_file.extract_zip.output_path
  source_code_hash = data.archive_file.extract_zip.output_base64sha256
  
  handler          = "lambda_extract.lambda_handler"
  runtime          = "python3.12"
  timeout          = 120 # 2 minutes
  memory_size      = 512

  environment {
    variables = {
      RAW_BUCKET_NAME = aws_s3_bucket.raw_zone.bucket
    }
  }
}

# ==========================================
# 3. TRANSFORM LAMBDA FUNCTION
# ==========================================

resource "aws_lambda_function" "transform_lambda" {
  function_name    = "nyc-transform-pipeline"
  role             = aws_iam_role.transform_lambda_role.arn
  
  filename         = data.archive_file.transform_zip.output_path
  source_code_hash = data.archive_file.transform_zip.output_base64sha256
  
  # Note: handler format is "filename.function_name" (without .py)
  handler          = "lambda_transform.lambda_handler"
  runtime          = "python3.12"
  timeout          = 300 # 5 minutes
  memory_size      = 2048 # 2GB to handle Pandas merge in-memory

  # Using the official AWS Managed Layer for Pandas (us-east-1 ARN)
  layers = [
    "arn:aws:lambda:${data.aws_region.current.name}:336392948345:layer:AWSSDKPandas-Python312:14"
  ]

  environment {
    variables = {
      RAW_BUCKET_NAME     = aws_s3_bucket.raw_zone.bucket
      CURATED_BUCKET_NAME = aws_s3_bucket.curated_zone.bucket
    }
  }
}