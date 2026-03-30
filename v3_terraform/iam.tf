# ==========================================
# 1. EXTRACT LAMBDA ROLE & POLICIES
# ==========================================

# I create the trust policy that allows AWS Lambda to assume this role
resource "aws_iam_role" "extract_lambda_role" {
  name = "nyc_extract_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# I attach a strict custom policy to the Extract role
resource "aws_iam_role_policy" "extract_s3_invoke_policy" {
  name = "nyc_extract_s3_invoke_policy"
  role = aws_iam_role.extract_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Extract Lambda can ONLY interact with the RAW bucket
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_zone.arn,
          "${aws_s3_bucket.raw_zone.arn}/*"
        ]
      },
      {
        # Extract Lambda is allowed to trigger the Transform Lambda directly
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:nyc-transform-pipeline"
      }
    ]
  })
}

# I attach the AWS managed policy so my Lambda can write logs to CloudWatch
resource "aws_iam_role_policy_attachment" "extract_logs" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ==========================================
# 2. TRANSFORM LAMBDA ROLE & POLICIES
# ==========================================

# I create the trust policy for the Transform Lambda
resource "aws_iam_role" "transform_lambda_role" {
  name = "nyc_transform_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# I attach a strict custom policy to the Transform role
resource "aws_iam_role_policy" "transform_s3_policy" {
  name = "nyc_transform_s3_policy"
  role = aws_iam_role.transform_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Transform Lambda can ONLY READ from Raw zone
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.raw_zone.arn}/*"]
      },
      {
        # Transform Lambda can ONLY WRITE to Curated zone
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.curated_zone.arn}/*"]
      }
    ]
  })
}

# CloudWatch logs access for Transform Lambda
resource "aws_iam_role_policy_attachment" "transform_logs" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}