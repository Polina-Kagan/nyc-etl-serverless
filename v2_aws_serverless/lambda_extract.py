import os
import requests
import boto3
import logging
from botocore.exceptions import ClientError

# I am configuring the root logger to send my logs directly to AWS CloudWatch.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# I am initializing the S3 client. 
# Notice that I do NOT use .env anymore. AWS Lambda automatically provides 
# secure IAM credentials to my code under the hood!
s3_client = boto3.client('s3')

# I am dynamically fetching my bucket name from Lambda's Environment Variables.
# This makes my code reusable across different environments (dev, prod).
RAW_BUCKET_NAME = os.environ.get('RAW_BUCKET_NAME')

def upload_to_s3_from_url(url, bucket_name, s3_key):
    """
    I am streaming the file from the internet directly to S3.
    This prevents my Lambda function from running out of /tmp storage space.
    """
    logger.info(f"I am starting to stream data from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() 

        # I use upload_fileobj for smart, memory-efficient multipart uploads
        s3_client.upload_fileobj(
            Fileobj=response.raw,
            Bucket=bucket_name,
            Key=s3_key
        )
        logger.info(f"Success! File is now in s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"Uh oh, the download failed: {e}")
        raise

def lookup_exists(bucket_name, s3_key):
    """
    I am checking if the dimension table is already in my S3 bucket
    to ensure my Lambda function is idempotent.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def lambda_handler(event, context):
    """
    This is the main entry point for my AWS Lambda function.
    It expects an 'event' dictionary which can contain parameters.
    """
    # I am extracting the year and month from the event trigger.
    # If they are not provided, I default to March 2025.
    year = event.get("year", "2025")
    month = event.get("month", "03")
    
    taxi_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    
    taxi_s3_key = f"yellow_taxi_{year}_{month}.parquet"
    lookup_s3_key = "taxi_zone_lookup.csv"

    # Step 1: Handle the Lookup Table
    logger.info("Checking if the taxi zone lookup table exists...")
    if lookup_exists(RAW_BUCKET_NAME, lookup_s3_key):
        logger.info("Lookup table found in S3. Skipping download.")
    else:
        logger.info("Lookup table not found. Downloading...")
        upload_to_s3_from_url(lookup_url, RAW_BUCKET_NAME, lookup_s3_key)

    # Step 2: Handle the Taxi Fact Table
    logger.info(f"Downloading taxi trips fact table for {year}-{month}...")
    upload_to_s3_from_url(taxi_url, RAW_BUCKET_NAME, taxi_s3_key)

    # I return a standard HTTP response to signal a successful execution
    return {
        'statusCode': 200,
        'body': f'Extract pipeline successfully completed for {year}-{month}!'
    }