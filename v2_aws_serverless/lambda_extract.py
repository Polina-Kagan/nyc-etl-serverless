import os
import json
import urllib.request
import boto3
import logging
from botocore.exceptions import ClientError

# I am configuring the logger to track my pipeline's execution in AWS CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# I initialize the S3 client to save files, and the Lambda client to trigger the next step
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# I dynamically fetch my raw bucket name from the environment variables
RAW_BUCKET_NAME = os.environ.get('RAW_BUCKET_NAME')

def upload_to_s3_from_url(url, bucket_name, s3_key):
    """
    I stream the file directly to S3 using Python's built-in urllib.
    This eliminates the need for the external 'requests' library and keeps my Lambda lightweight!
    """
    logger.info(f"I am starting to stream data from {url}...")
    try:
        # I add a standard User-Agent header so the CloudFront server doesn't block the request
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            s3_client.upload_fileobj(
                Fileobj=response,
                Bucket=bucket_name,
                Key=s3_key
            )
        logger.info(f"Success! File is now in s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"Uh oh, the download failed: {e}")
        raise

def lookup_exists(bucket_name, s3_key):
    """I check if the dimension table already exists so I don't download it twice."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def lambda_handler(event, context):
    """The main entry point for my Extract Lambda."""
    year = event.get("year", "2025")
    month = event.get("month", "01") # Testing January!
    
    taxi_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    
    taxi_s3_key = f"yellow_taxi_{year}_{month}.parquet"
    lookup_s3_key = "taxi_zone_lookup.csv"

    # Step 1: Ensure the lookup table is in the raw zone
    logger.info("Checking if the taxi zone lookup table exists...")
    if lookup_exists(RAW_BUCKET_NAME, lookup_s3_key):
        logger.info("Lookup table found in S3. Skipping download.")
    else:
        logger.info("Lookup table not found. Downloading...")
        upload_to_s3_from_url(lookup_url, RAW_BUCKET_NAME, lookup_s3_key)

    # Step 2: Download the main fact table
    logger.info(f"Downloading taxi trips fact table for {year}-{month}...")
    upload_to_s3_from_url(taxi_url, RAW_BUCKET_NAME, taxi_s3_key)

    # Step 3: THE ORCHESTRATION MAGIC
    transform_payload = {
        "year": year,
        "month": month
    }
    
    logger.info("Extract completed. I am now invoking the Transform Lambda...")
    
    lambda_client.invoke(
        FunctionName='nyc-transform-pipeline',
        InvocationType='Event',
        Payload=json.dumps(transform_payload)
    )

    return {
        'statusCode': 200,
        'body': f'Extract pipeline finished! Transform Lambda successfully invoked for {year}-{month}.'
    }