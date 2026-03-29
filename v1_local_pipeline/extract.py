import os
import requests
import boto3
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# I am setting up my logging configuration to track the pipeline's execution.
# This is much better than using simple print() statements!
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# I am loading my AWS credentials securely from the local .env file.
load_dotenv()

# I am initializing my AWS S3 client using the boto3 library.
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# I specify the name of my Bronze/Raw zone bucket here.
RAW_BUCKET_NAME = 'nyc-raw-zone-2026-polina' 

def upload_to_s3_from_url(url, bucket_name, s3_key):
    """
    I am downloading the file via a stream and uploading it directly to my S3 bucket.
    I use upload_fileobj instead of put_object to handle raw network streams 
    without needing the 'seek' operation.
    """
    logger.info(f"I am starting to download data from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() 

        # I use upload_fileobj, which is smart enough to handle raw unseekable streams
        # by performing multipart uploads automatically behind the scenes.
        s3_client.upload_fileobj(
            Fileobj=response.raw,
            Bucket=bucket_name,
            Key=s3_key
        )
        logger.info(f"Success! I uploaded the file to s3://{bucket_name}/{s3_key}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Uh oh, I encountered a network error: {e}")
        raise
    except ClientError as e:
        logger.error(f"AWS S3 Error: {e}")
        raise

def lookup_exists(bucket_name, s3_key):
    """
    I am checking if my dimension table (CSV) is already in the raw zone.
    This makes my ETL script idempotent (safe to retry multiple times).
    """
    try:
        # head_object retrieves metadata without downloading the whole file
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def main():
    # I define the parameters for the dataset I want to extract
    year = "2025"
    month = "02"
    
    taxi_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    
    # I set the destination file names in my S3 bucket
    taxi_s3_key = f"yellow_taxi_{year}_{month}.parquet"
    lookup_s3_key = "taxi_zone_lookup.csv"

    # Step 1: I handle the dimension table (Lookup CSV)
    logger.info("I am checking if my lookup dictionary exists in S3...")
    if lookup_exists(RAW_BUCKET_NAME, lookup_s3_key):
        logger.info("The lookup file is already in my bucket. I am skipping the download.")
    else:
        logger.info("Lookup file not found. I am downloading it now...")
        upload_to_s3_from_url(lookup_url, RAW_BUCKET_NAME, lookup_s3_key)

    # Step 2: I handle the fact table (Taxi Parquet)
    logger.info(f"I am downloading the taxi trips fact table for {year}-{month}...")
    upload_to_s3_from_url(taxi_url, RAW_BUCKET_NAME, taxi_s3_key)

    logger.info("My Extract process is fully complete!")

if __name__ == "__main__":
    main()