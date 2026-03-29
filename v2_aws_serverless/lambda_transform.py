import os
import sys
import subprocess
import logging

# I am configuring the logger for AWS CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- THE RUNTIME HACK (V2) ---
# PyArrow in the AWS Layer is compiled without C++ zstd support.
# So, I am dynamically installing 'fastparquet' and 'zstandard' 
# to bypass PyArrow entirely and decompress the files on the fly!
logger.info("I am installing fastparquet and zstandard on the fly...")
subprocess.check_call([
    sys.executable, "-m", "pip", "install", 
    "fastparquet", "zstandard", "-t", "/tmp/"
])

sys.path.append("/tmp/")
# ------------------------

import io
import boto3
import pandas as pd

s3_client = boto3.client('s3')

RAW_BUCKET = os.environ.get('RAW_BUCKET_NAME')
CURATED_BUCKET = os.environ.get('CURATED_BUCKET_NAME')

def read_parquet_from_s3(bucket, key):
    logger.info(f"Downloading {key} from {bucket}...")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    # MAGIC HAPPENS HERE: I am forcing Pandas to use the fastparquet engine
    return pd.read_parquet(io.BytesIO(response['Body'].read()), engine='fastparquet')

def read_csv_from_s3(bucket, key):
    logger.info(f"Downloading {key} from {bucket}...")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()), compression='gzip')

def write_parquet_to_s3(df, bucket, key):
    logger.info(f"Uploading cleaned data to s3://{bucket}/{key}...")
    out_buffer = io.BytesIO()
    # I can use pyarrow to write uncompressed/snappy parquet files, it only struggles with reading zstd
    df.to_parquet(out_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
    logger.info("Upload successful!")

def transform_data(taxi_df, lookup_df):
    """My Data Quality (DQ) contract for the Silver Zone."""
    logger.info(f"Original row count: {len(taxi_df)}")

    taxi_df = taxi_df.drop_duplicates()
    taxi_df = taxi_df.dropna(subset=['passenger_count', 'trip_distance', 'fare_amount'])
    taxi_df = taxi_df[taxi_df['passenger_count'] > 0]
    taxi_df = taxi_df[(taxi_df['trip_distance'] > 0) & (taxi_df['trip_distance'] < 100)]
    taxi_df = taxi_df[taxi_df['fare_amount'] > 0]

    taxi_df['tpep_pickup_datetime'] = pd.to_datetime(taxi_df['tpep_pickup_datetime'])
    taxi_df['tpep_dropoff_datetime'] = pd.to_datetime(taxi_df['tpep_dropoff_datetime'])

    logger.info("Joining with lookup table...")
    enriched_df = taxi_df.merge(
        lookup_df, left_on='PULocationID', right_on='LocationID', how='inner'
    )
    enriched_df = enriched_df.rename(columns={'Borough': 'pickup_borough', 'Zone': 'pickup_zone'})
    enriched_df = enriched_df.drop(columns=['LocationID'])

    logger.info(f"Cleaned row count: {len(enriched_df)}")
    return enriched_df

def lambda_handler(event, context):
    """The entry point for the Transform Lambda."""
    year = event.get("year", "2025")
    month = event.get("month", "02") 
    
    taxi_key = f"yellow_taxi_{year}_{month}.parquet"
    lookup_key = "taxi_zone_lookup.csv"
    curated_key = f"curated_yellow_taxi_{year}_{month}.parquet"

    # Step 1: Read raw data
    taxi_df = read_parquet_from_s3(RAW_BUCKET, taxi_key)
    lookup_df = read_csv_from_s3(RAW_BUCKET, lookup_key)

    # Step 2: Transform
    cleaned_df = transform_data(taxi_df, lookup_df)

    # Step 3: Load to Curated
    write_parquet_to_s3(cleaned_df, CURATED_BUCKET, curated_key)

    return {
        'statusCode': 200,
        'body': f'Transformation successful! Cleaned data saved to {CURATED_BUCKET}'
    }