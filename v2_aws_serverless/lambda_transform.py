import os
import sys
import subprocess
import logging

# I am configuring the logger for AWS CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- THE ULTIMATE RUNTIME HACK (Based on my previous successful architecture) ---
# The default AWS Pandas layer has a crippled PyArrow without zstd support.
# I am dynamically installing a fresh, fully-featured pyarrow directly into /tmp/
if not os.path.exists("/tmp/pyarrow"):
    logger.info("Installing fresh pyarrow and numpy to bypass AWS layer limits...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", 
        "pyarrow==15.0.0", "numpy<2", 
        "-t", "/tmp/", "--no-cache-dir"
    ])

# MAGIC HAPPENS HERE: I insert /tmp/ at index 0 so Python prioritizes MY pyarrow 
# over the broken one provided by the AWS environment.
sys.path.insert(0, "/tmp/")
# -----------------------------------------------------------------------------

import io
import boto3
import pandas as pd
import pyarrow.parquet as pq

s3_client = boto3.client('s3')

RAW_BUCKET = os.environ.get('RAW_BUCKET_NAME')
CURATED_BUCKET = os.environ.get('CURATED_BUCKET_NAME')

def read_parquet_from_s3(bucket, key):
    logger.info(f"Downloading {key} from {bucket}...")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read()
    
    # I use pyarrow.parquet directly to read the file. 
    # This C++ implementation is vastly more memory-efficient than other engines.
    # To prevent OutOfMemory (OOM) errors, I only load the columns I actually need for analytics!
    columns_to_read = [
        'passenger_count', 'trip_distance', 'fare_amount', 
        'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'PULocationID', 'DOLocationID', 
        'tip_amount', 'total_amount'
    ]
    
    table = pq.read_table(io.BytesIO(file_content), columns=columns_to_read)
    return table.to_pandas()

def read_csv_from_s3(bucket, key):
    logger.info(f"Downloading {key} from {bucket}...")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()), compression='gzip')

def write_parquet_to_s3(df, bucket, key):
    logger.info(f"Uploading cleaned data to s3://{bucket}/{key}...")
    out_buffer = io.BytesIO()
    # I explicitly use snappy compression for the curated zone
    df.to_parquet(out_buffer, index=False, compression='snappy')
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