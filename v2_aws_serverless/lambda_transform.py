import os
import sys
import subprocess
import logging
import shutil

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- THE IMPROVED RUNTIME HACK ---
# We force install pyarrow into a specific subfolder in /tmp
lib_path = "/tmp/python_libs"
if not os.path.exists(lib_path):
    os.makedirs(lib_path)
    logger.info("Installing fresh pyarrow and numpy to /tmp/python_libs...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", 
        "pyarrow==15.0.0", "numpy<2", 
        "-t", lib_path, "--no-cache-dir"
    ])

# CRITICAL: We insert our path at the VERY BEGINNING to override /opt/python
sys.path.insert(0, lib_path)

# Verification: Let's log WHERE we are taking pyarrow from
try:
    import pyarrow
    logger.info(f"PyArrow successfully imported from: {pyarrow.__file__}")
except Exception as e:
    logger.error(f"Failed to import pyarrow: {e}")
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
    
    # To optimize memory usage, I only select the columns necessary for the analysis.
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
    return pd.read_csv(io.BytesIO(response['Body'].read()))

def write_parquet_to_s3(df, bucket, key):
    logger.info(f"Uploading cleaned data to s3://{bucket}/{key}...")
    out_buffer = io.BytesIO()
    # I am using 'snappy' compression for the Curated zone to balance speed and storage efficiency.
    df.to_parquet(out_buffer, index=False, compression='snappy')
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
    logger.info("Upload successful!")

def transform_data(taxi_df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """
    My Ironclad Data Quality (DQ) contract.
    I don't trust the source data, so I verify it across 4 levels of 'paranoia'.
    """
    
    initial_count = len(taxi_df)
    logger.info(f"Starting transformation. Initial row count: {initial_count}")

    # LEVEL 0: The Void Check
    # If the dataframe is empty, I raise a hard error to trigger the future DLQ/Alerting system.
    if taxi_df.empty:
        raise ValueError("CRITICAL ERROR: Received an empty dataframe from the source.")
        
    if lookup_df.empty:
        raise ValueError("CRITICAL ERROR: Location lookup table is empty.")

    # LEVEL 1: Schema Drift Protection
    # I enforce a strict list of required columns to prevent the pipeline from breaking silently.
    required_columns = [
        'passenger_count', 'trip_distance', 'fare_amount', 
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID'
    ]
    missing_cols = [col for col in required_columns if col not in taxi_df.columns]
    if missing_cols:
        raise KeyError(f"Schema Drift detected! Missing mandatory columns: {missing_cols}")

    # LEVEL 2: Type Safety & Safe Coercion
    # I use errors='coerce' to handle corrupted date strings. Invalid dates will become NaT, 
    # which I then safely remove to maintain data integrity.
    taxi_df['tpep_pickup_datetime'] = pd.to_datetime(taxi_df['tpep_pickup_datetime'], errors='coerce')
    taxi_df['tpep_dropoff_datetime'] = pd.to_datetime(taxi_df['tpep_dropoff_datetime'], errors='coerce')
    
    taxi_df = taxi_df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    # LEVEL 3: Business Logic & Physical Constraints
    # I filter out 'impossible' data points (ghost cars, time travelers, and outliers).
    taxi_df = taxi_df.drop_duplicates()
    taxi_df = taxi_df.dropna(subset=['passenger_count', 'trip_distance', 'fare_amount'])
    
    clean_df = taxi_df[
        (taxi_df['passenger_count'] > 0) & 
        (taxi_df['trip_distance'] > 0) & 
        (taxi_df['trip_distance'] < 150) & 
        (taxi_df['fare_amount'] > 0) &
        (taxi_df['fare_amount'] < 2000) &
        (taxi_df['tpep_dropoff_datetime'] > taxi_df['tpep_pickup_datetime']) 
    ].copy() 

    # Data Enrichment (Join)
    logger.info("Enriching data with location lookups...")
    enriched_df = clean_df.merge(
        lookup_df, left_on='PULocationID', right_on='LocationID', how='inner'
    )
    enriched_df = enriched_df.rename(columns={'Borough': 'pickup_borough', 'Zone': 'pickup_zone'})
    enriched_df = enriched_df.drop(columns=['LocationID'])

    # Final reporting for Observability
    dropped_rows = initial_count - len(enriched_df)
    logger.info(f"Dropped {dropped_rows} corrupted/invalid rows.")
    logger.info(f"Successfully processed {len(enriched_df)} clean rows.")
    
    return enriched_df

def lambda_handler(event, context):
    # Defaulting to Jan 2025 for testing
    year = event.get("year", "2025")
    month = event.get("month", "01") 
    
    taxi_key = f"yellow_taxi_{year}_{month}.parquet"
    lookup_key = "taxi_zone_lookup.csv"
    curated_key = f"curated_yellow_taxi_{year}_{month}.parquet"

    # Step 1: E (Extract)
    taxi_df = read_parquet_from_s3(RAW_BUCKET, taxi_key)
    lookup_df = read_csv_from_s3(RAW_BUCKET, lookup_key)

    # Step 2: T (Transform)
    cleaned_df = transform_data(taxi_df, lookup_df)

    # Step 3: L (Load)
    write_parquet_to_s3(cleaned_df, CURATED_BUCKET, curated_key)

    return {
        'statusCode': 200,
        'body': f'ETL successful! Data saved to {CURATED_BUCKET}'
    }