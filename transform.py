import os
import io
import boto3
import pandas as pd
import logging
from dotenv import load_dotenv

# I am setting up logging so I can monitor my data transformations
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# I am securely loading my AWS credentials
load_dotenv()

# I initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# My bucket names for the Data Lake zones
RAW_BUCKET = 'nyc-raw-zone-2026-polina'
CURATED_BUCKET = 'nyc-curated-zone-2026-polina'

def read_parquet_from_s3(bucket, key):
    """I am reading a Parquet file directly from S3 into a Pandas DataFrame in memory."""
    logger.info(f"Downloading {key} from {bucket} into memory...")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    # I use io.BytesIO to treat the raw bytes as a file in memory
    return pd.read_parquet(io.BytesIO(response['Body'].read()))

def read_csv_from_s3(bucket, key):
    """I am reading a CSV file directly from S3 into a Pandas DataFrame in memory."""
    logger.info(f"Downloading {key} from {bucket} into memory...")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    
    # We specify compression='gzip' because the raw network stream 
    # from CloudFront was compressed on the fly to save bandwidth!
    return pd.read_csv(io.BytesIO(response['Body'].read()), compression='gzip')

def write_parquet_to_s3(df, bucket, key):
    """I am writing a Pandas DataFrame directly to S3 as a Parquet file without using the local disk."""
    logger.info(f"Uploading cleaned data to s3://{bucket}/{key}...")
    out_buffer = io.BytesIO()
    # I convert the DataFrame to parquet format in the memory buffer
    df.to_parquet(out_buffer, index=False)
    # I upload the buffer's contents to S3
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
    logger.info("Upload successful!")

def transform_data(taxi_df, lookup_df):
    """
    This is my Data Quality (DQ) contract. 
    I am acting as a paranoid Data Engineer, filtering out bad records.
    """
    logger.info(f"Original row count: {len(taxi_df)}")

    # 1. Dropping duplicates (System glitches can send the same record twice)
    taxi_df = taxi_df.drop_duplicates()

    # 2. Applying Business Rules (Filtering anomalies)
    # I am dropping rows with missing essential data
    taxi_df = taxi_df.dropna(subset=['passenger_count', 'trip_distance', 'fare_amount'])
    
    # Passengers cannot be 0, and realistically shouldn't be negative
    taxi_df = taxi_df[taxi_df['passenger_count'] > 0]
    
    # Distance must be positive, and > 100 miles is highly suspicious for a city taxi
    taxi_df = taxi_df[(taxi_df['trip_distance'] > 0) & (taxi_df['trip_distance'] < 100)]
    
    # Fare and total amount must be positive (refunds or errors are negative)
    taxi_df = taxi_df[taxi_df['fare_amount'] > 0]

    # 3. Type Casting (Ensuring dates are actually datetime objects)
    taxi_df['tpep_pickup_datetime'] = pd.to_datetime(taxi_df['tpep_pickup_datetime'])
    taxi_df['tpep_dropoff_datetime'] = pd.to_datetime(taxi_df['tpep_dropoff_datetime'])

    # 4. Data Enrichment (JOINing with the lookup table)
    # I am joining the pickup location ID to get the actual Borough and Zone names
    logger.info("Enriching data: Joining with lookup table...")
    enriched_df = taxi_df.merge(
        lookup_df, 
        left_on='PULocationID', 
        right_on='LocationID', 
        how='inner'
    )
    
    # I am renaming the new columns so they make sense for business users
    enriched_df = enriched_df.rename(columns={
        'Borough': 'pickup_borough',
        'Zone': 'pickup_zone'
    })
    
    # I drop the redundant LocationID column from the lookup table
    enriched_df = enriched_df.drop(columns=['LocationID'])

    logger.info(f"Cleaned row count: {len(enriched_df)}")
    return enriched_df

def main():
    # Define my files based on the 2025 data we extracted
    year = "2025"
    month = "01"
    
    taxi_key = f"yellow_taxi_{year}_{month}.parquet"
    lookup_key = "taxi_zone_lookup.csv"
    curated_key = f"curated_yellow_taxi_{year}_{month}.parquet"

    # Step 1: Extract from Raw Zone (In-Memory)
    taxi_df = read_parquet_from_s3(RAW_BUCKET, taxi_key)
    lookup_df = read_csv_from_s3(RAW_BUCKET, lookup_key)

    # Step 2: Transform (Apply Data Quality rules)
    logger.info("Starting data transformation...")
    cleaned_df = transform_data(taxi_df, lookup_df)

    # Step 3: Load to Curated Zone
    write_parquet_to_s3(cleaned_df, CURATED_BUCKET, curated_key)
    
    logger.info("Transform process successfully completed! Silver Zone is ready.")

if __name__ == "__main__":
    main()