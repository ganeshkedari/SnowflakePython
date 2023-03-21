import json

import boto3
import numpy as np
import pandas as pd
import snowflake.connector

# Read configuration details from a separate file
with open('config.json', 'r') as f:
    config = json.load(f)

# Set up Snowflake connection
ctx = snowflake.connector.connect(
    user=config['snowflake_user'],
    password=config['snowflake_password'],
    account=config['snowflake_account'],
    warehouse=config['snowflake_warehouse'],
    database=config['snowflake_database'],
    schema=config['snowflake_schema']

)

# Set up S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=config['aws_access_key_id'],
    aws_secret_access_key=config['aws_secret_access_key']
)


# Define function to fetch data from Snowflake and save as parquet files on S3
def fetch_and_save_to_s3(query, s3_bucket, s3_prefix, chunk_size):
    # Set up Snowflake cursor and execute query
    print("query : ", query)
    cur = ctx.cursor()
    cur.execute(query)

    # Read results from cursor into Pandas DataFrame
    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    # Split DataFrame into chunks
    for i, chunk in enumerate(np.array_split(df, len(df) // chunk_size + 1)):
        # Save chunk as parquet file on S3
        s3_key = f"{s3_prefix}/chunk_{i}.parquet"
        filename = f"./snowflakeExport/chunk_{i}.parquet"
        print("File : ", s3_key)
        try:
            chunk.to_parquet(filename)
            s3.put_object(Body=chunk.to_parquet(), Bucket=s3_bucket, Key=s3_key)
        except Exception as e:
            print(f"Failed to upload chunk {i} to S3: {str(e)}")


# Example usage
query = config['query']
s3_bucket = config['s3_bucket']
s3_prefix = config['s3_prefix']
chunk_size = config['chunk_size']


fetch_and_save_to_s3(query, s3_bucket, s3_prefix, chunk_size)
