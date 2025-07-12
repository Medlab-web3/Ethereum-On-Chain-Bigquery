import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

# Google Cloud settings
GCS_BUCKET = os.environ.get('GCS_BUCKET', 'crypto-data-bucket-xyz')
CSV_BLOB = os.environ.get('CSV_BLOB', 'onchain_data/eth_daily_tx_count.csv')

# Indicator settings
DAYS_BACK = int(os.environ.get('DAYS_BACK', 30))
DAYS_PER_BATCH = int(os.environ.get('DAYS_PER_BATCH', 7))
TABLE = 'bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions'

def download_blob_to_dataframe(bucket_name, blob_name):
   
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    content = blob.download_as_text()
    return pd.read_csv(pd.compat.StringIO(content))

def upload_dataframe_to_blob(df, bucket_name, blob_name):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

def get_last_date(df):
   
    if df is None or df.empty:
        return None
    return pd.to_datetime(df['day']).max().date()

def fetch_data_from_bigquery(start_date, end_date):
    
    client = bigquery.Client()
    query = f"""
    SELECT
      DATE(block_timestamp) AS day,
      COUNT(*) AS tx_count
    FROM `{TABLE}`
    WHERE DATE(block_timestamp) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY day
    ORDER BY day
    """
    return client.query(query).to_dataframe()

def main(request=None):
    # 1. Load previous data from GCS if exists
    df_old = download_blob_to_dataframe(GCS_BUCKET, CSV_BLOB)

    # 2. Determine the last loaded day
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=DAYS_BACK)
    last_date = get_last_date(df_old)
    if last_date:
        fetch_start = last_date + timedelta(days=1)
    else:
        fetch_start = start_date

    # 3. If no new data to fetch, exit
    if fetch_start > today:
        print("No new data to fetch.")
        return "No update needed."

    # 4. Fetch new data in batches
    df_all_batches = []
    while fetch_start <= today:
        fetch_end = min(fetch_start + timedelta(days=DAYS_PER_BATCH - 1), today)
        print(f"Fetching batch: {fetch_start} to {fetch_end}")
        df_batch = fetch_data_from_bigquery(fetch_start, fetch_end)
        if not df_batch.empty:
            df_all_batches.append(df_batch)
        fetch_start = fetch_end + timedelta(days=1)

    if not df_all_batches:
        print("No new data found.")
        return "No update needed."

    df_new = pd.concat(df_all_batches)

    # 5. Merge with previous data and remove duplicates
    if df_old is not None:
        df_all = pd.concat([df_old, df_new]).drop_duplicates(subset=['day']).sort_values('day')
    else:
        df_all = df_new

    # 6. Save the final DataFrame to GCS
    upload_dataframe_to_blob(df_all, GCS_BUCKET, CSV_BLOB)
    print(f"Updated data up to {today}. New rows: {len(df_new)}.")
    return f"Update completed. New rows: {len(df_new)}."

if __name__ == "__main__":
    main()
