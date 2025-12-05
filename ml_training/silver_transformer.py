import os
import io
import sys
from datetime import datetime, timezone
from typing import List

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()


# ---------- CONFIG ----------
STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT_NAME", "telecomdatalake2")
STORAGE_KEY = os.environ.get("STORAGE_ACCOUNT_KEY", "<paste_key_here>")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "telecom-bronze")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "telecom-silver")
# Optional prefix to process a single partition (e.g. "year=2025/month=11/day=26")
PREFIX = os.environ.get("PREFIX", "")  

BATCH_BYTES_LIMIT = 200 * 1024 * 1024  # 200 MB before flush (optional)

# ---------- INIT CLIENT ----------
BLOB_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
blob_service = BlobServiceClient(account_url=BLOB_URL, credential=STORAGE_KEY)
bronze_client = blob_service.get_container_client(BRONZE_CONTAINER)
silver_client = blob_service.get_container_client(SILVER_CONTAINER)

# create silver container if not exists
try:
    silver_client.create_container()
    print("Created silver container")
except Exception:
    pass

# ---------- HELPERS ----------
def list_bronze_blobs(prefix: str) -> List[str]:
    """List blob names under bronze container with prefix."""
    blob_list = bronze_client.list_blobs(name_starts_with=prefix)
    return [b.name for b in blob_list]

def download_to_df(blob_name: str) -> pd.DataFrame:
    """Download a single parquet blob and return DataFrame."""
    blob = bronze_client.get_blob_client(blob_name)
    stream = io.BytesIO()
    download_stream = blob.download_blob()
    download_stream.readinto(stream)
    stream.seek(0)
    # read with pyarrow
    table = pq.read_table(stream)
    df = table.to_pandas()
    return df

def upload_df_to_silver(df: pd.DataFrame, path: str):
    """Write df as parquet to silver container with overwrite behavior."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression='snappy')
    buf.seek(0)
    blob = silver_client.get_blob_client(path)
    blob.upload_blob(buf, overwrite=True)
    print("Uploaded to silver:", path)

# ---------- CLEANING LOGIC ----------
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning, typing, dedupe, and simple feature engineering."""
    if df.empty:
        return df

    # Ensure column names normalized
    df.columns = [c.strip() for c in df.columns]

    # parse timestamp -> UTC datetime
    if 'timestamp_utc' in df.columns:
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True, errors='coerce')

    # drop rows missing critical info
    required = ['event_id', 'timestamp_utc', 'tower_id']
    for col in required:
        if col not in df.columns:
            print(f"WARNING: required column {col} missing in bronze file")
    df = df.dropna(subset=[c for c in required if c in df.columns])

    # dedupe by event_id keeping latest timestamp
    if 'event_id' in df.columns:
        df = df.sort_values('timestamp_utc').drop_duplicates(subset=['event_id'], keep='last')

    # numeric casts and safe coercion
    numeric_cols = ['handoff_count', 'call_duration_sec', 'signal_strength',
                    'latency_ms', 'jitter_ms', 'throughput_kbps', 'packet_loss_percent',
                    'drop_probability', 'dropped']
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # basic validation / clipping
    if 'signal_strength' in df.columns:
        df.loc[df['signal_strength'] < -140, 'signal_strength'] = None
        df.loc[df['signal_strength'] > -10, 'signal_strength'] = None

    if 'call_duration_sec' in df.columns:
        df.loc[df['call_duration_sec'] < 0, 'call_duration_sec'] = None

    if 'throughput_kbps' in df.columns:
        df.loc[df['throughput_kbps'] < 0, 'throughput_kbps'] = None

    # fill small NaNs if you want (optional)
    # df['packet_loss_percent'] = df['packet_loss_percent'].fillna(0)

    # add partition/date fields
    if 'timestamp_utc' in df.columns:
        df['event_date'] = df['timestamp_utc'].dt.date
        df['year'] = df['timestamp_utc'].dt.year.astype('int')
        df['month'] = df['timestamp_utc'].dt.month.astype('int')
        df['day'] = df['timestamp_utc'].dt.day.astype('int')
        # minute bucket
        df['event_minute'] = df['timestamp_utc'].dt.floor('T')

    # reorder columns (bring event_id first if exists)
    cols = list(df.columns)
    if 'event_id' in cols:
        cols.insert(0, cols.pop(cols.index('event_id')))
        df = df[cols]

    return df

# ---------- MAIN PROCESS ----------
def process_prefix(prefix: str):
    print("Listing bronze blobs with prefix:", prefix or "<all>")
    blobs = list_bronze_blobs(prefix)
    if not blobs:
        print("No bronze blobs found for prefix:", prefix)
        return

    # Option: group by partition folder (year=.../month=.../day=...)
    partitions = {}
    for b in blobs:
        # get partition part up to last slash
        parts = b.split('/')
        if len(parts) >= 4:
            partition = '/'.join(parts[:3])  # e.g. year=.../month=.../day=...
        else:
            partition = 'root'
        partitions.setdefault(partition, []).append(b)

    for partition, blob_names in partitions.items():
        print(f"\nProcessing partition {partition} -> {len(blob_names)} files")
        dfs = []
        for name in blob_names:
            try:
                df = download_to_df(name)
                print(f"  downloaded {name} rows={len(df)}")
                dfs.append(df)
            except Exception as e:
                print("  error reading", name, e)

        if not dfs:
            print("  nothing to process in this partition")
            continue

        combined = pd.concat(dfs, ignore_index=True, sort=False)
        print("  combined rows:", len(combined))

        cleaned = clean_df(combined)
        print("  cleaned rows:", len(cleaned))

        if cleaned.empty:
            print("  cleaned df empty, skipping upload")
            continue

        # build target path: preserve partition folder and unique filename
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        # partition folder should be e.g. year=2025/month=11/day=26
        target_folder = partition
        target_filename = f"telecom_silver_{now}.parquet"
        target_path = f"{target_folder}/{target_filename}"

        # upload to silver
        upload_df_to_silver(cleaned, target_path)

def main():
    prefix = PREFIX
    process_prefix(prefix)

if __name__ == "__main__":
    main()
