# gold_transformer.py
import os
import io
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


# load env
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "telecom-silver")
GOLD_CONTAINER = os.getenv("GOLD_CONTAINER", "telecom-gold")
PREFIX = os.getenv("PREFIX", "")   # e.g. year=2025/month=11/day=26 or empty for all

BLOB_URL = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

blob_service = BlobServiceClient(account_url=BLOB_URL, credential=STORAGE_ACCOUNT_KEY)
silver_container_client = blob_service.get_container_client(SILVER_CONTAINER)
gold_container_client = blob_service.get_container_client(GOLD_CONTAINER)

def ensure_gold_container():
    try:
        gold_container_client.create_container()
        print(f"Created gold container: {GOLD_CONTAINER}")
    except ResourceExistsError:
        pass

def list_silver_blobs(prefix=PREFIX):
    # return list of blob names under prefix
    print("Listing silver blobs with prefix:", prefix or "<all>")
    blobs = silver_container_client.list_blobs(name_starts_with=prefix)
    return [b.name for b in blobs]

def download_parquet_to_df(blob_name):
    blob = silver_container_client.get_blob_client(blob_name)
    stream = io.BytesIO()
    print("Downloading", blob_name)
    downloader = blob.download_blob()
    downloader.readinto(stream)
    stream.seek(0)
    table = pq.read_table(stream)
    df = table.to_pandas()
    return df

def upload_parquet_bytes(dataframe, remote_path):
    # dataframe -> parquet in memory -> upload
    table = pa.Table.from_pandas(dataframe)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    blob_client = gold_container_client.get_blob_client(remote_path)
    blob_client.upload_blob(buffer, overwrite=True)
    print("Uploaded to gold:", remote_path)

def compute_tower_daily(df):
    # ensure timestamp is datetime
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['date'] = df['timestamp_utc'].dt.date
    grouped = df.groupby(['tower_id', 'date']).agg(
        total_calls = ('event_id','count'),
        dropped_calls = ('dropped','sum'),
        drop_rate = ('dropped','mean'),
        avg_signal = ('signal_strength','mean'),
        avg_latency = ('latency_ms','mean'),
        avg_jitter = ('jitter_ms','mean'),
        avg_throughput = ('throughput_kbps','mean'),
        avg_packet_loss = ('packet_loss_percent','mean')
    ).reset_index()
    # normalize/round
    grouped['drop_rate'] = grouped['drop_rate'].round(4)
    grouped = grouped.round(4)
    grouped['year'] = grouped['date'].apply(lambda d: d.year)
    grouped['month'] = grouped['date'].apply(lambda d: d.month)
    grouped['day'] = grouped['date'].apply(lambda d: d.day)
    return grouped

def compute_region_hourly(df):
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['hour'] = df['timestamp_utc'].dt.floor('H')
    grouped = df.groupby(['region_type','hour']).agg(
        total_calls = ('event_id','count'),
        dropped_calls = ('dropped','sum'),
        drop_rate = ('dropped','mean'),
        avg_latency = ('latency_ms','mean'),
        avg_jitter = ('jitter_ms','mean'),
    ).reset_index()
    grouped['drop_rate'] = grouped['drop_rate'].round(4)
    grouped['year'] = grouped['hour'].dt.year
    grouped['month'] = grouped['hour'].dt.month
    grouped['day'] = grouped['hour'].dt.day
    grouped['hour_of_day'] = grouped['hour'].dt.hour
    return grouped

def compute_device_os_daily(df):
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['date'] = df['timestamp_utc'].dt.date
    grouped = df.groupby(['device_os','date']).agg(
        total_calls = ('event_id','count'),
        dropped_calls = ('dropped','sum'),
        drop_rate = ('dropped','mean'),
        avg_latency = ('latency_ms','mean'),
    ).reset_index()
    grouped['drop_rate'] = grouped['drop_rate'].round(4)
    grouped['year'] = grouped['date'].apply(lambda d: d.year)
    grouped['month'] = grouped['date'].apply(lambda d: d.month)
    grouped['day'] = grouped['date'].apply(lambda d: d.day)
    return grouped

def main(prefix=PREFIX):
    ensure_gold_container()
    blob_names = list_silver_blobs(prefix)
    if not blob_names:
        print("No silver files found for prefix:", prefix)
        return

    # read all silver files into one dataframe (careful about memory; sample expects manageable size)
    dfs = []
    for b in blob_names:
        try:
            df = download_parquet_to_df(b)
            dfs.append(df)
        except Exception as e:
            print("Error reading", b, "->", e)
    if not dfs:
        print("No readable silver files.")
        return

    full = pd.concat(dfs, ignore_index=True)
    print("Combined rows:", len(full))

    # --- 1) tower_daily_kpis ---
    tower_df = compute_tower_daily(full)
    # write partitioned parquet per date; we will write one file per run keyed by timestamp
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    tower_remote = f"tower_daily_kpis/year={now[0:4]}/month={now[4:6]}/day={now[6:8]}/tower_daily_kpis_{now}.parquet"
    upload_parquet_bytes(tower_df, tower_remote)

    # --- 2) region_hourly_metrics ---
    region_df = compute_region_hourly(full)
    region_remote = f"region_hourly_metrics/year={now[0:4]}/month={now[4:6]}/day={now[6:8]}/region_hourly_metrics_{now}.parquet"
    upload_parquet_bytes(region_df, region_remote)

    # --- 3) device_os_summary ---
    device_df = compute_device_os_daily(full)
    device_remote = f"device_os_summary/year={now[0:4]}/month={now[4:6]}/day={now[6:8]}/device_os_summary_{now}.parquet"
    upload_parquet_bytes(device_df, device_remote)

    print("Gold tables created: rows ->",
          f"tower:{len(tower_df)} region:{len(region_df)} device:{len(device_df)}")

if __name__ == "__main__":
    # optional CLI arg prefix
    prefix_arg = sys.argv[1] if len(sys.argv) > 1 else os.getenv("PREFIX", "")
    main(prefix_arg)
