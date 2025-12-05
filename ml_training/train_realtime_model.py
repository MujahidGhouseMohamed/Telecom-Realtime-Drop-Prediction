import os
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from xgboost import XGBClassifier
import joblib

load_dotenv()

STORAGE_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER")

def load_silver_latest():
    if STORAGE_CONN is None:
        raise Exception("AZURE_STORAGE_CONNECTION_STRING missing in .env!")

    client = BlobServiceClient.from_connection_string(STORAGE_CONN)
    container = client.get_container_client(SILVER_CONTAINER)

    blobs = [b for b in container.list_blobs() if b.name.endswith(".parquet")]
    if not blobs:
        raise Exception("No parquet files found in silver container.")

    latest = sorted(blobs, key=lambda b: b.last_modified)[-1]
    print("🔍 Loading:", latest.name)

    # Read blob into memory safely
    blob_data = container.download_blob(latest.name).readall()
    df = pd.read_parquet(BytesIO(blob_data))

    print("📌 Silver Columns:", df.columns.tolist())
    return df


def preprocess(df):
    # Map region categorical
    region_map = {"urban": 0, "rural": 1, "highway": 2}
    df["region_type"] = df["region_type"].str.lower().map(region_map).fillna(0).astype(int)

    # Extract hour
    df["hour_num"] = pd.to_datetime(df["timestamp_utc"]).dt.hour

    # Use ACTUAL silver columns
    df = df[[
        "region_type",
        "signal_strength",
        "latency_ms",
        "jitter_ms",
        "throughput_kbps",
        "packet_loss_percent",
        "hour_num"
    ]]

    # Label: treat packet_loss_percent > 1% as drop
    df["label"] = (df["packet_loss_percent"] > 1.0).astype(int)

    return df


def train_model():
    df = load_silver_latest()
    df = preprocess(df)

    X = df.drop("label", axis=1)
    y = df["label"]

    model = XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.9,
        eval_metric="logloss"
    )

    print("Training model")
    model.fit(X, y)

    joblib.dump(model, "model.joblib")
    print("Model saved: model.joblib")


if __name__ == "__main__":
    train_model()
