import os
import json
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from azure.eventhub import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient

import pyarrow as pa
import pyarrow.parquet as pq
import io

# ------------------------------------
# Load secrets from .env
# ------------------------------------
load_dotenv()

EVENTHUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "telecom_calls")

STORAGE_ACCOUNT_URL = os.getenv("STORAGE_ACCOUNT_URL")       # Example: https://teledatalake2.blob.core.windows.net
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")       # Access key
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "telecom-bronze")

if not EVENTHUB_CONNECTION_STR or not STORAGE_ACCOUNT_KEY:
    raise ValueError(" Missing Azure secrets. Check your .env file.")


# ------------------------------------
# Initialize Blob service
# ------------------------------------
blob_service = BlobServiceClient(
    account_url=STORAGE_ACCOUNT_URL,
    credential=STORAGE_ACCOUNT_KEY
)

container_client = blob_service.get_container_client(CONTAINER_NAME)

# Buffer to batch events
buffer_data = []


# ------------------------------------
# EventHub → ADLS Handler
# ------------------------------------
def on_event(partition_context, event):
    global buffer_data

    try:
        data = json.loads(event.body_as_str())
        buffer_data.append(data)

        print(f"📥 Received: {data}")

        # Write once buffer reaches 200 events
        if len(buffer_data) >= 200:
            write_to_adls(buffer_data)
            buffer_data = []

    except Exception as e:
        print(" Error processing event:", e)


# ------------------------------------
# Write to ADLS Gen2 as Parquet
# ------------------------------------
def write_to_adls(records):
    if not records:
        return

    df = pd.DataFrame(records)

    now = datetime.utcnow()
    folder_path = f"year={now.year}/month={now.month}/day={now.day}"
    file_name = f"telecom_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    full_path = f"{folder_path}/{file_name}"

    print(f"⬆ Uploading {len(records)} → {full_path}")

    # Convert DF → Parquet (memory)
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    blob_client = container_client.get_blob_client(full_path)
    blob_client.upload_blob(buffer, overwrite=True)

    print(f" Uploaded to ADLS: {full_path}")


# ------------------------------------
# Start Consumer
# ------------------------------------
def main():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENTHUB_NAME,
    )

    print(" Listening for telecom events (Event Hub → ADLS)...")

    with client:
        client.receive(
            on_event=on_event,
            starting_position="-1"  # read from beginning
        )


if __name__ == "__main__":
    main()
