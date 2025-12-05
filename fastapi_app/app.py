from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pydantic import BaseModel
import joblib
import pandas as pd
import os
import requests
from io import BytesIO
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Load environment variables
load_dotenv()

EVENTHUB_CONN = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
MODEL_URL = os.getenv("MODEL_URL")  # SAS URL of model in Blob Storage

# --------------------------------
# Load ML model from Blob Storage
# --------------------------------
def load_model():
    try:
        print("Downloading model.joblib from Blob Storage...")
        response = requests.get(MODEL_URL)
        response.raise_for_status()

        model = joblib.load(BytesIO(response.content))
        print(" Model loaded successfully")
        return model

    except Exception as e:
        print(" ERROR loading model:", e)
        raise e

model = load_model()

# FastAPI app
app = FastAPI(title="Telecom Real-Time Drop Prediction API")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="static")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Input Schema
class CallEvent(BaseModel):
    region_type: str
    signal_strength: float
    latency_ms: float
    jitter_ms: float
    throughput_kbps: float
    packet_loss_percent: float
    hour_num: int

# Send to EventHub
def send_to_eventhub(data_dict):
    try:
        if EVENTHUB_CONN and EVENTHUB_NAME:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=EVENTHUB_CONN,
                eventhub_name=EVENTHUB_NAME
            )
            batch = producer.create_batch()
            batch.add(EventData(str(data_dict)))
            producer.send_batch(batch)
            print("✔ Sent to Event Hub")
        else:
            print("⚠ EventHub not configured")
    except Exception as e:
        print("EventHub Error:", e)

# Predict endpoint
@app.post("/predict")
def predict(event: CallEvent):

    region_map = {"urban": 0, "rural": 1, "highway": 2}
    region_val = region_map.get(event.region_type.lower(), 0)

    df = pd.DataFrame([{
        "region_type": region_val,
        "signal_strength": float(event.signal_strength),
        "latency_ms": float(event.latency_ms),
        "jitter_ms": float(event.jitter_ms),
        "throughput_kbps": float(event.throughput_kbps),
        "packet_loss_percent": float(event.packet_loss_percent),
        "hour_num": int(event.hour_num)
    }])

    pred = model.predict(df)[0]
    prob = model.predict_proba(df)[0][1]

    output = {
        "drop_probability": float(prob),
        "drop_risk": "HIGH" if prob >= 0.6 else "LOW",
        "predicted_label": int(pred)
    }

    send_to_eventhub(output)
    return output
