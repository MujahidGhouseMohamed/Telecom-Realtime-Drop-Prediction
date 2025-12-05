#  Telecom Real-Time Call Drop Prediction System  
### Kafka + Azure Event Hub + FastAPI + ML + ADLS + Real-Time Dashboard

A production-style real-time telecom analytics system that predicts call drop probability from streaming network telemetry.

---

##  System Architecture

```
Mobile Network → Producer → Kafka → Mirror to Azure Event Hub → ADLS (Bronze/Silver/Gold)
        ↘──── FastAPI ML Inference API → Web UI (Live Prediction)
```

---

##  Repository Structure

```
telecom-realtime-drop-system/
│
├── api/
│   ├── app.py                   # FastAPI App (UI + Prediction API)
│   ├── requirements.txt         # Dependencies
│   └── static/
│       └── index.html           # Beautiful web UI
│
├── kafka_pipeline/
│   ├── telecom_producer.py          # Creates and streams telecom events
│   ├── mirror_to_azure.py           # Mirrors Kafka → Azure Event Hub
│   ├── telecom_consumer_to_adls.py  # Writes events to ADLS
│
├── ml_training/
│   ├── train_realtime_model.py      # Real-time optimized model
│   ├── silver_transformer.py        # Bronze → Silver cleanup
│   ├── gold_transformer.py          # Silver → Gold aggregation
│ 
├── .gitignore
└── README.md
```

---

##  Tech Stack

### **Backend**
- FastAPI  
- Uvicorn  
- Jinja2 Templates  

### **Streaming**
- Apache Kafka  
- Azure Event Hub  

### **Storage**
- Azure Data Lake Storage (ADLS Gen2)  
- Azure Blob Storage (Model Hosting)  

### **ML**
- scikit-learn  
- XGBoost  
- pandas  

### **Cloud**
- Azure App Service  
- Azure Storage Account  
- SAS Tokens  

---

## ML Prediction Model

Model Inputs:

| Feature | Description |
|--------|-------------|
| region_type | Urban / Rural / Highway |
| signal_strength | dBm |
| latency_ms | Network latency |
| jitter_ms | Variation in latency |
| throughput_kbps | Internet speed |
| packet_loss_percent | Packet loss |
| hour_num | Hour of the day |

Model Output:

```json
{
  "drop_probability": 0.92,
  "drop_risk": "HIGH",
  "predicted_label": 1
}
```

---

## API Usage

### `POST /predict`

#### Request
```json
{
  "region_type": "Urban",
  "signal_strength": -85,
  "latency_ms": 120,
  "jitter_ms": 15,
  "throughput_kbps": 2500,
  "packet_loss_percent": 1.2,
  "hour_num": 14
}
```

#### Response
```json
{
  "drop_probability": 0.92,
  "drop_risk": "HIGH",
  "predicted_label": 1
}
```

---

## 🌐 Azure Deployment

### 1️ Model hosted in Azure Blob Storage

Provide a SAS-protected URL like:

```
telecomapp-g0dqfna5fbayfvbf.centralindia-01.azurewebsites.net
```



##  Local Installation

### Clone repo
```
git clone https://github.com/MujahidGhouseMohamed/Telecom-Realtime-Drop-Prediction.git
cd telecom-realtime-drop-system/api
```

### Install dependencies
```
pip install -r requirements.txt
```

### Run API locally
```
uvicorn app:app --reload
```

---

##  Kafka Real-Time Pipeline

### Start Producer
```
python kafka_pipeline/telecom_producer.py
```

### Mirror Kafka → Azure Event Hub
```
python kafka_pipeline/mirror_to_azure.py
```

### Consume to ADLS
```
python kafka_pipeline/telecom_consumer_to_adls.py
```

---

##  Data Engineering Layers

```
Bronze → Raw events
Silver → Clean transformed data
Gold → Aggregated telecom KPIs
```

Used for dashboards + ML retraining.

---


##  Resume / Interview Value

- Real-time ML deployment  
- Kafka + Azure streaming integration  
- End-to-end DE + ML + Cloud workflow  
- Production-grade UI + API  
- Demonstrates cloud engineering skills  

---

##  Author  
**Mujahid Ghouse Mohamed**  
This project showcases real-time telecom analytics + machine learning + Azure cloud architecture.
