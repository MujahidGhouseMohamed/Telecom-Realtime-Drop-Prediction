import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

# ------------------------------------
# Load environment variables
# ------------------------------------
load_dotenv()

LOCAL_KAFKA = os.getenv("LOCAL_KAFKA", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telecom_calls")

EVENT_HUB_FQDN = os.getenv("EVENTHUB_FQDN")            # Example: tele-ns.servicebus.windows.net:9093
EVENT_HUB_CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")  # Actual EventHub key v

if not EVENT_HUB_FQDN or not EVENT_HUB_CONN_STR:
    raise ValueError(" Missing EVENTHUB_FQDN or EVENTHUB_CONNECTION_STRING in environment variables!")


# ------------------------------------
# Create Kafka Consumer
# ------------------------------------
def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=LOCAL_KAFKA,
        group_id="telecom-mirror",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


# ------------------------------------
# Create Event Hub Producer
# ------------------------------------
def create_producer():
    return KafkaProducer(
        bootstrap_servers=EVENT_HUB_FQDN,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=EVENT_HUB_CONN_STR,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


# ------------------------------------
# Mirror Function
# ------------------------------------
def mirror_data():
    consumer = create_consumer()
    producer = create_producer()

    print(" Kafka → Azure Event Hub Mirroring started...")
    print(f" Listening on local Kafka: {LOCAL_KAFKA}")
    print(f" Forwarding to Azure Event Hub: {EVENT_HUB_FQDN}")

    while True:
        try:
            for msg in consumer:
                producer.send(TOPIC, msg.value)
                print(f" Mirrored to Azure Event Hub: {msg.value}")

        except Exception as e:
            print(f" Error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            producer = create_producer()  # recreate connection


if __name__ == "__main__":
    mirror_data()
