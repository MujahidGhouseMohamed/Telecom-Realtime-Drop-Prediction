import json
import random
import time
import uuid
from datetime import datetime
from math import exp

from kafka import KafkaProducer

# ---------- CONFIG ----------

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "telecom_calls"

# Tower profiles: mean & std dev for signal (dBm), latency (ms), jitter (ms)
TOWER_PROFILES = [
    {
        "tower_id": "TWR_U01",
        "region_type": "urban",
        "city": "Chennai",
        "base_signal_mean": -80,
        "base_signal_std": 5,
        "base_latency_mean": 60,
        "base_latency_std": 15,
        "base_jitter_mean": 10,
        "base_jitter_std": 5,
    },
    {
        "tower_id": "TWR_U02",
        "region_type": "urban",
        "city": "Bengaluru",
        "base_signal_mean": -78,
        "base_signal_std": 6,
        "base_latency_mean": 55,
        "base_latency_std": 12,
        "base_jitter_mean": 9,
        "base_jitter_std": 4,
    },
    {
        "tower_id": "TWR_R01",
        "region_type": "rural",
        "city": "Villupuram",
        "base_signal_mean": -90,
        "base_signal_std": 7,
        "base_latency_mean": 90,
        "base_latency_std": 25,
        "base_jitter_mean": 20,
        "base_jitter_std": 8,
    },
    {
        "tower_id": "TWR_H01",
        "region_type": "highway",
        "city": "Chennai–Trichy Highway",
        "base_signal_mean": -88,
        "base_signal_std": 8,
        "base_latency_mean": 100,
        "base_latency_std": 30,
        "base_jitter_mean": 25,
        "base_jitter_std": 10,
    },
]


DEVICE_OS_CHOICES = ["android", "ios", "feature_phone"]
CALL_TYPE_CHOICES = ["voice", "video", "data"]
MOBILITY_CHOICES = ["stationary", "walking", "driving"]


def time_of_day_traffic_multiplier(now: datetime) -> float:
    """Higher multiplier during peak hours."""
    hour = now.hour

    # Rough Indian usage pattern
    if 8 <= hour <= 11:      # morning peak
        return 1.8
    elif 18 <= hour <= 22:   # evening peak
        return 2.2
    elif 0 <= hour <= 5:     # late night
        return 0.3
    else:                    # normal hours
        return 1.0


def network_degradation_factor() -> float:
    """
    Occasionally simulate bad network episodes.
    Returns multiplier for latency/jitter and penalty for signal.
    """
    # 5% chance that we are in a degraded network period
    if random.random() < 0.05:
        latency_mult = random.uniform(1.5, 3.0)
        jitter_mult = random.uniform(1.5, 3.0)
        signal_penalty = random.uniform(5, 15)  # extra dB loss
    else:
        latency_mult = 1.0
        jitter_mult = 1.0
        signal_penalty = 0.0

    return latency_mult, jitter_mult, signal_penalty


def sample_from_profile(profile: dict) -> dict:
    """Generate base network metrics for a tower based on its profile."""
    latency_mult, jitter_mult, signal_penalty = network_degradation_factor()

    signal_strength = random.gauss(
        profile["base_signal_mean"], profile["base_signal_std"]
    ) - signal_penalty

    latency_ms = max(
        10.0,
        random.gauss(profile["base_latency_mean"], profile["base_latency_std"])
        * latency_mult,
    )

    jitter_ms = max(
        1.0,
        random.gauss(profile["base_jitter_mean"], profile["base_jitter_std"])
        * jitter_mult,
    )

    # Throughput roughly inversely related to latency and jitter
    base_throughput = random.uniform(200, 2000)  # kbps
    throughput_kbps = max(
        50.0, base_throughput * (100.0 / (100.0 + latency_ms + jitter_ms))
    )

    # Packet loss a bit higher if latency+jitter are bad
    base_loss = random.uniform(0.0, 2.0)
    extra_loss = max(0.0, (latency_ms - 80) * 0.02 + (jitter_ms - 20) * 0.03)
    packet_loss_percent = max(0.0, min(20.0, base_loss + extra_loss))

    return {
        "signal_strength": round(signal_strength, 2),
        "latency_ms": round(latency_ms, 2),
        "jitter_ms": round(jitter_ms, 2),
        "throughput_kbps": round(throughput_kbps, 2),
        "packet_loss_percent": round(packet_loss_percent, 2),
    }


def compute_drop_probability(event: dict) -> float:
    """
    Compute a realistic drop probability based on metrics.
    We'll use a simple logistic-like function.
    """
    signal = event["signal_strength"]
    latency = event["latency_ms"]
    jitter = event["jitter_ms"]
    packet_loss = event["packet_loss_percent"]
    mobility = event["mobility"]
    handoff = event["handoff_count"]

    # Feature weights (tuned manually to feel realistic)
    w_signal = -0.10  # better signal -> less drop
    w_latency = 0.015
    w_jitter = 0.03
    w_packet_loss = 0.12
    w_handoff = 0.4

    mobility_weight = {
        "stationary": 0.0,
        "walking": 0.25,
        "driving": 0.6,
    }[mobility]

    # Linear score
    score = 0.0
    score += w_signal * (signal + 80)       # centered around -80 dBm
    score += w_latency * (latency - 80)     # centered around 80ms
    score += w_jitter * (jitter - 20)       # centered around 20ms
    score += w_packet_loss * packet_loss
    score += w_handoff * handoff
    score += mobility_weight

    # Pass through logistic to get probability between 0 and 1
    prob = 1.0 / (1.0 + exp(-score))
    return prob


def generate_event() -> dict:
    now = datetime.utcnow().isoformat()

    profile = random.choice(TOWER_PROFILES)
    network = sample_from_profile(profile)

    device_os = random.choices(
        DEVICE_OS_CHOICES, weights=[0.7, 0.25, 0.05], k=1
    )[0]

    call_type = random.choices(
        CALL_TYPE_CHOICES, weights=[0.6, 0.25, 0.15], k=1
    )[0]

    mobility = random.choices(
        MOBILITY_CHOICES, weights=[0.5, 0.25, 0.25], k=1
    )[0]

    # More handoffs if highway + driving
    if profile["region_type"] == "highway" and mobility == "driving":
        handoff_count = random.choices([0, 1, 2, 3], weights=[0.2, 0.4, 0.3, 0.1])[0]
    else:
        handoff_count = random.choices([0, 1, 2], weights=[0.6, 0.3, 0.1])[0]

    call_duration_sec = max(
        5,
        int(random.gauss(60, 30))
    )

    base_event = {
        "event_id": str(uuid.uuid4()),
        "timestamp_utc": now,
        "tower_id": profile["tower_id"],
        "region_type": profile["region_type"],
        "city": profile["city"],
        "device_os": device_os,
        "call_type": call_type,
        "mobility": mobility,
        "handoff_count": handoff_count,
        "call_duration_sec": call_duration_sec,
    }

    base_event.update(network)

    # Compute drop probability & label
    drop_prob = compute_drop_probability(base_event)
    dropped = 1 if random.random() < drop_prob else 0

    base_event["drop_probability"] = round(drop_prob, 4)
    base_event["dropped"] = dropped

    return base_event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing events to topic '{TOPIC_NAME}'... Ctrl+C to stop.")

    try:
        while True:
            now = datetime.now()
            traffic_mult = time_of_day_traffic_multiplier(now)

            # Base sleep roughly 0.3s, modified by traffic
            # Higher traffic -> smaller sleep -> more events
            base_sleep = 0.3
            sleep_time = max(0.05, base_sleep / traffic_mult)

            event = generate_event()
            producer.send(TOPIC_NAME, value=event)

            print(f"Sent: {event['event_id']} | tower={event['tower_id']} "
                  f"drop_prob={event['drop_probability']} dropped={event['dropped']}")

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
