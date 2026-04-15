"""
data_simulator/producer.py
──────────────────────────
Kafka producer that simulates a continuous stream of ML feature events.
Events are based on the schema defined in configs/config.yaml.

Usage:
    python data_simulator/producer.py
    python data_simulator/producer.py --topic ml-features --interval 0.5 --drift-after 200
"""

import json
import random
import time
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml
from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_fixed

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("producer")

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "configs" / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── Event Generation ──────────────────────────────────────────────────────────

DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def generate_normal_event(event_id: int) -> dict:
    """Generate a healthy feature event within expected distributions."""
    return {
        "event_id": f"evt_{event_id:08d}",
        "user_id": f"user_{random.randint(1, 100_000)}",
        "age": random.randint(18, 75),
        "purchase_amount": round(random.uniform(0.0, 500.0), 2),
        "session_duration": random.randint(30, 3600),
        "page_views": random.randint(1, 50),
        "device_type": random.choice(DEVICE_TYPES),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def generate_drifted_event(event_id: int) -> dict:
    """
    Generate a feature event with injected drift / anomalies.
    Simulates: age distribution shift, null injection, out-of-range values.
    """
    event = generate_normal_event(event_id)

    drift_type = random.choice(["age_shift", "null_injection", "amount_spike", "bad_device"])

    if drift_type == "age_shift":
        # Age distribution shifts toward elderly population
        event["age"] = random.randint(65, 110)

    elif drift_type == "null_injection":
        # Randomly null out a key feature
        null_target = random.choice(["age", "purchase_amount", "session_duration"])
        event[null_target] = None

    elif drift_type == "amount_spike":
        # Purchase amounts spike to unusual ranges
        event["purchase_amount"] = round(random.uniform(5000.0, 50000.0), 2)

    elif drift_type == "bad_device":
        # Unknown device type (schema violation)
        event["device_type"] = random.choice(["smartwatch", "tv", "unknown", ""])

    return event


# ── Kafka Producer ────────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(5), wait=wait_fixed(3))
def create_producer(bootstrap_servers: str) -> KafkaProducer:
    logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=10,
    )
    logger.info("Kafka producer connected.")
    return producer


def run_producer(
    topic: str,
    bootstrap_servers: str,
    interval: float,
    drift_after: int,
    total_events: int,
) -> None:
    producer = create_producer(bootstrap_servers)
    logger.info(
        f"Starting stream → topic='{topic}' | interval={interval}s | "
        f"drift injected after event #{drift_after}"
    )

    for i in range(1, total_events + 1):
        try:
            if i > drift_after:
                event = generate_drifted_event(i)
                label = "DRIFTED"
            else:
                event = generate_normal_event(i)
                label = "normal"

            producer.send(topic, key=event["user_id"], value=event)

            if i % 50 == 0 or label == "DRIFTED":
                logger.info(f"[{label}] Sent event #{i}: {event['event_id']}")

            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Producer stopped by user.")
            break
        except Exception as e:
            logger.error(f"Failed to send event #{i}: {e}")

    producer.flush()
    producer.close()
    logger.info(f"Producer finished. Sent {i} events.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    cfg = load_config()
    parser = argparse.ArgumentParser(description="ML Feature Stream Producer")
    parser.add_argument("--topic", default=cfg["kafka"]["topic"], help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", default=cfg["kafka"]["bootstrap_servers"])
    parser.add_argument("--interval", type=float, default=0.2, help="Seconds between events")
    parser.add_argument("--drift-after", type=int, default=300, help="Inject drift after N events")
    parser.add_argument("--total-events", type=int, default=10_000, help="Total events to send")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_producer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        interval=args.interval,
        drift_after=args.drift_after,
        total_events=args.total_events,
    )
