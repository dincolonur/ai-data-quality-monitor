"""
data_simulator/producer.py
───────────────────────────
Kafka producer that simulates a continuous ML feature stream.
Supports 4 controlled incident modes for testing the monitoring system.

Normal mode (default):
    python data_simulator/producer.py

Incident modes:
    python data_simulator/producer.py --incident null_spike
    python data_simulator/producer.py --incident range_violation
    python data_simulator/producer.py --incident schema_corruption
    python data_simulator/producer.py --incident distribution_drift

    --incident-after N    start incident after N normal events (default 200)
    --incident-duration S duration of incident in seconds (default 60)
"""

import json
import logging
import random
import time
import argparse
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_fixed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("producer")

CONFIG_PATH = Path(__file__).parent.parent / "configs" / "config.yaml"

DEVICE_TYPES     = ["mobile", "desktop", "tablet"]
BAD_DEVICE_TYPES = ["smartwatch", "tv", "unknown", "", None]


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ════════════════════════════════════════════════════════════════════════════════
# Incident Types
# ════════════════════════════════════════════════════════════════════════════════

class Incident(str, Enum):
    NULL_SPIKE          = "null_spike"
    RANGE_VIOLATION     = "range_violation"
    SCHEMA_CORRUPTION   = "schema_corruption"
    DISTRIBUTION_DRIFT  = "distribution_drift"
    NONE                = "none"


# ════════════════════════════════════════════════════════════════════════════════
# Event Generators
# ════════════════════════════════════════════════════════════════════════════════

def _base_event(event_id: int) -> dict:
    return {
        "event_id":        f"evt_{event_id:08d}",
        "user_id":         f"user_{random.randint(1, 100_000)}",
        "age":             random.randint(18, 75),
        "purchase_amount": round(random.uniform(0.0, 500.0), 2),
        "session_duration":random.randint(30, 3600),
        "page_views":      random.randint(1, 50),
        "device_type":     random.choice(DEVICE_TYPES),
        "timestamp":       int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def generate_normal(event_id: int) -> dict:
    """Healthy event within expected distributions."""
    return _base_event(event_id)


def generate_null_spike(event_id: int) -> dict:
    """
    Incident: NULL SPIKE
    Randomly nulls one or more key features (~80 % of events are corrupted).
    Simulates a data pipeline outage or upstream schema change.
    """
    event = _base_event(event_id)
    target_features = ["age", "purchase_amount", "session_duration"]
    # Null out 1–3 features with 80 % probability
    if random.random() < 0.80:
        for feat in random.sample(target_features, k=random.randint(1, 2)):
            event[feat] = None
    return event


def generate_range_violation(event_id: int) -> dict:
    """
    Incident: RANGE VIOLATION
    Injects extreme out-of-bounds values.
    Simulates a sensor miscalibration or unit-of-measure bug.
    """
    event = _base_event(event_id)
    violation = random.choice(["age_overflow", "negative_amount", "zero_duration"])
    if violation == "age_overflow":
        event["age"] = random.randint(130, 999)
    elif violation == "negative_amount":
        event["purchase_amount"] = round(random.uniform(-5000.0, -0.01), 2)
    elif violation == "zero_duration":
        event["session_duration"] = random.randint(-500, -1)
    return event


def generate_schema_corruption(event_id: int) -> dict:
    """
    Incident: SCHEMA CORRUPTION
    Introduces unknown device types and structurally wrong values.
    Simulates a new client app sending unexpected payloads.
    """
    event = _base_event(event_id)
    corruption = random.choice(["bad_device", "string_in_numeric", "missing_user"])
    if corruption == "bad_device":
        event["device_type"] = random.choice(BAD_DEVICE_TYPES)
    elif corruption == "string_in_numeric":
        # Age arrives as a string (type mismatch — Spark will cast to null)
        event["age"] = "unknown"
    elif corruption == "missing_user":
        event["user_id"] = None
    return event


def generate_distribution_drift(event_id: int) -> dict:
    """
    Incident: DISTRIBUTION DRIFT
    Shifts age distribution toward elderly population and spikes purchase amounts.
    Simulates a real demographic or seasonal change in the user base.
    """
    event = _base_event(event_id)
    event["age"]             = random.randint(65, 105)
    event["purchase_amount"] = round(random.uniform(2000.0, 20000.0), 2)
    event["page_views"]      = random.randint(1, 5)   # very low engagement
    return event


GENERATORS = {
    Incident.NONE:               generate_normal,
    Incident.NULL_SPIKE:         generate_null_spike,
    Incident.RANGE_VIOLATION:    generate_range_violation,
    Incident.SCHEMA_CORRUPTION:  generate_schema_corruption,
    Incident.DISTRIBUTION_DRIFT: generate_distribution_drift,
}

INCIDENT_DESCRIPTIONS = {
    Incident.NULL_SPIKE:
        "NULL SPIKE — nulling out age/purchase_amount/session_duration in ~80% of events",
    Incident.RANGE_VIOLATION:
        "RANGE VIOLATION — injecting out-of-bounds age / negative amounts / invalid durations",
    Incident.SCHEMA_CORRUPTION:
        "SCHEMA CORRUPTION — unknown device types, type mismatches, missing user_id",
    Incident.DISTRIBUTION_DRIFT:
        "DISTRIBUTION DRIFT — age shifted to 65–105, purchase_amount spiked to 2k–20k",
}


# ════════════════════════════════════════════════════════════════════════════════
# Kafka Producer
# ════════════════════════════════════════════════════════════════════════════════

@retry(stop=stop_after_attempt(5), wait=wait_fixed(3))
def create_producer(bootstrap_servers: str) -> KafkaProducer:
    logger.info(f"Connecting to Kafka at {bootstrap_servers}…")
    p = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=10,
    )
    logger.info("Kafka producer connected.")
    return p


# ════════════════════════════════════════════════════════════════════════════════
# Main Stream Loop
# ════════════════════════════════════════════════════════════════════════════════

def run(
    topic: str,
    bootstrap_servers: str,
    interval: float,
    incident: Incident,
    incident_after: int,
    incident_duration: float,
    total_events: int,
) -> None:
    producer = create_producer(bootstrap_servers)

    incident_start_time: Optional[float] = None
    incident_active = False
    sent = 0

    logger.info(
        f"Stream starting → topic='{topic}' interval={interval}s "
        f"incident={incident.value} after={incident_after} events "
        f"for {incident_duration}s"
    )

    for i in range(1, total_events + 1):
        try:
            now = time.time()

            # ── Incident state machine ────────────────────────────────────
            if incident != Incident.NONE:
                if not incident_active and i > incident_after:
                    incident_active    = True
                    incident_start_time = now
                    logger.warning(
                        f"\n{'='*60}\n"
                        f"  INCIDENT STARTED at event #{i}\n"
                        f"  Type: {INCIDENT_DESCRIPTIONS[incident]}\n"
                        f"  Duration: {incident_duration}s\n"
                        f"{'='*60}"
                    )

                if incident_active and (now - incident_start_time) >= incident_duration:
                    incident_active = False
                    logger.warning(
                        f"\n{'='*60}\n"
                        f"  INCIDENT ENDED at event #{i} "
                        f"(lasted {incident_duration:.0f}s)\n"
                        f"  Returning to normal events.\n"
                        f"{'='*60}"
                    )

            # ── Choose generator ──────────────────────────────────────────
            if incident_active:
                event = GENERATORS[incident](i)
                label = f"[{incident.value.upper()}]"
            else:
                event = generate_normal(i)
                label = "[normal]"

            producer.send(topic, key=event.get("user_id"), value=event)
            sent += 1

            # ── Progress log every 100 events ─────────────────────────────
            if i % 100 == 0:
                logger.info(f"  {label} event #{i:06d} | sent={sent}")

            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info(f"\nStopped by user after {sent} events.")
            break
        except Exception as e:
            logger.error(f"Send error at event #{i}: {e}")

    producer.flush()
    producer.close()
    logger.info(f"Producer finished. Total sent: {sent}")


# ════════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════════

def parse_args():
    cfg = load_config()
    p = argparse.ArgumentParser(description="ML Feature Stream Producer with incident injection")
    p.add_argument("--topic",             default=cfg["kafka"]["topic"])
    p.add_argument("--bootstrap-servers", default=cfg["kafka"]["bootstrap_servers"])
    p.add_argument("--interval",  type=float, default=0.2,
                   help="Seconds between events")
    p.add_argument("--incident",
                   choices=[i.value for i in Incident],
                   default=Incident.NONE.value,
                   help="Incident type to inject (default: none)")
    p.add_argument("--incident-after",    type=int,   default=200,
                   help="Start incident after N normal events")
    p.add_argument("--incident-duration", type=float, default=60.0,
                   help="Incident duration in seconds")
    p.add_argument("--total-events",      type=int,   default=10_000)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        topic             = args.topic,
        bootstrap_servers = args.bootstrap_servers,
        interval          = args.interval,
        incident          = Incident(args.incident),
        incident_after    = args.incident_after,
        incident_duration = args.incident_duration,
        total_events      = args.total_events,
    )
