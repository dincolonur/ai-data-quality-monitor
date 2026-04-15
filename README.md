# AI Data Quality Monitor for Streaming ML Pipelines

A real-time data quality and drift detection system for machine learning pipelines. The system ingests streaming feature events, validates them in near real-time, detects statistical drift against a reference baseline, and triggers structured alerts when problems are found.

---

## Project Goals

1. **Ingest streaming data** from a Kafka topic populated by a simulated ML feature producer
2. **Validate feature quality** — catch nulls, out-of-range values, type mismatches, and schema violations in each micro-batch
3. **Detect data drift** — compare live distributions against a baseline using KS test, PSI, Chi-Squared, and null-rate monitoring
4. **Trigger alerts** — dispatch structured warnings and critical alerts to logs and optionally to Slack when anomalies are detected

This project demonstrates skills in streaming data systems, data engineering, ML-aware monitoring, and production-style system design.

---

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│         Data Generator / Producer           │
│   Python · kafka-python · Drift injection   │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│              Apache Kafka                   │
│      Topic: ml-features · Port 9092         │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│       Spark Structured Streaming            │
│   PySpark · foreachBatch · Trigger: 30s     │
└──────────┬──────────────────────┬───────────┘
           │                      │
           ▼                      ▼
┌──────────────────┐   ┌──────────────────────┐
│ Feature          │   │   Drift Detection    │
│ Validation       │   │  KS · PSI · Chi2     │
│ Null / Range /   │   │  Null rate drift     │
│ Schema checks    │   │  vs. baseline        │
└────────┬─────────┘   └──────────┬───────────┘
         │                        │
         └──────────┬─────────────┘
                    ▼
┌─────────────────────────────────────────────┐
│           Alerting & Logging                │
│  Structured logs · Slack webhook · History  │
└─────────────────────────────────────────────┘
```

See `docs/architecture.png` for the rendered diagram.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Streaming Ingestion | Apache Kafka (Confluent Platform 7.5) |
| Stream Processing | Apache Spark 3.4 — Structured Streaming |
| Feature Validation | PySpark |
| Drift Detection | NumPy, SciPy (KS test, PSI, Chi-Squared) |
| Containerization | Docker & Docker Compose |
| Alerting | Python `logging` + optional Slack webhook |
| Notebooks | Jupyter, Matplotlib, Seaborn |

---

## Project Structure

```
ai-data-quality-monitor/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .gitignore
│
├── data_simulator/
│   ├── producer.py          # Kafka producer — simulates ML feature events + drift injection
│   └── sample_events.json   # Sample normal and drifted event payloads
│
├── streaming_job/
│   ├── spark_job.py         # Spark Structured Streaming entry point
│   ├── validation.py        # Feature validation: nulls, ranges, categories
│   ├── drift.py             # Drift detection: KS, PSI, Chi-Squared, null rate
│   └── alerts.py            # Alert dispatcher: logs + Slack
│
├── configs/
│   └── config.yaml          # All system configuration
│
├── notebooks/
│   └── exploration.ipynb    # EDA, baseline profiling, drift visualisation
│
└── docs/
    └── architecture.png     # System architecture diagram
```

---

## How to Run

### Prerequisites

- **Docker & Docker Compose** — for Kafka, Zookeeper, and Spark
- **Python 3.10+**
- **Java 11+** — required by Spark

---

### Step 1 — Start the Infrastructure

```bash
docker-compose up -d
```

This starts:
- **Zookeeper** on port `2181`
- **Kafka broker** on port `9092` (internal: `29092`)
- **Kafka UI** on `http://localhost:8080` — inspect topics in your browser
- **Spark master** on `http://localhost:8081`
- **Spark worker** (2 cores, 2 GB)

Verify Kafka is healthy:
```bash
docker-compose ps
```

---

### Step 2 — Install Python Dependencies

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

### Step 3 — Start the Feature Producer

```bash
python data_simulator/producer.py
```

Optional flags:
```bash
python data_simulator/producer.py \
  --topic ml-features \
  --interval 0.2 \
  --drift-after 300 \
  --total-events 5000
```

The producer sends normal feature events and then injects drift (age shift, null injection, amount spikes, bad device types) after `--drift-after` events.

---

### Step 4 — Run the Spark Streaming Job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  streaming_job/spark_job.py
```

The job will:
1. Read from the `ml-features` Kafka topic
2. Validate each micro-batch against the rules in `configs/config.yaml`
3. Build a baseline distribution from the first 5 batches
4. Run drift detection on all subsequent batches
5. Log alerts to stdout and optionally to Slack

---

### Step 5 — Explore Locally in the Notebook

```bash
jupyter notebook notebooks/exploration.ipynb
```

The notebook lets you simulate both normal and drifted data, visualise distributions, and prototype KS / PSI / Chi-Squared detection logic interactively.

---

## Configuration

All system parameters are in `configs/config.yaml`:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "ml-features"

drift:
  baseline_window_batches: 5    # batches used to build baseline
  ks:
    significance_level: 0.05
  psi:
    warning_threshold: 0.1
    alert_threshold: 0.2

alerts:
  enabled: true
  slack_webhook_url: null       # replace with your Slack webhook URL
```

---

## Monitored Features

| Feature | Type | Validation Rule |
|---|---|---|
| `user_id` | string | not null, non-empty |
| `age` | int | 0 – 120 |
| `purchase_amount` | float | >= 0.0 |
| `session_duration` | int | >= 0 |
| `page_views` | int | >= 0 |
| `device_type` | string | mobile / desktop / tablet |
| `timestamp` | long | valid epoch ms |

---

## Drift Detection Methods

**Kolmogorov-Smirnov (KS) Test** — two-sample test for distribution shift on continuous features. Alerts when p-value falls below the configured significance level (default: 0.05).

**Population Stability Index (PSI)** — binned comparison of reference vs. current distribution. PSI < 0.1 is stable, 0.1–0.2 is a warning, > 0.2 is a critical shift.

**Chi-Squared Test** — categorical drift detection for features like `device_type`. Detects when category frequency distributions diverge significantly.

**Null Rate Drift** — monitors per-feature null rates across batches and alerts when the delta exceeds a threshold (default: 5 percentage points).

---

## Extending the Project

- Integrate **Feast** as a feature store to pull reference baselines
- Add **MLflow** to correlate data drift events with model performance metrics
- Connect **Grafana + Prometheus** for live dashboards
- Deploy on **Kubernetes** with Helm and persist alerts to a database

---

## License

MIT
