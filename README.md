# AI Data Quality Monitor for Streaming ML Pipelines

A production-grade real-time data quality and drift detection system for machine learning pipelines. Ingests streaming feature events from Kafka, validates them per micro-batch, detects statistical drift using RFF-MMD, and routes actionable alerts to logs, Slack, email, and a live HTML dashboard — all with hysteresis and backoff to prevent alert flapping.

---

## Architecture

```
Data Generator / Producer  ──────────────────────────────────────────
  python data_simulator/producer.py                                  │
  [normal events]  [null_spike]  [range_violation]                   │
  [schema_corruption]  [distribution_drift]                          │
                  │                                                   │
                  ▼                                                   │
         Apache Kafka  ←─────────────────────────────────────────────┘
         topic: ml-features
                  │
                  ▼
    Spark Structured Streaming
    foreachBatch · Trigger: 30s · Checkpointing
                  │
          ┌───────┴────────┐
          ▼                ▼
  Feature Validation    RFF-MMD Drift Detection
  Nulls · Ranges        WarmupManager (3 phases)
  Categories            KS · PSI · Chi-Squared · Null-rate
          │                ▼
          └────────┬────────┘
                   ▼
          Alert Dispatcher
          ├── StructuredLogHandler  (always on)
          ├── FileLogHandler        → logs/alerts.jsonl
          ├── SlackHandler          → Slack webhook
          ├── EmailHandler          → SMTP
          └── DashboardHandler      → docs/dashboard_state.json
                                           │
                                           ▼
                                   docs/dashboard.html
                                   (auto-refreshes every 5s)
```

See `docs/architecture.png` for the rendered diagram.

---

## Key Design Decisions

**RFF-MMD as primary drift detector** — Random Fourier Features approximate the kernel MMD in O(n·D) instead of O(n²). This gives a single numeric drift score per window that is fast enough for streaming and can be calibrated from data.

**Three-phase warm-up** — Before any alert fires, the system goes through WARMUP (collecting baseline) → CALIBRATE (scoring under normal conditions) → MONITORING (comparing against calibrated threshold). This eliminates cold-start false positives entirely.

**Hysteresis + backoff** — Two consecutive violations must occur before an alert fires, and the same alert key cannot re-fire within 120 seconds. This prevents flapping during transient spikes.

**Complementary checks alongside MMD** — KS test, PSI, Chi-Squared, and null-rate monitoring provide per-feature explainability after MMD flags a window-level shift.

---

## Project Structure

```
ai-data-quality-monitor/
├── Makefile                          # One-command operations (Option E)
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .gitignore
│
├── data_simulator/
│   ├── producer.py                   # Kafka producer + 4 incident modes
│   └── sample_events.json
│
├── streaming_job/
│   ├── spark_job.py                  # Entry point — BatchProcessor
│   ├── validation.py                 # Null / range / category checks
│   ├── drift.py                      # RFF-MMD + WarmupManager + KS/PSI/Chi2
│   ├── alerts.py                     # Multi-handler dispatcher + hysteresis
│   ├── dashboard.py                  # HTML dashboard generator (Option B)
│   ├── feature_store.py              # Feature registry + baseline (Option A)
│   └── model_monitor.py              # Prediction/label/perf tracking (Option D)
│
├── configs/
│   └── config.yaml
│
├── notebooks/
│   └── exploration.ipynb
│
├── logs/
│   └── alerts.jsonl                  # Persistent alert log (auto-created)
│
└── docs/
    ├── architecture.png
    ├── dashboard.html                # Live HTML dashboard (Option B)
    └── dashboard_state.json          # Updated by streaming job
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Streaming Ingestion | Apache Kafka (Confluent Platform 7.5) |
| Stream Processing | Apache Spark 3.4 — Structured Streaming |
| Drift Detection | RFF-MMD · KS Test · PSI · Chi-Squared (NumPy, SciPy) |
| Containerisation | Docker & Docker Compose |
| Alerting | Logging · JSONL file · Slack webhook · SMTP email |
| Dashboard | Self-contained HTML + Chart.js (Option B) |
| Feature Store | In-process JSON registry (Option A) |
| Model Monitoring | Prediction/label/performance tracking (Option D) |

---

## Quick Start (One Command — Option E)

```bash
make run
```

This starts the Docker infrastructure (Kafka, Zookeeper, Spark), generates the dashboard HTML, and prints the next steps. Then open two more terminals:

```bash
# Terminal 1 — Spark streaming job
make spark-job

# Terminal 2 — Feature producer
make producer
```

Open `docs/dashboard.html` in your browser — it auto-refreshes every 5 seconds.

---

## Manual Setup

### 1. Install dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start infrastructure

```bash
make infra-up
# Kafka UI  → http://localhost:8080
# Spark UI  → http://localhost:8081
```

### 3. Start the Spark job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  streaming_job/spark_job.py
```

### 4. Start the producer

```bash
python data_simulator/producer.py --interval 0.2 --total-events 10000
```

### 5. Open the dashboard

```bash
make dashboard
```

---

## Warm-up Phases

The system goes through three phases before alerting:

| Phase | Batches | What happens |
|---|---|---|
| **WARMUP** | 1–5 | Collecting raw rows to fit the RFF-MMD baseline |
| **CALIBRATE** | 6–10 | Scoring under normal conditions; setting the 99th-percentile threshold |
| **MONITORING** | 11+ | Full drift detection active; alerts can fire |

Configured in `configs/config.yaml` under `warmup:`.

---

## Example Log Output

```
# Warm-up phase
2026-04-15 10:00:02 [INFO] drift — [WARMUP 1/5] Collected 312 baseline rows. 4 batch(es) remaining.
2026-04-15 10:00:32 [INFO] drift — [WARMUP 2/5] Collected 634 baseline rows. 3 batch(es) remaining.

# Calibration phase
2026-04-15 10:02:32 [INFO] drift — [CALIBRATE 1/5] MMD score=0.000821. 4 batch(es) remaining.
2026-04-15 10:03:02 [INFO] drift — [CALIBRATE 2/5] MMD score=0.000934. 3 batch(es) remaining.
2026-04-15 10:04:32 [INFO] drift — [Calibrate → Monitoring] Threshold set to 0.003471. Drift detection is now ACTIVE.

# Normal monitoring
2026-04-15 10:05:02 [INFO] spark_job — [batch 11] phase=monitoring mmd=0.00112 threshold=0.00347 val_issues=0 rows=287
2026-04-15 10:05:32 [INFO] drift — [batch 12] Complementary checks: no drift detected.

# Incident begins (null_spike)
2026-04-15 10:06:02 [WARNING] validation — [batch 13] ⚠ 2 issue type(s) found in 290 rows (validity rate: 74.1%)
2026-04-15 10:06:02 [WARNING] validation —   → [null_value] feature='age' count=162 rate=55.86%
2026-04-15 10:06:02 [WARNING] validation —   → [null_value] feature='purchase_amount' count=148 rate=51.03%
2026-04-15 10:06:02 [INFO] alerts — [Hysteresis] key='validation_null_value_age' violation 1/2 — waiting.

# Second consecutive bad window — alert fires
2026-04-15 10:06:32 [WARNING] alerts — [ALERT:HIGH] batch=14 type=validation_null_value | what=Feature 'age' has null_value in 171 rows (59.0%) | why=Null Value detected above threshold | value=0.59
2026-04-15 10:06:32 [WARNING] alerts — [ALERT:HIGH] batch=14 type=validation_null_value | what=Feature 'purchase_amount' has null_value in 153 rows (52.8%) | why=Null Value detected above threshold | value=0.528

# Distribution drift detected (distribution_drift incident)
2026-04-15 10:10:02 [WARNING] drift — [batch 22] RFF-MMD DRIFT: score=0.031470 > threshold=0.003471
2026-04-15 10:10:02 [WARNING] drift — [KS DRIFT] feature='age' ks=0.7821 p=0.000001
2026-04-15 10:10:02 [WARNING] drift — [PSI ALERT] feature='purchase_amount' psi=0.4120 (>0.2)
2026-04-15 10:10:02 [WARNING] alerts — [Hysteresis] key='rff_mmd_drift' violation 1/2 — waiting.
2026-04-15 10:10:32 [CRITICAL] alerts — [ALERT:CRITICAL] batch=23 type=rff_mmd_drift | what=RFF-MMD drift score=0.029810 exceeds threshold=0.003471 | why=Feature distribution has shifted significantly from baseline | value=0.02981
```

---

## Incident Testing

Four controlled incidents can be injected via the producer:

| Incident | Command | What it simulates |
|---|---|---|
| Null spike | `make incident-null` | 80% of events have nulled features (pipeline outage) |
| Range violation | `make incident-range` | Out-of-bounds age, negative amounts, invalid durations |
| Schema corruption | `make incident-schema` | Unknown device types, type mismatches, missing user_id |
| Distribution drift | `make incident-drift` | Age shifts to 65–105, purchase amounts spike 10×  |

Each incident starts after 200 normal events, runs for 60–120 seconds, then returns to normal. The system should detect the issue within 2 consecutive bad windows and avoid re-alerting for 2 minutes after the first alert.

---

## Alert Channels

| Channel | Config key | Format |
|---|---|---|
| Console log | always on | Structured `[ALERT:SEVERITY]` lines |
| File log | `alerts.log_file` | JSONL — one JSON object per alert |
| Slack | `alerts.slack_webhook_url` | Block-kit message with severity colour |
| Email | `alerts.email.enabled` | Plain-text SMTP with subject `[SEVERITY] type (batch N)` |
| Dashboard | `alerts.dashboard_state_path` | JSON state read by `docs/dashboard.html` |

Each alert includes: **what** failed, **why** it failed, **severity**, and the **metric value** that triggered it.

---

## Hysteresis & Backoff

Configured in `configs/config.yaml` under `alerts.hysteresis`:

```yaml
alerts:
  hysteresis:
    required_consecutive: 2   # 2 bad windows before alert fires
    cooldown_seconds: 120     # no re-alert for 2 minutes per key
```

Each alert key (e.g. `validation_null_value_age`, `rff_mmd_drift`) is tracked independently.

---

## Extensions

### Option A — Feature Store (`streaming_job/feature_store.py`)

A `FeatureRegistry` stores named baseline snapshots (mean, std, percentiles, frequency distributions) in a JSON file. Use it to version and compare baselines across model versions or time periods.

```python
registry = FeatureRegistry("feature_store/registry.json")
registry.register_baseline("v1_2026-Q1", baseline_df)
report = registry.compare_to_baseline("v1_2026-Q1", current_df)
```

### Option B — Live Dashboard (`streaming_job/dashboard.py`)

A self-contained HTML dashboard at `docs/dashboard.html` reads `docs/dashboard_state.json` every 5 seconds. Shows RFF-MMD score over time, per-feature null rates, batch validity rates, and a scrollable alert history table with severity badges.

```bash
make dashboard    # generate + open in browser
```

### Option C — Slack Alerts

Set `alerts.slack_webhook_url` in `configs/config.yaml`:

```yaml
alerts:
  slack_webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

Alerts of severity MEDIUM and above are posted as Slack Block Kit messages.

### Option D — Model Monitoring (`streaming_job/model_monitor.py`)

`ModelMonitor` tracks prediction drift (KS on score distributions), label drift (z-test on positive rate), and performance degradation (rolling accuracy + AUC) from a joined prediction + ground-truth stream.

```python
monitor = ModelMonitor(config)
report = monitor.process_prediction_batch(predictions, labels, batch_id)
```

### Option E — One-Command Docker Setup (`Makefile`)

```bash
make run             # start everything
make incident-drift  # inject a distribution shift
make infra-down      # stop all containers
make clean           # remove caches and venv
```

---

## Configuration Reference

All parameters live in `configs/config.yaml`:

```yaml
warmup:
  window_batches: 5          # batches to collect baseline
  calibration_batches: 5     # batches to calibrate threshold
  calibration_percentile: 99 # threshold = 99th pct of normal scores

drift:
  rff_mmd:
    n_components: 200        # RFF dimensionality
    sigma: null              # bandwidth (null = median heuristic)

alerts:
  hysteresis:
    required_consecutive: 2
    cooldown_seconds: 120
  log_file: "logs/alerts.jsonl"
  dashboard_state_path: "docs/dashboard_state.json"
  slack_webhook_url: null
  email:
    enabled: false
    smtp_host: "smtp.gmail.com"
    recipients: ["oncall@example.com"]
```

---

## License

MIT
