# AI Data Quality Monitor for Streaming ML Pipelines

A production-grade real-time data quality and drift detection system for machine learning pipelines. Ingests streaming feature events from Kafka, validates them per micro-batch, detects statistical drift using RFF-MMD, and routes actionable alerts to logs, Slack, email, and a live web dashboard — all with hysteresis and backoff to prevent alert flapping.

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
                                   Control Panel UI (localhost:7070)
                                   + docs/dashboard.html
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
├── Makefile                          # One-command operations
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
│   ├── dashboard.py                  # HTML dashboard generator
│   ├── feature_store.py              # Feature registry + baseline
│   └── model_monitor.py              # Prediction/label/perf tracking
│
├── ui/
│   ├── app.py                        # FastAPI control panel backend
│   └── static/
│       └── index.html                # Control panel frontend (Chart.js)
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
    ├── dashboard.html                # Static HTML dashboard
    └── dashboard_state.json          # Updated by streaming job
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Streaming Ingestion | Apache Kafka (Confluent Platform 7.5) |
| Stream Processing | Apache Spark 3.5 — Structured Streaming |
| Drift Detection | RFF-MMD · KS Test · PSI · Chi-Squared (NumPy, SciPy) |
| Containerisation | Docker & Docker Compose |
| Alerting | Logging · JSONL file · Slack webhook · SMTP email |
| Control Panel | FastAPI + uvicorn + Chart.js (localhost:7070) |
| Static Dashboard | Self-contained HTML + Chart.js |
| Feature Store | In-process JSON registry |
| Model Monitoring | Prediction/label/performance tracking |

---

## Quick Start

### Step 1 — Install Python dependencies (once)

```bash
make setup
```

### Step 2 — Start the Docker infrastructure

```bash
make infra-up
```

This starts Zookeeper, Kafka, Kafka UI, and Spark (master + worker).

| Service | URL | Description |
|---|---|---|
| Kafka UI | http://localhost:8080 | Browse topics, messages, consumer groups |
| Spark Master UI | http://localhost:8081 | Spark cluster status, running jobs |

### Step 3 — Open the Control Panel

In a new terminal:

```bash
make ui
```

Then open **http://localhost:7070** in your browser.

From the Control Panel you can start/stop the producer, inject incidents, launch the Spark job, and watch live metrics and alerts — without touching the terminal.

### Step 4 (optional) — Run from the CLI instead

```bash
# Terminal 1 — Spark streaming job
make spark-job

# Terminal 2 — Feature producer
make producer
```

---

## Service URLs

All URLs available once `make infra-up` and `make ui` are running:

| URL | Service | Notes |
|---|---|---|
| http://localhost:7070 | Control Panel | Main UI — start/stop jobs, view metrics and alerts |
| http://localhost:7070/api/status | Status API | JSON — Kafka reachability, producer/Spark PID, dashboard state |
| http://localhost:7070/api/logs | Log Snapshot | JSON — last 100 log lines (REST fallback if SSE not connecting) |
| http://localhost:7070/api/alerts | Alerts API | JSON — last 50 alerts from `logs/alerts.jsonl` |
| http://localhost:7070/api/metrics | Metrics API | JSON — full `dashboard_state.json` snapshot |
| http://localhost:7070/api/config | Config API | JSON — current `configs/config.yaml` (GET to read, PUT to update) |
| http://localhost:7070/api/logs/stream | Live Log Stream | SSE stream — real-time producer and Spark stdout |
| http://localhost:8080 | Kafka UI | Browse topics, messages, consumer groups |
| http://localhost:8081 | Spark Master UI | Cluster status, active/completed jobs |

### Debugging with the API directly

If the UI appears blank or logs aren't showing, hit the JSON endpoints directly in your browser:

```bash
# Check if Kafka is up and processes are running
curl http://localhost:7070/api/status

# See recent log output (errors, launch commands, process exits)
curl http://localhost:7070/api/logs

# See fired alerts
curl http://localhost:7070/api/alerts
```

The `/api/logs` endpoint is especially useful — it shows the exact command used to launch `spark-submit` and any `FileNotFoundError` or crash output from the producer or Spark job.

---

## Control Panel (localhost:7070)

The web UI is a FastAPI + Chart.js application with:

- **Live status bar** — Kafka reachability, producer PID, Spark job PID, warm-up phase
- **Producer controls** — incident mode, event interval, total events, start/stop
- **Quick incident buttons** — one-click injection of any of the 4 incident types
- **Spark job controls** — start/stop spark-submit from the UI
- **Live metrics charts** — RFF-MMD score, per-feature null rates, batch validity rate (Chart.js, streaming)
- **Alert feed** — severity-badged alert history with what/why/value
- **Live log stream** — SSE-based tail of producer and Spark stdout, colour-coded by source
- **Config editor** — hysteresis tuning (required consecutive violations, cooldown) saved back to `configs/config.yaml`

```bash
make ui    # starts uvicorn on :7070
```

> Requires `make setup` to be run first.

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

## Incident Testing

Four controlled incidents can be injected via the producer or directly from the Control Panel:

| Incident | CLI command | What it simulates |
|---|---|---|
| Null spike | `make incident-null` | 80% of events have nulled features (pipeline outage) |
| Range violation | `make incident-range` | Out-of-bounds age, negative amounts, invalid durations |
| Schema corruption | `make incident-schema` | Unknown device types, type mismatches, missing user_id |
| Distribution drift | `make incident-drift` | Age shifts to 65–105, purchase amounts spike 10× |

Each incident starts after 200 normal events, runs for 60–120 seconds, then returns to normal. The system detects the issue within 2 consecutive bad windows and suppresses re-alerting for 2 minutes after the first alert.

---

## Example Log Output

```
# Warm-up phase
2026-04-15 10:00:02 [INFO] drift — [WARMUP 1/5] Collected 312 baseline rows. 4 batch(es) remaining.
2026-04-15 10:00:32 [INFO] drift — [WARMUP 2/5] Collected 634 baseline rows. 3 batch(es) remaining.

# Calibration phase
2026-04-15 10:02:32 [INFO] drift — [CALIBRATE 1/5] MMD score=0.000821. 4 batch(es) remaining.
2026-04-15 10:04:32 [INFO] drift — [Calibrate → Monitoring] Threshold set to 0.003471. Drift detection is now ACTIVE.

# Normal monitoring
2026-04-15 10:05:02 [INFO] spark_job — [batch 11] phase=monitoring mmd=0.00112 threshold=0.00347 val_issues=0 rows=287

# Incident begins (null_spike)
2026-04-15 10:06:02 [WARNING] validation — [batch 13] ⚠ 2 issue type(s) found in 290 rows (validity rate: 74.1%)
2026-04-15 10:06:02 [INFO] alerts — [Hysteresis] key='validation_null_value_age' violation 1/2 — waiting.

# Second consecutive bad window — alert fires
2026-04-15 10:06:32 [WARNING] alerts — [ALERT:HIGH] batch=14 type=validation_null_value | what=Feature 'age' has null_value in 171 rows (59.0%) | why=Null Value detected above threshold | value=0.59

# Distribution drift detected
2026-04-15 10:10:32 [CRITICAL] alerts — [ALERT:CRITICAL] batch=23 type=rff_mmd_drift | what=RFF-MMD drift score=0.029810 exceeds threshold=0.003471 | why=Feature distribution has shifted significantly from baseline | value=0.02981
```

---

## Alert Channels

| Channel | Config key | Format |
|---|---|---|
| Console log | always on | Structured `[ALERT:SEVERITY]` lines |
| File log | `alerts.log_file` | JSONL — one JSON object per alert |
| Slack | `alerts.slack_webhook_url` | Block-kit message with severity colour |
| Email | `alerts.email.enabled` | Plain-text SMTP with subject `[SEVERITY] type (batch N)` |
| Dashboard | `alerts.dashboard_state_path` | JSON state read by the Control Panel and `docs/dashboard.html` |

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

## Docker Notes

The stack uses `apache/spark:3.5.0` (official multi-arch image) which runs natively on both `linux/amd64` and `linux/arm64` (Apple Silicon). Kafka and Zookeeper use Confluent Platform 7.5 images.

```bash
make infra-up    # start all containers
make infra-down  # stop and remove containers + volumes
```

---

## Makefile Reference

```
make setup            Install Python dependencies into .venv
make infra-up         Start Kafka + Spark via Docker Compose
make infra-down       Stop all containers
make ui               Start web control panel on :7070
make spark-job        Submit Spark streaming job via spark-submit
make producer         Start normal feature stream
make dashboard        Generate + open static HTML dashboard

make incident-null    Inject null spike (60s)
make incident-range   Inject range violations (60s)
make incident-schema  Inject schema corruption (60s)
make incident-drift   Inject distribution drift (120s)

make lint             Run flake8
make clean            Remove caches and .venv
make clean-data       Clear runtime state files (alerts, checkpoints)
```

---

## Extensions

**Feature Store** (`streaming_job/feature_store.py`) — A `FeatureRegistry` stores named baseline snapshots (mean, std, percentiles, frequency distributions) in a JSON file. Use it to version and compare baselines across model versions or time periods.

**Model Monitoring** (`streaming_job/model_monitor.py`) — `ModelMonitor` tracks prediction drift (KS on score distributions), label drift (z-test on positive rate), and performance degradation (rolling accuracy + AUC) from a joined prediction + ground-truth stream.

**Slack Alerts** — Set `alerts.slack_webhook_url` in `configs/config.yaml` to a Slack incoming webhook URL. Alerts of severity MEDIUM and above are posted as Block Kit messages.

---

## License

MIT
