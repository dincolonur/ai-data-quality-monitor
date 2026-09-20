# AI Data Quality Monitor for Streaming ML Pipelines

A production-grade real-time data quality and drift detection system for machine learning pipelines. Built with Apache Kafka and Spark Structured Streaming, it validates feature events per micro-batch, detects statistical drift using RFF-MMD, and routes actionable alerts to a live web dashboard — all with hysteresis and backoff to prevent alert flapping.

**One command to run. No local Python or Spark installation required.**

```bash
make up
# → open http://localhost:7070
```

---

## Demo

1. `make up` — builds and starts the full stack
2. Open **http://localhost:7070**
3. Click **▶ Start** on the Producer
4. Click **▶ Start** on the Spark Job
5. Wait ~5 minutes for warm-up → calibration → monitoring phases
6. Click a **Quick Incident** button (e.g. Null Spike or Drift)
7. Watch alerts fire in the right panel within 2 batches (~1 minute)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Browser  →  http://localhost:7070 (Control Panel)              │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  FastAPI + Chart.js                                        │ │
│  │  ▶ Producer  ▶ Spark Job  │ Metrics  │ Alerts  │ Logs     │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────┬─────────────────────────────────────────────────┘
               │ subprocess (inside Docker)
    ┌──────────┴──────────────────────────────────┐
    │                                             │
    ▼                                             ▼
Producer                               spark-submit (local[2])
data_simulator/producer.py             streaming_job/spark_job.py
    │                                             │
    │ JSON events                                 │ foreachBatch
    ▼                                             │
Apache Kafka ────────────────────────────────────┘
topic: ml-features
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
           Feature Validation    RFF-MMD Drift Detection
           Nulls · Ranges        WarmupManager
           Categories            KS · PSI · Chi² · Null-rate
                    │                    │
                    └─────────┬──────────┘
                              ▼
                     Alert Dispatcher
                     ├── Console log   (always on)
                     ├── alerts.jsonl  (file)
                     ├── Slack webhook (optional)
                     └── dashboard_state.json
                                │
                                ▼
                     Control Panel — live charts
```

---

## Key Design Decisions

**RFF-MMD as primary drift detector** — Random Fourier Features approximate kernel MMD in O(n·D) instead of O(n²). One numeric drift score per micro-batch, fast enough for streaming and threshold-calibrated from data.

**Three-phase warm-up** — WARMUP (collect baseline) → CALIBRATE (score normal traffic, set 99th-pct threshold) → MONITORING (compare against threshold). Eliminates cold-start false positives entirely.

**Hysteresis + backoff** — Two consecutive bad windows must occur before an alert fires, and the same alert cannot re-fire within 120 seconds. Prevents flapping during transient spikes.

**Complementary checks alongside MMD** — KS, PSI, Chi-Squared, and null-rate provide per-feature explainability after MMD flags a shift.

---

## How It Works — Design & Logic

### The core question

Every 30 seconds, Spark asks: **"Is the data my ML model is receiving right now still healthy?"** It answers that in two complementary ways — fast rule-based validation and statistical drift detection — then routes the result to alerts if something looks wrong.

The overall flow is:

```
Data arrives → Kafka → Spark reads it → Validate + Detect Drift → Alert
```

---

### Layer 1: Data Ingestion (Kafka)

The **producer** (`data_simulator/producer.py`) generates fake user events — age, purchase amount, device type, etc. — and publishes them to a Kafka **topic** called `ml-features`.

Think of the topic as a pipe. Events flow in continuously. Spark subscribes to that pipe and reads from it in controlled batches, regardless of how fast events arrive. This is why Kafka is used instead of a file or direct API: it decouples the producer's pace from the consumer's processing schedule, and it buffers events safely if Spark falls behind.

---

### Layer 2: Stream Processing (Spark Structured Streaming)

Spark reads the Kafka stream and groups events into **micro-batches** — every 30 seconds, it collects whatever arrived and hands the batch to a Python function called `process_batch`.

```
time: 0s ──── 30s ──── 60s ──── 90s
          [batch 1] [batch 2] [batch 3]
```

Each batch is a small DataFrame — just like a table in Pandas. The `BatchProcessor` class owns the stateful lifecycle: it persists across batches so it can accumulate history, track which warm-up phase the system is in, and maintain calibration data from previous batches.

---

### Layer 3: Feature Validation (the fast checks)

The first thing `process_batch` does is run cheap, rule-based checks on every row:

- **Null check** — is `age` or `purchase_amount` missing?
- **Range check** — is `age` between 0–120? Is `purchase_amount` ≥ 0?
- **Category check** — is `device_type` one of mobile/desktop/tablet?

These catch obvious data corruption within one batch (~30 seconds). The result is a **validity rate**: the percentage of rows in this batch that passed all checks. If it drops below the configured threshold, an alert fires.

---

### Layer 4: Drift Detection (the smart check)

Validation only catches rules you wrote in advance. **Distribution drift** is subtler — the data looks valid but the underlying distribution has shifted. Imagine your model was trained on users aged 18–75, but now you're getting users aged 65–105. Every event passes validation, but the model will perform poorly because it's scoring on data it was never trained on.

This is what **RFF-MMD** detects.

#### What is MMD?

**Maximum Mean Discrepancy** is a statistical test that answers: "Do these two groups of numbers come from the same distribution?" It compares the **baseline** (what normal data looks like) against the **current batch**. The result is a single scalar score — near 0 means the distributions match, a high score means they've diverged.

The naive implementation of MMD computes a similarity between every pair of data points, which is O(n²). For streaming with large batches, this is too slow.

#### What are Random Fourier Features?

**Random Fourier Features** (RFF) are a mathematical approximation technique based on Bochner's theorem. Instead of comparing every pair of points, you project the data into a random D-dimensional space using random weights, then compare the mean embeddings of the two groups. The math guarantees this approximates the true MMD with high probability.

```python
# Simplified
W, b = random_weights()                  # sampled once, fixed for the session
Z_baseline = cos(X_baseline @ W + b)    # project baseline into D-dim space
Z_current  = cos(X_current  @ W + b)    # project current batch into same space
mmd_score  = ||mean(Z_baseline) - mean(Z_current)||²
```

This runs in O(n·D) — linear in the number of rows — making it fast enough for every micro-batch. D is set to 200 by default (configurable via `drift.rff_mmd.n_components`).

#### Complementary checks

MMD gives you one number: "something shifted." To know *which feature* shifted, the system also runs KS (Kolmogorov-Smirnov), PSI (Population Stability Index), and Chi-Squared tests per feature after MMD flags a batch. These are slower to compute but provide explainability — you can see that `age` drifted while `purchase_amount` stayed stable.

---

### Layer 5: The Three-Phase Warm-up

The system can't compare against a baseline it doesn't have yet, and it can't set a threshold without knowing what normal MMD scores look like on real data. So it progresses through three phases automatically:

| Phase | Batches | What happens |
|---|---|---|
| **WARMUP** | 1–5 | Collect raw rows. Build the baseline distribution from normal traffic. No scoring yet. |
| **CALIBRATE** | 6–10 | Run MMD against the baseline using known-good traffic. Record the scores. |
| **MONITORING** | 11+ | Set the alert threshold at the 99th percentile of calibration scores. Compare every new batch against it. |

The key insight is that the threshold is **learned from your data**, not set manually. Whatever MMD scores look like during normal operation, the 99th percentile becomes the line between "normal" and "alert." This means the system self-calibrates to your specific feature distributions without any tuning.

---

### Layer 6: Hysteresis + Backoff (preventing false alarms)

A single high MMD score can be noise — a small batch, late-arriving events, a momentary spike. Two defenses prevent that from causing spurious alerts:

**Hysteresis**: an alert only fires after **2 consecutive batches** exceed the threshold. One bad reading is ignored; two in a row means it's real.

**Cooldown**: after an alert fires, the same alert type is silenced for **120 seconds**. This prevents one incident from generating a new alert notification every 30 seconds.

These two together mean the system is sensitive enough to catch real problems quickly, but quiet enough that you don't start ignoring the alerts.

---

### Layer 7: Alert Dispatcher

The `AlertDispatcher` routes alerts to multiple channels simultaneously — log file, Slack webhook, email, and the dashboard state file. Each channel is a separate handler class. Adding a new destination (e.g. PagerDuty) means adding one new handler, not touching the core detection logic.

Alerts carry severity levels (LOW / MEDIUM / HIGH / CRITICAL) derived from how far the metric deviated from its threshold.

---

### Layer 8: The Control Panel

The FastAPI app (`ui/app.py`) does three things:

**Process management** — when you click ▶ Start Spark, the app runs `spark-submit` as a subprocess, captures its stdout/stderr into an in-memory ring buffer, and streams it to your browser via SSE (Server-Sent Events). This is how the Live Logs panel works — it's a persistent HTTP connection that pushes new lines as they arrive.

**State polling** — the Spark job writes a `dashboard_state.json` file after every batch. The control panel serves this file at `/api/metrics`. The browser polls it every 5 seconds and updates the charts. No WebSocket or database needed — a single JSON file on disk is the shared state between Spark and the UI.

**Live config** — the PUT `/api/config` endpoint deep-merges changes into `config.yaml`. The Spark job reads config at each batch, so changes to hysteresis settings take effect within one batch interval.

---

### The single design principle

Each layer has **one job**. Kafka buffers. Spark batches. Validation checks rules. MMD detects drift. The dispatcher routes. The UI displays. None of them know about each other except through well-defined interfaces (a DataFrame in, a dict of results out). This makes each piece easy to replace — swap Kafka for Kinesis, swap MMD for CUSUM, swap FastAPI for Streamlit — without touching the others.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Streaming Ingestion | Apache Kafka (Confluent 7.5) |
| Stream Processing | PySpark 3.5 — Structured Streaming |
| Drift Detection | RFF-MMD · KS · PSI · Chi-Squared (NumPy/SciPy) |
| Containerisation | Docker & Docker Compose |
| Control Panel | FastAPI + uvicorn + Chart.js |
| Alerting | Console · JSONL file · Slack webhook · SMTP |

---

## Quick Start (Docker)

**Requirements:** Docker Desktop (no Python, Java, or Spark needed locally)

```bash
git clone https://github.com/yourusername/ai-data-quality-monitor
cd ai-data-quality-monitor
make up
```

Open **http://localhost:7070** — the control panel loads immediately. Infrastructure takes ~20 seconds to be fully ready (watch the Kafka status dot turn green).

```bash
make down      # stop everything
make logs      # tail control-panel output
make restart   # rebuild after code changes
```

---

## Service URLs

| URL | Service |
|---|---|
| **http://localhost:7070** | **Control Panel — start here** |
| http://localhost:7070/api/status | JSON — Kafka, producer, Spark status |
| http://localhost:7070/api/logs | JSON — last 100 log lines (debug) |
| http://localhost:7070/api/alerts | JSON — last 50 alerts |
| http://localhost:7070/api/metrics | JSON — full dashboard state |
| http://localhost:8080 | Kafka UI — browse topics and messages |
| http://localhost:8081 | Spark cluster UI |

---

## Control Panel Walkthrough

### Status bar (top)
Shows Kafka reachability (green/red dot), Producer PID, Spark Job PID, and the current warm-up phase badge.

### Left sidebar — controls

**Producer** — configure and start the data generator:
- Incident Mode: choose the failure type before starting
- Event Interval: events per second (default 0.2s)
- Click **▶ Start** to begin streaming

**Spark Streaming Job** — start the detection engine:
- Click **▶ Start** — spark-submit launches inside the container
- The job downloads the Kafka connector (~30s on first run), then begins processing

**Quick Incidents** — one-click incident injection (stops and restarts the producer):
- **Null Spike** — 80% of events drop age/purchase_amount
- **Range** — out-of-bounds values (age 200, negative amounts)
- **Schema** — unknown device types, type mismatches
- **Drift** — age shifts to 65–105, purchase amounts spike 10×

**Hysteresis Config** — tune alert sensitivity live, saved back to `configs/config.yaml`.

### Center — live metrics (update every 5s)
- **RFF-MMD Score** chart — rises sharply during drift incidents
- **Feature Null Rates** chart — spikes during null-spike incidents
- **Batch Validity Rate** chart — drops below 100% during any data quality issue

### Right panel — alerts + logs

**Alerts tab** — severity-badged feed (LOW / MEDIUM / HIGH / CRITICAL) with what failed, why, and the exact metric value that triggered it.

**Live Logs tab** — SSE stream of producer and Spark stdout, colour-coded by source. If SSE isn't connecting, check `http://localhost:7070/api/logs` for a JSON snapshot.

---

## Warm-up Phases

The system goes through three phases before any alert can fire:

| Phase | Batches | What happens |
|---|---|---|
| **WARM-UP** | 1–5 | Collecting raw rows to fit the RFF-MMD baseline |
| **CALIBRATING** | 6–10 | Scoring normal traffic; setting the 99th-pct threshold |
| **MONITORING** | 11+ | Full drift detection active; alerts can fire |

At the default 30-second trigger interval, monitoring begins after ~5 minutes. Configured in `configs/config.yaml` under `warmup:`.

---

## Incident Testing

| Incident | What it simulates | Detection |
|---|---|---|
| **Null Spike** | 80% of events null out age / purchase_amount | Validity rate drops, null-rate alert within 2 batches |
| **Range Violation** | Age 200+, negative amounts, session_duration < 0 | Validation alert within 2 batches |
| **Schema Corruption** | Unknown device types, type mismatches, missing user_id | Validation alert within 2 batches |
| **Distribution Drift** | Age shifts to 65–105, purchase 10× spike | RFF-MMD CRITICAL alert within 2 batches |

Each incident starts after 200 normal events, runs for 60–120 seconds, then returns to normal. The system suppresses re-alerting for 2 minutes after the first alert.

---

## Project Structure

```
ai-data-quality-monitor/
├── Dockerfile                        # Control panel + producer image
├── docker-compose.yml                # Full stack definition
├── Makefile                          # make up / make down / make logs
├── requirements.txt
│
├── data_simulator/
│   └── producer.py                   # Kafka producer + 4 incident modes
│
├── streaming_job/
│   ├── spark_job.py                  # Entry point — Spark Structured Streaming
│   ├── validation.py                 # Null / range / category checks
│   ├── drift.py                      # RFF-MMD + KS / PSI / Chi-Squared
│   ├── alerts.py                     # Multi-handler dispatcher + hysteresis
│   ├── dashboard.py                  # Static HTML dashboard generator
│   ├── feature_store.py              # Feature registry + baseline versioning
│   └── model_monitor.py              # Prediction / label / performance tracking
│
├── ui/
│   ├── app.py                        # FastAPI backend (9 API endpoints)
│   └── static/index.html             # Control panel frontend
│
├── configs/
│   └── config.yaml                   # All tunable parameters
│
├── notebooks/
│   └── exploration.ipynb             # Interactive drift analysis (RFF-MMD + KS + PSI)
│
├── logs/
│   └── alerts.jsonl                  # Persistent alert log (auto-created)
│
└── docs/
    ├── architecture.png
    └── dashboard_state.json          # Live metrics state (written by Spark job)
```

---

## Configuration Reference

`configs/config.yaml` — all parameters, all tunable at runtime:

```yaml
warmup:
  window_batches: 5          # batches to collect baseline (WARMUP phase)
  calibration_batches: 5     # batches to calibrate threshold (CALIBRATE phase)
  calibration_percentile: 99 # threshold = 99th pct of calibration scores

drift:
  rff_mmd:
    n_components: 200        # RFF dimensionality (higher = more accurate, slower)
    sigma: null              # RBF bandwidth — null = auto (median heuristic)

alerts:
  hysteresis:
    required_consecutive: 2  # bad windows before alert fires
    cooldown_seconds: 120    # silence window after alert fires (same key)
  log_file: "logs/alerts.jsonl"
  slack_webhook_url: null    # set to enable Slack alerts
  email:
    enabled: false
    smtp_host: "smtp.gmail.com"
    recipients: ["oncall@example.com"]
```

### Environment Variables (Docker)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka address (set automatically in Docker) |
| `SPARK_MASTER` | `local[2]` | Spark master URL — use `spark://spark-master:7077` for cluster mode |

---

## Local Development (without Docker)

If you want to run the Python code directly (e.g. for faster iteration):

```bash
# 1. Install deps
make setup

# 2. Start infrastructure only (no control-panel container)
make infra-up

# 3. Start control panel (local Python)
make ui       # → http://localhost:7070

# 4. Or run CLI directly
make spark-job    # Terminal 1
make producer     # Terminal 2
```

Requires `spark-submit` on your PATH (or the `_find_spark_submit` function will locate it automatically from common install directories).

---

## API Reference

The control panel exposes a REST API at `http://localhost:7070`:

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/status` | Kafka reachability, producer/Spark PID |
| GET | `/api/logs` | Last 100 log lines as JSON |
| GET | `/api/logs/stream` | SSE — live log tail |
| GET | `/api/alerts` | Last N alerts from `alerts.jsonl` |
| GET | `/api/metrics` | Full `dashboard_state.json` snapshot |
| GET | `/api/config` | Current `config.yaml` |
| PUT | `/api/config` | Patch `config.yaml` fields live |
| POST | `/api/producer/start` | Start producer with params |
| POST | `/api/producer/stop` | Stop producer |
| POST | `/api/spark/start` | Launch spark-submit |
| POST | `/api/spark/stop` | Stop Spark job |

---

## Alert Channels

| Channel | Config key | Format |
|---|---|---|
| Console log | always on | `[ALERT:SEVERITY] batch=N type=...` |
| File | `alerts.log_file` | JSONL — one object per alert |
| Slack | `alerts.slack_webhook_url` | Block Kit with severity colour |
| Email | `alerts.email.enabled` | SMTP with subject `[SEVERITY] type (batch N)` |
| Dashboard | `alerts.dashboard_state_path` | JSON polled by the control panel |

---

## Makefile Reference

```
make up               Build images + start full stack (Docker)
make down             Stop and remove all containers + volumes
make stop             Stop producer + Spark job (stack stays up)
make stop-producer    Stop only the producer
make stop-spark       Stop only the Spark job
make status           Show Kafka / producer / Spark status as JSON
make logs             Tail control-panel container logs
make restart          Rebuild and restart control panel

make setup            Install Python deps into .venv (local dev)
make infra-up         Start infra only — no control panel (local dev)
make ui               Start control panel locally on :7070
make spark-job        Run spark-submit locally
make producer         Run producer locally

make incident-null    Inject null spike (60s)
make incident-range   Inject range violations (60s)
make incident-schema  Inject schema corruption (60s)
make incident-drift   Inject distribution drift (120s)

make lint             Run flake8
make clean            Remove caches and .venv
make clean-data       Clear alerts.jsonl, dashboard_state.json, checkpoints
```

---

## License

MIT
