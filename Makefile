# ──────────────────────────────────────────────────────────────────────────────
# AI Data Quality Monitor — Makefile
# ──────────────────────────────────────────────────────────────────────────────
#
# Docker (recommended — no local Python/Spark required):
#   make up          build images + start full stack → open http://localhost:7070
#   make down        stop and remove all containers
#   make logs        tail control-panel logs
#   make restart     rebuild + restart (after code changes)
#
# Local development (requires local Python + spark-submit):
#   make setup       install Python deps into .venv
#   make infra-up    start infrastructure containers only (no control panel)
#   make ui          start control panel at :7070 (local Python)
#   make spark-job   submit Spark job via local spark-submit
#   make producer    start normal feature stream
#
# Incident testing (local):
#   make incident-null      inject null spike
#   make incident-range     inject range violations
#   make incident-schema    inject schema corruption
#   make incident-drift     inject distribution drift
#
# ──────────────────────────────────────────────────────────────────────────────

PYTHON      := python3
VENV        := .venv
PIP         := $(VENV)/bin/pip
PYTHON_VENV := $(VENV)/bin/python
SPARK_PKG   := org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0

.PHONY: all setup up down logs restart \
        stop stop-producer stop-spark status \
        infra-up infra-down infra-logs \
        producer spark-job dashboard ui run \
        incident-null incident-range incident-schema incident-drift \
        test lint clean clean-data help

# ── Default ────────────────────────────────────────────────────────────────────
all: help

# ── Docker (recommended) ───────────────────────────────────────────────────────

up:
	@echo "Building images and starting full stack…"
	docker compose up -d --build
	@echo ""
	@echo "══════════════════════════════════════════════════════"
	@echo "  ✓ Stack is up"
	@echo ""
	@echo "  Control Panel → http://localhost:7070"
	@echo "  Kafka UI      → http://localhost:8080"
	@echo "  Spark UI      → http://localhost:8081"
	@echo ""
	@echo "  Open http://localhost:7070 and click ▶ Start"
	@echo "══════════════════════════════════════════════════════"

down:
	docker compose down -v
	@echo "✓ Stack stopped."

logs:
	docker compose logs -f control-panel

restart:
	docker compose up -d --build control-panel
	@echo "✓ Control panel rebuilt and restarted."

# Stop individual processes (without tearing down the full stack)
stop-producer:
	@curl -s -X POST http://localhost:7070/api/producer/stop | python3 -m json.tool

stop-spark:
	@curl -s -X POST http://localhost:7070/api/spark/stop | python3 -m json.tool

stop: stop-spark stop-producer
	@echo "✓ Producer and Spark job stopped. Stack still running."

status:
	@curl -s http://localhost:7070/api/status | python3 -m json.tool

# ── Setup ──────────────────────────────────────────────────────────────────────
setup: $(VENV)/bin/activate
	@echo "✓ Python environment ready."

$(VENV)/bin/activate: requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	touch $(VENV)/bin/activate

# ── Infrastructure (local dev — no control-panel container) ────────────────────
infra-up:
	@echo "Starting infrastructure (Kafka + Spark)…"
	docker compose up -d zookeeper kafka kafka-ui spark-master spark-worker
	@echo "Waiting for Kafka to be ready…"
	@sleep 8
	@echo "✓ Infrastructure running."
	@echo "  Kafka UI   → http://localhost:8080"
	@echo "  Spark UI   → http://localhost:8081"
	@echo "  Run 'make ui' to start the control panel locally."

infra-down:
	docker compose down -v
	@echo "✓ Infrastructure stopped."

infra-logs:
	docker compose logs -f kafka

# ── Producer ───────────────────────────────────────────────────────────────────
producer: setup
	$(PYTHON_VENV) data_simulator/producer.py \
		--interval 0.2 \
		--total-events 10000

# Incident modes (run instead of 'make producer')
incident-null: setup
	$(PYTHON_VENV) data_simulator/producer.py \
		--incident null_spike \
		--incident-after 100 \
		--incident-duration 60

incident-range: setup
	$(PYTHON_VENV) data_simulator/producer.py \
		--incident range_violation \
		--incident-after 100 \
		--incident-duration 60

incident-schema: setup
	$(PYTHON_VENV) data_simulator/producer.py \
		--incident schema_corruption \
		--incident-after 100 \
		--incident-duration 60

incident-drift: setup
	$(PYTHON_VENV) data_simulator/producer.py \
		--incident distribution_drift \
		--incident-after 200 \
		--incident-duration 120

# ── Spark Job ──────────────────────────────────────────────────────────────────
spark-job: setup
	spark-submit \
		--packages $(SPARK_PKG) \
		--conf spark.sql.shuffle.partitions=4 \
		streaming_job/spark_job.py

# ── Dashboard (static HTML) ────────────────────────────────────────────────────
dashboard: setup
	$(PYTHON_VENV) streaming_job/dashboard.py --open
	@echo "Dashboard generated at docs/dashboard.html"
	@echo "Keep the Spark job running to see live updates."

# ── Control Panel UI ───────────────────────────────────────────────────────────
ui: setup
	@echo ""
	@echo "══════════════════════════════════════════════"
	@echo "  Control Panel → http://localhost:7070"
	@echo "══════════════════════════════════════════════"
	@echo ""
	$(PYTHON_VENV) -m uvicorn ui.app:app --host 0.0.0.0 --port 7070 --log-level warning

# ── End-to-End Run ─────────────────────────────────────────────────────────────
run: setup infra-up
	@echo ""
	@echo "══════════════════════════════════════════════════════"
	@echo "  Infrastructure is up. Start the control panel:     "
	@echo ""
	@echo "    make ui                                          "
	@echo "    → then open http://localhost:7070               "
	@echo ""
	@echo "  From the UI you can start the producer and         "
	@echo "  inject incidents without touching the terminal.    "
	@echo ""
	@echo "  Or use the CLI directly:                           "
	@echo "    make spark-job   (Terminal 1)                   "
	@echo "    make producer    (Terminal 2)                   "
	@echo "══════════════════════════════════════════════════════"

# ── Linting ────────────────────────────────────────────────────────────────────
lint: setup
	$(VENV)/bin/flake8 streaming_job/ data_simulator/ --max-line-length=100 --ignore=E501,W503 || true

# ── Cleanup ────────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .venv spark-warehouse derby.log metastore_db
	@echo "✓ Cleaned."

clean-data:
	rm -f docs/dashboard_state.json logs/alerts.jsonl /tmp/dq_baseline.json
	rm -rf /tmp/spark_checkpoints/dq_monitor
	@echo "✓ Runtime data cleared."

# ── Help ───────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "AI Data Quality Monitor"
	@echo "─────────────────────────────────────────────────────"
	@echo "  Docker (no local deps required):"
	@echo "  make up               Build + start full stack → :7070"
	@echo "  make down             Stop all containers + volumes"
	@echo "  make stop             Stop producer + Spark job (stack stays up)"
	@echo "  make stop-producer    Stop only the producer"
	@echo "  make stop-spark       Stop only the Spark job"
	@echo "  make status           Show Kafka / producer / Spark status"
	@echo "  make logs             Tail control-panel logs"
	@echo "  make restart          Rebuild control panel after code changes"
	@echo ""
	@echo "  Local development:"
	@echo "  make setup            Install Python deps into .venv"
	@echo "  make infra-up         Start infra only (no control panel)"
	@echo "  make ui               Start control panel locally (:7070)"
	@echo "  make spark-job        Submit Spark job via local spark-submit"
	@echo "  make producer         Start normal feature stream"
	@echo ""
	@echo "  Incident testing:"
	@echo "  make incident-null    Null spike (60s)"
	@echo "  make incident-range   Range violations (60s)"
	@echo "  make incident-schema  Schema corruption (60s)"
	@echo "  make incident-drift   Distribution drift (120s)"
	@echo ""
	@echo "  make lint             Run flake8"
	@echo "  make clean            Remove caches and .venv"
	@echo "  make clean-data       Clear runtime state (alerts, checkpoints)"
	@echo ""
