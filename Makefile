# ──────────────────────────────────────────────────────────────────────────────
# AI Data Quality Monitor — Makefile  (Option E: One-Command Setup)
# ──────────────────────────────────────────────────────────────────────────────
# Usage:
#   make setup       install Python deps in virtualenv
#   make infra-up    start Kafka + Spark via Docker Compose
#   make infra-down  stop and remove containers
#   make producer    start the normal feature stream
#   make spark-job   submit the Spark streaming job
#   make dashboard   generate and open the HTML dashboard
#   make run         start everything end-to-end (infra + producer + spark)
#
# Incident testing:
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

.PHONY: all setup infra-up infra-down infra-logs producer spark-job dashboard ui run \
        incident-null incident-range incident-schema incident-drift \
        test lint clean help

# ── Default ────────────────────────────────────────────────────────────────────
all: help

# ── Setup ──────────────────────────────────────────────────────────────────────
setup: $(VENV)/bin/activate
	@echo "✓ Python environment ready."

$(VENV)/bin/activate: requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	touch $(VENV)/bin/activate

# ── Infrastructure ─────────────────────────────────────────────────────────────
infra-up:
	@echo "Starting Kafka + Spark stack…"
	docker compose up -d
	@echo "Waiting for Kafka to be ready…"
	@sleep 8
	@echo "✓ Infrastructure running."
	@echo "  Kafka UI   → http://localhost:8080"
	@echo "  Spark UI   → http://localhost:8081"

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
	@echo "  make setup            Install Python dependencies"
	@echo "  make infra-up         Start Kafka + Spark (Docker)"
	@echo "  make infra-down       Stop all containers"
	@echo "  make spark-job        Submit Spark streaming job"
	@echo "  make producer         Start normal feature stream"
	@echo "  make dashboard        Generate + open HTML dashboard"
	@echo "  make run              One-command: infra + dashboard"
	@echo ""
	@echo "  Incident testing:"
	@echo "  make incident-null    Null spike (60s)"
	@echo "  make incident-range   Range violations (60s)"
	@echo "  make incident-schema  Schema corruption (60s)"
	@echo "  make incident-drift   Distribution drift (120s)"
	@echo ""
	@echo "  make ui               Start web control panel (:7070)"
	@echo "  make lint             Run flake8"
	@echo "  make clean            Remove caches and venv"
	@echo "  make clean-data       Clear runtime state files"
	@echo ""
