"""
ui/app.py
──────────
FastAPI control panel backend for the AI Data Quality Monitor.

Endpoints
─────────
GET  /                          Serve the control panel HTML
GET  /api/status                Kafka reachability + process states
POST /api/producer/start        Launch producer with given params
POST /api/producer/stop         Kill producer process
POST /api/spark/start           Launch spark-submit job
POST /api/spark/stop            Kill Spark process
GET  /api/metrics               Latest dashboard_state.json snapshot
GET  /api/alerts                Last N alerts from logs/alerts.jsonl
GET  /api/logs/stream           Server-Sent Events — live log tail
GET  /api/config                Read configs/config.yaml
PUT  /api/config                Patch configs/config.yaml fields

Run:
    python ui/app.py
    # or: uvicorn ui.app:app --reload --port 7070
"""

import asyncio
import glob
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Optional

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "configs" / "config.yaml"
ALERTS_PATH = ROOT / "logs" / "alerts.jsonl"
DASH_STATE  = ROOT / "docs" / "dashboard_state.json"
STATIC_DIR  = Path(__file__).parent / "static"

# ── In-memory log ring buffer (shared across all processes) ────────────────────
LOG_BUFFER: deque = deque(maxlen=600)


def _append_log(source: str, line: str) -> None:
    LOG_BUFFER.append({
        "ts": time.strftime("%H:%M:%S"),
        "source": source,
        "line": line.rstrip(),
    })


# ════════════════════════════════════════════════════════════════════════════════
# Process Manager
# ════════════════════════════════════════════════════════════════════════════════

class ManagedProcess:
    """Wraps a subprocess.Popen, pipes stdout/stderr to LOG_BUFFER.

    When auto_restart=True the process is restarted automatically on unexpected
    exit (non-zero return code), with an exponential back-off up to 60 seconds.
    Call stop() to permanently cancel it.
    """

    def __init__(self, name: str, cmd: list[str], cwd: Path = ROOT, auto_restart: bool = False):
        self.name         = name
        self.cmd          = cmd
        self.cwd          = cwd
        self.auto_restart = auto_restart
        self._proc: Optional[subprocess.Popen] = None
        self._thread      = None
        self._stopped     = False   # set by stop() to prevent restart loops

    def start(self) -> str | None:
        """Start the process. Returns an error string on failure, None on success."""
        self._stopped = False
        if self.is_running():
            return None
        _append_log("system", f"[{self.name}] launching: {' '.join(self.cmd)}")
        try:
            self._proc = subprocess.Popen(
                self.cmd,
                cwd=str(self.cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError as e:
            msg = f"[{self.name}] ERROR — binary not found: {e}"
            _append_log("system", msg)
            return msg
        except Exception as e:
            msg = f"[{self.name}] ERROR — failed to launch: {e}"
            _append_log("system", msg)
            return msg
        import threading
        self._thread = threading.Thread(target=self._drain, daemon=True)
        self._thread.start()
        _append_log("system", f"[{self.name}] started (PID {self._proc.pid})")
        return None

    def _drain(self) -> None:
        """Read process output line by line, then handle exit / auto-restart."""
        try:
            for line in self._proc.stdout:
                _append_log(self.name, line)
        except Exception as e:
            _append_log("system", f"[{self.name}] drain error: {e}")
        rc = self._proc.poll()
        _append_log("system", f"[{self.name}] process exited (returncode={rc})")

        if self.auto_restart and not self._stopped and rc != 0:
            delay = 5
            _append_log("system", f"[{self.name}] unexpected exit — restarting in {delay}s…")
            time.sleep(delay)
            if not self._stopped:
                err = self.start()
                if err:
                    _append_log("system", f"[{self.name}] auto-restart failed: {err}")

    def stop(self) -> None:
        self._stopped = True
        if not self._proc:
            return
        try:
            os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
        except Exception:
            try:
                self._proc.terminate()
            except Exception:
                pass
        self._proc = None
        _append_log("system", f"[{self.name}] stopped")

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def pid(self) -> Optional[int]:
        return self._proc.pid if self._proc else None


# Singleton process handles
# Spark auto-restarts on unexpected exit; producer does not (incidents are intentional stops)
_producer = ManagedProcess("producer", [],  auto_restart=False)
_spark    = ManagedProcess("spark",    [],  auto_restart=True)


# ════════════════════════════════════════════════════════════════════════════════
# FastAPI App
# ════════════════════════════════════════════════════════════════════════════════

app = FastAPI(title="DQ Monitor Control Panel", version="1.0")

# Serve static files (index.html)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


# ── Status ─────────────────────────────────────────────────────────────────────

def _kafka_reachable(host: str = "localhost", port: int = 9092, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@app.get("/api/status")
async def get_status():
    cfg = yaml.safe_load(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    bs  = cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    host, port = (bs.split(":") + ["9092"])[:2]

    return {
        "kafka": {
            "reachable": _kafka_reachable(host, int(port)),
            "bootstrap_servers": bs,
        },
        "producer": {
            "running": _producer.is_running(),
            "pid": _producer.pid,
        },
        "spark": {
            "running": _spark.is_running(),
            "pid": _spark.pid,
        },
        "dashboard_state_exists": DASH_STATE.exists(),
    }


# ── Producer ───────────────────────────────────────────────────────────────────

class ProducerParams(BaseModel):
    incident:          str   = "none"
    interval:          float = 0.2
    incident_after:    int   = 200
    incident_duration: float = 60.0
    total_events:      int   = 10000


@app.post("/api/producer/start")
async def producer_start(params: ProducerParams):
    if _producer.is_running():
        return {"status": "already_running", "pid": _producer.pid}

    python = sys.executable
    cmd = [
        python, str(ROOT / "data_simulator" / "producer.py"),
        "--incident",          params.incident,
        "--interval",          str(params.interval),
        "--incident-after",    str(params.incident_after),
        "--incident-duration", str(params.incident_duration),
        "--total-events",      str(params.total_events),
    ]
    _producer.cmd = cmd
    err = _producer.start()
    if err:
        raise HTTPException(status_code=500, detail=err)
    return {"status": "started", "pid": _producer.pid, "params": params.dict()}


@app.post("/api/producer/stop")
async def producer_stop():
    if not _producer.is_running():
        return {"status": "not_running"}
    _producer.stop()
    return {"status": "stopped"}


# ── Spark Job ──────────────────────────────────────────────────────────────────

def _spark_pkg() -> str:
    """Return the Kafka connector package string matching the installed PySpark version.

    PySpark 3.x → Scala 2.12 → spark-sql-kafka-0-10_2.12:<version>
    PySpark 4.x → Scala 2.13 → spark-sql-kafka-0-10_2.13:<version>
    """
    try:
        import pyspark
        version = pyspark.__version__  # e.g. "3.5.3" or "4.0.0"
    except ImportError:
        version = "3.5.3"
    major = int(version.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"


def _find_spark_submit() -> str:
    """Locate spark-submit: pyspark bundle → PATH → SPARK_HOME → common dirs."""
    # 1. PySpark's bundled spark-submit (works when pip install pyspark is used)
    try:
        import pyspark
        candidate = os.path.join(os.path.dirname(pyspark.__file__), "bin", "spark-submit")
        if os.path.isfile(candidate):
            return candidate
    except ImportError:
        pass
    # 2. Already on PATH
    found = shutil.which("spark-submit")
    if found:
        return found
    # 3. SPARK_HOME env var
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        candidate = os.path.join(spark_home, "bin", "spark-submit")
        if os.path.isfile(candidate):
            return candidate
    # 4. Common macOS/Linux install locations
    patterns = [
        os.path.expanduser("~/Documents/server/spark-*/bin/spark-submit"),
        os.path.expanduser("~/spark-*/bin/spark-submit"),
        "/opt/spark/bin/spark-submit",
        "/usr/local/bin/spark-submit",
    ]
    for pattern in patterns:
        matches = sorted(glob.glob(pattern), reverse=True)
        if matches:
            return matches[0]
    return "spark-submit"  # fallback — will raise FileNotFoundError if missing


@app.post("/api/spark/start")
async def spark_start():
    if _spark.is_running():
        return {"status": "already_running", "pid": _spark.pid}

    spark_submit = _find_spark_submit()
    pkg = _spark_pkg()
    master = os.environ.get("SPARK_MASTER", "local[2]")
    _append_log("system", f"[spark] spark-submit: {spark_submit}")
    _append_log("system", f"[spark] master={master}  package={pkg}")
    cmd = [
        spark_submit,
        "--master", master,
        "--packages", pkg,
        "--conf", "spark.sql.shuffle.partitions=4",
        "--conf", "spark.ui.port=4040",
        str(ROOT / "streaming_job" / "spark_job.py"),
    ]
    _spark.cmd = cmd
    err = _spark.start()
    if err:
        raise HTTPException(status_code=500, detail=err)
    return {"status": "started", "pid": _spark.pid, "spark_submit": spark_submit}


@app.post("/api/spark/stop")
async def spark_stop():
    if not _spark.is_running():
        return {"status": "not_running"}
    _spark.stop()
    return {"status": "stopped"}


# ── Metrics ────────────────────────────────────────────────────────────────────

@app.get("/api/metrics")
async def get_metrics():
    if not DASH_STATE.exists():
        return {"alerts": [], "metrics_history": [], "summary": {}}
    try:
        return json.loads(DASH_STATE.read_text())
    except Exception as e:
        raise HTTPException(500, f"Could not read dashboard state: {e}")


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
    if not ALERTS_PATH.exists():
        return []
    lines = ALERTS_PATH.read_text().strip().splitlines()
    parsed = []
    for line in reversed(lines[-limit * 2:]):
        try:
            parsed.append(json.loads(line))
        except Exception:
            pass
        if len(parsed) >= limit:
            break
    return parsed


@app.delete("/api/alerts")
async def clear_alerts():
    """Truncate the alerts log file."""
    if ALERTS_PATH.exists():
        ALERTS_PATH.write_text("")
    return {"status": "cleared"}


# ── Config ─────────────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config():
    if not CONFIG_PATH.exists():
        raise HTTPException(404, "Config file not found")
    return yaml.safe_load(CONFIG_PATH.read_text())


@app.put("/api/config")
async def update_config(request: Request):
    patch = await request.json()
    cfg = yaml.safe_load(CONFIG_PATH.read_text())

    def deep_merge(base: dict, updates: dict) -> dict:
        for k, v in updates.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged = deep_merge(cfg, patch)
    CONFIG_PATH.write_text(yaml.dump(merged, default_flow_style=False, sort_keys=False))
    return {"status": "saved", "config": merged}


# ── Log snapshot (REST fallback) ───────────────────────────────────────────────

@app.get("/api/logs")
async def get_logs(limit: int = 100):
    """Return the last N log entries as JSON — useful if SSE isn't connecting."""
    buf = list(LOG_BUFFER)
    return buf[-limit:]


# ── SSE Log Stream ─────────────────────────────────────────────────────────────

async def _log_generator(request: Request):
    """Yield new log lines as SSE events every 500ms."""
    cursor = len(LOG_BUFFER)
    yield "data: {\"line\": \"Connected to log stream.\"}\n\n"
    while True:
        if await request.is_disconnected():
            break
        buf = list(LOG_BUFFER)
        new_lines = buf[cursor:]
        cursor = len(buf)
        for entry in new_lines:
            payload = json.dumps(entry)
            yield f"data: {payload}\n\n"
        await asyncio.sleep(0.5)


@app.get("/api/logs/stream")
async def logs_stream(request: Request):
    return StreamingResponse(
        _log_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── Entrypoint ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "═" * 54)
    print("  AI Data Quality Monitor — Control Panel")
    print("  http://localhost:7070")
    print("═" * 54 + "\n")
    uvicorn.run("ui.app:app", host="0.0.0.0", port=7070, reload=False, log_level="warning")
