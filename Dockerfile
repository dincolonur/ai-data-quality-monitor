# ── AI Data Quality Monitor — Control Panel + Producer Image ──────────────────
# Python 3.11 + OpenJDK 21 (17 was removed in Debian trixie)

FROM python:3.11-slim

# Install Java (PySpark needs it) and curl (health checks)
# Create an arch-neutral symlink so JAVA_HOME works on both arm64 and amd64
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
    && ln -sf /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/java-21-openjdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure runtime directories exist
RUN mkdir -p logs docs

EXPOSE 7070

# Default: run the FastAPI control panel
CMD ["python", "-m", "uvicorn", "ui.app:app", "--host", "0.0.0.0", "--port", "7070", "--log-level", "warning"]
