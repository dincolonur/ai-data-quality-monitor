"""
streaming_job/spark_job.py
───────────────────────────
Entry point for the Spark Structured Streaming job.
Reads feature events from Kafka, runs validation and drift detection
on each micro-batch, and dispatches alerts.

Run with:
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \\
      streaming_job/spark_job.py
"""

import json
import logging
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
)

from streaming_job.validation import validate_batch
from streaming_job.drift import Baseline, run_drift_detection
from streaming_job.alerts import AlertDispatcher

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("spark_job")

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "configs" / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── Feature Schema ────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),  nullable=True),
    StructField("user_id",          StringType(),  nullable=True),
    StructField("age",              IntegerType(), nullable=True),
    StructField("purchase_amount",  DoubleType(),  nullable=True),
    StructField("session_duration", IntegerType(), nullable=True),
    StructField("page_views",       IntegerType(), nullable=True),
    StructField("device_type",      StringType(),  nullable=True),
    StructField("timestamp",        LongType(),    nullable=True),
])


# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints/dq_monitor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Kafka Source ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession, config: dict) -> DataFrame:
    kafka_cfg = config["kafka"]
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg["bootstrap_servers"])
        .option("subscribe", kafka_cfg["topic"])
        .option("startingOffsets", kafka_cfg.get("starting_offsets", "latest"))
        .option("failOnDataLoss", "false")
        .load()
    )

    # Deserialize JSON value
    parsed = raw_stream.select(
        F.from_json(
            F.col("value").cast("string"),
            EVENT_SCHEMA
        ).alias("data"),
        F.col("offset"),
        F.col("partition"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "offset", "partition", "kafka_timestamp")

    return parsed


# ── Batch Processor ───────────────────────────────────────────────────────────

class BatchProcessor:
    """Stateful processor called for each streaming micro-batch."""

    def __init__(self, config: dict):
        self.config = config
        self.baseline = Baseline()
        self.dispatcher = AlertDispatcher(config)
        self.batch_count = 0
        self.baseline_window = config.get("drift", {}).get("baseline_window_batches", 5)

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        row_count = batch_df.count()

        if row_count == 0:
            logger.debug(f"[batch {batch_id}] Empty batch — skipping.")
            batch_df.unpersist()
            return

        logger.info(f"[batch {batch_id}] Processing {row_count} rows...")

        # Phase 1: Feature Validation
        validation_summary = validate_batch(batch_df, batch_id=batch_id)
        self.dispatcher.dispatch_validation_issues(batch_id, validation_summary)

        # Phase 2: Fit or update baseline
        if self.batch_count < self.baseline_window:
            logger.info(
                f"[batch {batch_id}] Accumulating baseline "
                f"({self.batch_count + 1}/{self.baseline_window})"
            )
            if not self.baseline.initialized:
                self.baseline.fit(batch_df)
            self.batch_count += 1
            batch_df.unpersist()
            return

        # Phase 3: Drift Detection (after baseline is established)
        drift_report = run_drift_detection(batch_df, self.baseline, batch_id=batch_id)
        self.dispatcher.dispatch_drift_report(batch_id, drift_report)

        self.batch_count += 1
        batch_df.unpersist()


# ── Main Entry Point ──────────────────────────────────────────────────────────

def main():
    config = load_config()
    logger.info("Starting AI Data Quality Monitor — Spark Streaming Job")
    logger.info(f"Config: {config}")

    spark = create_spark_session(config.get("app_name", "DataQualityMonitor"))
    spark.sparkContext.setLogLevel("WARN")

    stream_df = read_kafka_stream(spark, config)
    processor = BatchProcessor(config)

    query = (
        stream_df.writeStream
        .foreachBatch(processor.process_batch)
        .option(
            "checkpointLocation",
            config.get("spark", {}).get("checkpoint_location", "/tmp/spark_checkpoints/dq_monitor"),
        )
        .trigger(processingTime=config.get("spark", {}).get("trigger_interval", "30 seconds"))
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
