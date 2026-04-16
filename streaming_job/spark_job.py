"""
streaming_job/spark_job.py
───────────────────────────
Spark Structured Streaming entry point.

Pipeline per micro-batch
────────────────────────
1. Parse JSON events from Kafka
2. Feature validation (nulls, ranges, categories)
3. RFF-MMD drift detection via WarmupManager (warmup → calibrate → monitor)
4. Dispatch alerts through multi-handler AlertDispatcher
5. Push metrics to live HTML dashboard

Run:
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \\
      streaming_job/spark_job.py
"""

import logging
import sys
from pathlib import Path

# Ensure the project root is on sys.path so streaming_job package is importable
# when launched via spark-submit (which doesn't add the parent dir automatically)
_ROOT = Path(__file__).parent.parent.resolve()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
)

from streaming_job.validation import validate_batch, compute_null_rates
from streaming_job.drift import (
    WarmupManager, WarmupPhase, Baseline,
    run_drift_detection, NUMERIC_FEATURES,
)
from streaming_job.alerts import AlertDispatcher

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("spark_job")

CONFIG_PATH = Path(__file__).parent.parent / "configs" / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── Feature Schema ─────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("user_id",          StringType(),  True),
    StructField("age",              IntegerType(), True),
    StructField("purchase_amount",  DoubleType(),  True),
    StructField("session_duration", IntegerType(), True),
    StructField("page_views",       IntegerType(), True),
    StructField("device_type",      StringType(),  True),
    StructField("timestamp",        LongType(),    True),
])


# ── Spark Session ──────────────────────────────────────────────────────────────
def create_spark_session(app_name: str, spark_cfg: dict) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions",
                str(spark_cfg.get("shuffle_partitions", 4)))
        .getOrCreate()
    )


# ── Kafka Source ───────────────────────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession, kafka_cfg: dict) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg["bootstrap_servers"])
        .option("subscribe",               kafka_cfg["topic"])
        .option("startingOffsets",         kafka_cfg.get("starting_offsets", "latest"))
        .option("failOnDataLoss",          "false")
        .load()
    )
    return (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("d"),
            F.col("offset"),
            F.col("partition"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "offset", "partition", "kafka_ts")
    )


# ════════════════════════════════════════════════════════════════════════════════
# Batch Processor
# ════════════════════════════════════════════════════════════════════════════════
class BatchProcessor:
    """
    Stateful foreachBatch handler.
    Owns the WarmupManager, Baseline, and AlertDispatcher for the streaming lifetime.
    """

    def __init__(self, config: dict):
        self.config = config
        warmup_cfg = config.get("warmup", {})

        self.warmup_mgr = WarmupManager(
            warmup_batches        = warmup_cfg.get("window_batches", 5),
            calibration_batches   = warmup_cfg.get("calibration_batches", 5),
            calibration_percentile= warmup_cfg.get("calibration_percentile", 99.0),
        )
        self.baseline   = Baseline()
        self.dispatcher = AlertDispatcher(config)

        # Generate the dashboard HTML once at startup
        try:
            from streaming_job.dashboard import generate_dashboard
            generate_dashboard()
        except Exception as e:
            logger.warning(f"Could not generate dashboard HTML: {e}")

    # ── Main entry ─────────────────────────────────────────────────────────────
    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        row_count = batch_df.count()

        if row_count == 0:
            logger.debug(f"[batch {batch_id}] Empty — skipping.")
            batch_df.unpersist()
            return

        logger.info(f"[batch {batch_id}] ── Processing {row_count} rows ──")

        # ── 1. Validation ──────────────────────────────────────────────────
        val_summary = validate_batch(batch_df, batch_id=batch_id)
        self.dispatcher.dispatch_validation_issues(batch_id, val_summary)

        # ── 2. Fit complementary baseline (once, after warmup phase) ───────
        if (
            self.warmup_mgr.phase != WarmupPhase.WARMUP
            and not self.baseline.initialized
        ):
            self.baseline.fit(batch_df)

        # ── 3. Drift detection ──────────────────────────────────────────────
        drift_report = run_drift_detection(
            df=batch_df,
            warmup_manager=self.warmup_mgr,
            baseline=self.baseline,
            batch_id=batch_id,
        )
        self.dispatcher.dispatch_drift_report(batch_id, drift_report)

        # ── 4. Push metrics to dashboard ────────────────────────────────────
        null_rates = compute_null_rates(batch_df)
        metrics = {
            "phase":         drift_report["phase"],
            "mmd_score":     drift_report.get("mmd_score"),
            "mmd_threshold": drift_report.get("mmd_threshold"),
            "mmd_drift":     drift_report.get("mmd_drift", False),
            "validity_rate": val_summary.get("validity_rate"),
            "row_count":     row_count,
            **{f"null_rate_{feat}": round(null_rates.get(feat, 0.0), 4)
               for feat in NUMERIC_FEATURES},
        }
        self.dispatcher.push_dashboard_metrics(batch_id, metrics)

        # ── 5. Log one-liner summary ────────────────────────────────────────
        phase    = drift_report["phase"]
        mmd      = drift_report.get("mmd_score")
        thr      = drift_report.get("mmd_threshold")
        n_issues = val_summary.get("issue_count", 0)
        mmd_str  = f"{mmd:.5f}" if mmd is not None else "N/A"
        thr_str  = f"{thr:.5f}" if thr is not None else "N/A"
        logger.info(
            f"[batch {batch_id}] phase={phase} "
            f"mmd={mmd_str} "
            f"threshold={thr_str} "
            f"val_issues={n_issues} "
            f"rows={row_count}"
        )

        batch_df.unpersist()


# ── Entry Point ────────────────────────────────────────────────────────────────
def main() -> None:
    config = load_config()
    logger.info("Starting AI Data Quality Monitor")

    spark_cfg = config.get("spark", {})
    spark = create_spark_session(config.get("app_name", "DataQualityMonitor"), spark_cfg)
    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))

    stream_df = read_kafka_stream(spark, config["kafka"])
    processor = BatchProcessor(config)

    query = (
        stream_df.writeStream
        .foreachBatch(processor.process_batch)
        .option("checkpointLocation", spark_cfg.get(
            "checkpoint_location", "/tmp/spark_checkpoints/dq_monitor"
        ))
        .trigger(processingTime=spark_cfg.get("trigger_interval", "30 seconds"))
        .start()
    )

    logger.info("Streaming query started — awaiting termination (Ctrl+C to stop).")
    query.awaitTermination()


if __name__ == "__main__":
    main()
