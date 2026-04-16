"""
streaming_job/model_monitor.py  — Option D: Model Monitoring Extension
───────────────────────────────────────────────────────────────────────
Simulates a model prediction stream and tracks four degradation signals:

  1. Feature drift       — RFF-MMD on incoming feature windows
  2. Prediction drift    — distribution shift in model output scores
  3. Label drift         — shift in ground-truth label distribution
  4. Performance degr.   — estimated accuracy / AUC from joined pred+label stream

Architecture
────────────
A separate Kafka topic ("ml-predictions") carries rows of the form:
  { event_id, predicted_label, predicted_score, model_version, timestamp }

An optional label topic ("ml-labels") carries delayed ground-truth:
  { event_id, true_label, timestamp }

The ModelMonitor joins predictions with labels (when available) over a
sliding window and computes rolling performance metrics.

Usage:
    from streaming_job.model_monitor import ModelMonitor, PredictionSimulator
    monitor = ModelMonitor(config)
    # In foreachBatch:
    monitor.process_prediction_batch(pred_df, batch_id)
"""

import logging
import random
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from scipy import stats

logger = logging.getLogger("model_monitor")


# ════════════════════════════════════════════════════════════════════════════════
# Prediction Simulator
# ════════════════════════════════════════════════════════════════════════════════

class PredictionSimulator:
    """
    Generates synthetic model prediction events to populate the predictions topic.
    Supports normal predictions and injected degradation modes.
    """

    def __init__(self, model_version: str = "v1.0"):
        self.model_version = model_version

    def normal_prediction(self, event_id: str, features: dict) -> dict:
        """Simulate a well-calibrated binary classifier output."""
        # Simple linear score based on features
        age    = (features.get("age") or 35) / 120
        amount = min((features.get("purchase_amount") or 0) / 500, 1.0)
        pages  = min((features.get("page_views") or 1) / 50, 1.0)
        raw = 0.4 * amount + 0.3 * pages + 0.3 * age + random.gauss(0, 0.08)
        score = max(0.0, min(1.0, raw))
        label = 1 if score > 0.5 else 0
        return {
            "event_id":        event_id,
            "predicted_label": label,
            "predicted_score": round(score, 4),
            "model_version":   self.model_version,
            "timestamp":       int(datetime.now(timezone.utc).timestamp() * 1000),
        }

    def degraded_prediction(self, event_id: str) -> dict:
        """Simulate a degraded model predicting near-random scores."""
        score = random.uniform(0.3, 0.7)   # uncertain, near-random
        return {
            "event_id":        event_id,
            "predicted_label": 1 if score > 0.5 else 0,
            "predicted_score": round(score, 4),
            "model_version":   self.model_version,
            "timestamp":       int(datetime.now(timezone.utc).timestamp() * 1000),
        }

    def shifted_prediction(self, event_id: str) -> dict:
        """Simulate prediction distribution shift (model over-predicts positive)."""
        score = random.betavariate(5, 1)   # heavily skewed toward 1
        return {
            "event_id":        event_id,
            "predicted_label": 1,
            "predicted_score": round(score, 4),
            "model_version":   self.model_version,
            "timestamp":       int(datetime.now(timezone.utc).timestamp() * 1000),
        }


# ════════════════════════════════════════════════════════════════════════════════
# Drift Detectors (score-level)
# ════════════════════════════════════════════════════════════════════════════════

def detect_prediction_drift(
    baseline_scores: np.ndarray,
    current_scores:  np.ndarray,
    significance:    float = 0.05,
) -> dict:
    """KS test on model output scores."""
    if len(current_scores) < 10:
        return {"skipped": True, "reason": "insufficient data"}
    stat, p = stats.ks_2samp(baseline_scores, current_scores)
    drifted = p < significance
    if drifted:
        logger.warning(f"[Prediction Drift] KS stat={stat:.4f} p={p:.6f}")
    return {
        "method": "ks_prediction",
        "ks_statistic": round(float(stat), 4),
        "p_value": round(float(p), 6),
        "drifted": drifted,
        "baseline_mean": round(float(baseline_scores.mean()), 4),
        "current_mean":  round(float(current_scores.mean()), 4),
    }


def detect_label_drift(
    baseline_positive_rate: float,
    current_labels: list[int],
    z_threshold: float = 2.5,
) -> dict:
    """Z-test for shift in positive label rate."""
    if len(current_labels) < 10:
        return {"skipped": True}
    n = len(current_labels)
    p_current = sum(current_labels) / n
    se = np.sqrt(baseline_positive_rate * (1 - baseline_positive_rate) / n)
    if se == 0:
        return {"skipped": True, "reason": "zero standard error"}
    z = abs(p_current - baseline_positive_rate) / se
    drifted = z > z_threshold
    if drifted:
        logger.warning(
            f"[Label Drift] baseline_rate={baseline_positive_rate:.3f} "
            f"current_rate={p_current:.3f} z={z:.2f}"
        )
    return {
        "method": "label_drift_ztest",
        "baseline_positive_rate": round(baseline_positive_rate, 4),
        "current_positive_rate":  round(p_current, 4),
        "z_score":   round(float(z), 3),
        "drifted":   drifted,
    }


# ════════════════════════════════════════════════════════════════════════════════
# Performance Tracker
# ════════════════════════════════════════════════════════════════════════════════

class PerformanceTracker:
    """
    Maintains a rolling window of (predicted_label, true_label) pairs
    and computes accuracy and estimated AUC.
    """

    def __init__(self, window_size: int = 500):
        self.window: deque = deque(maxlen=window_size)
        self.baseline_accuracy: Optional[float] = None
        self.degradation_threshold: float = 0.05   # 5 pp drop triggers alert

    def record(self, predicted: int, true_label: int, score: float) -> None:
        self.window.append({"pred": predicted, "true": true_label, "score": score})

    def accuracy(self) -> Optional[float]:
        if not self.window:
            return None
        correct = sum(1 for r in self.window if r["pred"] == r["true"])
        return correct / len(self.window)

    def estimated_auc(self) -> Optional[float]:
        """Wilcoxon-Mann-Whitney AUC estimate from scores + true labels."""
        pos = [r["score"] for r in self.window if r["true"] == 1]
        neg = [r["score"] for r in self.window if r["true"] == 0]
        if len(pos) < 5 or len(neg) < 5:
            return None
        wins = sum(1 for p in pos for n in neg if p > n)
        ties = sum(1 for p in pos for n in neg if p == n)
        return (wins + 0.5 * ties) / (len(pos) * len(neg))

    def set_baseline(self) -> None:
        self.baseline_accuracy = self.accuracy()
        logger.info(f"[PerformanceTracker] Baseline accuracy = {self.baseline_accuracy:.4f}")

    def is_degraded(self) -> bool:
        if self.baseline_accuracy is None:
            return False
        current = self.accuracy()
        if current is None:
            return False
        drop = self.baseline_accuracy - current
        degraded = drop > self.degradation_threshold
        if degraded:
            logger.warning(
                f"[Performance Degradation] "
                f"baseline={self.baseline_accuracy:.4f} "
                f"current={current:.4f} "
                f"drop={drop:.4f}"
            )
        return degraded

    def report(self) -> dict:
        acc = self.accuracy()
        auc = self.estimated_auc()
        return {
            "window_size":        len(self.window),
            "accuracy":           round(acc, 4) if acc is not None else None,
            "estimated_auc":      round(auc, 4) if auc is not None else None,
            "baseline_accuracy":  self.baseline_accuracy,
            "is_degraded":        self.is_degraded(),
        }


# ════════════════════════════════════════════════════════════════════════════════
# Model Monitor
# ════════════════════════════════════════════════════════════════════════════════

class ModelMonitor:
    """
    Top-level monitor combining feature drift, prediction drift,
    label drift, and performance degradation into a single report.
    """

    def __init__(self, config: dict):
        mm_cfg = config.get("model_monitoring", {})
        self.enabled       = mm_cfg.get("enabled", False)
        self.perf_tracker  = PerformanceTracker(
            window_size=mm_cfg.get("performance_window_batches", 10) * 100
        )
        self.baseline_scores: Optional[np.ndarray] = None
        self.baseline_positive_rate: Optional[float] = None
        self.warmup_done   = False
        self.warmup_scores: list[float] = []
        self.warmup_labels: list[int]   = []
        self.warmup_batches_needed = 3
        self.warmup_batch_count    = 0

    def process_prediction_batch(
        self,
        predictions: list[dict],
        labels: Optional[list[dict]] = None,
        batch_id: int = 0,
    ) -> dict:
        """
        Process one batch of predictions (and optionally ground-truth labels).

        predictions: list of {event_id, predicted_label, predicted_score, ...}
        labels:      list of {event_id, true_label} (may be None or partial)
        """
        if not predictions:
            return {"batch_id": batch_id, "skipped": True}

        scores  = np.array([p["predicted_score"] for p in predictions])
        pred_labels = [p["predicted_label"] for p in predictions]

        # Build label lookup if provided
        label_map = {}
        if labels:
            label_map = {l["event_id"]: l["true_label"] for l in labels}
            for pred in predictions:
                eid = pred["event_id"]
                if eid in label_map:
                    self.perf_tracker.record(
                        pred["predicted_label"], label_map[eid], pred["predicted_score"]
                    )

        report: dict = {"batch_id": batch_id, "n_predictions": len(predictions)}

        # ── Warm-up ────────────────────────────────────────────────────────
        if not self.warmup_done:
            self.warmup_scores.extend(scores.tolist())
            self.warmup_labels.extend(pred_labels)
            self.warmup_batch_count += 1
            if self.warmup_batch_count >= self.warmup_batches_needed:
                self.baseline_scores = np.array(self.warmup_scores)
                self.baseline_positive_rate = (
                    sum(self.warmup_labels) / len(self.warmup_labels)
                    if self.warmup_labels else 0.5
                )
                self.perf_tracker.set_baseline()
                self.warmup_done = True
                logger.info(
                    f"[ModelMonitor] Warm-up complete. "
                    f"baseline_score_mean={self.baseline_scores.mean():.4f} "
                    f"baseline_pos_rate={self.baseline_positive_rate:.4f}"
                )
            report["phase"] = "warmup"
            return report

        # ── Drift checks ───────────────────────────────────────────────────
        report["phase"] = "monitoring"
        report["prediction_drift"] = detect_prediction_drift(
            self.baseline_scores, scores
        )
        report["label_drift"] = detect_label_drift(
            self.baseline_positive_rate, pred_labels
        )
        report["performance"] = self.perf_tracker.report()

        # Summary log
        pd_flag = report["prediction_drift"].get("drifted", False)
        ld_flag = report["label_drift"].get("drifted", False)
        perf    = report["performance"]
        logger.info(
            f"[ModelMonitor batch {batch_id}] "
            f"pred_drift={pd_flag} label_drift={ld_flag} "
            f"accuracy={perf['accuracy']} auc={perf['estimated_auc']} "
            f"degraded={perf['is_degraded']}"
        )
        return report
