"""
streaming_job/drift.py
───────────────────────
Drift detection layer for the streaming ML pipeline.
Compares the distribution of incoming feature data against a reference baseline.

Supported methods:
  - Kolmogorov-Smirnov (KS) test       — continuous features
  - Population Stability Index (PSI)   — continuous & binned features
  - Chi-Squared test                   — categorical features
  - Null rate drift                    — all features
"""

import logging
import json
from pathlib import Path
from typing import Optional

import numpy as np
from scipy import stats
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("drift")

# ── Constants ─────────────────────────────────────────────────────────────────

PSI_EPSILON = 1e-4          # Avoid log(0)
KS_SIGNIFICANCE = 0.05      # p-value threshold for KS test
PSI_WARNING_THRESHOLD = 0.1
PSI_ALERT_THRESHOLD = 0.2
NULL_DRIFT_THRESHOLD = 0.05  # 5pp change in null rate triggers alert

NUMERIC_FEATURES = ["age", "purchase_amount", "session_duration", "page_views"]
CATEGORICAL_FEATURES = ["device_type"]


# ── Baseline Management ───────────────────────────────────────────────────────

class Baseline:
    """Holds reference distribution statistics for drift comparison."""

    def __init__(self):
        self.distributions: dict[str, np.ndarray] = {}   # raw reference samples
        self.null_rates: dict[str, float] = {}
        self.categorical_freqs: dict[str, dict[str, float]] = {}
        self.initialized: bool = False

    def fit(self, df: DataFrame) -> None:
        """Fit baseline from a reference DataFrame."""
        logger.info("Fitting baseline distributions...")

        for col in NUMERIC_FEATURES:
            if col in df.columns:
                values = [
                    row[col] for row in df.select(col).collect()
                    if row[col] is not None
                ]
                self.distributions[col] = np.array(values, dtype=float)

        for col in CATEGORICAL_FEATURES:
            if col in df.columns:
                total = df.count()
                freq = {}
                for row in df.groupBy(col).count().collect():
                    freq[row[col]] = row["count"] / total
                self.categorical_freqs[col] = freq

        total = df.count()
        if total > 0:
            for col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                self.null_rates[col] = null_count / total

        self.initialized = True
        logger.info(
            f"Baseline fitted: {len(self.distributions)} numeric features, "
            f"{len(self.categorical_freqs)} categorical features."
        )

    def save(self, path: str) -> None:
        """Persist baseline to disk as JSON."""
        data = {
            "distributions": {k: v.tolist() for k, v in self.distributions.items()},
            "null_rates": self.null_rates,
            "categorical_freqs": self.categorical_freqs,
        }
        Path(path).write_text(json.dumps(data, indent=2))
        logger.info(f"Baseline saved to {path}")

    def load(self, path: str) -> None:
        """Load baseline from disk."""
        data = json.loads(Path(path).read_text())
        self.distributions = {k: np.array(v) for k, v in data["distributions"].items()}
        self.null_rates = data["null_rates"]
        self.categorical_freqs = data["categorical_freqs"]
        self.initialized = True
        logger.info(f"Baseline loaded from {path}")


# ── KS Test ───────────────────────────────────────────────────────────────────

def ks_test(
    reference: np.ndarray,
    current: np.ndarray,
    feature: str,
    significance: float = KS_SIGNIFICANCE,
) -> dict:
    """
    Two-sample KS test between reference and current distributions.
    Returns statistic, p-value, and drift flag.
    """
    if len(current) < 10:
        return {"feature": feature, "method": "ks", "skipped": True, "reason": "insufficient_data"}

    ks_stat, p_value = stats.ks_2samp(reference, current)
    drifted = p_value < significance

    result = {
        "feature": feature,
        "method": "ks",
        "ks_statistic": round(float(ks_stat), 4),
        "p_value": round(float(p_value), 6),
        "drifted": drifted,
        "significance_level": significance,
    }

    if drifted:
        logger.warning(
            f"[KS DRIFT] feature='{feature}' ks={ks_stat:.4f} p={p_value:.6f} "
            f"(threshold={significance})"
        )
    return result


# ── PSI ───────────────────────────────────────────────────────────────────────

def compute_psi(
    reference: np.ndarray,
    current: np.ndarray,
    feature: str,
    n_bins: int = 10,
) -> dict:
    """
    Population Stability Index (PSI).
    PSI < 0.1: no significant change
    PSI 0.1–0.2: moderate change (warning)
    PSI > 0.2: significant shift (alert)
    """
    if len(current) < 10:
        return {"feature": feature, "method": "psi", "skipped": True, "reason": "insufficient_data"}

    bins = np.percentile(reference, np.linspace(0, 100, n_bins + 1))
    bins[0] = -np.inf
    bins[-1] = np.inf

    ref_counts, _ = np.histogram(reference, bins=bins)
    cur_counts, _ = np.histogram(current, bins=bins)

    ref_pct = (ref_counts / len(reference)) + PSI_EPSILON
    cur_pct = (cur_counts / len(current)) + PSI_EPSILON

    psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))

    if psi > PSI_ALERT_THRESHOLD:
        level = "alert"
        logger.warning(f"[PSI ALERT] feature='{feature}' PSI={psi:.4f} (>{PSI_ALERT_THRESHOLD})")
    elif psi > PSI_WARNING_THRESHOLD:
        level = "warning"
        logger.warning(f"[PSI WARNING] feature='{feature}' PSI={psi:.4f} (>{PSI_WARNING_THRESHOLD})")
    else:
        level = "ok"

    return {
        "feature": feature,
        "method": "psi",
        "psi": round(psi, 4),
        "level": level,
        "drifted": psi > PSI_WARNING_THRESHOLD,
    }


# ── Chi-Squared (Categorical) ─────────────────────────────────────────────────

def chi_squared_test(
    reference_freq: dict[str, float],
    current_counts: dict[str, int],
    feature: str,
    significance: float = KS_SIGNIFICANCE,
) -> dict:
    """Chi-squared test for categorical feature drift."""
    all_cats = sorted(set(reference_freq) | set(current_counts))
    total_current = sum(current_counts.values())

    if total_current < 10:
        return {"feature": feature, "method": "chi2", "skipped": True, "reason": "insufficient_data"}

    observed = np.array([current_counts.get(c, 0) for c in all_cats], dtype=float)
    expected = np.array([reference_freq.get(c, PSI_EPSILON) * total_current for c in all_cats])

    chi2_stat, p_value = stats.chisquare(f_obs=observed, f_exp=expected)
    drifted = p_value < significance

    if drifted:
        logger.warning(
            f"[CHI2 DRIFT] feature='{feature}' chi2={chi2_stat:.4f} p={p_value:.6f}"
        )

    return {
        "feature": feature,
        "method": "chi2",
        "chi2_statistic": round(float(chi2_stat), 4),
        "p_value": round(float(p_value), 6),
        "drifted": drifted,
    }


# ── Null Rate Drift ───────────────────────────────────────────────────────────

def check_null_rate_drift(
    baseline_null_rates: dict[str, float],
    current_null_rates: dict[str, float],
    threshold: float = NULL_DRIFT_THRESHOLD,
) -> list[dict]:
    """Detect significant changes in null rates between baseline and current batch."""
    results = []
    for feature, baseline_rate in baseline_null_rates.items():
        current_rate = current_null_rates.get(feature, 0.0)
        delta = abs(current_rate - baseline_rate)
        drifted = delta > threshold

        if drifted:
            logger.warning(
                f"[NULL DRIFT] feature='{feature}' "
                f"baseline={baseline_rate:.2%} current={current_rate:.2%} delta={delta:.2%}"
            )

        results.append({
            "feature": feature,
            "method": "null_rate_drift",
            "baseline_null_rate": round(baseline_rate, 4),
            "current_null_rate": round(current_rate, 4),
            "delta": round(delta, 4),
            "drifted": drifted,
        })
    return results


# ── Full Drift Report ─────────────────────────────────────────────────────────

def run_drift_detection(
    df: DataFrame,
    baseline: Baseline,
    batch_id: int = 0,
) -> dict:
    """
    Run all drift checks against the current micro-batch.
    Returns a structured drift report.
    """
    if not baseline.initialized:
        logger.warning(f"[batch {batch_id}] Baseline not initialized — skipping drift detection.")
        return {"batch_id": batch_id, "skipped": True}

    drift_results = []

    # Numeric features: KS + PSI
    for col in NUMERIC_FEATURES:
        if col not in df.columns or col not in baseline.distributions:
            continue

        current_vals = np.array(
            [row[col] for row in df.select(col).collect() if row[col] is not None],
            dtype=float,
        )

        drift_results.append(ks_test(baseline.distributions[col], current_vals, col))
        drift_results.append(compute_psi(baseline.distributions[col], current_vals, col))

    # Categorical features: Chi-squared
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns or col not in baseline.categorical_freqs:
            continue

        total = df.count()
        current_counts = {
            row[col]: row["count"]
            for row in df.groupBy(col).count().collect()
            if row[col] is not None
        }
        drift_results.append(
            chi_squared_test(baseline.categorical_freqs[col], current_counts, col)
        )

    # Null rate drift
    from streaming_job.validation import compute_null_rates
    current_null_rates = compute_null_rates(df)
    drift_results.extend(
        check_null_rate_drift(baseline.null_rates, current_null_rates)
    )

    drifted_features = [r["feature"] for r in drift_results if r.get("drifted")]
    report = {
        "batch_id": batch_id,
        "total_checks": len(drift_results),
        "drifted_feature_count": len(set(drifted_features)),
        "drifted_features": list(set(drifted_features)),
        "results": drift_results,
    }

    if drifted_features:
        logger.warning(
            f"[batch {batch_id}] DRIFT DETECTED in {len(set(drifted_features))} feature(s): "
            f"{list(set(drifted_features))}"
        )
    else:
        logger.info(f"[batch {batch_id}] No drift detected.")

    return report
