"""
streaming_job/drift.py
───────────────────────
Drift detection layer — primary method is RFF-MMD.

Pipeline:
  1. WARMUP   — first N batches collect raw feature data, no alerts fired
  2. CALIBRATE — next M batches compute normal-condition MMD scores to set threshold
  3. MONITOR  — every subsequent batch is scored; hysteresis tracks violations

RFF-MMD (Random Fourier Features Maximum Mean Discrepancy)
──────────────────────────────────────────────────────────
MMD measures the distance between two distributions in a reproducing kernel
Hilbert space.  The naive O(n²) kernel evaluation is approximated in O(n·D)
using Rahimi & Recht random features:

    z(x) = √(2/D) · cos(ω^T x + b),  ω ~ N(0, 1/σ²),  b ~ U(0, 2π)
    MMD² ≈ ‖ mean_z(baseline) − mean_z(current) ‖²

Complementary methods (KS, PSI, Chi-Squared, null-rate) are still available
as supporting diagnostics.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np
from scipy import stats
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("drift")

# ── Feature Groups ─────────────────────────────────────────────────────────────

NUMERIC_FEATURES     = ["age", "purchase_amount", "session_duration", "page_views"]
CATEGORICAL_FEATURES = ["device_type"]

PSI_EPSILON = 1e-4
KS_SIGNIFICANCE = 0.05
CHI2_SIGNIFICANCE = 0.05
PSI_WARNING = 0.1
PSI_ALERT   = 0.2
NULL_DRIFT_THRESHOLD = 0.05


# ════════════════════════════════════════════════════════════════════════════════
# RFF-MMD Detector
# ════════════════════════════════════════════════════════════════════════════════

class RFFMMDDetector:
    """
    Random Fourier Features approximation of Maximum Mean Discrepancy.

    Fits on a baseline distribution; subsequent calls to .score() return
    an MMD² estimate that quantifies how much the incoming window has shifted.
    Call .calibrate() after collecting a set of normal-condition scores to set
    a data-driven threshold.
    """

    def __init__(
        self,
        n_components: int = 200,
        sigma: Optional[float] = None,
        calibration_percentile: float = 99.0,
        random_state: int = 42,
    ):
        self.n_components = n_components
        self.sigma = sigma
        self.calibration_percentile = calibration_percentile
        self.rng = np.random.RandomState(random_state)

        self.omega_: Optional[np.ndarray] = None   # (D, n_features)
        self.bias_: Optional[np.ndarray] = None    # (D,)
        self.baseline_mean_: Optional[np.ndarray] = None
        self.threshold_: Optional[float] = None
        self.calibration_scores_: list[float] = []
        self.is_fitted: bool = False

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _median_heuristic(self, X: np.ndarray) -> float:
        """Estimate RBF bandwidth σ from median pairwise distance."""
        n = min(500, len(X))
        idx = self.rng.choice(len(X), n, replace=False)
        s = X[idx]
        sq = np.sum((s[:, None] - s[None, :]) ** 2, axis=-1)
        pos = sq[sq > 0]
        return float(np.sqrt(np.median(pos) / 2)) if len(pos) > 0 else 1.0

    def _transform(self, X: np.ndarray) -> np.ndarray:
        """Apply RFF mapping: z(x) = √(2/D) · cos(ω^T x + b)."""
        proj = X @ self.omega_.T + self.bias_       # (n, D)
        return np.cos(proj) * np.sqrt(2.0 / self.n_components)

    # ── Public API ─────────────────────────────────────────────────────────────

    def fit(self, baseline: np.ndarray) -> "RFFMMDDetector":
        """
        Fit on baseline data of shape (n_samples, n_features).
        Stores the mean RFF representation of the reference distribution.
        """
        n_features = baseline.shape[1]
        if self.sigma is None:
            self.sigma = self._median_heuristic(baseline)
            logger.info(f"[RFF-MMD] Auto-selected bandwidth σ={self.sigma:.4f}")

        self.omega_ = self.rng.normal(0, 1.0 / self.sigma, (self.n_components, n_features))
        self.bias_  = self.rng.uniform(0, 2 * np.pi, self.n_components)

        Z = self._transform(baseline)
        self.baseline_mean_ = Z.mean(axis=0)
        self.is_fitted = True
        logger.info(
            f"[RFF-MMD] Baseline fitted on {len(baseline)} samples, "
            f"{n_features} features, D={self.n_components}"
        )
        return self

    def score(self, current: np.ndarray) -> float:
        """Return MMD² estimate between baseline and current window."""
        if not self.is_fitted:
            raise RuntimeError("Call fit() before score().")
        Z = self._transform(current)
        mu = Z.mean(axis=0)
        return float(np.sum((mu - self.baseline_mean_) ** 2))

    def calibrate(self, scores: list[float]) -> float:
        """
        Set detection threshold from a list of scores collected under normal
        conditions.  Threshold = calibration_percentile of those scores.
        """
        self.calibration_scores_ = list(scores)
        self.threshold_ = float(np.percentile(scores, self.calibration_percentile))
        logger.info(
            f"[RFF-MMD] Threshold calibrated at {self.calibration_percentile}th "
            f"percentile of {len(scores)} scores → threshold={self.threshold_:.6f}"
        )
        return self.threshold_

    def is_drift(self, score: float) -> bool:
        """True if score exceeds calibrated threshold."""
        if self.threshold_ is None:
            raise RuntimeError("Call calibrate() before is_drift().")
        return score > self.threshold_

    def to_dict(self) -> dict:
        return {
            "sigma": self.sigma,
            "n_components": self.n_components,
            "threshold": self.threshold_,
            "calibration_scores_count": len(self.calibration_scores_),
        }


# ════════════════════════════════════════════════════════════════════════════════
# Warm-up / Calibration State Machine
# ════════════════════════════════════════════════════════════════════════════════

class WarmupPhase(Enum):
    WARMUP     = "warmup"       # collecting baseline data, no scoring
    CALIBRATE  = "calibrate"    # scoring under normal conditions, no alerts
    MONITORING = "monitoring"   # full drift detection active


@dataclass
class WarmupManager:
    """
    Manages the three-phase lifecycle:
      WARMUP → CALIBRATE → MONITORING

    During WARMUP we accumulate raw feature rows.
    During CALIBRATE we score each window to build the empirical threshold.
    During MONITORING we score and compare against the threshold.
    """
    warmup_batches: int = 5
    calibration_batches: int = 5
    calibration_percentile: float = 99.0

    phase: WarmupPhase = field(default=WarmupPhase.WARMUP, init=False)
    batch_count: int = field(default=0, init=False)
    collected_rows: list[dict] = field(default_factory=list, init=False)
    calibration_scores: list[float] = field(default_factory=list, init=False)
    detector: Optional[RFFMMDDetector] = field(default=None, init=False)

    def advance(self, batch_df: DataFrame, batch_id: int) -> tuple[WarmupPhase, Optional[float]]:
        """
        Process one batch.  Returns (current_phase, mmd_score_or_None).
        Score is None during WARMUP; during CALIBRATE it is computed but
        not yet compared against a threshold.
        """
        self.batch_count += 1

        # ── Extract numeric feature matrix ─────────────────────────────────
        rows = [
            {f: row[f] for f in NUMERIC_FEATURES}
            for row in batch_df.select(*NUMERIC_FEATURES).collect()
        ]
        clean = [r for r in rows if all(v is not None for v in r.values())]
        if not clean:
            logger.warning(f"[Warmup] batch {batch_id}: no complete rows, skipping.")
            return self.phase, None

        X = np.array([[r[f] for f in NUMERIC_FEATURES] for r in clean], dtype=float)

        # ── WARMUP ─────────────────────────────────────────────────────────
        if self.phase == WarmupPhase.WARMUP:
            self.collected_rows.extend(clean)
            remaining = self.warmup_batches - self.batch_count
            logger.info(
                f"[WARMUP {self.batch_count}/{self.warmup_batches}] "
                f"Collected {len(self.collected_rows)} baseline rows. "
                f"{max(0, remaining)} batch(es) remaining."
            )
            if self.batch_count >= self.warmup_batches:
                self._fit_detector()
                self.phase = WarmupPhase.CALIBRATE
                self.batch_count = 0
                logger.info("[Warmup → Calibrate] Baseline fitted. Starting calibration phase.")
            return WarmupPhase.WARMUP, None

        # ── CALIBRATE ──────────────────────────────────────────────────────
        if self.phase == WarmupPhase.CALIBRATE:
            score = self.detector.score(X)
            self.calibration_scores.append(score)
            remaining = self.calibration_batches - self.batch_count
            logger.info(
                f"[CALIBRATE {self.batch_count}/{self.calibration_batches}] "
                f"MMD score={score:.6f}. {max(0, remaining)} batch(es) remaining."
            )
            if self.batch_count >= self.calibration_batches:
                threshold = self.detector.calibrate(
                    self.calibration_scores, self.calibration_percentile
                )
                self.phase = WarmupPhase.MONITORING
                self.batch_count = 0
                logger.info(
                    f"[Calibrate → Monitoring] Threshold set to {threshold:.6f}. "
                    "Drift detection is now ACTIVE."
                )
            return WarmupPhase.CALIBRATE, score

        # ── MONITORING ─────────────────────────────────────────────────────
        score = self.detector.score(X)
        return WarmupPhase.MONITORING, score

    def _fit_detector(self) -> None:
        X = np.array(
            [[r[f] for f in NUMERIC_FEATURES] for r in self.collected_rows],
            dtype=float,
        )
        self.detector = RFFMMDDetector(
            n_components=200,
            calibration_percentile=self.calibration_percentile,
        )
        self.detector.fit(X)

    @property
    def is_monitoring(self) -> bool:
        return self.phase == WarmupPhase.MONITORING

    @property
    def threshold(self) -> Optional[float]:
        return self.detector.threshold_ if self.detector else None


# ════════════════════════════════════════════════════════════════════════════════
# Complementary Drift Methods (KS, PSI, Chi-Squared, Null Rate)
# ════════════════════════════════════════════════════════════════════════════════

class Baseline:
    """Reference distributions for KS / PSI / Chi2 / null-rate checks."""

    def __init__(self):
        self.distributions: dict[str, np.ndarray] = {}
        self.null_rates: dict[str, float] = {}
        self.categorical_freqs: dict[str, dict[str, float]] = {}
        self.initialized: bool = False

    def fit(self, df: DataFrame) -> None:
        logger.info("Fitting complementary baseline distributions...")
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

    def save(self, path: str) -> None:
        data = {
            "distributions": {k: v.tolist() for k, v in self.distributions.items()},
            "null_rates": self.null_rates,
            "categorical_freqs": self.categorical_freqs,
        }
        Path(path).write_text(json.dumps(data, indent=2))

    def load(self, path: str) -> None:
        data = json.loads(Path(path).read_text())
        self.distributions = {k: np.array(v) for k, v in data["distributions"].items()}
        self.null_rates = data["null_rates"]
        self.categorical_freqs = data["categorical_freqs"]
        self.initialized = True


def ks_test(reference: np.ndarray, current: np.ndarray, feature: str) -> dict:
    if len(current) < 10:
        return {"feature": feature, "method": "ks", "skipped": True}
    stat, p = stats.ks_2samp(reference, current)
    drifted = p < KS_SIGNIFICANCE
    if drifted:
        logger.warning(f"[KS] feature='{feature}' stat={stat:.4f} p={p:.6f}")
    return {"feature": feature, "method": "ks",
            "ks_statistic": round(float(stat), 4),
            "p_value": round(float(p), 6), "drifted": drifted}


def compute_psi(reference: np.ndarray, current: np.ndarray, feature: str, n_bins: int = 10) -> dict:
    if len(current) < 10:
        return {"feature": feature, "method": "psi", "skipped": True}
    bins = np.percentile(reference, np.linspace(0, 100, n_bins + 1))
    bins[0], bins[-1] = -np.inf, np.inf
    ref_pct = np.histogram(reference, bins=bins)[0] / len(reference) + PSI_EPSILON
    cur_pct = np.histogram(current,   bins=bins)[0] / len(current)   + PSI_EPSILON
    psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))
    level = "alert" if psi > PSI_ALERT else ("warning" if psi > PSI_WARNING else "ok")
    if level != "ok":
        logger.warning(f"[PSI {level.upper()}] feature='{feature}' psi={psi:.4f}")
    return {"feature": feature, "method": "psi",
            "psi": round(psi, 4), "level": level, "drifted": psi > PSI_WARNING}


def chi_squared_test(
    reference_freq: dict[str, float],
    current_counts: dict[str, int],
    feature: str,
) -> dict:
    total = sum(current_counts.values())
    if total < 10:
        return {"feature": feature, "method": "chi2", "skipped": True}
    cats = sorted(set(reference_freq) | set(current_counts))
    obs = np.array([current_counts.get(c, 0) for c in cats], dtype=float)
    exp = np.array([reference_freq.get(c, PSI_EPSILON) * total for c in cats])
    chi2, p = stats.chisquare(f_obs=obs, f_exp=exp)
    drifted = p < CHI2_SIGNIFICANCE
    if drifted:
        logger.warning(f"[Chi2] feature='{feature}' chi2={chi2:.4f} p={p:.6f}")
    return {"feature": feature, "method": "chi2",
            "chi2_statistic": round(float(chi2), 4),
            "p_value": round(float(p), 6), "drifted": drifted}


def check_null_rate_drift(
    baseline_rates: dict[str, float],
    current_rates: dict[str, float],
) -> list[dict]:
    results = []
    for feat, base_rate in baseline_rates.items():
        cur_rate = current_rates.get(feat, 0.0)
        delta = abs(cur_rate - base_rate)
        drifted = delta > NULL_DRIFT_THRESHOLD
        if drifted:
            logger.warning(
                f"[Null drift] feature='{feat}' "
                f"baseline={base_rate:.2%} current={cur_rate:.2%} Δ={delta:.2%}"
            )
        results.append({
            "feature": feat, "method": "null_rate_drift",
            "baseline_null_rate": round(base_rate, 4),
            "current_null_rate": round(cur_rate, 4),
            "delta": round(delta, 4), "drifted": drifted,
        })
    return results


# ════════════════════════════════════════════════════════════════════════════════
# Full Drift Report
# ════════════════════════════════════════════════════════════════════════════════

def run_drift_detection(
    df: DataFrame,
    warmup_manager: WarmupManager,
    baseline: Baseline,
    batch_id: int = 0,
) -> dict:
    """
    Run all drift checks for a micro-batch.

    Returns a structured dict with:
      - phase: warmup / calibrate / monitoring
      - mmd_score: RFF-MMD² value (None during warmup)
      - mmd_threshold: calibrated threshold (None until calibration complete)
      - mmd_drift: bool
      - complementary_results: KS / PSI / Chi2 / null-rate checks
    """
    # ── RFF-MMD (primary) ──────────────────────────────────────────────────
    phase, mmd_score = warmup_manager.advance(df, batch_id)
    threshold = warmup_manager.threshold
    mmd_drift = (
        warmup_manager.detector.is_drift(mmd_score)
        if (phase == WarmupPhase.MONITORING and mmd_score is not None)
        else False
    )

    report: dict = {
        "batch_id": batch_id,
        "phase": phase.value,
        "mmd_score": round(mmd_score, 6) if mmd_score is not None else None,
        "mmd_threshold": round(threshold, 6) if threshold is not None else None,
        "mmd_drift": mmd_drift,
        "complementary_results": [],
        "drifted_features": [],
    }

    if mmd_drift:
        logger.warning(
            f"[batch {batch_id}] RFF-MMD DRIFT: "
            f"score={mmd_score:.6f} > threshold={threshold:.6f}"
        )

    # ── Complementary checks (only during MONITORING) ──────────────────────
    if phase != WarmupPhase.MONITORING or not baseline.initialized:
        return report

    comp = []

    for col in NUMERIC_FEATURES:
        if col not in df.columns or col not in baseline.distributions:
            continue
        cur = np.array(
            [row[col] for row in df.select(col).collect() if row[col] is not None],
            dtype=float,
        )
        comp.append(ks_test(baseline.distributions[col], cur, col))
        comp.append(compute_psi(baseline.distributions[col], cur, col))

    for col in CATEGORICAL_FEATURES:
        if col not in df.columns or col not in baseline.categorical_freqs:
            continue
        cur_counts = {
            row[col]: row["count"]
            for row in df.groupBy(col).count().collect()
            if row[col] is not None
        }
        comp.append(chi_squared_test(baseline.categorical_freqs[col], cur_counts, col))

    from streaming_job.validation import compute_null_rates
    cur_null = compute_null_rates(df)
    comp.extend(check_null_rate_drift(baseline.null_rates, cur_null))

    drifted = list({r["feature"] for r in comp if r.get("drifted")})
    report["complementary_results"] = comp
    report["drifted_features"] = drifted

    if drifted:
        logger.warning(
            f"[batch {batch_id}] Complementary checks: drift in {len(drifted)} feature(s): {drifted}"
        )
    else:
        logger.info(f"[batch {batch_id}] Complementary checks: no drift detected.")

    return report
