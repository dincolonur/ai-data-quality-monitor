"""
streaming_job/feature_store.py  — Option A: Feature Store Integration
──────────────────────────────────────────────────────────────────────
Lightweight in-process feature registry that stores per-feature statistics
(mean, std, min, max, null rate, distribution snapshot) and makes them
available to the drift detector as a named baseline.

The registry persists to a JSON file so baselines survive process restarts.
The design mirrors the Feast / Tecton feature view concept but without
requiring a running server — suitable for local development and CI.

Usage in spark_job.py:
    from streaming_job.feature_store import FeatureRegistry
    registry = FeatureRegistry("feature_store/registry.json")
    registry.register_baseline("v1_baseline", df)
    baseline_stats = registry.get_baseline("v1_baseline")
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("feature_store")

NUMERIC_FEATURES     = ["age", "purchase_amount", "session_duration", "page_views"]
CATEGORICAL_FEATURES = ["device_type"]


# ── Feature Statistics ─────────────────────────────────────────────────────────

def compute_feature_stats(df: DataFrame) -> dict:
    """
    Compute a rich statistics snapshot from a DataFrame.
    Returns a dict with per-feature descriptive stats + distribution snapshots.
    """
    total = df.count()
    stats: dict = {"row_count": total, "computed_at": datetime.now(timezone.utc).isoformat()}

    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        vals = np.array(
            [row[col] for row in df.select(col).collect() if row[col] is not None],
            dtype=float,
        )
        null_count = total - len(vals)
        percentiles = np.percentile(vals, [5, 25, 50, 75, 95]).tolist() if len(vals) else []
        stats[col] = {
            "type":        "numeric",
            "count":       int(len(vals)),
            "null_count":  int(null_count),
            "null_rate":   round(null_count / total, 4) if total else 0,
            "mean":        round(float(vals.mean()), 4) if len(vals) else None,
            "std":         round(float(vals.std()),  4) if len(vals) else None,
            "min":         round(float(vals.min()),  4) if len(vals) else None,
            "max":         round(float(vals.max()),  4) if len(vals) else None,
            "p5_p25_p50_p75_p95": [round(p, 4) for p in percentiles],
            "sample":      vals[:200].tolist(),   # store small sample for KS/PSI
        }

    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        freq = {}
        for row in df.groupBy(col).count().collect():
            if row[col] is not None:
                freq[row[col]] = round(row["count"] / total, 4)
        stats[col] = {
            "type":       "categorical",
            "null_count": int(null_count),
            "null_rate":  round(null_count / total, 4) if total else 0,
            "frequencies": freq,
            "cardinality": len(freq),
        }

    return stats


# ════════════════════════════════════════════════════════════════════════════════
# Feature Registry
# ════════════════════════════════════════════════════════════════════════════════

class FeatureRegistry:
    """
    Persistent store for named feature baseline snapshots.

    Each baseline is identified by a version string (e.g. "v1", "2024-Q1").
    Baselines are saved to a JSON file and loaded on construction.
    """

    def __init__(self, registry_path: str = "feature_store/registry.json"):
        self.registry_path = Path(registry_path)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self._store: dict = self._load()

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if self.registry_path.exists():
            try:
                return json.loads(self.registry_path.read_text())
            except Exception as e:
                logger.warning(f"Could not load registry: {e}")
        return {}

    def _save(self) -> None:
        self.registry_path.write_text(json.dumps(self._store, indent=2))
        logger.debug(f"Registry saved → {self.registry_path}")

    # ── Public API ─────────────────────────────────────────────────────────────

    def register_baseline(self, name: str, df: DataFrame, overwrite: bool = False) -> dict:
        """
        Compute and store a feature baseline from a DataFrame.
        Returns the computed stats dict.
        """
        if name in self._store and not overwrite:
            logger.info(f"Baseline '{name}' already exists. Use overwrite=True to replace.")
            return self._store[name]

        logger.info(f"Computing baseline '{name}'…")
        stats = compute_feature_stats(df)
        self._store[name] = stats
        self._save()
        logger.info(
            f"Baseline '{name}' registered: "
            f"{stats['row_count']} rows, "
            f"{len([k for k in stats if k not in ('row_count', 'computed_at')])} features"
        )
        return stats

    def get_baseline(self, name: str) -> Optional[dict]:
        """Retrieve a stored baseline by name. Returns None if not found."""
        baseline = self._store.get(name)
        if baseline is None:
            logger.warning(f"Baseline '{name}' not found in registry.")
        return baseline

    def list_baselines(self) -> list[str]:
        return list(self._store.keys())

    def compare_to_baseline(self, name: str, df: DataFrame) -> dict:
        """
        Compare a live DataFrame against a stored baseline.
        Returns a delta report with drift indicators per feature.
        """
        baseline = self.get_baseline(name)
        if baseline is None:
            return {"error": f"Baseline '{name}' not found"}

        current = compute_feature_stats(df)
        report = {"baseline_name": name, "features": {}}

        for col in NUMERIC_FEATURES:
            if col not in baseline or col not in current:
                continue
            b, c = baseline[col], current[col]
            mean_shift = abs((c["mean"] or 0) - (b["mean"] or 0))
            std_ratio  = (c["std"] or 1) / (b["std"] or 1) if b["std"] else 1
            null_delta = abs(c["null_rate"] - b["null_rate"])
            report["features"][col] = {
                "mean_shift":   round(mean_shift, 4),
                "std_ratio":    round(std_ratio, 4),
                "null_delta":   round(null_delta, 4),
                "drift_signal": mean_shift > 2 * (b["std"] or 1) or null_delta > 0.05,
            }

        for col in CATEGORICAL_FEATURES:
            if col not in baseline or col not in current:
                continue
            b_freq = baseline[col]["frequencies"]
            c_freq = current[col]["frequencies"]
            new_cats = set(c_freq) - set(b_freq)
            missing  = set(b_freq) - set(c_freq)
            report["features"][col] = {
                "new_categories":     list(new_cats),
                "missing_categories": list(missing),
                "drift_signal":       bool(new_cats or missing),
            }

        return report

    def delete_baseline(self, name: str) -> bool:
        if name in self._store:
            del self._store[name]
            self._save()
            logger.info(f"Baseline '{name}' deleted.")
            return True
        return False

    def summary(self) -> None:
        """Print a human-readable registry summary."""
        if not self._store:
            print("Registry is empty.")
            return
        print(f"\nFeature Registry — {self.registry_path}")
        print(f"{'Name':<20} {'Rows':>8} {'Features':>10} {'Computed At'}")
        print("-" * 60)
        for name, stats in self._store.items():
            n_feats = len([k for k in stats if k not in ("row_count", "computed_at")])
            print(f"{name:<20} {stats.get('row_count','?'):>8} {n_feats:>10}  {stats.get('computed_at','?')[:19]}")
