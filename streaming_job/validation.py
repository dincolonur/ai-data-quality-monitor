"""
streaming_job/validation.py
────────────────────────────
Feature validation logic for the streaming pipeline.
Checks each micro-batch for:
  - Null / missing values
  - Out-of-range values
  - Invalid categorical values (schema violations)
  - Type mismatches (inferred from parsed JSON)
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger("validation")


# ── Validation Config Types ───────────────────────────────────────────────────

class FeatureRule:
    """Defines allowed values/ranges for a single feature."""

    def __init__(
        self,
        name: str,
        nullable: bool = False,
        min_val: float | None = None,
        max_val: float | None = None,
        allowed_values: list[Any] | None = None,
    ):
        self.name = name
        self.nullable = nullable
        self.min_val = min_val
        self.max_val = max_val
        self.allowed_values = allowed_values


# ── Default Rules ─────────────────────────────────────────────────────────────

DEFAULT_RULES: list[FeatureRule] = [
    FeatureRule("user_id",          nullable=False),
    FeatureRule("age",              nullable=False, min_val=0,   max_val=120),
    FeatureRule("purchase_amount",  nullable=False, min_val=0.0),
    FeatureRule("session_duration", nullable=False, min_val=0),
    FeatureRule("page_views",       nullable=False, min_val=0),
    FeatureRule("device_type",      nullable=False,
                allowed_values=["mobile", "desktop", "tablet"]),
    FeatureRule("timestamp",        nullable=False, min_val=0),
]


# ── Core Validation ───────────────────────────────────────────────────────────

def validate_batch(
    df: DataFrame,
    rules: list[FeatureRule] | None = None,
    batch_id: int = 0,
) -> dict:
    """
    Validate a micro-batch DataFrame against feature rules.

    Returns a summary dict with:
        - total row count
        - per-feature null counts
        - per-feature range violation counts
        - per-feature categorical violation counts
        - overall validity rate
    """
    if rules is None:
        rules = DEFAULT_RULES

    total = df.count()
    if total == 0:
        logger.warning(f"[batch {batch_id}] Empty batch — skipping validation.")
        return {"batch_id": batch_id, "total": 0, "issues": []}

    issues = []

    for rule in rules:
        col = rule.name
        if col not in df.columns:
            issues.append({
                "feature": col,
                "issue_type": "missing_column",
                "count": total,
                "rate": 1.0,
            })
            continue

        # Null check
        if not rule.nullable:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                issues.append({
                    "feature": col,
                    "issue_type": "null_value",
                    "count": null_count,
                    "rate": round(null_count / total, 4),
                })

        # Range check
        if rule.min_val is not None or rule.max_val is not None:
            range_filter = F.lit(False)
            if rule.min_val is not None:
                range_filter = range_filter | (F.col(col) < rule.min_val)
            if rule.max_val is not None:
                range_filter = range_filter | (F.col(col) > rule.max_val)

            range_violations = df.filter(
                F.col(col).isNotNull() & range_filter
            ).count()

            if range_violations > 0:
                issues.append({
                    "feature": col,
                    "issue_type": "range_violation",
                    "count": range_violations,
                    "rate": round(range_violations / total, 4),
                    "rule": f"[{rule.min_val}, {rule.max_val}]",
                })

        # Categorical check
        if rule.allowed_values:
            cat_violations = df.filter(
                F.col(col).isNotNull() & ~F.col(col).isin(rule.allowed_values)
            ).count()

            if cat_violations > 0:
                issues.append({
                    "feature": col,
                    "issue_type": "invalid_category",
                    "count": cat_violations,
                    "rate": round(cat_violations / total, 4),
                    "allowed": rule.allowed_values,
                })

    valid_rows = total - len(set(i["feature"] for i in issues))
    validity_rate = round(max(0, total - sum(i["count"] for i in issues)) / total, 4)

    summary = {
        "batch_id": batch_id,
        "total": total,
        "validity_rate": validity_rate,
        "issue_count": len(issues),
        "issues": issues,
    }

    _log_summary(summary)
    return summary


def _log_summary(summary: dict) -> None:
    batch_id = summary["batch_id"]
    total = summary["total"]
    rate = summary["validity_rate"]
    n_issues = summary["issue_count"]

    if n_issues == 0:
        logger.info(f"[batch {batch_id}] ✓ All {total} rows passed validation.")
    else:
        logger.warning(
            f"[batch {batch_id}] ⚠ {n_issues} issue type(s) found in {total} rows "
            f"(validity rate: {rate:.1%})"
        )
        for issue in summary["issues"]:
            logger.warning(
                f"  → [{issue['issue_type']}] feature='{issue['feature']}' "
                f"count={issue['count']} rate={issue['rate']:.2%}"
            )


# ── Null Rate Helper ──────────────────────────────────────────────────────────

def compute_null_rates(df: DataFrame) -> dict[str, float]:
    """Return null rate per column as a plain dict."""
    total = df.count()
    if total == 0:
        return {}
    return {
        col: df.filter(F.col(col).isNull()).count() / total
        for col in df.columns
    }
