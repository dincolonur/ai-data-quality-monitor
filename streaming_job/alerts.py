"""
streaming_job/alerts.py
────────────────────────
Alerting layer for the data quality monitor.
Supports:
  - Structured console / file logging (always active)
  - Slack webhook notifications (optional, via config)
  - Extensible handler pattern for future targets (PagerDuty, email, etc.)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("alerts")


# ── Alert Severity ────────────────────────────────────────────────────────────

class Severity:
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


# ── Alert Payload ─────────────────────────────────────────────────────────────

def build_alert_payload(
    batch_id: int,
    alert_type: str,
    severity: str,
    message: str,
    details: dict | None = None,
) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "details": details or {},
    }


# ── Handlers ──────────────────────────────────────────────────────────────────

def log_alert(payload: dict) -> None:
    """Write alert to structured log output."""
    level = payload.get("severity", "WARNING")
    msg = (
        f"[ALERT:{level}] batch={payload['batch_id']} "
        f"type={payload['alert_type']} — {payload['message']}"
    )
    if level == Severity.CRITICAL:
        logger.critical(msg)
    elif level == Severity.WARNING:
        logger.warning(msg)
    else:
        logger.info(msg)

    if payload.get("details"):
        logger.debug(f"  Alert details: {json.dumps(payload['details'], indent=2)}")


def slack_alert(payload: dict, webhook_url: str) -> None:
    """Send alert to a Slack channel via incoming webhook."""
    emoji = {
        Severity.INFO: ":information_source:",
        Severity.WARNING: ":warning:",
        Severity.CRITICAL: ":red_circle:",
    }.get(payload["severity"], ":bell:")

    text = (
        f"{emoji} *Data Quality Alert* — `{payload['alert_type']}`\n"
        f"*Severity:* {payload['severity']}\n"
        f"*Batch:* {payload['batch_id']}\n"
        f"*Time:* {payload['timestamp']}\n"
        f"*Message:* {payload['message']}"
    )

    if payload.get("details"):
        detail_lines = "\n".join(f"• `{k}`: {v}" for k, v in payload["details"].items())
        text += f"\n*Details:*\n{detail_lines}"

    try:
        resp = requests.post(
            webhook_url,
            json={"text": text},
            timeout=5,
        )
        resp.raise_for_status()
        logger.debug(f"Slack alert sent for batch {payload['batch_id']}")
    except requests.RequestException as e:
        logger.error(f"Failed to send Slack alert: {e}")


# ── Alert Dispatcher ──────────────────────────────────────────────────────────

class AlertDispatcher:
    """Central dispatcher that routes alerts to configured handlers."""

    def __init__(self, config: dict):
        self.config = config
        self.slack_webhook = config.get("alerts", {}).get("slack_webhook_url")
        self.enabled = config.get("alerts", {}).get("enabled", True)
        self._alert_history: list[dict] = []

    def dispatch(
        self,
        batch_id: int,
        alert_type: str,
        severity: str,
        message: str,
        details: dict | None = None,
    ) -> None:
        if not self.enabled:
            return

        payload = build_alert_payload(batch_id, alert_type, severity, message, details)
        self._alert_history.append(payload)

        # Always log
        log_alert(payload)

        # Slack if configured
        if self.slack_webhook and severity in (Severity.WARNING, Severity.CRITICAL):
            slack_alert(payload, self.slack_webhook)

    def dispatch_validation_issues(self, batch_id: int, validation_summary: dict) -> None:
        """Dispatch alerts based on a validation summary dict."""
        issues = validation_summary.get("issues", [])
        if not issues:
            return

        for issue in issues:
            rate = issue.get("rate", 0)
            severity = Severity.CRITICAL if rate > 0.2 else Severity.WARNING

            self.dispatch(
                batch_id=batch_id,
                alert_type=f"validation_{issue['issue_type']}",
                severity=severity,
                message=(
                    f"Feature '{issue['feature']}' has {issue['issue_type']} "
                    f"in {issue['count']} rows ({rate:.1%} of batch)"
                ),
                details={
                    "feature": issue["feature"],
                    "issue_type": issue["issue_type"],
                    "affected_rows": issue["count"],
                    "rate": f"{rate:.2%}",
                    **{k: v for k, v in issue.items() if k not in ("feature", "issue_type", "count", "rate")},
                },
            )

    def dispatch_drift_report(self, batch_id: int, drift_report: dict) -> None:
        """Dispatch alerts based on a drift detection report."""
        drifted = drift_report.get("drifted_features", [])
        if not drifted:
            return

        self.dispatch(
            batch_id=batch_id,
            alert_type="data_drift_detected",
            severity=Severity.CRITICAL if len(drifted) > 2 else Severity.WARNING,
            message=f"Drift detected in {len(drifted)} feature(s): {drifted}",
            details={
                "drifted_features": drifted,
                "total_checks": drift_report.get("total_checks", 0),
                "drifted_count": drift_report.get("drifted_feature_count", 0),
            },
        )

        # Per-feature alerts for detailed tracking
        for result in drift_report.get("results", []):
            if result.get("drifted") and not result.get("skipped"):
                method = result.get("method", "unknown")
                feature = result.get("feature", "?")

                detail = {k: v for k, v in result.items() if k not in ("feature", "method", "drifted")}
                self.dispatch(
                    batch_id=batch_id,
                    alert_type=f"drift_{method}",
                    severity=Severity.WARNING,
                    message=f"[{method.upper()}] Feature '{feature}' drift detected",
                    details={"feature": feature, **detail},
                )

    def get_history(self) -> list[dict]:
        return self._alert_history

    def clear_history(self) -> None:
        self._alert_history = []
