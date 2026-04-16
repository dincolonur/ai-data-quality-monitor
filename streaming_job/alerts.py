"""
streaming_job/alerts.py
────────────────────────
Multi-handler alerting layer with hysteresis and backoff.

Handlers:
  - StructuredLogHandler  — Python logger (always active)
  - FileLogHandler        — persistent JSONL file
  - SlackHandler          — Slack incoming webhook
  - EmailHandler          — SMTP email notifications
  - DashboardHandler      — writes JSON state for the live HTML dashboard

Hysteresis + Backoff:
  - HysteresisTracker: requires N consecutive violations before firing
  - Cooldown enforced per alert key — no more than one alert per key per interval
"""

import json
import logging
import os
import smtplib
import time
from collections import defaultdict
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger("alerts")


# ── Severity ───────────────────────────────────────────────────────────────────

class Severity:
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"

    @staticmethod
    def from_rate(rate: float) -> str:
        if rate >= 0.30:
            return Severity.CRITICAL
        if rate >= 0.15:
            return Severity.HIGH
        if rate >= 0.05:
            return Severity.MEDIUM
        return Severity.LOW

    @staticmethod
    def from_mmd(score: float, threshold: float) -> str:
        ratio = score / threshold if threshold else 1.0
        if ratio >= 3.0:
            return Severity.CRITICAL
        if ratio >= 2.0:
            return Severity.HIGH
        if ratio >= 1.0:
            return Severity.MEDIUM
        return Severity.LOW


# ════════════════════════════════════════════════════════════════════════════════
# Hysteresis Tracker
# ════════════════════════════════════════════════════════════════════════════════

class HysteresisTracker:
    """
    Guards against alert flapping by requiring N consecutive violations
    and enforcing a per-key cooldown after each alert fires.

    Args:
        required_consecutive: number of back-to-back bad windows before alert fires
        cooldown_seconds: minimum seconds between two alerts for the same key
    """

    def __init__(self, required_consecutive: int = 2, cooldown_seconds: float = 120.0):
        self.required = required_consecutive
        self.cooldown = cooldown_seconds
        self._counts:     dict[str, int]   = defaultdict(int)
        self._last_alert: dict[str, float] = {}

    def record(self, key: str, is_violation: bool) -> bool:
        """
        Record a measurement for a named key.
        Returns True only when the alert should actually fire.
        """
        if not is_violation:
            self._counts[key] = 0
            return False

        self._counts[key] += 1

        # Cooldown check
        now = time.time()
        last = self._last_alert.get(key, 0.0)
        if now - last < self.cooldown:
            remaining = int(self.cooldown - (now - last))
            logger.debug(
                f"[Hysteresis] key='{key}' violation #{self._counts[key]}, "
                f"in cooldown ({remaining}s remaining) — suppressed."
            )
            return False

        # Consecutive-violation check
        if self._counts[key] >= self.required:
            self._last_alert[key] = now
            self._counts[key] = 0
            logger.debug(f"[Hysteresis] key='{key}' threshold met — alert FIRES.")
            return True

        logger.debug(
            f"[Hysteresis] key='{key}' violation "
            f"{self._counts[key]}/{self.required} — waiting for consecutive threshold."
        )
        return False

    def state(self) -> dict:
        return {k: self._counts[k] for k in self._counts}


# ════════════════════════════════════════════════════════════════════════════════
# Alert Payload
# ════════════════════════════════════════════════════════════════════════════════

def build_payload(
    batch_id: int,
    alert_type: str,
    severity: str,
    message: str,
    what: str,
    why: str,
    metric_value: Optional[float] = None,
    details: Optional[dict] = None,
) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "what": what,
        "why": why,
        "metric_value": metric_value,
        "details": details or {},
    }


# ════════════════════════════════════════════════════════════════════════════════
# Handlers
# ════════════════════════════════════════════════════════════════════════════════

class StructuredLogHandler:
    """Emits structured log lines to Python's logging system."""

    LEVEL_MAP = {
        Severity.LOW:      logging.INFO,
        Severity.MEDIUM:   logging.WARNING,
        Severity.HIGH:     logging.WARNING,
        Severity.CRITICAL: logging.CRITICAL,
    }

    def send(self, payload: dict) -> None:
        level = self.LEVEL_MAP.get(payload["severity"], logging.WARNING)
        logger.log(
            level,
            f"[ALERT:{payload['severity'].upper()}] "
            f"batch={payload['batch_id']} "
            f"type={payload['alert_type']} | "
            f"what={payload['what']} | "
            f"why={payload['why']} | "
            f"value={payload.get('metric_value')}",
        )


class FileLogHandler:
    """Appends one JSON line per alert to a persistent log file."""

    def __init__(self, log_path: str):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, payload: dict) -> None:
        try:
            with open(self.log_path, "a") as f:
                f.write(json.dumps(payload) + "\n")
        except OSError as e:
            logger.error(f"FileLogHandler write failed: {e}")


class SlackHandler:
    """Posts alert messages to a Slack channel via incoming webhook."""

    EMOJI = {
        Severity.LOW:      ":information_source:",
        Severity.MEDIUM:   ":warning:",
        Severity.HIGH:     ":large_orange_circle:",
        Severity.CRITICAL: ":red_circle:",
    }

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, payload: dict) -> None:
        sev = payload["severity"]
        emoji = self.EMOJI.get(sev, ":bell:")
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} Data Quality Alert — {payload['alert_type']}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Severity:*\n`{sev.upper()}`"},
                    {"type": "mrkdwn", "text": f"*Batch:*\n`{payload['batch_id']}`"},
                    {"type": "mrkdwn", "text": f"*What:*\n{payload['what']}"},
                    {"type": "mrkdwn", "text": f"*Why:*\n{payload['why']}"},
                ],
            },
        ]
        if payload.get("metric_value") is not None:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Metric value:* `{payload['metric_value']}`"},
            })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Time: {payload['timestamp']}"}],
        })

        try:
            resp = requests.post(self.webhook_url, json={"blocks": blocks}, timeout=5)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"SlackHandler failed: {e}")


class EmailHandler:
    """Sends alert emails via SMTP."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        sender: str,
        recipients: list[str],
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = True,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.sender = sender
        self.recipients = recipients
        self.username = username
        self.password = password
        self.use_tls = use_tls

    def send(self, payload: dict) -> None:
        sev = payload["severity"].upper()
        subject = f"[{sev}] Data Quality Alert — {payload['alert_type']} (batch {payload['batch_id']})"

        body_lines = [
            f"Alert Type : {payload['alert_type']}",
            f"Severity   : {sev}",
            f"Batch      : {payload['batch_id']}",
            f"Time       : {payload['timestamp']}",
            "",
            f"WHAT       : {payload['what']}",
            f"WHY        : {payload['why']}",
        ]
        if payload.get("metric_value") is not None:
            body_lines.append(f"METRIC     : {payload['metric_value']}")
        if payload.get("details"):
            body_lines.append("")
            body_lines.append("Details:")
            for k, v in payload["details"].items():
                body_lines.append(f"  {k}: {v}")

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"]    = self.sender
        msg["To"]      = ", ".join(self.recipients)
        msg.attach(MIMEText("\n".join(body_lines), "plain"))

        try:
            server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10)
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())
            server.quit()
            logger.debug(f"EmailHandler sent alert to {self.recipients}")
        except Exception as e:
            logger.error(f"EmailHandler failed: {e}")


class DashboardHandler:
    """
    Writes / updates a JSON state file consumed by the live HTML dashboard.
    Maintains a rolling window of recent metrics and the last 50 alerts.
    """

    MAX_ALERTS   = 50
    MAX_HISTORY  = 200

    def __init__(self, state_path: str):
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load()

    def _load(self) -> dict:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except Exception:
                pass
        return {"alerts": [], "metrics_history": [], "summary": {}}

    def _save(self) -> None:
        try:
            self.state_path.write_text(json.dumps(self._state, indent=2))
        except OSError as e:
            logger.error(f"DashboardHandler save failed: {e}")

    def send(self, payload: dict) -> None:
        self._state["alerts"].insert(0, payload)
        self._state["alerts"] = self._state["alerts"][:self.MAX_ALERTS]
        self._save()

    def push_metrics(self, batch_id: int, metrics: dict) -> None:
        """Push per-batch metrics for time-series charts."""
        entry = {"batch_id": batch_id, "timestamp": datetime.now(timezone.utc).isoformat(), **metrics}
        self._state["metrics_history"].append(entry)
        self._state["metrics_history"] = self._state["metrics_history"][-self.MAX_HISTORY:]
        self._state["summary"] = {
            "last_batch": batch_id,
            "last_updated": entry["timestamp"],
            "total_alerts": len(self._state["alerts"]),
        }
        self._save()


# ════════════════════════════════════════════════════════════════════════════════
# Alert Dispatcher
# ════════════════════════════════════════════════════════════════════════════════

class AlertDispatcher:
    """
    Routes alerts to all configured handlers.
    Applies hysteresis before dispatching to prevent flapping.
    """

    def __init__(self, config: dict):
        self.config = config
        alert_cfg  = config.get("alerts", {})
        hyst_cfg   = config.get("alerts", {}).get("hysteresis", {})

        self.enabled   = alert_cfg.get("enabled", True)
        self.hysteresis = HysteresisTracker(
            required_consecutive = hyst_cfg.get("required_consecutive", 2),
            cooldown_seconds     = hyst_cfg.get("cooldown_seconds", 120.0),
        )

        # Always-on
        self._handlers = [StructuredLogHandler()]

        # File log
        if log_path := alert_cfg.get("log_file"):
            self._handlers.append(FileLogHandler(log_path))
            logger.info(f"FileLogHandler active → {log_path}")

        # Dashboard
        if dash_path := alert_cfg.get("dashboard_state_path"):
            self.dashboard_handler = DashboardHandler(dash_path)
            self._handlers.append(self.dashboard_handler)
            logger.info(f"DashboardHandler active → {dash_path}")
        else:
            self.dashboard_handler = None

        # Slack
        if webhook := alert_cfg.get("slack_webhook_url"):
            self._handlers.append(SlackHandler(webhook))
            logger.info("SlackHandler active.")

        # Email
        email_cfg = alert_cfg.get("email", {})
        if email_cfg.get("enabled") and email_cfg.get("recipients"):
            self._handlers.append(EmailHandler(
                smtp_host   = email_cfg.get("smtp_host", "smtp.gmail.com"),
                smtp_port   = email_cfg.get("smtp_port", 587),
                sender      = email_cfg.get("sender", ""),
                recipients  = email_cfg.get("recipients", []),
                username    = email_cfg.get("username"),
                password    = os.environ.get("ALERT_EMAIL_PASSWORD") or email_cfg.get("password"),
                use_tls     = email_cfg.get("use_tls", True),
            ))
            logger.info("EmailHandler active.")

        self._history: list[dict] = []

    # ── Core dispatch ──────────────────────────────────────────────────────────

    def dispatch(
        self,
        batch_id: int,
        alert_type: str,
        severity: str,
        what: str,
        why: str,
        metric_value: Optional[float] = None,
        details: Optional[dict] = None,
        hysteresis_key: Optional[str] = None,
        is_violation: bool = True,
    ) -> bool:
        """
        Dispatch an alert through all handlers.
        If hysteresis_key is given, the HysteresisTracker decides whether to fire.
        Returns True if the alert was actually sent.
        """
        if not self.enabled:
            return False

        key = hysteresis_key or alert_type
        if not self.hysteresis.record(key, is_violation):
            return False

        payload = build_payload(
            batch_id=batch_id,
            alert_type=alert_type,
            severity=severity,
            message=f"{what} — {why}",
            what=what,
            why=why,
            metric_value=metric_value,
            details=details,
        )
        self._history.append(payload)

        for handler in self._handlers:
            try:
                handler.send(payload)
            except Exception as e:
                logger.error(f"{handler.__class__.__name__} error: {e}")

        return True

    # ── Convenience dispatchers ────────────────────────────────────────────────

    def dispatch_validation_issues(self, batch_id: int, summary: dict) -> None:
        for issue in summary.get("issues", []):
            rate  = issue.get("rate", 0.0)
            sev   = Severity.from_rate(rate)
            itype = issue["issue_type"]
            feat  = issue["feature"]
            self.dispatch(
                batch_id=batch_id,
                alert_type=f"validation_{itype}",
                severity=sev,
                what=f"Feature '{feat}' has {itype} in {issue['count']} rows ({rate:.1%})",
                why=f"{itype.replace('_', ' ').title()} detected above threshold",
                metric_value=round(rate, 4),
                details=issue,
                hysteresis_key=f"validation_{itype}_{feat}",
                is_violation=True,
            )

    def dispatch_drift_report(self, batch_id: int, report: dict) -> None:
        # RFF-MMD primary drift
        if report.get("mmd_drift"):
            score     = report.get("mmd_score", 0.0)
            threshold = report.get("mmd_threshold", 0.0)
            sev = Severity.from_mmd(score, threshold)
            self.dispatch(
                batch_id=batch_id,
                alert_type="rff_mmd_drift",
                severity=sev,
                what=f"RFF-MMD drift score={score:.6f} exceeds threshold={threshold:.6f}",
                why="Feature distribution has shifted significantly from baseline",
                metric_value=round(score, 6),
                details={"threshold": threshold, "ratio": round(score / threshold, 3) if threshold else None},
                hysteresis_key="rff_mmd_drift",
                is_violation=True,
            )

        # Complementary per-feature drift
        for result in report.get("complementary_results", []):
            if result.get("drifted") and not result.get("skipped"):
                method = result["method"]
                feat   = result["feature"]
                self.dispatch(
                    batch_id=batch_id,
                    alert_type=f"drift_{method}",
                    severity=Severity.MEDIUM,
                    what=f"[{method.upper()}] Feature '{feat}' distribution drift",
                    why=f"{method.upper()} test flagged significant distribution change",
                    metric_value=result.get("ks_statistic") or result.get("psi") or result.get("delta"),
                    details=result,
                    hysteresis_key=f"drift_{method}_{feat}",
                    is_violation=True,
                )

    def push_dashboard_metrics(self, batch_id: int, metrics: dict) -> None:
        if self.dashboard_handler:
            self.dashboard_handler.push_metrics(batch_id, metrics)

    def get_history(self) -> list[dict]:
        return list(self._history)
