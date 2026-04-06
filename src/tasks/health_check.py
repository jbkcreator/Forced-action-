"""
Daily ops health check — calls /health/detailed and alerts if degraded or critical.

Cron (daily at 9 AM UTC, after scoring):
    0 9 * * * cd /opt/forced-action && python -m src.tasks.health_check >> /var/log/fa-health.log 2>&1
"""

import logging
import sys

import requests

from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def run_health_check(base_url: str = "https://forcedactionleads.com") -> dict:
    """
    Call /health/detailed, parse the result, and send an alert if status
    is 'degraded' or 'critical'. Returns the parsed response dict.
    """
    from src.services.email import send_alert

    url = f"{base_url}/health/detailed"
    try:
        resp = requests.get(url, timeout=20)
        data = resp.json()
    except Exception as exc:
        msg = f"Could not reach {url}: {exc}"
        logger.error("[health_check] %s", msg)
        send_alert("[FA] Health check unreachable", msg)
        return {}

    status = data.get("status", "unknown")
    checks = data.get("checks", {})
    checked_at = data.get("checked_at", "")

    logger.info("[health_check] status=%s checked_at=%s", status, checked_at)

    if status in ("degraded", "critical"):
        # Build a readable summary of failing checks
        lines = []
        for name, info in checks.items():
            s = info.get("status", "unknown")
            if s not in ("ok", "unconfigured"):
                detail = info.get("detail", "")
                lines.append(f"  {name}: {s}" + (f" — {detail}" if detail else ""))

        body = (
            f"System health status: {status.upper()}\n"
            f"Checked at: {checked_at}\n\n"
            + (("\n".join(lines) + "\n\n") if lines else "")
            + f"Full report: {url}"
        )

        send_alert(f"[FA] System health {status.upper()}", body)
        logger.warning("[health_check] Alert sent — status=%s issues=%d", status, len(lines))
    else:
        logger.info("[health_check] All systems OK")

    return data


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "https://forcedactionleads.com"
    result = run_health_check(base)
    print(f"Status: {result.get('status', 'unknown')}")
