"""
Daily ops health check — calls /health/detailed and alerts based on status.

/health/detailed reports four severity tiers (see src/api/main.py:
health_check_detailed for the authoritative definitions):
  "ok"       — no email.
  "warning"  — nothing is actually broken, some scraper(s) simply had no new
               data (or an unclassified zero-row day). Sent as a separate,
               deliberately calmer "[FA] Data availability notice" email —
               never the urgent subject line — since there's no action to
               take on a confirmed-empty day.
  "degraded" — a real, actionable problem (a scraper actually errored, a
               source went stale, or another subsystem is failing). Sent as
               the urgent "[FA] System health {STATUS}" alert.
  "critical" — the database is unreachable. Same urgent alert path.

Cron (daily at 9 AM UTC, after scoring):
    0 9 * * * cd /opt/forced-action && python -m src.tasks.health_check >> /var/log/fa-health.log 2>&1
"""

import logging
import sys

import requests

from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def _scraper_lines(scraper_check: dict) -> tuple[list, list]:
    """Render the scrapers check's "errors" and "data_unavailable" buckets
    (see health_check_detailed / _classify_scraper_issues in src/api/main.py)
    into separate human-readable line lists, so real errors and informational
    data gaps never get mixed into the same alarming block of text."""
    error_lines = [
        f"  {e['source']} ({e['date']}): {e.get('error_type', 'scraper_error')}"
        + (f" — {e['message']}" if e.get("message") else "")
        for e in scraper_check.get("errors", [])
    ]
    data_lines = [
        f"  {d['source']} ({d['date']}): {d.get('reason', 'no_data')}"
        for d in scraper_check.get("data_unavailable", [])
    ]
    return error_lines, data_lines


def run_health_check(base_url: str = "https://forcedactionleads.com") -> dict:
    """
    Call /health/detailed, parse the result, and alert appropriately for the
    reported severity tier. Returns the parsed response dict.
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

    scraper_error_lines, scraper_data_lines = _scraper_lines(checks.get("scrapers", {}))

    if status in ("degraded", "critical"):
        # Real, actionable problems only. The scraper check's informational
        # data_unavailable bucket never appears here as an alarming line —
        # it gets a one-line, clearly-non-actionable footer instead so an
        # on-call reader doesn't waste time investigating a confirmed-empty
        # scrape day while a real issue is present elsewhere.
        lines = list(scraper_error_lines)
        for name, info in checks.items():
            if name == "scrapers":
                continue
            s = info.get("status", "unknown")
            if s not in ("ok", "unconfigured"):
                detail = info.get("detail", "")
                lines.append(f"  {name}: {s}" + (f" — {detail}" if detail else ""))

        body = (
            f"System health status: {status.upper()}\n"
            f"Checked at: {checked_at}\n\n"
            + (("\n".join(lines) + "\n\n") if lines else "")
            + (
                f"Also: {len(scraper_data_lines)} scraper(s) had no data available "
                f"today (informational — no action needed, see full report).\n\n"
                if scraper_data_lines else ""
            )
            + f"Full report: {url}"
        )
        send_alert(f"[FA] System health {status.upper()}", body)
        logger.warning("[health_check] Alert sent — status=%s issues=%d", status, len(lines))

    elif status == "warning":
        # Nothing is broken — deliberately a separate, calmer email so it's
        # never confused with an incident requiring action.
        detail_text = "\n".join(scraper_data_lines) if scraper_data_lines else "  (see full report for details)"
        body = (
            f"Checked at: {checked_at}\n\n"
            f"No real errors detected — this looks like a data-availability gap "
            f"rather than a system problem (a scraper ran fine but had nothing "
            f"new to report, or returned zero rows without a specific reason). "
            f"Informational only; no action is expected:\n\n"
            f"{detail_text}\n\n"
            f"Full report: {url}"
        )
        send_alert("[FA] Data availability notice", body)
        logger.info("[health_check] Data availability notice sent — %d scraper(s)", len(scraper_data_lines))

    else:
        logger.info("[health_check] All systems OK")

    return data


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "https://forcedactionleads.com"
    result = run_health_check(base)
    print(f"Status: {result.get('status', 'unknown')}")
