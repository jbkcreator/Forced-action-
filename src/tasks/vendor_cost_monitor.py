"""Vendor Cost Monitor — daily cron entrypoint.

Orchestrates the full daily vendor cost monitoring cycle:
  1. Auto-resume expired pauses
  2. Aggregate vendor spend, detect anomalies, create/extend pauses
  3. Build the vendor cost summary
  4. Send the daily vendor cost report email
  5. Hand off summary data for Revenue Pulse

Cron line (add to scripts/cron/crontab.txt):
   30 6 * * *    cd /opt/forced-action && python -m src.tasks.vendor_cost_monitor --dry-run
   0 7 * * *     cd /opt/forced-action && python -m src.tasks.vendor_cost_monitor

Runs at 7 AM UTC (after the daily scoring cycle completes, before Revenue Pulse at 7:30).
"""

import logging
import sys
from datetime import datetime, timezone

from src.core.database import get_db_context
from src.services.vendor_cost_monitor import run_daily_monitor
from src.services.vendor_cost_report import (
    build_vendor_cost_summary,
    format_html_vendor_cost_report,
)

logger = logging.getLogger(__name__)


def run_vendor_cost_monitor(dry_run: bool = False) -> dict:
    """Execute the full daily vendor cost monitor cycle."""
    result: dict = {
        "dry_run": dry_run,
        "monitor_result": None,
        "report_sent": False,
        "errors": [],
    }

    # Step 0: Log Stripe daily fees before aggregation so they appear in today's totals
    try:
        from src.tasks.stripe_reconcile import log_stripe_daily_fees
        fee_result = log_stripe_daily_fees(dry_run=dry_run)
        result["stripe_fees"] = fee_result
        logger.info("[VendorCostMonitor] Stripe fees: $%.4f (%d tx)", fee_result.get("fee_usd", 0), fee_result.get("transactions", 0))
    except Exception as exc:
        logger.warning("[VendorCostMonitor] Stripe fee logging failed (non-fatal): %s", exc)
        result["stripe_fees"] = {"fee_usd": 0.0, "transactions": 0, "logged": False}

    with get_db_context() as db:
        # Step 1: Run the monitor cycle (aggregation, anomaly detection, pause create/extend)
        try:
            monitor_result = run_daily_monitor(db, dry_run=dry_run)
            result["monitor_result"] = monitor_result
            logger.info(
                "[VendorCostMonitor] Cycle complete: auto_resumed=%d created=%d extended=%d anomalies=%d",
                monitor_result.get("auto_resumed", 0),
                monitor_result.get("pauses_created", 0),
                monitor_result.get("pauses_extended", 0),
                len(monitor_result.get("anomalies", [])),
            )
        except Exception as exc:
            logger.error("[VendorCostMonitor] Monitor cycle failed: %s", exc)
            result["errors"].append(f"monitor_cycle: {exc}")

        # Step 2: Build the vendor cost summary for reporting
        try:
            summary = build_vendor_cost_summary(db)
            result["summary"] = summary
        except Exception as exc:
            logger.error("[VendorCostMonitor] Summary build failed: %s", exc)
            result["errors"].append(f"summary: {exc}")
            return result

        # Step 3: Send the daily vendor cost report email
        if not dry_run:
            try:
                html = format_html_vendor_cost_report(summary)
                _send_vendor_cost_email(html, summary)
                result["report_sent"] = True
                logger.info("[VendorCostMonitor] Vendor cost report email sent")
            except Exception as exc:
                logger.error("[VendorCostMonitor] Report email failed: %s", exc)
                result["errors"].append(f"report_email: {exc}")

    logger.info("[VendorCostMonitor] Finished (dry_run=%s) — %d error(s)", dry_run, len(result["errors"]))
    return result


def _send_vendor_cost_email(html: str, summary: dict) -> None:
    """Send the vendor cost report to REPORT_RECIPIENTS via existing email infra."""
    from config.settings import get_settings
    from src.services.email import send_email

    settings = get_settings()
    raw = settings.report_recipients
    if not raw:
        logger.info("[VendorCostMonitor] No REPORT_RECIPIENTS configured — skipping email")
        return

    recipients = [e.strip() for e in raw.split(",") if e.strip()]
    if not recipients:
        return

    totals = summary.get("vendor_totals", {})
    total = sum(totals.values())
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    subject = f"[Forced Action] Daily Vendor Cost Report {date_str} — ${total:.2f} total"

    # Plain-text fallback
    text_lines = [
        f"Forced Action Daily Vendor Cost Report — {date_str}",
        f"Total vendor spend: ${total:.2f}",
        "",
    ]
    for vendor, cost in sorted(totals.items()):
        text_lines.append(f"  {vendor}: ${cost:.2f}")
    text_lines.append("")

    pauses = summary.get("active_pauses", [])
    if pauses:
        text_lines.append(f"Active pauses: {len(pauses)}")
        for p in pauses:
            text_lines.append(
                f"  {p['vendor']}/{p['pause_target']}: "
                f"${p['cost_usd']:.2f} — {p['skipped_actions']} actions skipped — "
                f"auto-resume: {p.get('auto_resume_at', 'N/A')[:16]}"
            )
    else:
        text_lines.append("No active pauses")

    text = "\n".join(text_lines)

    sent = 0
    for addr in recipients:
        if send_email(to=addr, subject=subject, body_text=text, body_html=html):
            sent += 1
        else:
            logger.warning("[VendorCostMonitor] Failed to send vendor cost report to %s", addr)

    logger.info("[VendorCostMonitor] Vendor cost report sent to %d/%d recipients", sent, len(recipients))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    dry_run = "--dry-run" in sys.argv
    result = run_vendor_cost_monitor(dry_run=dry_run)
    print(result)
    if result.get("errors"):
        sys.exit(1)