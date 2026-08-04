"""
QUALITY-v2.2 Q4 — revenue canary sweep. Cron entry point, every 5 minutes
(client decision E1).

Runs all six canaries (checkout, payment_link, entitlement, delivery,
mail, model_api) via src.services.revenue_canary.run_all_checks, alerts on
any failure through src/services/email.py:send_alert with a 1h dedup
cooldown (revenue_canary_alert_log). A 5-minute-cadence check that
degrades for an hour would otherwise send 12 emails in that hour — the
FIRST alert still fires within 5 minutes of the break either way; only the
repeats are suppressed.

Usage:
    python -m src.tasks.revenue_canary_sweep
    python -m src.tasks.revenue_canary_sweep --dry-run
    python -m src.tasks.revenue_canary_sweep --kill-canary checkout
        # Deliberate-break acceptance test (spec §1.8 / decision E5): forces
        # the named check to report failure so the alert path can be proven
        # to fire, without waiting for a real outage. Mirrors
        # heartbeat_monitor.py's existing --kill-source for the scraper side.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.services.revenue_canary import CHECK_NAMES, run_all_checks
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_ALERT_COOLDOWN_HOURS = 1


def _recently_alerted(db, check_name: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_ALERT_COOLDOWN_HOURS)
    row = db.execute(sa_text("""
        SELECT 1 FROM revenue_canary_alert_log
        WHERE check_name = :name AND alerted_at >= :cutoff
    """), {"name": check_name, "cutoff": cutoff}).fetchone()
    return row is not None


def _record_alerted(db, check_name: str) -> None:
    db.execute(sa_text(
        "INSERT INTO revenue_canary_alert_log (check_name, alerted_at) VALUES (:name, NOW())"
    ), {"name": check_name})
    db.commit()


def run_sweep(db, kill: Optional[str] = None, dry_run: bool = False) -> dict:
    """Run all six checks; alert (with a 1h cooldown per check) on any
    failure. Returns {"results": [...], "alerted": [...]}."""
    results = run_all_checks(db, kill=kill)
    failed = [r for r in results if not r.ok]
    alerted: list[str] = []

    logger.info(
        "[RevenueCanary] %d/%d checks OK (%s)",
        len(results) - len(failed), len(results),
        ", ".join(f"{r.name}={'OK' if r.ok else 'FAIL'}" for r in results),
    )

    for r in failed:
        if _recently_alerted(db, r.name):
            logger.info(
                "[RevenueCanary] %s already alerted in the last %dh — skipping",
                r.name, _ALERT_COOLDOWN_HOURS,
            )
            continue
        subject = f"[FA][REVENUE CANARY] {r.name} FAILED"
        body = (
            f"Revenue canary check '{r.name}' failed.\n\n"
            f"Detail:   {r.detail}\n"
            f"Latency:  {r.latency_ms}ms\n"
            f"Time:     {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n\n"
            "Action: this is a synthetic, forward-looking check that runs every "
            "5 minutes — revenue is breaking RIGHT NOW, not yesterday. Investigate immediately."
        )
        if dry_run:
            logger.info("[RevenueCanary][DRY] would alert:\n%s\n%s", subject, body)
            continue
        try:
            from src.services.email import send_alert
            sent = send_alert(subject, body)
            if sent:
                _record_alerted(db, r.name)
                alerted.append(r.name)
                logger.warning("[RevenueCanary] ALERT SENT for %s: %s", r.name, r.detail)
            else:
                logger.error(
                    "[RevenueCanary] alert delivery returned False for %s; "
                    "dedup row NOT recorded so the next tick will retry", r.name,
                )
        except Exception:
            logger.error("[RevenueCanary] failed to send alert for %s", r.name, exc_info=True)

    return {
        "results": [
            {"name": r.name, "ok": r.ok, "detail": r.detail, "latency_ms": r.latency_ms}
            for r in results
        ],
        "alerted": alerted,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Revenue-path canary sweep")
    ap.add_argument("--dry-run", action="store_true", help="Evaluate + print, don't send alerts")
    ap.add_argument(
        "--kill-canary", default=None, choices=CHECK_NAMES,
        help="Deliberate-break acceptance test (spec §1.8): force this named "
             "check to report failure and confirm the alert fires.",
    )
    args = ap.parse_args()

    with get_db_context() as db:
        result = run_sweep(db, kill=args.kill_canary, dry_run=args.dry_run)

    print(f"\nRevenue canary sweep — {len(result['results'])} checks:")
    for r in result["results"]:
        marker = "OK  " if r["ok"] else "FAIL"
        print(f"  {marker}  {r['name']:<14} ({r['latency_ms']}ms)  {r['detail']}")
    if result["alerted"]:
        print(f"\nAlerted: {', '.join(result['alerted'])}")


if __name__ == "__main__":
    main()
