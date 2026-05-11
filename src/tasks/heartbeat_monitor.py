"""
Heartbeat monitor — real-time freshness watchdog for every required scraper.

Designed to run every 15 minutes via cron. For each source listed in
HEARTBEAT_SLAS, computes:

    age_minutes = now - max(scraper_run_stats.created_at WHERE run_success=true)

and emits one routed alert per source that has exceeded its SLA window.
Dedup is via the existing scraper_alert_log table with a per-rule
cooldown so the same stale source doesn't email every 15 minutes.

Why derived from scraper_run_stats and not a dedicated `heartbeats` table:
every scraper already writes a stats row on completion (success or failure).
Reusing that data means zero scraper code changes — adding a new source to
the watchdog is a one-line entry in HEARTBEAT_SLAS below.

Run:
    */15 * * * *  python -m src.tasks.heartbeat_monitor

Demo / staging helpers:
    python -m src.tasks.heartbeat_monitor --list-slas
    python -m src.tasks.heartbeat_monitor --dry-run
    python -m src.tasks.heartbeat_monitor --kill-source foreclosures   # insert
        # a synthetic 'long-ago' successful-run row so the next monitor tick
        # treats the source as stale and emits a real alert email.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional

from sqlalchemy import func

from src.core.database import get_db_context
from src.core.models import ScraperAlertLog, ScraperRunStats
from src.services.email import send_alert
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SLA registry — minutes since last successful run before we page
# ─────────────────────────────────────────────────────────────────────────────
# Keep this in sync with scripts/cron/crontab.txt. The window is the scraper's
# scheduled interval + a grace period:
#   daily cron at 02–06 UTC → 1500 min (25h)
#   weekly cron              → 10_140 min (~7d + grace)
#   manual-only (PRR upload) → not in this table — they're a separate freshness
#                              metric tracked by the daily ops report.
HEARTBEAT_SLAS: Dict[str, int] = {
    # Daily scrapers — alert if no successful row in the last 25 hours
    "foreclosures":      1500,
    "permits":           1500,
    "roofing_permits":   1500,
    "violations":        1500,
    "probate":           1500,
    "evictions":         1500,
    "divorce_filings":   1500,
    "bankruptcy":        1500,
    "lien_ml":           1500,
    "lien_tcl":          1500,
    "lien_ccl":          1500,
    "lien_hoa":          1500,
    "lien_tl":           1500,
    "judgments":         1500,
    "sunbiz":            1500,
    # Weather / incident — daily but allow extra grace because portals can lag
    "storm_damage":      1800,
    "fire_incidents":    1800,
    "flood_damage":      1800,
    "insurance_claims":  1800,
}

# Dedup: one alert per stale source per 24h.  At a 15-min tick cadence a broken
# scraper would otherwise produce 96 emails per source per day.  With a 24h
# cooldown ops sees exactly one alert when the incident starts and one daily
# "still down" reminder until it resolves.
DEDUP_COOLDOWN_HOURS    = 24
DEMO_STALE_AGE_HOURS    = 48  # how old the synthetic row is when --kill-source used


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Heartbeat:
    source_type: str
    sla_minutes: int
    last_success_at: Optional[datetime]
    age_minutes: Optional[int]      # None when there's no successful row ever
    is_stale: bool

    def alert_subject(self) -> str:
        return f"[FA][HEARTBEAT] {self.source_type} stale — no run in {self.age_label()}"

    def age_label(self) -> str:
        if self.age_minutes is None:
            return "(no successful run ever recorded)"
        if self.age_minutes < 120:
            return f"{self.age_minutes} min"
        hours = self.age_minutes / 60.0
        if hours < 48:
            return f"{hours:.1f} h"
        return f"{hours/24:.1f} d"

    def alert_body(self) -> str:
        if self.last_success_at is None:
            last_line = "Last successful run: never (no row in scraper_run_stats)"
        else:
            last_line = f"Last successful run: {self.last_success_at.isoformat(timespec='seconds')}"
        return (
            f"Source:              {self.source_type}\n"
            f"{last_line}\n"
            f"Age:                 {self.age_label()}\n"
            f"SLA window:          {self.sla_minutes} min "
            f"({self.sla_minutes/60:.0f} h)\n"
            f"Tripped at:          {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n\n"
            "Action: SSH to the scraper host, check the cron + scraper logs for\n"
            "this source. If the scraper crashed, restart it; if the source portal\n"
            "is down, confirm and silence this alert via scraper_alert_log dedup."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Core query
# ─────────────────────────────────────────────────────────────────────────────

def compute_heartbeats(now: Optional[datetime] = None) -> list[Heartbeat]:
    """Compute heartbeat status for every source in HEARTBEAT_SLAS."""
    now = now or datetime.now(timezone.utc)
    out: list[Heartbeat] = []

    with get_db_context() as session:
        for source_type, sla_minutes in HEARTBEAT_SLAS.items():
            last_success = (
                session.query(func.max(ScraperRunStats.created_at))
                .filter(
                    ScraperRunStats.source_type == source_type,
                    ScraperRunStats.run_success.is_(True),
                )
                .scalar()
            )
            if last_success is None:
                out.append(Heartbeat(
                    source_type=source_type, sla_minutes=sla_minutes,
                    last_success_at=None, age_minutes=None, is_stale=True,
                ))
                continue

            if last_success.tzinfo is None:
                last_success = last_success.replace(tzinfo=timezone.utc)
            age = int((now - last_success).total_seconds() // 60)
            out.append(Heartbeat(
                source_type=source_type, sla_minutes=sla_minutes,
                last_success_at=last_success, age_minutes=age,
                is_stale=age > sla_minutes,
            ))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Dedup
# ─────────────────────────────────────────────────────────────────────────────

def _recently_alerted(source_type: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=DEDUP_COOLDOWN_HOURS)
    try:
        with get_db_context() as session:
            row = (
                session.query(ScraperAlertLog)
                .filter(
                    ScraperAlertLog.source_type == source_type,
                    ScraperAlertLog.alert_type == "heartbeat_missed",
                    ScraperAlertLog.alerted_at >= cutoff,
                )
                .first()
            )
            return row is not None
    except Exception as exc:
        logger.warning("[Heartbeat] dedup lookup failed (not suppressing): %s", exc)
        return False


def _record_alerted(source_type: str) -> None:
    try:
        with get_db_context() as session:
            session.add(ScraperAlertLog(
                source_type=source_type,
                county_id="hillsborough",          # county-agnostic for now
                alert_type="heartbeat_missed",
            ))
    except Exception as exc:
        logger.warning("[Heartbeat] could not write dedup row: %s", exc)


def _clear_dedup_for_recovered_sources(beats: list["Heartbeat"]) -> None:
    """
    When a previously-stale source produces a new successful run, wipe its
    heartbeat dedup rows so the NEXT failure gets an immediate alert instead
    of being suppressed by the lingering 24h cooldown from the previous
    incident.
    """
    recovered = [b.source_type for b in beats if not b.is_stale]
    if not recovered:
        return
    try:
        with get_db_context() as session:
            deleted = (
                session.query(ScraperAlertLog)
                .filter(
                    ScraperAlertLog.source_type.in_(recovered),
                    ScraperAlertLog.alert_type == "heartbeat_missed",
                )
                .delete(synchronize_session=False)
            )
            if deleted:
                logger.info(
                    "[Heartbeat] cleared %d stale dedup row(s) for recovered "
                    "source(s): %s",
                    deleted, ", ".join(recovered),
                )
            session.commit()
    except Exception as exc:
        logger.warning("[Heartbeat] dedup cleanup on recovery failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Main entry points
# ─────────────────────────────────────────────────────────────────────────────

def run_once(dry_run: bool = False) -> list[Heartbeat]:
    """Compute heartbeats, route alerts for stale-and-not-recently-paged ones."""
    beats = compute_heartbeats()
    stale = [b for b in beats if b.is_stale]
    logger.info(
        "[Heartbeat] checked %d sources, %d stale (%s)",
        len(beats), len(stale),
        ", ".join(b.source_type for b in stale) or "—",
    )

    # Wipe dedup rows for any source that's no longer stale — so the NEXT
    # failure gets an immediate alert instead of being suppressed by the
    # 24h cooldown left over from a previous incident.
    if not dry_run:
        _clear_dedup_for_recovered_sources(beats)

    for b in stale:
        if _recently_alerted(b.source_type):
            logger.info(
                "[Heartbeat] %s already alerted in the last %dh — skipping",
                b.source_type, DEDUP_COOLDOWN_HOURS,
            )
            continue
        subject = b.alert_subject()
        body    = b.alert_body()
        if dry_run:
            logger.info("[Heartbeat][DRY] would send:\n%s\n\n%s", subject, body)
            continue
        try:
            send_alert(subject, body)
            _record_alerted(b.source_type)
            logger.info("[Heartbeat] ALERT SENT for %s (age=%s)", b.source_type, b.age_label())
        except Exception as exc:
            logger.error("[Heartbeat] failed to send alert for %s: %s", b.source_type, exc)
    return beats


def print_slas() -> None:
    print("\nHeartbeat SLA registry:\n")
    print(f"  {'Source':<22} {'SLA (min)':>10}  {'SLA (h)':>10}")
    print(f"  {'-'*22}  {'-'*10}  {'-'*10}")
    for source, mins in HEARTBEAT_SLAS.items():
        print(f"  {source:<22} {mins:>10}  {mins/60:>10.1f}")
    print(f"\nDedup cooldown: {DEDUP_COOLDOWN_HOURS}h per source "
          "(auto-cleared when source recovers).")


def kill_source_for_demo(source_type: str) -> None:
    """
    Insert a synthetic OLD successful-run row for the given source so the next
    monitor tick interprets the source as stale and emits a real alert.

    Use only in staging / demos.
    """
    if source_type not in HEARTBEAT_SLAS:
        raise SystemExit(
            f"[Heartbeat] '{source_type}' is not in HEARTBEAT_SLAS. "
            f"Known sources: {', '.join(HEARTBEAT_SLAS.keys())}"
        )

    fake_age = timedelta(hours=DEMO_STALE_AGE_HOURS)
    cutoff = datetime.now(timezone.utc) - fake_age

    with get_db_context() as session:
        # Mark every existing successful row for this source as belonging to a
        # past life — set created_at into the past. We DO NOT delete rows.
        # This way --kill-source is fully reversible by re-running the scraper.
        session.query(ScraperRunStats).filter(
            ScraperRunStats.source_type == source_type,
            ScraperRunStats.run_success.is_(True),
            ScraperRunStats.created_at >= datetime.now(timezone.utc) - timedelta(days=7),
        ).update(
            {ScraperRunStats.created_at: cutoff},
            synchronize_session=False,
        )

        # Also bust any heartbeat dedup row so the next run actually emails.
        session.query(ScraperAlertLog).filter(
            ScraperAlertLog.source_type == source_type,
            ScraperAlertLog.alert_type == "heartbeat_missed",
        ).delete(synchronize_session=False)

        session.commit()

    logger.info(
        "[Heartbeat][DEMO] '%s' last_successful_run pushed to %s (%dh ago). "
        "Run the monitor now to see the alert fire.",
        source_type, cutoff.isoformat(timespec='seconds'), DEMO_STALE_AGE_HOURS,
    )


def main():
    ap = argparse.ArgumentParser(description="Heartbeat monitor — per-scraper freshness watchdog")
    ap.add_argument("--list-slas",    action="store_true", help="Print the SLA registry and exit")
    ap.add_argument("--dry-run",      action="store_true", help="Evaluate + print, don't send email")
    ap.add_argument("--kill-source",  default=None,
                    help="Demo: mark all recent successful runs for this source "
                         "as stale (DEMO_STALE_AGE_HOURS hours ago). Reversible "
                         "by running the scraper again.")
    args = ap.parse_args()

    if args.list_slas:
        print_slas(); return

    if args.kill_source:
        kill_source_for_demo(args.kill_source)
        print(f"\nNext: run the monitor to trigger the alert:")
        print(f"  python -m src.tasks.heartbeat_monitor")
        return

    beats = run_once(dry_run=args.dry_run)

    # Operator-facing summary
    print(f"\nChecked {len(beats)} sources:")
    for b in sorted(beats, key=lambda x: (not x.is_stale, x.source_type)):
        marker = "STALE" if b.is_stale else "OK   "
        print(f"  {marker}  {b.source_type:<22} age={b.age_label():<10} sla={b.sla_minutes/60:.0f}h")


if __name__ == "__main__":
    main()
