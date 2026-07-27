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

from sqlalchemy import and_, func, or_

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
    # Lifecycle Data Engine outcome connectors (src/connectors/registry.py is the
    # single source of truth for these sla_minutes/off_days values — copied
    # in here at each connector's go-live, per that file's own convention).
    "foreclosure_outcomes":    1500,
    "tax_deed_outcomes":       1500,
    "appraiser_sale_outcomes": 10_140,   # weekly, after the appraiser bulk refresh
    "outcome_label_layer":     1500,
    "dor_sale_outcomes":       131_040,
}

# Sources that are intentionally NOT scheduled on certain weekdays. Heartbeat
# skips them on those days so a M-Sat scraper isn't reported as "stale" on a
# Sunday morning by design. Python weekday(): Mon=0, Tue=1, ..., Sat=5, Sun=6.
#
# Everything except foreclosures runs M-Sat in cron, so Sunday (6) is their
# off-day. foreclosures runs daily and has no entry here → checked every day.
# Keep this in sync with scripts/cron/crontab.txt.
SOURCE_OFF_DAYS: Dict[str, set] = {
    "violations":       {6},
    "permits":          {6},
    "roofing_permits":  {6},
    "probate":          {6},
    "evictions":        {6},
    "divorce_filings":  {6},
    "bankruptcy":       {6},
    "lien_ml":          {6},
    "lien_tcl":         {6},
    "lien_ccl":         {6},
    "lien_hoa":         {6},
    "lien_tl":          {6},
    "judgments":        {6},
    "sunbiz":           {6},
    "storm_damage":     {6},
    "fire_incidents":   {6},
    "flood_damage":     {6},
    "insurance_claims": {6},
    "tax_deed_outcomes": {6},   # tax-deed scraper/connector are Mon-Sat only
    # dor_sale_outcomes has no off-days (monthly cadence).
}

# Sources that run in more than one county. For these we check freshness
# per (source_type, county_id) instead of source-wide, because a source-wide
# MAX(created_at) lets one county's daily success mask another county dying
# (this is exactly how Pinellas violations went stale for 25 days unnoticed —
# Hillsborough's daily 'violations' success kept the aggregate fresh).
#
# Kept EXPLICIT (not derived from scraper_run_stats history): a (source, county)
# pair that dies long enough would drop out of the data and silently stop being
# watched — the very failure this map exists to prevent. Add the county here and
# it is watched even if it has never produced a row.
#
# Expansion: nearly every scraper in HEARTBEAT_SLAS runs both counties in cron.
# Enable a source below only once Pinellas is confirmed producing successful
# rows for it, so first-deploy doesn't flood ops with true-but-not-yet-actionable
# "pinellas stale" pages for sources whose Pinellas coverage is still being
# stood up. Confirm health with:
#   SELECT source_type, county_id, MAX(created_at) FILTER (WHERE run_success)
#   FROM scraper_run_stats WHERE created_at > now()-interval '10 days'
#   GROUP BY 1,2 ORDER BY 1,2;
# Seeded from a per-(source, county) freshness check of scraper_run_stats
# (2026-07-17): every source below was confirmed producing successful rows in
# BOTH counties within the last 10 days, so enabling per-county checks won't
# false-page on deploy. `violations`/pinellas is included as the fix target —
# it is stale now (Bug #1 blocked its stats row) but self-clears on the next
# daily run once source_type='violations' records correctly.
#
# A source is added here only once BOTH counties are confirmed producing
# successful rows for it, so per-county checks never false-page on deploy.
# Sources not yet listed stay source-wide until their per-county health is
# confirmed (e.g. sunbiz, whose run-health verdict is fixed under a separate
# task, gets added here once that lands).
MULTI_COUNTY_SOURCES: Dict[str, set] = {
    source: {"hillsborough", "pinellas"}
    for source in (
        "violations",
        "foreclosures",
        "permits",
        "roofing_permits",
        "probate",
        "evictions",
        "divorce_filings",
        "bankruptcy",
        "lien_ml",
        "lien_tcl",
        "lien_ccl",
        "lien_hoa",
        "lien_tl",
        "storm_damage",
        "fire_incidents",
        "flood_damage",
        "insurance_claims",
    )
}

_WEEKDAY_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

# County used in scraper_alert_log for source-wide (non-multi-county) beats.
_DEFAULT_ALERT_COUNTY = "hillsborough"

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
    county_id: Optional[str] = None  # set for multi-county sources; None = source-wide

    def label(self) -> str:
        """Human/dedup label: 'violations/pinellas' for county beats, else 'violations'."""
        return f"{self.source_type}/{self.county_id}" if self.county_id else self.source_type

    def alert_county(self) -> str:
        """County to key scraper_alert_log on (NOT NULL column)."""
        return self.county_id or _DEFAULT_ALERT_COUNTY

    def alert_subject(self) -> str:
        return f"[FA][HEARTBEAT] {self.label()} stale — no run in {self.age_label()}"

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
            f"Source:              {self.label()}\n"
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

def _beat_for(session, source_type: str, sla_minutes: int, county_id: Optional[str],
              now: datetime) -> Heartbeat:
    """Build one Heartbeat from the last successful run of (source_type[, county])."""
    query = session.query(func.max(ScraperRunStats.created_at)).filter(
        ScraperRunStats.source_type == source_type,
        ScraperRunStats.run_success.is_(True),
    )
    if county_id is not None:
        query = query.filter(ScraperRunStats.county_id == county_id)
    last_success = query.scalar()

    if last_success is None:
        return Heartbeat(
            source_type=source_type, sla_minutes=sla_minutes,
            last_success_at=None, age_minutes=None, is_stale=True,
            county_id=county_id,
        )

    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=timezone.utc)
    age = int((now - last_success).total_seconds() // 60)
    return Heartbeat(
        source_type=source_type, sla_minutes=sla_minutes,
        last_success_at=last_success, age_minutes=age,
        is_stale=age > sla_minutes, county_id=county_id,
    )


def compute_heartbeats(now: Optional[datetime] = None) -> list[Heartbeat]:
    """Compute heartbeat status for every source in HEARTBEAT_SLAS.

    Sources whose entry in SOURCE_OFF_DAYS includes today's weekday are skipped
    entirely — they're not expected to have run, so a "stale" alert would be
    a false positive.

    Sources listed in MULTI_COUNTY_SOURCES are checked once per expected county
    (a separate Heartbeat per county), so one county's success can't mask
    another county going stale.
    """
    now = now or datetime.now(timezone.utc)
    today_wd = now.weekday()
    out: list[Heartbeat] = []

    with get_db_context() as session:
        for source_type, sla_minutes in HEARTBEAT_SLAS.items():
            if today_wd in SOURCE_OFF_DAYS.get(source_type, set()):
                logger.info(
                    "[Heartbeat] %s skipped — %s is an off-day for this source",
                    source_type, _WEEKDAY_NAMES[today_wd],
                )
                continue

            counties = MULTI_COUNTY_SOURCES.get(source_type)
            if counties:
                for county_id in sorted(counties):
                    out.append(_beat_for(session, source_type, sla_minutes, county_id, now))
            else:
                out.append(_beat_for(session, source_type, sla_minutes, None, now))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Dedup
# ─────────────────────────────────────────────────────────────────────────────

def _recently_alerted(source_type: str, county_id: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=DEDUP_COOLDOWN_HOURS)
    try:
        with get_db_context() as session:
            row = (
                session.query(ScraperAlertLog)
                .filter(
                    ScraperAlertLog.source_type == source_type,
                    ScraperAlertLog.county_id == county_id,
                    ScraperAlertLog.alert_type == "heartbeat_missed",
                    ScraperAlertLog.alerted_at >= cutoff,
                )
                .first()
            )
            return row is not None
    except Exception as exc:
        logger.warning("[Heartbeat] dedup lookup failed (not suppressing): %s", exc)
        return False


def _record_alerted(source_type: str, county_id: str) -> None:
    try:
        with get_db_context() as session:
            session.add(ScraperAlertLog(
                source_type=source_type,
                county_id=county_id,
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
    recovered = [b for b in beats if not b.is_stale]
    if not recovered:
        return
    pair_conds = [
        and_(
            ScraperAlertLog.source_type == b.source_type,
            ScraperAlertLog.county_id == b.alert_county(),
        )
        for b in recovered
    ]
    try:
        with get_db_context() as session:
            deleted = (
                session.query(ScraperAlertLog)
                .filter(
                    or_(*pair_conds),
                    ScraperAlertLog.alert_type == "heartbeat_missed",
                )
                .delete(synchronize_session=False)
            )
            if deleted:
                logger.info(
                    "[Heartbeat] cleared %d stale dedup row(s) for recovered "
                    "source(s): %s",
                    deleted, ", ".join(b.label() for b in recovered),
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
        ", ".join(b.label() for b in stale) or "—",
    )

    # Wipe dedup rows for any source that's no longer stale — so the NEXT
    # failure gets an immediate alert instead of being suppressed by the
    # 24h cooldown left over from a previous incident.
    if not dry_run:
        _clear_dedup_for_recovered_sources(beats)

    for b in stale:
        if _recently_alerted(b.source_type, b.alert_county()):
            logger.info(
                "[Heartbeat] %s already alerted in the last %dh — skipping",
                b.label(), DEDUP_COOLDOWN_HOURS,
            )
            continue
        subject = b.alert_subject()
        body    = b.alert_body()
        if dry_run:
            logger.info("[Heartbeat][DRY] would send:\n%s\n\n%s", subject, body)
            continue
        try:
            sent = send_alert(subject, body)
            if sent:
                _record_alerted(b.source_type, b.alert_county())
                logger.info("[Heartbeat] ALERT SENT for %s (age=%s)", b.label(), b.age_label())
            else:
                logger.error(
                    "[Heartbeat] alert delivery returned False for %s; "
                    "dedup row NOT recorded so the next tick will retry",
                    b.label(),
                )
        except Exception as exc:
            logger.error("[Heartbeat] failed to send alert for %s: %s", b.label(), exc)
    return beats


def print_slas() -> None:
    print("\nHeartbeat SLA registry:\n")
    print(f"  {'Source':<22} {'SLA (min)':>10}  {'SLA (h)':>10}  {'Off-days':<20}")
    print(f"  {'-'*22}  {'-'*10}  {'-'*10}  {'-'*20}")
    for source, mins in HEARTBEAT_SLAS.items():
        off = SOURCE_OFF_DAYS.get(source, set())
        off_label = ",".join(_WEEKDAY_NAMES[d] for d in sorted(off)) if off else "—"
        print(f"  {source:<22} {mins:>10}  {mins/60:>10.1f}  {off_label:<20}")
    if MULTI_COUNTY_SOURCES:
        print("\nMulti-county sources (checked per county):")
        for source, counties in MULTI_COUNTY_SOURCES.items():
            print(f"  {source:<22} {', '.join(sorted(counties))}")
    print(f"\nDedup cooldown: {DEDUP_COOLDOWN_HOURS}h per (source, county) "
          "(auto-cleared when it recovers).")
    print("Sources are skipped on their off-days — no alerts fire for "
          "intentionally-not-scheduled days.")


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
    print(f"\nChecked {len(beats)} heartbeats:")
    for b in sorted(beats, key=lambda x: (not x.is_stale, x.label())):
        marker = "STALE" if b.is_stale else "OK   "
        print(f"  {marker}  {b.label():<28} age={b.age_label():<10} sla={b.sla_minutes/60:.0f}h")


if __name__ == "__main__":
    main()
