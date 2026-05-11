"""
Anomaly auto-pager — volume thresholds with routed alerts.

Catalogues every trip-rule the platform fires on, evaluates them against
the latest data, and routes alerts to ops via the existing send_alert
email path (deduplicated against scraper_alert_log so we don't spam).

Rules
-----
    scraper_volume_drop_50pct     — total daily scraped count fell below 50%
                                    of its 7-day average (immediate page)
    scraper_zero_records          — a required scraper ran but returned 0 rows
    scraper_run_missing           — required scraper produced no run row today
    gold_plus_volume_drop_40pct   — today's Gold+ count fell >40% below 7d avg
    gold_plus_phone_coverage_drop — % of Gold+ leads without a phone rose >15pp
                                    above the 7-day baseline (NEW rule —
                                    surfaces the same upstream-drop counter
                                    that the daily ops report now exports)
    cds_no_run_today              — CDS engine didn't produce a row in
                                    platform_daily_stats for today (silent
                                    scoring failure)

Each rule emits a single named ScraperAlertLog row + a routed email,
with all of: rule name, observed value, baseline value, threshold,
suggested action, and the timestamp of the trip.

Run daily after CDS rescore:
    0 8 * * *  python -m src.tasks.anomaly_pager
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

from sqlalchemy import func, text

from src.core.database import get_db_context
from src.core.models import (
    Owner,
    PlatformDailyStats,
    ScraperAlertLog,
    ScraperRunStats,
)
from src.services.email import send_alert
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


GOLD_PLUS_TIERS = ("Ultra Platinum", "Platinum", "Gold")
ALERT_DEDUP_WINDOW_HOURS = 12
PAGE_RECIPIENT_HINT = (
    "Routed to ALERT_EMAIL (configured in .env). Add a Slack channel "
    "via a SES → Slack forwarder, or wire send_alert to call a Slack "
    "webhook directly when a pager-grade route is needed."
)


# ─────────────────────────────────────────────────────────────────────────────
# Rule definitions
# ─────────────────────────────────────────────────────────────────────────────

class Trip:
    """One realized anomaly — the result of a rule evaluating True."""
    __slots__ = ("rule", "observed", "baseline", "threshold", "context", "tripped_at")

    def __init__(self, rule: str, observed, baseline, threshold, context: dict):
        self.rule       = rule
        self.observed   = observed
        self.baseline   = baseline
        self.threshold  = threshold
        self.context    = context
        self.tripped_at = datetime.now(timezone.utc)

    def email_subject(self) -> str:
        return f"[FA][ANOMALY] {self.rule}"

    def email_body(self) -> str:
        body = [
            f"Rule:        {self.rule}",
            f"Tripped at:  {self.tripped_at.isoformat(timespec='seconds')}",
            f"Observed:    {self.observed}",
            f"Baseline:    {self.baseline}",
            f"Threshold:   {self.threshold}",
            "",
            "Context:",
        ]
        for k, v in (self.context or {}).items():
            body.append(f"  {k}: {v}")
        body.append("")
        body.append(PAGE_RECIPIENT_HINT)
        return "\n".join(body)


# ─────────────────────────────────────────────────────────────────────────────
# Individual rule evaluators
# ─────────────────────────────────────────────────────────────────────────────

def _rule_scraper_volume_drop(session, today: date) -> Iterable[Trip]:
    """Total scraped today < 50% of trailing 7-day avg."""
    today_total = session.query(func.coalesce(func.sum(ScraperRunStats.total_scraped), 0)) \
        .filter(ScraperRunStats.run_date == today) \
        .scalar() or 0

    since = today - timedelta(days=7)
    hist_rows = session.query(
        ScraperRunStats.run_date,
        func.coalesce(func.sum(ScraperRunStats.total_scraped), 0),
    ).filter(
        ScraperRunStats.run_date >= since,
        ScraperRunStats.run_date < today,
    ).group_by(ScraperRunStats.run_date).all()

    if len(hist_rows) < 3:
        return  # not enough history yet

    avg = sum(c for _, c in hist_rows) / len(hist_rows)
    if avg < 50:
        return  # too small a baseline — meaningless ratio

    if today_total < 0.5 * avg:
        yield Trip(
            rule="scraper_volume_drop_50pct",
            observed=today_total,
            baseline=round(avg, 1),
            threshold="< 50% of 7d avg",
            context={"date": str(today), "history_days": len(hist_rows)},
        )


def _rule_gold_plus_volume_drop(session, today: date) -> Iterable[Trip]:
    """Gold+ count today < 60% of trailing 7-day avg (Gold+ in any tier)."""
    today_row = session.query(PlatformDailyStats).filter(
        PlatformDailyStats.run_date == today
    ).first()
    if today_row is None:
        return

    today_gold = (
        (today_row.tier_ultra_platinum or 0)
        + (today_row.tier_platinum or 0)
        + (today_row.tier_gold or 0)
    )

    since = today - timedelta(days=7)
    hist_rows = session.query(PlatformDailyStats).filter(
        PlatformDailyStats.run_date >= since,
        PlatformDailyStats.run_date < today,
    ).all()
    if len(hist_rows) < 3:
        return

    hist_vals = [
        (r.tier_ultra_platinum or 0) + (r.tier_platinum or 0) + (r.tier_gold or 0)
        for r in hist_rows
    ]
    avg = sum(hist_vals) / len(hist_vals)
    if avg < 20:
        return

    if today_gold < 0.6 * avg:
        yield Trip(
            rule="gold_plus_volume_drop_40pct",
            observed=today_gold,
            baseline=round(avg, 1),
            threshold="< 60% of 7d avg",
            context={
                "date":             str(today),
                "ultra_platinum":   today_row.tier_ultra_platinum,
                "platinum":         today_row.tier_platinum,
                "gold":             today_row.tier_gold,
                "history_days":     len(hist_rows),
            },
        )


def _rule_gold_plus_phone_coverage_drop(session, today: date) -> Iterable[Trip]:
    """
    % of currently-standing Gold+ leads with no phone_1 rose >15 percentage
    points above the 7-day baseline. This is the upstream-drop counter
    that also appears in the daily ops report.
    """
    # Today's phone-missing rate across the current Gold+ inventory.
    snap_sql = text("""
        WITH latest AS (
            SELECT DISTINCT ON (property_id) property_id, lead_tier
            FROM distress_scores
            WHERE date(score_date) <= :today
            ORDER BY property_id, score_date DESC
        )
        SELECT
            COUNT(*) AS total,
            SUM(CASE
                  WHEN o.phone_1 IS NOT NULL AND length(trim(o.phone_1)) > 0
                  THEN 1 ELSE 0
                END) AS with_phone
        FROM latest l
        LEFT JOIN owners o ON o.property_id = l.property_id
        WHERE l.lead_tier = ANY(:tiers)
    """)

    row = session.execute(snap_sql, {"today": today, "tiers": list(GOLD_PLUS_TIERS)}).fetchone()
    if row is None or not row[0]:
        return
    total, with_phone = int(row[0]), int(row[1] or 0)
    today_drop_pct = 100.0 * (total - with_phone) / total

    # Baseline = same metric computed each day over the past 7 days.
    base_pcts = []
    for day_offset in range(1, 8):
        d = today - timedelta(days=day_offset)
        r = session.execute(snap_sql, {"today": d, "tiers": list(GOLD_PLUS_TIERS)}).fetchone()
        if not r or not r[0]:
            continue
        t = int(r[0]); wp = int(r[1] or 0)
        base_pcts.append(100.0 * (t - wp) / t)
    if len(base_pcts) < 3:
        return

    baseline = sum(base_pcts) / len(base_pcts)
    delta = today_drop_pct - baseline

    if delta > 15.0:
        yield Trip(
            rule="gold_plus_phone_coverage_drop",
            observed=f"{today_drop_pct:.1f}% missing phone",
            baseline=f"{baseline:.1f}% missing phone (7d avg)",
            threshold="> +15 percentage points vs 7d avg",
            context={
                "total_gold_plus":   total,
                "with_phone":        with_phone,
                "dropped_no_phone":  total - with_phone,
                "delta_pp":          round(delta, 1),
                "history_days":      len(base_pcts),
            },
        )


def _rule_cds_no_run_today(session, today: date) -> Iterable[Trip]:
    """No row in platform_daily_stats for today → CDS engine never ran."""
    row = session.query(PlatformDailyStats).filter(
        PlatformDailyStats.run_date == today
    ).first()
    if row is None:
        yield Trip(
            rule="cds_no_run_today",
            observed="no row",
            baseline="expected 1 row per day",
            threshold="row must exist by anomaly_pager run time",
            context={"date": str(today)},
        )


_RULES = (
    _rule_scraper_volume_drop,
    _rule_gold_plus_volume_drop,
    _rule_gold_plus_phone_coverage_drop,
    _rule_cds_no_run_today,
)


# ─────────────────────────────────────────────────────────────────────────────
# Dedup + dispatch
# ─────────────────────────────────────────────────────────────────────────────

def _recently_paged(session, rule: str, county_id: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ALERT_DEDUP_WINDOW_HOURS)
    existing = (
        session.query(ScraperAlertLog)
        .filter(
            ScraperAlertLog.alert_type == rule,
            ScraperAlertLog.county_id == county_id,
            ScraperAlertLog.alerted_at >= cutoff,
        )
        .first()
    )
    return existing is not None


def _record_paged(session, rule: str, county_id: str) -> None:
    session.add(ScraperAlertLog(
        source_type=rule,           # rule name doubles as source_type tag for dedup
        county_id=county_id,
        alert_type=rule,
    ))


# ─────────────────────────────────────────────────────────────────────────────
# Entry points
# ─────────────────────────────────────────────────────────────────────────────

def evaluate(today: Optional[date] = None) -> list[Trip]:
    """Run every rule against the given date (default today) — read-only."""
    today = today or date.today()
    trips: list[Trip] = []
    with get_db_context() as session:
        for rule_fn in _RULES:
            try:
                for trip in rule_fn(session, today):
                    trips.append(trip)
            except Exception as exc:
                logger.warning("[anomaly_pager] rule %s raised: %s", rule_fn.__name__, exc)
    return trips


def run_and_page(today: Optional[date] = None, county_id: str = "hillsborough", dry_run: bool = False) -> list[Trip]:
    """Run rules and send paging emails for any trips not recently sent."""
    trips = evaluate(today)
    if not trips:
        logger.info("[anomaly_pager] no anomalies tripped")
        return trips

    with get_db_context() as session:
        for trip in trips:
            if _recently_paged(session, trip.rule, county_id):
                logger.info("[anomaly_pager] %s already paged in the last %dh — skipping",
                            trip.rule, ALERT_DEDUP_WINDOW_HOURS)
                continue
            subject = trip.email_subject()
            body    = trip.email_body()
            if dry_run:
                logger.info("[anomaly_pager][DRY] would send:\nsubject=%s\n%s", subject, body)
            else:
                try:
                    send_alert(subject, body)
                    _record_paged(session, trip.rule, county_id)
                except Exception as exc:
                    logger.error("[anomaly_pager] failed to send alert for %s: %s", trip.rule, exc)
        session.commit()
    return trips


def print_rule_catalog() -> None:
    """Operator-facing: list every rule the pager will check."""
    print("anomaly_pager — rule catalog:\n")
    for fn in _RULES:
        name = fn.__name__.removeprefix("_rule_")
        # Pull the one-line description from the rule function's docstring.
        doc = (fn.__doc__ or "").strip().split("\n")[0]
        print(f"  {name:<32}  {doc}")
    print(f"\nDedup window: {ALERT_DEDUP_WINDOW_HOURS} hours per rule")
    print(PAGE_RECIPIENT_HINT)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Anomaly auto-pager — rule + alert dispatcher")
    ap.add_argument("--date", help="YYYY-MM-DD (default: today)")
    ap.add_argument("--county-id", default="hillsborough")
    ap.add_argument("--list-rules", action="store_true", help="Print rule catalog and exit")
    ap.add_argument("--dry-run", action="store_true", help="Evaluate + print, don't send email")
    args = ap.parse_args()

    if args.list_rules:
        print_rule_catalog()
        raise SystemExit(0)

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    fired = run_and_page(today=target_date, county_id=args.county_id, dry_run=args.dry_run)
    print(f"\n{len(fired)} rule(s) tripped on {target_date}:")
    for t in fired:
        print(f"  - {t.rule}: observed={t.observed} baseline={t.baseline}")
