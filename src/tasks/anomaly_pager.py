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
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

from sqlalchemy import String, cast, func, text

from src.core.database import get_db_context
from src.core.models import (
    BuildingPermit,
    CodeViolation,
    Deed,
    Foreclosure,
    LegalAndLien,
    LegalProceeding,
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
# Content-quality rule config — duplicate-rate + field-coverage detectors.
# Each entry: source_name → (Model, unique_key_attr, [required_field_attrs]).
# Only sources with a stable per-source unique key are included; Incidents
# (fire/flood/storm/insurance) are intentionally excluded for v1 — they have
# only composite keys which produce false positives on duplicate detection.
# ─────────────────────────────────────────────────────────────────────────────
CONTENT_QUALITY_SOURCES = {
    "foreclosures":      (Foreclosure,     "case_number",       ["plaintiff", "auction_date", "case_status"]),
    "violations":        (CodeViolation,   "record_number",     ["violation_type", "description", "status"]),
    "permits":           (BuildingPermit,  "permit_number",     ["permit_type", "status"]),
    "liens":             (LegalAndLien,    "instrument_number", ["creditor", "debtor", "amount"]),
    "legal_proceedings": (LegalProceeding, "case_number",       ["associated_party", "case_status"]),
    "deeds":             (Deed,            "instrument_number", ["grantor", "grantee", "record_date", "sale_price"]),
}

# Days of the week each source is intentionally NOT scheduled (Python weekday:
# Mon=0, Sun=6). Mirrors heartbeat_monitor.SOURCE_OFF_DAYS. Skip the rule for
# that source on those days — no false positives when a M-Sat scraper is
# legitimately not running on Sunday. Foreclosures runs daily → empty set.
RULE_OFF_DAYS = {
    "foreclosures":      set(),
    "violations":        {6},
    "permits":           {6},
    "liens":             {6},
    "legal_proceedings": {6},
    "deeds":             {6},
}

# Duplicate-rate detection: defensive / kept-as-canary. The unique constraint
# on each source's key plus the pre-insert dedup in db_deduplicator.py means
# a key cannot legitimately appear on two date_added values, so in current
# production this rule never fires — its blind-spot is already covered by
# load_validator's zero-record check + anomaly_pager's volume_drop. Keep
# the constants here so the rule's behaviour stays tunable if a future
# scraper bypasses dedup or the schema changes. See full rationale in the
# docstring of _rule_scraper_duplicate_rate below.
DUPLICATE_RATE_THRESHOLD       = 0.95
DUPLICATE_RATE_MIN_TODAY_ROWS  = 5

# Field-coverage detection: alert if % of today's rows with a non-null/non-empty
# value for a required field drops sharply vs the 7-day baseline. Catches DOM
# extraction failures where one column silently becomes blank.
FIELD_COVERAGE_DROP_PP         = 30.0
FIELD_COVERAGE_MIN_ROWS        = 10
FIELD_COVERAGE_HISTORY_DAYS    = 7

# Filing-date freshness detection — closes the "scraper writes new rows but
# their event dates are stuck in the past" blind spot. For each source, the
# value below names the column that records when the event actually happened
# (court filing date, permit issue date, etc.) as distinct from `date_added`
# which records when the row landed in our DB. A healthy scraper sees
# MAX(<filing_col>) advance over time; a stuck one keeps writing rows whose
# filing dates anchor to an old window.
FILING_DATE_FRESHNESS = {
    "foreclosures":      "filing_date",
    "violations":        "opened_date",
    "permits":           "issue_date",
    "liens":             "filing_date",
    "legal_proceedings": "filing_date",
    "deeds":             "record_date",
}

# Trip if today's MAX(filing_date) is the same as or older than yesterday's
# MAX(filing_date), for N consecutive days. 1 day of "didn't advance" is
# normal (weekends, slow news days). 2+ days is the signal.
FILING_DATE_STALE_DAYS         = 2
FILING_DATE_MIN_TODAY_ROWS     = 5

# Soft-launch gate: while SHIP_CONTENT_QUALITY_ALERTS is 0 (default), trips
# from these new rules are LOGGED but not emailed and not recorded in
# scraper_alert_log. Flip to 1 in .env after a clean 1-week dry-run.
SOFT_LAUNCH_RULES = {
    "scraper_duplicate_rate_high",
    "scraper_field_coverage_drop",
    "scraper_filing_date_not_advancing",
}


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


def _rule_scraper_duplicate_rate(session, today: date) -> Iterable[Trip]:
    """% of today's rows whose unique key already existed yesterday is too high.

    DEFENSIVE / DEAD-CODE-IN-PRODUCTION:
    With the current data model — unique constraint on each source's primary
    key (foreclosures.case_number, code_violations.record_number, …) plus the
    pre-insert dedup in src/utils/db_deduplicator.py — a single key cannot
    exist on two different date_added values at the same time. So in
    production this rule's set-intersection is always empty and it never
    fires. The redundant failure modes are covered by:

      - load_validator zero-record alert
        (broken scraper writes no new INSERTs → today's row count drops to 0)
      - anomaly_pager.scraper_volume_drop_50pct
        (broken scraper produces only a fraction of usual new records)

    The rule is kept as defense-in-depth: if a future scraper bypasses
    dedup, or the schema relaxes its unique constraint, or upsert semantics
    change to copy rows across dates, this rule will catch the regression
    before stale leads start scoring. Until then it's a no-op.

    Mocked unit tests in tests/test_anomaly_pager_content_quality.py verify
    the set-intersection logic itself is correct (TestDuplicateRateRuleMocked).
    """
    yesterday = today - timedelta(days=1)
    weekday = today.weekday()

    for source, (model, key_attr, _fields) in CONTENT_QUALITY_SOURCES.items():
        if weekday in RULE_OFF_DAYS.get(source, set()):
            continue

        key_col = getattr(model, key_attr)
        date_col = model.date_added

        today_keys = {
            row[0] for row in session.query(key_col).filter(date_col == today).all()
            if row[0] is not None
        }
        if len(today_keys) < DUPLICATE_RATE_MIN_TODAY_ROWS:
            continue

        yesterday_keys = {
            row[0] for row in session.query(key_col).filter(date_col == yesterday).all()
            if row[0] is not None
        }
        if not yesterday_keys:
            continue  # nothing to compare against — yesterday was empty or off-day

        dup_count = len(today_keys & yesterday_keys)
        dup_rate = dup_count / len(today_keys)

        if dup_rate >= DUPLICATE_RATE_THRESHOLD:
            sample = sorted(today_keys & yesterday_keys)[:3]
            yield Trip(
                rule="scraper_duplicate_rate_high",
                observed=f"{source}: {dup_rate*100:.1f}% of today's batch ({dup_count}/{len(today_keys)}) existed yesterday",
                baseline="<5% normal carry-over",
                threshold=f">= {DUPLICATE_RATE_THRESHOLD*100:.0f}% repeats vs yesterday",
                context={
                    "source":            source,
                    "today_count":       len(today_keys),
                    "yesterday_count":   len(yesterday_keys),
                    "duplicate_count":   dup_count,
                    "duplicate_rate":    round(dup_rate, 3),
                    "duplicate_sample":  sample,
                    "date":              str(today),
                },
            )


def _rule_scraper_field_coverage_drop(session, today: date) -> Iterable[Trip]:
    """% of today's rows with a non-null value for a required field dropped
    sharply vs the 7-day baseline.

    Catches HTML scraper degradation where one column silently goes blank
    (e.g. plaintiff field empty after a DOM change). Row count stays normal
    so volume rules don't catch it; only content does.
    """
    weekday = today.weekday()

    for source, (model, _key_attr, fields) in CONTENT_QUALITY_SOURCES.items():
        if weekday in RULE_OFF_DAYS.get(source, set()):
            continue

        date_col = model.date_added
        total_today = session.query(func.count()).select_from(model).filter(
            date_col == today
        ).scalar() or 0
        if total_today < FIELD_COVERAGE_MIN_ROWS:
            continue

        for field_name in fields:
            field_col = getattr(model, field_name)

            today_pct = _field_coverage_pct(session, model, field_col, date_col, today, total_today)

            # Baseline: per-day coverage % over the previous N days. Need >=3 days.
            baseline_pcts = []
            for day_offset in range(1, FIELD_COVERAGE_HISTORY_DAYS + 1):
                d = today - timedelta(days=day_offset)
                day_total = session.query(func.count()).select_from(model).filter(
                    date_col == d
                ).scalar() or 0
                if day_total < FIELD_COVERAGE_MIN_ROWS:
                    continue
                baseline_pcts.append(_field_coverage_pct(session, model, field_col, date_col, d, day_total))
            if len(baseline_pcts) < 3:
                continue

            baseline_pct = sum(baseline_pcts) / len(baseline_pcts)
            delta_pp = baseline_pct - today_pct

            if delta_pp > FIELD_COVERAGE_DROP_PP:
                yield Trip(
                    rule="scraper_field_coverage_drop",
                    observed=f"{source}.{field_name}: {today_pct:.1f}% non-null today",
                    baseline=f"{baseline_pct:.1f}% non-null (avg of last {len(baseline_pcts)} days)",
                    threshold=f"> {FIELD_COVERAGE_DROP_PP:.0f}pp drop",
                    context={
                        "source":         source,
                        "field":          field_name,
                        "total_today":    total_today,
                        "today_pct":      round(today_pct, 1),
                        "baseline_pct":   round(baseline_pct, 1),
                        "delta_pp":       round(delta_pp, 1),
                        "history_days":   len(baseline_pcts),
                        "date":           str(today),
                    },
                )


def _field_coverage_pct(session, model, field_col, date_col, target_date: date, total: int) -> float:
    """Returns % of rows on target_date where field_col is non-null and non-empty.

    For string columns, also rejects whitespace-only values — that's what a
    failed-extraction empty span looks like in the DB. For Numeric/Date columns
    the cast-to-string still works since length() on a stringified non-null
    value is always > 0 (so the trim/length is a no-op there).
    """
    non_null = session.query(func.count()).select_from(model).filter(
        date_col == target_date,
        field_col.isnot(None),
        func.length(func.trim(cast(field_col, String))) > 0,
    ).scalar() or 0
    return 100.0 * non_null / total if total else 0.0


def _rule_scraper_filing_date_not_advancing(session, today: date) -> Iterable[Trip]:
    """MAX(<event-date column>) for today's batch is not newer than yesterday's.

    Closes the "stale event-date" blind spot: scraper writes new rows
    (unique keys, populated fields, normal volume) but the underlying
    event dates — filing_date, opened_date, issue_date — are anchored
    in the past. Court systems file new cases continuously; a healthy
    scraper sees the max advance over time. A stuck one doesn't.

    Logic: if MAX(filing_col) on today's batch is <= MAX(filing_col)
    on each of the previous FILING_DATE_STALE_DAYS batches, fire.
    Requires at least FILING_DATE_MIN_TODAY_ROWS rows today so a slow
    Saturday doesn't trip the rule.
    """
    weekday = today.weekday()

    for source, filing_col_name in FILING_DATE_FRESHNESS.items():
        if weekday in RULE_OFF_DAYS.get(source, set()):
            continue
        if source not in CONTENT_QUALITY_SOURCES:
            continue
        model, _key_attr, _fields = CONTENT_QUALITY_SOURCES[source]
        filing_col = getattr(model, filing_col_name)
        date_col = model.date_added

        # Skip tiny batches — a few rows can naturally have the same MAX.
        total_today = session.query(func.count()).select_from(model).filter(
            date_col == today
        ).scalar() or 0
        if total_today < FILING_DATE_MIN_TODAY_ROWS:
            continue

        today_max = session.query(func.max(filing_col)).filter(date_col == today).scalar()
        if today_max is None:
            continue  # all rows have NULL filing date — field-coverage rule's domain

        # Compare against each of the previous N days. Only fires if today's
        # max is NOT strictly greater than ANY of them — i.e. the scraper
        # has been stuck for FILING_DATE_STALE_DAYS days running.
        prior_maxes = []
        for day_offset in range(1, FILING_DATE_STALE_DAYS + 1):
            d = today - timedelta(days=day_offset)
            d_total = session.query(func.count()).select_from(model).filter(
                date_col == d
            ).scalar() or 0
            if d_total < FILING_DATE_MIN_TODAY_ROWS:
                continue  # weekend / off-day with thin data — skip comparison
            d_max = session.query(func.max(filing_col)).filter(date_col == d).scalar()
            if d_max is not None:
                prior_maxes.append((d, d_max))

        if not prior_maxes:
            continue  # no useful baseline

        # If today's max strictly exceeds at least one prior day's max,
        # we ARE advancing. Only fire if today fails to beat EVERY prior.
        if any(today_max > pm for _, pm in prior_maxes):
            continue

        yield Trip(
            rule="scraper_filing_date_not_advancing",
            observed=f"{source}.{filing_col_name}: today's max={today_max}",
            baseline=", ".join(f"{d}={m}" for d, m in prior_maxes),
            threshold=f"today's max must exceed prior {FILING_DATE_STALE_DAYS} days",
            context={
                "source":           source,
                "filing_column":    filing_col_name,
                "today_count":      total_today,
                "today_max":        str(today_max),
                "prior_maxes":      [(str(d), str(m)) for d, m in prior_maxes],
                "date":             str(today),
            },
        )


_RULES = (
    _rule_scraper_volume_drop,
    _rule_gold_plus_volume_drop,
    _rule_gold_plus_phone_coverage_drop,
    _rule_cds_no_run_today,
    _rule_scraper_duplicate_rate,
    _rule_scraper_field_coverage_drop,
    _rule_scraper_filing_date_not_advancing,
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


def _is_soft_launched(rule: str) -> bool:
    """Soft-launch rules log-only until SHIP_CONTENT_QUALITY_ALERTS=1 in env.
    Trips still evaluate (visible in --dry-run + cron logs) but no email is sent
    and no dedup row is written, so flipping the flag later doesn't suppress
    the first real alert via stale cooldown.
    """
    if rule not in SOFT_LAUNCH_RULES:
        return False
    return os.getenv("SHIP_CONTENT_QUALITY_ALERTS", "0").strip() not in ("1", "true", "True")


def run_and_page(today: Optional[date] = None, county_id: str = "hillsborough", dry_run: bool = False) -> list[Trip]:
    """Run rules and send paging emails for any trips not recently sent."""
    trips = evaluate(today)
    if not trips:
        logger.info("[anomaly_pager] no anomalies tripped")
        return trips

    with get_db_context() as session:
        for trip in trips:
            if _is_soft_launched(trip.rule):
                logger.info(
                    "[anomaly_pager][SOFT-LAUNCH] %s would have fired: %s",
                    trip.rule, trip.context,
                )
                continue
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
    ship_content = os.getenv("SHIP_CONTENT_QUALITY_ALERTS", "0").strip() in ("1", "true", "True")
    print("anomaly_pager — rule catalog:\n")
    for fn in _RULES:
        name = fn.__name__.removeprefix("_rule_")
        doc = (fn.__doc__ or "").strip().split("\n")[0]
        # Map fn name → trip's `rule` string to flag soft-launch state.
        marker = ""
        fn_to_trip = {
            "scraper_duplicate_rate":            "scraper_duplicate_rate_high",
            "scraper_field_coverage_drop":       "scraper_field_coverage_drop",
            "scraper_filing_date_not_advancing": "scraper_filing_date_not_advancing",
        }
        if name in fn_to_trip:
            marker = "  [LIVE]" if ship_content else "  [SOFT-LAUNCH — log-only]"
        print(f"  {name:<32}  {doc}{marker}")
    print(f"\nDedup window: {ALERT_DEDUP_WINDOW_HOURS} hours per rule")
    print(f"Soft-launch flag: SHIP_CONTENT_QUALITY_ALERTS={'1 (live)' if ship_content else '0 (log-only)'}")
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
