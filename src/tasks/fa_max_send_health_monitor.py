"""
FA Max send-infrastructure health monitor (WP-T2-1).

WP-T2-1's own Done-When line is "sustained inbox placement above 95% at
target volume" — genuine inbox-placement measurement requires a seed-list
provider (e.g. GlockApps, 250ok). No such integration exists in this repo,
and SOT.md Part 6's integration inventory does not list one. This monitor
does NOT claim to measure placement; it is a proxy built from what the
existing stack actually observes:

  - Instantly warmup health score for the FA Max passthrough campaign's
    connected mailbox(es) (same signal email_deliverability_monitor.py
    already uses for the rest of the platform).
  - Relay's own observed bounce/skip rate for 'sent' vs 'failed'/'skipped'
    FA Max items over the trailing window (src.services.relay.queue).

True placement measurement is a live-lane blocker, not a code gap — it
needs a client/vendor decision on a seed-list provider before it can be
built at all. Do not wire this monitor's output into a "Done" claim for the
95% placement criterion.

Alerts post to the FA Max EXCEPTIONS Slack lane (WP-2's queue), with the
same durable dedup this repo already uses for scraper ops alerts
(ScraperAlertLog, ALERT_DEDUP_WINDOW_HOURS) — a page is only recorded as
delivered if post_exceptions_alert() actually succeeded, so a Slack outage
leaves the trip eligible for the next run rather than silently dropped.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services import instantly_service
from src.services.relay.slack_post import post_exceptions_alert
from src.utils.venture_config import get_venture_config

logger = logging.getLogger(__name__)

FA_MAX_VENTURE = "fa_max_lending"
WARMUP_SCORE_FLOOR = 70
ROLLING_HOURS = 24
MIN_SENT_FLOOR = 10
FAILURE_RATE_CONCERN = 0.10
ALERT_DEDUP_WINDOW_HOURS = 12


@dataclass
class Trip:
    rule: str
    detail: str


def _warmup_trip(campaign_id: Optional[str]) -> Optional[Trip]:
    if not campaign_id:
        return None
    accounts = instantly_service.list_accounts()
    emails = [a.get("email") for a in accounts if a.get("email")]
    if not emails:
        return None
    warmup_by_email = {
        item.get("email"): item
        for item in instantly_service.get_warmup_analytics(emails)
        if item.get("email")
    }
    unhealthy = [
        email for email in emails
        if int((warmup_by_email.get(email) or {}).get("health_score") or 0) < WARMUP_SCORE_FLOOR
    ]
    if not unhealthy:
        return None
    return Trip(
        rule="fa_max_warmup_score_low",
        detail=f"Mailbox(es) below warmup floor {WARMUP_SCORE_FLOOR}: {', '.join(unhealthy)} "
               f"(proxy only — not a placement measurement, see module docstring)",
    )


def _relay_failure_trip(session, now: datetime) -> Optional[Trip]:
    since = now - timedelta(hours=ROLLING_HOURS)
    row = session.execute(
        text(
            "SELECT "
            "  count(*) FILTER (WHERE status = 'sent') AS sent, "
            "  count(*) FILTER (WHERE status IN ('failed', 'skipped')) AS failed "
            "FROM relay_approval_queue "
            "WHERE venture_key = :venture AND updated_at >= :since"
        ),
        {"venture": FA_MAX_VENTURE, "since": since},
    ).mappings().one()
    sent, failed = int(row["sent"] or 0), int(row["failed"] or 0)
    total = sent + failed
    if total < MIN_SENT_FLOOR:
        return None
    rate = failed / total
    if rate <= FAILURE_RATE_CONCERN:
        return None
    return Trip(
        rule="fa_max_relay_failure_rate_high",
        detail=f"{failed}/{total} FA Max relay items failed or were blocked in the "
               f"trailing {ROLLING_HOURS}h ({rate:.1%})",
    )


def evaluate(*, now: Optional[datetime] = None) -> list[Trip]:
    now = now or datetime.now(timezone.utc)
    trips: list[Trip] = []
    venture = get_venture_config(FA_MAX_VENTURE)
    trips.append(_warmup_trip(venture.relay_instantly_campaign_id))
    with get_db_context() as session:
        trips.append(_relay_failure_trip(session, now))
    return [t for t in trips if t is not None]


def _recently_paged(session, rule: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ALERT_DEDUP_WINDOW_HOURS)
    existing = session.execute(
        text(
            "SELECT id FROM scraper_alert_log "
            "WHERE alert_type = :rule AND county_id = :venture AND alerted_at >= :cutoff LIMIT 1"
        ),
        {"rule": rule, "venture": FA_MAX_VENTURE, "cutoff": cutoff},
    ).first()
    return existing is not None


def _record_paged(session, rule: str) -> None:
    from src.core.models import ScraperAlertLog
    session.add(ScraperAlertLog(source_type=rule, county_id=FA_MAX_VENTURE, alert_type=rule))


def run_and_page(*, dry_run: bool = False) -> list[Trip]:
    trips = evaluate()
    if not trips:
        logger.info("[fa_max_send_health] no trips")
        return []

    with get_db_context() as session:
        for trip in trips:
            if _recently_paged(session, trip.rule):
                logger.info("[fa_max_send_health] %s already paged in last %dh", trip.rule, ALERT_DEDUP_WINDOW_HOURS)
                continue
            if dry_run:
                logger.info("[fa_max_send_health][DRY] would page %s: %s", trip.rule, trip.detail)
                continue
            if post_exceptions_alert(venture_key=FA_MAX_VENTURE, rule=trip.rule, message=trip.detail):
                _record_paged(session, trip.rule)
            else:
                logger.error("[fa_max_send_health] alert for %s NOT delivered — left eligible for next run", trip.rule)
        session.commit()
    return trips


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FA Max send-infrastructure health monitor")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    fired = run_and_page(dry_run=args.dry_run)
    logger.info("fa_max_send_health_monitor: %d rule(s) tripped", len(fired))
