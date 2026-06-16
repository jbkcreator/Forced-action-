"""
reactivation_eligibility — Subscriber lifecycle predicates and reactivation gates.

Corrected definitions (S0):

dormant
    No user-initiated activity (reply, click, wallet debit, deal outcome) in N days.
    Distinct from outbound contact recency — a subscriber can receive daily emails
    and still be "dormant" if they never click, reply, or spend.

past_subscriber
    churned_at IS NOT NULL — proof the subscriber held an active paid plan at some
    point regardless of current status. Status alone is insufficient: a subscriber
    could be status='churned' from a failed trial with no real paid history.

unconverted
    is_trial=True AND the trial has lapsed (status != 'active' OR trial_ends_at past).

not_recently_contacted
    message_outcomes.sent_at — the last time *we* sent them something (outbound).
    Tracked separately from dormant to prevent over-messaging recently-contacted
    subscribers who happen to be inactive.

on_cooldown
    last_reactivation_attempt_at within the cooldown window. Prevents re-sending
    reactivation messages to the same subscriber in quick succession.

sold-out ZIP supply gate
    New Gold+ leads in the ZIP that are (a) not under active exclusivity lock for
    this vertical AND (b) not recorded as sold in lead_quality_snapshots.
    Uses gold_plus_zip_snapshots as a fast pre-filter; falls back to a direct
    distress_scores query if the snapshot is stale or absent.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import Subscriber

logger = logging.getLogger(__name__)

DORMANT_DAYS = 30
CONTACT_COOLDOWN_DAYS = 14
REACTIVATION_COOLDOWN_DAYS = 3
SUPPLY_STALENESS_DAYS = 1


# ── Lifecycle predicates ──────────────────────────────────────────────────────

def is_dormant(subscriber_id: int, db: Session, inactive_days: int = DORMANT_DAYS) -> bool:
    """True when the subscriber has had no user-side engagement for inactive_days days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=inactive_days)
    row = db.execute(text("""
        SELECT 1 WHERE
            EXISTS (
                SELECT 1 FROM message_outcomes
                WHERE subscriber_id = :sid
                  AND (replied_at >= :cutoff OR clicked_at >= :cutoff)
            )
            OR EXISTS (
                SELECT 1 FROM wallet_transactions
                WHERE subscriber_id = :sid
                  AND txn_type = 'debit'
                  AND created_at >= :cutoff
            )
            OR EXISTS (
                SELECT 1 FROM deal_outcomes
                WHERE subscriber_id = :sid
                  AND created_at >= :cutoff
            )
    """), {"sid": subscriber_id, "cutoff": cutoff}).first()
    return row is None


def last_outbound_at(subscriber_id: int, db: Session) -> Optional[datetime]:
    """Most recent time we sent an outbound message to this subscriber."""
    row = db.execute(text("""
        SELECT MAX(sent_at) FROM message_outcomes WHERE subscriber_id = :sid
    """), {"sid": subscriber_id}).first()
    if not row or row[0] is None:
        return None
    ts = row[0]
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def is_recently_contacted(
    subscriber_id: int,
    db: Session,
    within_days: int = CONTACT_COOLDOWN_DAYS,
) -> bool:
    """True if we sent at least one outbound message within within_days days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=within_days)
    row = db.execute(text("""
        SELECT 1 FROM message_outcomes
        WHERE subscriber_id = :sid AND sent_at >= :cutoff
        LIMIT 1
    """), {"sid": subscriber_id, "cutoff": cutoff}).first()
    return row is not None


def is_past_subscriber(subscriber: Subscriber) -> bool:
    """True if the subscriber has a recorded paid history (churned_at IS NOT NULL)."""
    return subscriber.churned_at is not None


def is_unconverted(subscriber: Subscriber) -> bool:
    """True if subscriber started a trial but never converted to an active paid plan."""
    if not subscriber.is_trial:
        return False
    if subscriber.status == "active":
        return False
    if subscriber.trial_ends_at is None:
        return True
    now = datetime.now(timezone.utc)
    trial_end = subscriber.trial_ends_at
    if trial_end.tzinfo is None:
        trial_end = trial_end.replace(tzinfo=timezone.utc)
    return trial_end <= now


def on_cooldown(
    subscriber: Subscriber,
    cooldown_days: int = REACTIVATION_COOLDOWN_DAYS,
) -> bool:
    """True if a reactivation was attempted within the cooldown window."""
    if subscriber.last_reactivation_attempt_at is None:
        return False
    last = subscriber.last_reactivation_attempt_at
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return last > datetime.now(timezone.utc) - timedelta(days=cooldown_days)


# ── Supply gate ───────────────────────────────────────────────────────────────

def _has_sold_out_zip_supply(zip_code: str, county_id: str, vertical: str, db: Session) -> bool:
    """
    True when the ZIP has new Gold+ leads that are unassigned, unlocked (for this
    vertical), and not recorded as sold.

    Uses gold_plus_zip_snapshots as a fast pre-filter to skip the heavier direct
    query when no leads scored today. Falls back to the direct query when the
    snapshot is absent or stale.
    """
    cutoff = date.today() - timedelta(days=SUPPLY_STALENESS_DAYS)
    snap = db.execute(text("""
        SELECT gold_plus_lead_count
        FROM gold_plus_zip_snapshots
        WHERE zip_code    = :zip
          AND county_id   = :county
          AND snapshot_date >= :cutoff
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"zip": zip_code, "county": county_id, "cutoff": cutoff}).first()

    if snap is not None and snap[0] == 0:
        return False

    return _has_sold_out_zip_supply_direct(zip_code, county_id, vertical, db)


def _has_sold_out_zip_supply_direct(
    zip_code: str,
    county_id: str,
    vertical: str,
    db: Session,
) -> bool:
    """
    Full supply check: Gold+ leads in ZIP that are not under active exclusivity
    lock for this vertical and not recorded as sold within the last 60 days.
    """
    count = db.execute(text("""
        SELECT COUNT(DISTINCT p.id)
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE p.zip        = :zip
          AND p.county_id  = :county
          AND ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
          AND ds.score_date >= CURRENT_DATE - INTERVAL '1 day'
          AND NOT EXISTS (
              SELECT 1 FROM zip_territories zt
              WHERE zt.zip_code   = p.zip
                AND zt.vertical   = :vertical
                AND zt.county_id  = p.county_id
                AND zt.status IN ('locked', 'grace')
          )
          AND NOT EXISTS (
              SELECT 1 FROM lead_quality_snapshots lqs
              WHERE lqs.property_id = p.id
                AND lqs.outcome     = 'sold'
                AND lqs.snapshot_at >= NOW() - INTERVAL '60 days'
          )
    """), {"zip": zip_code, "county": county_id, "vertical": vertical}).scalar()
    return (count or 0) > 0


# ── Eligibility gates ─────────────────────────────────────────────────────────

def check_county_live_eligibility(
    subscriber: Subscriber,
    county_id: str,
    db: Session,
) -> tuple[bool, str]:
    """
    Returns (eligible, reason) for County-Live reactivation.

    A subscriber is eligible when they:
      - are not on reactivation cooldown
      - are a past paid subscriber, or currently dormant/lapsed
      - have a phone or email on file
      - have demonstrated geo-interest in the target county
    """
    if on_cooldown(subscriber):
        return False, "on_cooldown"

    qualifies_by_lifecycle = (
        is_past_subscriber(subscriber)
        or is_dormant(subscriber.id, db)
        or subscriber.status in ("churned", "cancelled", "grace")
    )
    if not qualifies_by_lifecycle:
        return False, "not_dormant_or_lapsed"

    if not subscriber.email and not subscriber.phone:
        return False, "no_contact_info"

    from src.services.geo_interest import get_subscriber_counties_of_interest
    counties = get_subscriber_counties_of_interest(subscriber.id, db)
    if county_id not in counties:
        return False, f"no_county_interest:{county_id}"

    return True, "eligible"


def check_sold_out_zip_eligibility(
    subscriber: Subscriber,
    zip_code: str,
    vertical: str,
    county_id: str,
    db: Session,
) -> tuple[bool, str]:
    """
    Returns (eligible, reason) for Sold-Out ZIP reactivation.

    A subscriber is eligible when they:
      - are not on reactivation cooldown
      - are a past paid subscriber, or currently dormant/lapsed
      - have a phone or email on file
      - have demonstrated geo-interest in the target ZIP
      - the ZIP has new Gold+ leads that are unassigned/unlocked/unsold
    """
    if on_cooldown(subscriber):
        return False, "on_cooldown"

    qualifies_by_lifecycle = (
        is_past_subscriber(subscriber)
        or is_dormant(subscriber.id, db)
        or subscriber.status in ("churned", "cancelled", "grace")
    )
    if not qualifies_by_lifecycle:
        return False, "not_dormant_or_lapsed"

    if not subscriber.email and not subscriber.phone:
        return False, "no_contact_info"

    from src.services.geo_interest import get_subscriber_zips_of_interest
    zips = get_subscriber_zips_of_interest(subscriber.id, db)
    all_zips = set(zips["locked"] + zips["waitlist"] + zips["engagement"] + zips["wallet"])
    if zip_code not in all_zips:
        return False, f"no_zip_interest:{zip_code}"

    if not _has_sold_out_zip_supply(zip_code, county_id, vertical, db):
        return False, f"no_supply:{zip_code}"

    return True, "eligible"
