"""M10 / B2 — Lead Delivery + Free→Paid Attribution.

Assigns a graded Lead (a scored property) to exactly one paying CustomerAccount,
atomically (no double-delivery), spending one unit of per-grade entitlement.
Records the free→paid motion and the rejection→credit loop.

Pure matching rules (headroom, tie-breaker ranking) live here as standalone
functions so they are trivially testable; the transactional claim and the DB
helpers are called by the daily sweep and the B1 conversion hook.

Glossary: Lead = scored `properties` row; grade = `distress_scores.lead_tier`;
account = `customer_accounts`; coverage = locked `zip_territories` via subscriber.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Accounts in these states still receive leads (B1 throttle: past_due keeps free
# Bronze flowing and is still served; full cut only at churned).
_SERVED_STATUSES = ("active", "past_due", "free_trial")

# Plan-tier priority (tie-breaker 1): higher rank wins first call on a lead.
_TIER_RANK = {
    "dominator": 40,
    "pro": 30,
    "starter": 20,
    "free_trial": 10,
}


def tier_rank(plan_tier: Optional[str]) -> int:
    """Rank a plan tier for the tie-breaker. Unknown/None tiers rank lowest."""
    return _TIER_RANK.get(plan_tier or "", 0)


def bucket_for(entitlement: Optional[dict], grade: str) -> int:
    """The per-grade allowance for a grade, 0 if the account isn't entitled."""
    if not entitlement:
        return 0
    return int(entitlement.get(grade, 0) or 0)


def headroom(bucket: int, credits: int, delivered_this_cycle: int) -> int:
    """Leads still owed this cycle for a grade (never negative).

    headroom = bucket(grade) + credits(grade) - deliveries(grade, this cycle).
    Rejected deliveries are excluded from `delivered_this_cycle` by the caller,
    so they do not count against entitlement.
    """
    return max(0, bucket + credits - delivered_this_cycle)


def grade_key(lead_tier: str) -> str:
    """Map a CDS lead_tier ('Gold', 'Ultra Platinum') to an entitlement bucket
    key ('gold', 'ultra_platinum')."""
    return (lead_tier or "").strip().lower().replace(" ", "_")


@dataclass(frozen=True)
class Candidate:
    """A scored account eligible to receive a given lead."""
    account_id: object
    plan_tier: Optional[str]
    headroom: int
    last_delivered_at: Optional[datetime]
    vertical: Optional[str] = None
    period_end: Optional[datetime] = None


@dataclass(frozen=True)
class Lead:
    """A graded lead ready for delivery: a scored property + its grade and the
    verticals (trades) it qualifies for."""
    property_id: int
    zip_code: str
    county_id: str
    grade: str                 # distress_scores.lead_tier, e.g. 'Gold'
    verticals: list[str]       # qualifying trades from vertical_scores


def pick_winner(candidates: list[Candidate]) -> Optional[Candidate]:
    """Resolve the §3.1b-2 tie-breaker: tier priority → most headroom →
    round-robin (oldest last_delivered_at; never-delivered wins first).
    Returns the single best candidate, or None if there are none.
    """
    eligible = [c for c in candidates if c.headroom > 0]
    if not eligible:
        return None
    return min(
        eligible,
        key=lambda c: (
            -tier_rank(c.plan_tier),                       # higher tier first
            -c.headroom,                                   # more headroom first
            (c.last_delivered_at is not None, c.last_delivered_at),  # None (starved) first, else oldest
        ),
    )


def _delivered_this_cycle(db: Session, account_id, grade: str, period_end) -> int:
    """Count deliveries of a grade that consumed a slot in the account's current
    cycle. Counts ALL statuses (a rejected lead still consumed its slot — the
    compensation is the +1 credit, not a free re-count, so we never double-credit).
    When the account has no billing period (trial), the allowance is lifetime.
    """
    if period_end is None:
        row = db.execute(text(
            "SELECT count(*) FROM deliveries WHERE account_id = :a AND grade = :g "
            "AND billing_period_end IS NULL"
        ), {"a": str(account_id), "g": grade}).scalar()
    else:
        row = db.execute(text(
            "SELECT count(*) FROM deliveries WHERE account_id = :a AND grade = :g "
            "AND billing_period_end = :pe"
        ), {"a": str(account_id), "g": grade, "pe": period_end}).scalar()
    return int(row or 0)


def candidates_for(db: Session, lead: Lead) -> list[Candidate]:
    """Find accounts eligible for a lead: own a locked ZIP territory for the
    lead's (zip, county, one of its verticals), are in a served state, are
    entitled to the lead's grade, and still have headroom this cycle.
    """
    gkey = grade_key(lead.grade)
    rows = db.execute(text("""
        SELECT ca.account_id, ca.plan_tier, ca.lead_entitlement, ca.lead_credits,
               ca.current_period_end, zt.vertical,
               (SELECT max(d.delivered_at) FROM deliveries d WHERE d.account_id = ca.account_id) AS last_delivered_at
        FROM zip_territories zt
        JOIN customer_accounts ca ON ca.subscriber_id = zt.subscriber_id
        WHERE zt.zip_code = :zip AND zt.county_id = :county
          AND zt.status = 'locked'
          AND zt.vertical = ANY(:verticals)
          AND ca.status = ANY(:served)
    """), {
        "zip": lead.zip_code, "county": lead.county_id,
        "verticals": list(lead.verticals), "served": list(_SERVED_STATUSES),
    }).fetchall()

    candidates: list[Candidate] = []
    for r in rows:
        bucket = bucket_for(r.lead_entitlement, gkey)
        if bucket <= 0:
            continue  # not entitled to this grade
        credits = bucket_for(r.lead_credits, gkey)
        used = _delivered_this_cycle(db, r.account_id, lead.grade, r.current_period_end)
        room = headroom(bucket, credits, used)
        if room <= 0:
            continue  # exhausted this cycle
        candidates.append(Candidate(
            account_id=r.account_id, plan_tier=r.plan_tier, headroom=room,
            last_delivered_at=r.last_delivered_at, vertical=r.vertical,
            period_end=r.current_period_end,
        ))
    return candidates


def claim(db: Session, lead: Lead):
    """Atomically assign a lead to its best-matched account (§12.3).

    Locks the property row FOR UPDATE so concurrent matchers serialize; enforces
    EXCLUSIVE delivery (one lead → one account) by bailing if any delivery already
    exists for the property; then inserts the delivery (entitlement spend = the row
    itself, counted inside this same transaction). Returns the Delivery, or None if
    the lead is already claimed or no account qualifies (→ undelivered pool).

    Caller owns the transaction boundary (commit/rollback).
    """
    from src.core.models import Delivery

    # Serialize all matchers for this lead on the property row.
    db.execute(text("SELECT id FROM properties WHERE id = :pid FOR UPDATE"),
               {"pid": lead.property_id})

    # Exclusivity: if anyone already has this lead, it's taken.
    taken = db.execute(text("SELECT 1 FROM deliveries WHERE property_id = :pid LIMIT 1"),
                       {"pid": lead.property_id}).fetchone()
    if taken:
        logger.info("lead %s already claimed — skipping", lead.property_id)
        return None

    winner = pick_winner(candidates_for(db, lead))
    if winner is None:
        return None  # no coverage / all exhausted → undelivered pool

    delivery = Delivery(
        property_id=lead.property_id,
        account_id=winner.account_id,
        grade=lead.grade,
        vertical=winner.vertical,
        billing_period_end=winner.period_end,
    )
    db.add(delivery)
    db.flush()
    return delivery


def record_free_to_paid(db: Session, account, *, first_paid_plan: Optional[str], converted_at: datetime):
    """Record first-touch free→paid attribution (§12.8) when an account first
    converts to paid. One row per account (idempotent). first_free_delivery_id is
    the earliest free (pre-conversion) delivery; last + count are stored so a
    multi-touch model can be derived later. Free leads = deliveries made before
    conversion (delivered, not rejected). Returns the row, or None on replay.
    """
    from src.core.models import FreeToPaidAttribution

    existing = db.execute(
        text("SELECT 1 FROM free_to_paid_attribution WHERE account_id = :a LIMIT 1"),
        {"a": str(account.account_id)},
    ).fetchone()
    if existing:
        return None

    rows = db.execute(text("""
        SELECT id FROM deliveries
        WHERE account_id = :a AND status = 'delivered' AND delivered_at < :conv
        ORDER BY delivered_at ASC, id ASC
    """), {"a": str(account.account_id), "conv": converted_at}).fetchall()

    attr = FreeToPaidAttribution(
        account_id=account.account_id,
        first_free_delivery_id=rows[0].id if rows else None,
        last_free_delivery_id=rows[-1].id if rows else None,
        free_leads_count=len(rows),
        converted_at=converted_at,
        first_paid_plan=first_paid_plan,
    )
    db.add(attr)
    db.flush()
    return attr


def reject_delivery(db: Session, delivery_id: int, reason: str, *, now: Optional[datetime] = None):
    """Mark a delivered lead rejected for a data-quality reason (§3.1b) and grant
    a +1 same-grade credit to the account. Idempotent (re-rejecting is a no-op, no
    double credit). The reason is stored on the row for the Truth Engine (M6) to
    consume later — no event is emitted today (M6/bus not built). Returns the
    Delivery, or None if it doesn't exist.
    """
    from src.core.models import CustomerAccount, Delivery

    d = db.query(Delivery).filter(Delivery.id == delivery_id).first()
    if d is None:
        logger.warning("reject_delivery: no delivery %s", delivery_id)
        return None
    if d.status == "rejected":
        return d  # idempotent — already credited

    d.status = "rejected"
    d.rejection_reason = reason
    d.rejected_at = now or datetime.now(timezone.utc)

    account = db.query(CustomerAccount).filter(CustomerAccount.account_id == d.account_id).first()
    if account is not None:
        gkey = grade_key(d.grade)
        credits = dict(account.lead_credits or {})        # reassign so JSONB change is tracked
        credits[gkey] = int(credits.get(gkey, 0) or 0) + 1
        account.lead_credits = credits
    db.flush()
    return d


def rejection_rate(db: Session, account_id) -> float:
    """Abuse guardrail (§3.1b): fraction of an account's deliveries that were
    rejected. 0.0 when the account has no deliveries."""
    row = db.execute(text("""
        SELECT count(*) FILTER (WHERE status = 'rejected')::float / NULLIF(count(*), 0)
        FROM deliveries WHERE account_id = :a
    """), {"a": str(account_id)}).scalar()
    return float(row or 0.0)
