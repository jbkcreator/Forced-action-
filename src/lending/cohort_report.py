"""90-day cohort and batch tracking for LendingFlow purchases (Runbook § 4.6, Item 6).

A batch is one purchase in ``lending.lendingflow_batches`` (name, purchase date, lead count). Delivered LendingFlow
leads fill the batches in arrival order, oldest batch first; leads that arrived before the first purchase date are
not counted. Per batch:

* leads bought / delivered
* funded within 30 / 60 / 90 days of delivery: a GHL opportunity entering a funded stage (``FUNDED_STAGE_KEYS``,
  from ``lending.ghl_stage_events``) on the lead's phone, credited to the latest lead on that phone delivered before
  it. A window is reported only once every lead of a full batch is that old; until then it is None, not 0.
* fees collected and Revenue Per Lead (fees / leads delivered): the fee is the opportunity's monetary value sent with
  the funded stage webhook (``FEE_FIELD``). None, not 0, while no funded loan has a fee.
* replacement requests submitted / credited / denied (``lending.lendingflow_replacements``)

A query, not a database view (docs/lending/cohort-tracking.md § 4). Usage:

    python -m src.lending.cohort_report                       # the report
    python -m src.lending.cohort_report add-batch --name "LF 2026-10 #1" --purchased 2026-10-20 --leads 100
    python -m src.lending.cohort_report request-replacement --lead 123
    python -m src.lending.cohort_report decide-replacement --lead 123 --status credited
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Sequence

from sqlalchemy import text

from config.lending_cohorts import (
    FEE_FIELD,
    FUNDED_STAGE_KEYS,
    FUNDED_WINDOWS_DAYS,
    REPLACEMENT_CREDITED,
    REPLACEMENT_DENIED,
    REPLACEMENT_REQUESTED,
)
from config.lending_followup import REPLACEMENT_DAY_TIMEZONE
from src.lending.replacement_eligibility import lead_attempts

logger = logging.getLogger(__name__)

# Leads ranked by arrival (from the first purchase date, ET) fill the batches ranked by purchase date. A funded stage
# entry is credited to the latest lead on that phone delivered before it, and each lead counts its first one only.
# The fee is accepted only as a plain amount once "$", "," and spaces are removed; anything else is ignored.
_COHORTS = text("""
    WITH batches AS (
        SELECT id, name, purchased_at, lead_count,
               sum(lead_count) OVER (ORDER BY purchased_at, id) AS upto
          FROM lending.lendingflow_batches
    ),
    leads AS (
        SELECT id, phone, received_at, row_number() OVER (ORDER BY received_at, id) AS rank
          FROM lending.lendingflow_leads
         WHERE received_at >= (SELECT min(purchased_at) FROM batches)::timestamp AT TIME ZONE :tz
    ),
    placed AS (
        SELECT l.id, l.phone, l.received_at, b.id AS batch_id
          FROM leads l JOIN batches b ON l.rank > b.upto - b.lead_count AND l.rank <= b.upto
    ),
    funded_events AS (
        SELECT DISTINCT ON (e.id) e.id, e.event_at, p.id AS lead_id,
               regexp_replace(e.raw_event ->> :fee_field, '[$,\\s]', '', 'g') AS fee
          FROM lending.ghl_stage_events e
          JOIN placed p ON p.phone = e.phone AND p.received_at <= e.event_at
         WHERE e.stage_key = ANY(:funded)
         ORDER BY e.id, p.received_at DESC
    ),
    funded AS (
        SELECT DISTINCT ON (lead_id) lead_id, event_at,
               CASE WHEN fee ~ '^[0-9]+(\\.[0-9]+)?$' THEN fee::numeric END AS fee
          FROM funded_events
         ORDER BY lead_id, event_at
    )
    SELECT b.id, b.name, b.purchased_at, b.lead_count,
           count(p.id) AS delivered,
           max(p.received_at) AS last_received_at,
           count(f.lead_id) FILTER (WHERE f.event_at <= p.received_at + make_interval(days => :w1)) AS funded_w1,
           count(f.lead_id) FILTER (WHERE f.event_at <= p.received_at + make_interval(days => :w2)) AS funded_w2,
           count(f.lead_id) FILTER (WHERE f.event_at <= p.received_at + make_interval(days => :w3)) AS funded_w3,
           count(f.lead_id) AS funded_to_date,
           count(f.fee) AS fees_recorded,
           sum(f.fee) AS fees_total,
           count(r.id) AS replacements_requested,
           count(r.id) FILTER (WHERE r.status = :credited) AS replacements_credited,
           count(r.id) FILTER (WHERE r.status = :denied) AS replacements_denied
      FROM batches b
      LEFT JOIN placed p ON p.batch_id = b.id
      LEFT JOIN funded f ON f.lead_id = p.id
      LEFT JOIN lending.lendingflow_replacements r ON r.lead_id = p.id
     GROUP BY b.id, b.name, b.purchased_at, b.lead_count
     ORDER BY b.purchased_at, b.id
""")

_UNPLACED = text("""
    SELECT count(*) FROM lending.lendingflow_leads
     WHERE received_at >= (SELECT min(purchased_at) FROM lending.lendingflow_batches)::timestamp AT TIME ZONE :tz
""")


@dataclass(frozen=True)
class Cohort:
    batch_id: int
    name: str
    purchased_at: date
    leads_bought: int
    leads_delivered: int
    funded: dict[int, Optional[int]]  # window days -> funded count; None while the window is still open
    funded_to_date: int
    fees_recorded: int
    fees_total: Optional[Decimal]
    revenue_per_lead: Optional[Decimal]
    replacements_requested: int
    replacements_credited: int
    replacements_denied: int


def _window_closed(leads_bought: int, delivered: int, last_received_at: Optional[datetime], days: int,
                   now: datetime) -> bool:
    return delivered >= leads_bought and last_received_at is not None and now >= last_received_at + timedelta(days=days)


def cohorts(db, *, now: Optional[datetime] = None) -> tuple[list[Cohort], int]:
    """One ``Cohort`` per batch, oldest first, and the number of leads delivered beyond the recorded batches."""
    now = now or datetime.now(timezone.utc)
    w1, w2, w3 = FUNDED_WINDOWS_DAYS
    rows = db.execute(_COHORTS, {"tz": REPLACEMENT_DAY_TIMEZONE, "fee_field": FEE_FIELD,
                                 "funded": sorted(FUNDED_STAGE_KEYS), "w1": w1, "w2": w2, "w3": w3,
                                 "credited": REPLACEMENT_CREDITED, "denied": REPLACEMENT_DENIED}).mappings().all()
    result = []
    for row in rows:
        funded = {days: row[key] if _window_closed(row["lead_count"], row["delivered"], row["last_received_at"],
                                                    days, now) else None
                  for days, key in zip(FUNDED_WINDOWS_DAYS, ("funded_w1", "funded_w2", "funded_w3"))}
        fees_total = row["fees_total"] if row["fees_recorded"] else None
        result.append(Cohort(
            batch_id=row["id"], name=row["name"], purchased_at=row["purchased_at"], leads_bought=row["lead_count"],
            leads_delivered=row["delivered"], funded=funded, funded_to_date=row["funded_to_date"],
            fees_recorded=row["fees_recorded"], fees_total=fees_total,
            revenue_per_lead=(fees_total / row["delivered"]).quantize(Decimal("0.01"))
            if fees_total is not None and row["delivered"] else None,
            replacements_requested=row["replacements_requested"],
            replacements_credited=row["replacements_credited"], replacements_denied=row["replacements_denied"],
        ))
    if not result:
        return result, 0
    since_first = db.execute(_UNPLACED, {"tz": REPLACEMENT_DAY_TIMEZONE}).scalar() or 0
    return result, max(since_first - sum(c.leads_delivered for c in result), 0)


class ReplacementRefused(ValueError):
    """A replacement request or decision that the rules do not allow."""


def add_batch(db, *, name: str, purchased_at: date, lead_count: int, note: Optional[str] = None) -> int:
    if lead_count <= 0:
        raise ValueError("lead_count must be positive")
    return db.execute(text("INSERT INTO lending.lendingflow_batches (name, purchased_at, lead_count, note) "
                           "VALUES (:name, :purchased, :count, :note) RETURNING id"),
                      {"name": name, "purchased": purchased_at, "count": lead_count, "note": note}).scalar_one()


def request_replacement(db, lead_id: int, *, note: Optional[str] = None) -> int:
    """Record a replacement request for a lead that meets the replacement rule. Refuses an unknown or ineligible
    lead, and a lead already requested."""
    attempts = lead_attempts(db, [lead_id])
    if not attempts:
        raise ReplacementRefused(f"LendingFlow lead {lead_id} not found")
    if not attempts[0].eligible:
        raise ReplacementRefused(f"lead {lead_id} does not meet the replacement rule "
                                 f"({attempts[0].attempts} attempts over {attempts[0].call_days} days)")
    new_id = db.execute(text("INSERT INTO lending.lendingflow_replacements (lead_id, status, note) "
                             "VALUES (:lead, :status, :note) ON CONFLICT (lead_id) DO NOTHING RETURNING id"),
                        {"lead": lead_id, "status": REPLACEMENT_REQUESTED, "note": note}).scalar()
    if new_id is None:
        raise ReplacementRefused(f"lead {lead_id} already has a replacement request")
    return new_id


def decide_replacement(db, lead_id: int, status: str, *, note: Optional[str] = None) -> None:
    """Mark a requested replacement credited or denied (LendingFlow's answer)."""
    if status not in (REPLACEMENT_CREDITED, REPLACEMENT_DENIED):
        raise ReplacementRefused(f"status must be {REPLACEMENT_CREDITED} or {REPLACEMENT_DENIED}")
    updated = db.execute(text("UPDATE lending.lendingflow_replacements SET status = :status, decided_at = now(), "
                              "note = COALESCE(:note, note), updated_at = now() "
                              "WHERE lead_id = :lead AND status = :requested"),
                         {"status": status, "note": note, "lead": lead_id, "requested": REPLACEMENT_REQUESTED}).rowcount
    if not updated:
        raise ReplacementRefused(f"lead {lead_id} has no open replacement request")


def _log_report(rows: list[Cohort], unplaced: int) -> None:
    def show(value) -> str:
        return "open" if value is None else str(value)

    for c in rows:
        logger.info("[cohort] batch=%s %r purchased=%s bought=%d delivered=%d %s funded_to_date=%d fees=%s "
                    "(recorded on %d) revenue_per_lead=%s replacements requested=%d credited=%d denied=%d",
                    c.batch_id, c.name, c.purchased_at, c.leads_bought, c.leads_delivered,
                    " ".join(f"funded_{d}d={show(n)}" for d, n in c.funded.items()), c.funded_to_date,
                    show(c.fees_total), c.fees_recorded, show(c.revenue_per_lead), c.replacements_requested,
                    c.replacements_credited, c.replacements_denied)
    if not rows:
        logger.info("[cohort] no batches recorded (add one with: add-batch)")
    elif unplaced:
        logger.warning("[cohort] %d leads delivered beyond the recorded batches: record the next purchase", unplaced)


def main(argv: Optional[Sequence[str]] = None) -> int:
    from src.lending.db import lending_session

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="LendingFlow 90-day cohort tracking (Runbook § 4.6)")
    commands = parser.add_subparsers(dest="command")
    batch = commands.add_parser("add-batch", help="record a LendingFlow purchase")
    batch.add_argument("--name", required=True)
    batch.add_argument("--purchased", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    batch.add_argument("--leads", required=True, type=int)
    batch.add_argument("--note")
    request = commands.add_parser("request-replacement", help="record a replacement request for an eligible lead")
    request.add_argument("--lead", required=True, type=int)
    request.add_argument("--note")
    decide = commands.add_parser("decide-replacement", help="record LendingFlow's answer to a request")
    decide.add_argument("--lead", required=True, type=int)
    decide.add_argument("--status", required=True, choices=(REPLACEMENT_CREDITED, REPLACEMENT_DENIED))
    decide.add_argument("--note")
    args = parser.parse_args(argv)

    try:
        with lending_session() as db:
            if args.command == "add-batch":
                batch_id = add_batch(db, name=args.name, purchased_at=args.purchased, lead_count=args.leads,
                                     note=args.note)
                logger.info("[cohort] batch %s recorded", batch_id)
            elif args.command == "request-replacement":
                request_replacement(db, args.lead, note=args.note)
                logger.info("[cohort] replacement requested for lead %s", args.lead)
            elif args.command == "decide-replacement":
                decide_replacement(db, args.lead, args.status, note=args.note)
                logger.info("[cohort] replacement for lead %s marked %s", args.lead, args.status)
            else:
                _log_report(*cohorts(db))
    except (ReplacementRefused, ValueError) as exc:
        logger.error("[cohort] %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
