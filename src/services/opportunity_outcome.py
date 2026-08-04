"""
LEARN-v2.2 T-LEARN-03 — win/loss reason-code service.

The terminal outcome of one Agent Lane opportunity lives in
agent_lane_opportunity_outcomes (one row per opportunity_thread_id).

  - outcome='won'  needs no reason. Auto-coded from payment.received fleet
                   events (NULL-tolerant — fires only when a payment.received
                   carries an opportunity_thread_id).
  - outcome='lost' carries exactly one of the spec's eight loss codes.
                   Supplied by a human (Josh) via the admin tap endpoint,
                   except 'no_response', which the 30-day timeout sweep
                   auto-codes.

All writes are idempotent: ON CONFLICT (opportunity_thread_id) DO NOTHING.
A thread that is already terminal is never overwritten.
"""
from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# The spec's eight loss codes — a closed set, mirrored by the DB CHECK
# constraint on agent_lane_opportunity_outcomes.reason_code.
LOSS_REASON_CODES = (
    "timing",
    "price",
    "trust",
    "fit",
    "no_urgency",
    "wrong_contact",
    "competitor",
    "no_response",
)


def has_outcome(db: Session, opportunity_thread_id: str) -> bool:
    """True if this thread already has a terminal outcome row."""
    row = db.execute(text("""
        SELECT 1 FROM agent_lane_opportunity_outcomes
        WHERE opportunity_thread_id = :thread
        LIMIT 1
    """), {"thread": opportunity_thread_id}).first()
    return row is not None


def record_win(
    db: Session,
    opportunity_thread_id: str,
    *,
    coded_by: str,
    source_ref: str | None = None,
) -> bool:
    """Record a 'won' outcome for a thread. Idempotent.

    Returns True if a new row was inserted, False if the thread was already
    terminal (ON CONFLICT DO NOTHING).
    """
    result = db.execute(text("""
        INSERT INTO agent_lane_opportunity_outcomes
            (opportunity_thread_id, outcome, reason_code, coded_by, source_ref)
        VALUES
            (:thread, 'won', NULL, :coded_by, :source_ref)
        ON CONFLICT (opportunity_thread_id) DO NOTHING
    """), {
        "thread": opportunity_thread_id,
        "coded_by": coded_by,
        "source_ref": source_ref,
    })
    return result.rowcount > 0


def record_loss(
    db: Session,
    opportunity_thread_id: str,
    *,
    reason_code: str,
    coded_by: str,
    source_ref: str | None = None,
) -> bool:
    """Record a 'lost' outcome with one of the eight loss codes. Idempotent.

    Validation happens BEFORE any db access so the ValueError path is
    unit-testable without a real session.

    Returns True if a new row was inserted, False if the thread was already
    terminal (ON CONFLICT DO NOTHING).

    Raises:
        ValueError: if reason_code is not one of LOSS_REASON_CODES.
    """
    if reason_code not in LOSS_REASON_CODES:
        raise ValueError(
            f"invalid loss reason_code {reason_code!r}; "
            f"must be one of {LOSS_REASON_CODES}"
        )

    result = db.execute(text("""
        INSERT INTO agent_lane_opportunity_outcomes
            (opportunity_thread_id, outcome, reason_code, coded_by, source_ref)
        VALUES
            (:thread, 'lost', :reason_code, :coded_by, :source_ref)
        ON CONFLICT (opportunity_thread_id) DO NOTHING
    """), {
        "thread": opportunity_thread_id,
        "reason_code": reason_code,
        "coded_by": coded_by,
        "source_ref": source_ref,
    })
    return result.rowcount > 0


def sweep_payment_wins(db: Session) -> int:
    """Auto-code wins from payment.received fleet events.

    Reads fleet_events WHERE event_type='payment.received' AND
    opportunity_thread_id IS NOT NULL that have no outcome row yet, and codes
    each as a win. NULL-tolerant: if no payment.received carries a thread_id
    (the case today — no thread-carrying emitter exists yet), this codes
    nothing and returns 0.

    Returns the number of new win rows inserted.
    """
    rows = db.execute(text("""
        SELECT fe.id AS event_id, fe.opportunity_thread_id
        FROM fleet_events fe
        WHERE fe.event_type = 'payment.received'
          AND fe.opportunity_thread_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM agent_lane_opportunity_outcomes o
              WHERE o.opportunity_thread_id = fe.opportunity_thread_id
          )
        ORDER BY fe.occurred_at
    """)).fetchall()

    coded = 0
    for row in rows:
        try:
            if record_win(
                db,
                row.opportunity_thread_id,
                coded_by="payment_fleet_event",
                source_ref=f"fleet_event:{row.event_id}",
            ):
                coded += 1
        except Exception as exc:
            logger.warning(
                "sweep_payment_wins: failed to code thread=%s event_id=%s: %s",
                row.opportunity_thread_id, row.event_id, exc,
            )
    return coded
