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

# Single source of INSERT SQL, shared by the single-row and bulk paths. Passing
# a list of param dicts to db.execute() runs it as an executemany.
_INSERT_OUTCOME_SQL = """
    INSERT INTO agent_lane_opportunity_outcomes
        (opportunity_thread_id, outcome, reason_code, coded_by, source_ref)
    VALUES
        (:opportunity_thread_id, :outcome, :reason_code, :coded_by, :source_ref)
    ON CONFLICT (opportunity_thread_id) DO NOTHING
"""


def _insert_outcome(
    db: Session,
    *,
    opportunity_thread_id: str,
    outcome: str,
    reason_code: str | None,
    coded_by: str,
    source_ref: str | None,
) -> bool:
    """Single-row idempotent outcome insert. Returns True if a row was inserted."""
    result = db.execute(text(_INSERT_OUTCOME_SQL), {
        "opportunity_thread_id": opportunity_thread_id,
        "outcome": outcome,
        "reason_code": reason_code,
        "coded_by": coded_by,
        "source_ref": source_ref,
    })
    return result.rowcount > 0


def insert_outcomes_bulk(db: Session, rows: list[dict]) -> int:
    """Bulk-insert outcome rows in a single executemany. Idempotent
    (ON CONFLICT DO NOTHING). Each dict must carry opportunity_thread_id,
    outcome, reason_code, coded_by, source_ref.

    Returns the number of rows inserted.
    """
    if not rows:
        return 0
    result = db.execute(text(_INSERT_OUTCOME_SQL), rows)
    return result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0


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
    return _insert_outcome(
        db,
        opportunity_thread_id=opportunity_thread_id,
        outcome="won",
        reason_code=None,
        coded_by=coded_by,
        source_ref=source_ref,
    )


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

    return _insert_outcome(
        db,
        opportunity_thread_id=opportunity_thread_id,
        outcome="lost",
        reason_code=reason_code,
        coded_by=coded_by,
        source_ref=source_ref,
    )


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

    outcome_rows = [
        {
            "opportunity_thread_id": row.opportunity_thread_id,
            "outcome": "won",
            "reason_code": None,
            "coded_by": "payment_fleet_event",
            "source_ref": f"fleet_event:{row.event_id}",
        }
        for row in rows
    ]
    try:
        return insert_outcomes_bulk(db, outcome_rows)
    except Exception as exc:
        logger.warning("sweep_payment_wins: bulk insert failed: %s", exc)
        return 0
