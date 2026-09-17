"""WP-9 Dial List — call-disposition write-back (spec Q7).

When Josh works the dial list and records a call outcome, it lands in the
existing ``agent_lane_opportunity_outcomes`` table — the one canonical
terminal-outcome store, keyed by ``opportunity_thread_id`` (one row per
thread). No parallel outcome store. Writes reuse
``opportunity_outcome.record_win`` / ``record_loss``, which are idempotent via
``ON CONFLICT (opportunity_thread_id) DO NOTHING`` — a rerun on an already
terminal thread is a no-op, never an overwrite.

This module is the buildable half. The Slack-button / operator UI tap that
CALLS ``record_dial_disposition`` — capturing Josh's per-entry outcome — is
cross-team (WP-2 operator surface / WP-1 event spine) and stays out here.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.services.opportunity_outcome import (
    LOSS_REASON_CODES,
    record_loss,
    record_win,
)

logger = logging.getLogger(__name__)

VALID_OUTCOMES = ("won", "lost")
_SOURCE = "dial_list"


@dataclass(frozen=True, slots=True)
class DispositionResult:
    opportunity_thread_id: str
    outcome: str
    reason_code: Optional[str]
    source_ref: str
    inserted: bool  # False = thread already terminal (idempotent no-op)


def _source_ref(opportunity_thread_id: str, as_of: date) -> str:
    """Deterministic dial-list source_ref — identical inputs, identical ref."""
    return f"{_SOURCE}:{opportunity_thread_id}:{as_of.isoformat()}"


def record_dial_disposition(
    session: Session,
    *,
    opportunity_thread_id: str,
    outcome: str,
    loss_code: Optional[str] = None,
    actor: Optional[str] = None,
    as_of: Optional[date] = None,
) -> DispositionResult:
    """Record a dial-list call outcome into agent_lane_opportunity_outcomes.

    Idempotent: the underlying table is one-row-per-thread, so a rerun on an
    already-terminal thread inserts nothing (``inserted=False``).

    Validation runs before any DB access (business-rule boundary):
      - ``outcome`` must be 'won' or 'lost';
      - a 'won' outcome must not carry a ``loss_code``;
      - a 'lost' outcome requires one of the eight ``LOSS_REASON_CODES``.

    Raises:
        ValueError: on an invalid outcome / loss_code combination.
        SQLAlchemyError: re-raised after rollback + ERROR log on a DB failure.
    """
    if outcome not in VALID_OUTCOMES:
        raise ValueError(
            f"invalid outcome {outcome!r}; must be one of {VALID_OUTCOMES}"
        )
    if outcome == "won" and loss_code is not None:
        raise ValueError("a 'won' outcome must not carry a loss_code")
    if outcome == "lost":
        if loss_code is None:
            raise ValueError("a 'lost' outcome requires a loss_code")
        if loss_code not in LOSS_REASON_CODES:
            raise ValueError(
                f"invalid loss_code {loss_code!r}; must be one of {LOSS_REASON_CODES}"
            )

    effective_as_of = as_of or date.today()
    source_ref = _source_ref(opportunity_thread_id, effective_as_of)
    coded_by = f"{_SOURCE}:{actor}" if actor else _SOURCE

    try:
        if outcome == "won":
            inserted = record_win(
                session,
                opportunity_thread_id,
                coded_by=coded_by,
                source_ref=source_ref,
            )
        else:
            inserted = record_loss(
                session,
                opportunity_thread_id,
                reason_code=loss_code,  # validated above
                coded_by=coded_by,
                source_ref=source_ref,
            )
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        logger.error(
            "record_dial_disposition failed for thread %s (outcome=%s)",
            opportunity_thread_id, outcome, exc_info=True,
        )
        raise

    # When nothing was inserted the thread was already terminal — report the
    # CANONICAL stored outcome, not the attempted one, so a caller updating a
    # Slack card shows the real DB state (a later 'lost' tap on a 'won' thread
    # must not display 'lost'). record_win/record_loss return only a bool, so
    # read the persisted row back here.
    result_outcome, result_code = outcome, loss_code
    if not inserted:
        stored = session.execute(
            text(
                "SELECT outcome, reason_code FROM agent_lane_opportunity_outcomes "
                "WHERE opportunity_thread_id = :tid"
            ),
            {"tid": opportunity_thread_id},
        ).mappings().first()
        if stored is not None:
            result_outcome = stored["outcome"]
            result_code = stored["reason_code"]

    return DispositionResult(
        opportunity_thread_id=opportunity_thread_id,
        outcome=result_outcome,
        reason_code=result_code,
        source_ref=source_ref,
        inserted=inserted,
    )
