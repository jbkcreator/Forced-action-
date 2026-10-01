"""Validate the cause attached to an unfunded outcome.

A confirmed cause is chosen by a person, never inferred: this module checks
that a cause is one of the brief's six, that an unfunded outcome is not left
without one, and records a person's correction on the logged call.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import text as sa_text

from config.call_outcome_causes import UNFUNDED_CAUSES


class MissingCause(ValueError):
    """An unfunded outcome was recorded without a cause."""


class UnknownCause(ValueError):
    """A cause outside the agreed list was recorded."""


def require_cause(cause: Optional[str]) -> str:
    """Return the normalized cause for an unfunded outcome, or raise."""
    if cause is None or not cause.strip():
        raise MissingCause("an unfunded outcome needs a cause")
    normalized = cause.strip().lower()
    if normalized not in UNFUNDED_CAUSES:
        raise UnknownCause(f"unknown cause {cause!r}; expected one of {sorted(UNFUNDED_CAUSES)}")
    return normalized


def record_unfunded_cause(session, *, dialer_call_id: str, cause: Optional[str]) -> bool:
    """Set the confirmed cause on a logged call, replacing any provisional default.

    The call log assigns a provisional cause per disposition code; a person's
    correction lands here and clears the provisional flag. Writes the
    ``unfunded_cause`` columns the call-disposition migration adds
    (``migrations/apply_lending_call_dispositions_dialer.py``). Returns False
    when no logged call has that id. Does not commit.
    """
    normalized = require_cause(cause)
    result = session.execute(
        sa_text(
            "UPDATE lending.call_dispositions "
            "SET unfunded_cause = :cause, unfunded_cause_provisional = false, updated_at = now() "
            "WHERE dialer_call_id = :call_id"
        ),
        {"cause": normalized, "call_id": dialer_call_id},
    )
    return result.rowcount == 1
