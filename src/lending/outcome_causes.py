"""Validate the cause attached to an unfunded outcome.

The cause is chosen by a person, never inferred, so this module only checks
that a recorded cause is one of the brief's six and that an unfunded outcome
is not left without one.
"""
from __future__ import annotations

from typing import Optional

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
