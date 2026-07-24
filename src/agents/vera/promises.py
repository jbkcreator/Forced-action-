"""
Vera's promise store — read/write helpers for vera_promises (VERA-v2.2 V4).

A "promise" is an open commitment Vera tracks per her constitution's standing
job #3 (open commitments, owner, age, overdue, MRR-at-risk — Josh's included).

Same role split as facts.py:
  - record_promise()/close_promise() WRITE via the normal app DB role
    (src.core.database.get_db_context) — vera_readonly holds no write grants.
  - open_promises() READS via the read-only vera_readonly connection
    (src.agents.vera.db), consistent with every other Vera check.

record_promise() is the ONE ingestion seam. Today it's called by the
--add-promise CLI and by tests; when Phase 2's reply-forwarding mailbox lands,
its parser calls this same function unchanged — no other code changes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from src.agents.vera.db import vera_db
from src.core.database import get_db_context
from src.core.models import VeraPromise

_VALID_STATUSES = {"open", "closed", "cancelled"}


@dataclass
class PromiseRow:
    """Read-side view of a vera_promises row. Pure helpers below take `now`
    so they stay testable without touching the clock."""
    id: int
    thread_id: Optional[str]
    description: str
    owner: str
    source: str
    mrr_at_risk_cents: Optional[int]
    status: str
    due_at: Optional[datetime]
    observed_at: datetime

    def age_days(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.now(timezone.utc)
        observed = self.observed_at
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        return max(0, (now - observed).days)

    def is_overdue(self, now: Optional[datetime] = None) -> bool:
        if self.due_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        due = self.due_at if self.due_at.tzinfo else self.due_at.replace(tzinfo=timezone.utc)
        return now > due


def record_promise(
    description: str,
    *,
    owner: str,
    source: str,
    thread_id: Optional[str] = None,
    mrr_at_risk_cents: Optional[int] = None,
    due_at: Optional[datetime] = None,
) -> VeraPromise:
    """Append one open commitment. THE single writer for vera_promises."""
    with get_db_context() as session:
        promise = VeraPromise(
            description=description,
            owner=owner,
            source=source,
            thread_id=thread_id,
            mrr_at_risk_cents=mrr_at_risk_cents,
            due_at=due_at,
            status="open",
        )
        session.add(promise)
        session.flush()
        session.refresh(promise)
        return promise


def close_promise(promise_id: int, *, status: str = "closed") -> bool:
    """Flip a promise to closed|cancelled and stamp closed_at. Returns False
    if no such open row existed (nothing to close)."""
    if status not in _VALID_STATUSES or status == "open":
        raise ValueError(f"close status must be 'closed' or 'cancelled', got {status!r}")
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE vera_promises SET status = :status, closed_at = :now "
                "WHERE id = :id AND status = 'open'"
            ),
            {"status": status, "now": datetime.now(timezone.utc), "id": promise_id},
        )
        return result.rowcount > 0


def open_promises() -> list[PromiseRow]:
    """All open promises, newest first. Read-only (vera_readonly)."""
    with vera_db.session_scope() as session:
        rows = session.execute(
            text(
                "SELECT id, thread_id, description, owner, source, mrr_at_risk_cents, "
                "status, due_at, observed_at FROM vera_promises "
                "WHERE status = 'open' ORDER BY observed_at DESC"
            )
        ).mappings().all()
    return [
        PromiseRow(
            id=r["id"], thread_id=r["thread_id"], description=r["description"],
            owner=r["owner"], source=r["source"], mrr_at_risk_cents=r["mrr_at_risk_cents"],
            status=r["status"], due_at=r["due_at"], observed_at=r["observed_at"],
        )
        for r in rows
    ]
