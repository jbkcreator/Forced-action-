"""Recording-disclosure evidence on the call log (Go Live G12).

The mechanism that plays the disclosure (a dialer recording announcement or a
required first script line) is confirmed with the client; this module keeps the
evidence: a flag on every call row, and the list of calls that lack it.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import text


def mark_disclosure_logged(db, dialer_call_id: str) -> bool:
    """True when a call row was found and flagged. Does not commit."""
    result = db.execute(
        text("UPDATE lending.call_dispositions SET recording_disclosure_logged = true, updated_at = now() "
             "WHERE aircall_call_id = :call_id"),
        {"call_id": dialer_call_id},
    )
    return result.rowcount > 0


def calls_missing_disclosure(db, *, since: datetime) -> list[str]:
    rows = db.execute(
        text("SELECT aircall_call_id FROM lending.call_dispositions "
             "WHERE NOT recording_disclosure_logged AND call_ended_at >= :since ORDER BY call_ended_at"),
        {"since": since},
    ).scalars().all()
    return list(rows)
