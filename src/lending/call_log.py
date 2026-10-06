"""Recording-disclosure evidence on the call log (Go Live G12).

The mechanism that plays the disclosure (a dialer recording announcement or a
required first script line) is confirmed with the client; this module keeps the
evidence: a flag on every call row, and the list of calls that lack it.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from src.lending.missed_call_text import call_record_fields


def mark_disclosure_logged(db, dialer_call_id: str) -> bool:
    """True when a call row was found and flagged. Does not commit."""
    result = db.execute(
        text("UPDATE lending.call_dispositions SET recording_disclosure_logged = true, updated_at = now() "
             "WHERE dialer_call_id = :call_id"),
        {"call_id": dialer_call_id},
    )
    return result.rowcount > 0


def calls_missing_disclosure(db, *, since: datetime) -> list[str]:
    rows = db.execute(
        text("SELECT dialer_call_id FROM lending.call_dispositions "
             "WHERE NOT recording_disclosure_logged AND call_ended_at >= :since ORDER BY call_ended_at"),
        {"since": since},
    ).scalars().all()
    return list(rows)


def record_call_attempts(db, records: Iterable[Mapping[str, Any]]) -> list[str]:
    """Write one call-log row per finished dialer call record, so the attempt cap counts
    it even when no call event is pushed. Existing rows (the webhook's) are left as they
    are. Returns the phone of each newly written row. Does not commit."""
    rows = []
    for record in records:
        fields = call_record_fields(record)
        if fields is None:
            continue
        rows.append({"c": fields["call_id"], "p": fields["phone"], "d": fields["direction"],
                     "e": fields["ended_at"], "r": json.dumps(dict(record), default=str)})
    if not rows:
        return []
    inserted = db.execute(
        text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, direction, call_ended_at, raw_event) "
             "SELECT c, p, d, e, CAST(r AS jsonb) FROM jsonb_to_recordset(CAST(:rows AS jsonb)) "
             "AS x(c text, p text, d text, e timestamptz, r text) "
             "ON CONFLICT (dialer_call_id) DO NOTHING RETURNING phone"),
        {"rows": json.dumps(rows, default=str)},
    ).scalars().all()
    return list(inserted)
