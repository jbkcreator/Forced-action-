"""One calling rule for every lead: both platforms dial until someone gets through, then the other stops.

"Got through" (inside GHL_CALLING_LOOKBACK_DAYS) is a call with a live-conversation result (REACHED_CODES: connected,
callback, DNC request, nurture, booked) or, for a GHL call whose result was never posted, GHL's answered status
(REACHED_GHL_STATUSES) with at least REACHED_MIN_TALK_SECONDS of talk. Unanswered calls change nothing: they only count
toward the shared attempt limits and show on the other platform (tags / card field).

- ``ghl_calling_phones``: phones GHL owns, which BatchDialer must not dial. The dialer load skips them and the dialer
  sweep deletes any already loaded. A phone becomes GHL's once it was reached in GHL.
- ``reached_in_batchdialer_phones``: phones the ISA reached in BatchDialer; the GHL call gate holds them off Josh's call
  lists (``ndl-do-not-call-now``).

A LendingFlow or website lead's phone is never GHL's here: its dialer membership belongs to the inbound dialer flow
(``src/lending/lendingflow_dialer.py``, campaigns "LendingFlow" / "Website"), which deletes it once it is booked or
connected in GHL (the same rule).
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from config.lending_compliance import (
    GHL_CALLING_LOOKBACK_DAYS,
    REACHED_CODES,
    REACHED_GHL_STATUSES,
    REACHED_MIN_TALK_SECONDS,
)

_REACHED_CALL = (
    "d.phone = p AND d.call_source = :source AND d.call_ended_at > :since "
    "AND (d.disposition = ANY(:codes) "
    "     OR (d.disposition IS NULL AND d.disposition_raw = ANY(:statuses) "
    "         AND COALESCE(d.talk_duration_sec, 0) >= :min_talk))"
)

_GHL_CALLING = text(
    "SELECT p FROM unnest(CAST(:phones AS text[])) AS p "
    "WHERE NOT EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.phone = p) "
    "  AND NOT EXISTS (SELECT 1 FROM lending.web_leads w WHERE w.phone = p) "
    f"  AND EXISTS (SELECT 1 FROM lending.call_dispositions d WHERE {_REACHED_CALL})"
)

_REACHED_IN_BATCHDIALER = text(
    "SELECT p FROM unnest(CAST(:phones AS text[])) AS p "
    f"WHERE EXISTS (SELECT 1 FROM lending.call_dispositions d WHERE {_REACHED_CALL})"
)


def _params(phones: Sequence[str], source: str, now: Optional[datetime]) -> dict:
    since = (now or datetime.now(timezone.utc)) - timedelta(days=GHL_CALLING_LOOKBACK_DAYS)
    return {"phones": list(phones), "source": source, "since": since, "codes": list(REACHED_CODES),
            "statuses": list(REACHED_GHL_STATUSES), "min_talk": REACHED_MIN_TALK_SECONDS}


def ghl_calling_phones(db, phones: Sequence[str], *, now: Optional[datetime] = None) -> set[str]:
    """The subset of ``phones`` (already normalized) that GHL owns. One query for the whole list."""
    if not phones:
        return set()
    return {row[0] for row in db.execute(_GHL_CALLING, _params(phones, "ghl", now))}


def reached_in_batchdialer_phones(db, phones: Sequence[str], *, now: Optional[datetime] = None) -> set[str]:
    """The subset of ``phones`` the ISA reached in BatchDialer inside the window. One query for the whole list."""
    if not phones:
        return set()
    return {row[0] for row in db.execute(_REACHED_IN_BATCHDIALER, _params(phones, "batchdialer", now))}
