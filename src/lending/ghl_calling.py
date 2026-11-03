"""Phones being worked from GoHighLevel, which must never also be dialed by BatchDialer.

A phone is GHL's when it is a warm inbound lead (LendingFlow or the website form; Josh works those
in GHL), or when it was called from GHL inside the lookback window (GHL calls reach
``lending.call_dispositions`` through the GHL call import, ``call_source = 'ghl'``). The dialer load
skips these phones and the dialer sweep takes any already loaded out of BatchDialer.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from config.lending_compliance import GHL_CALLING_LOOKBACK_DAYS

_GHL_CALLING = text(
    "SELECT p FROM unnest(CAST(:phones AS text[])) AS p "
    "WHERE EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.phone = p) "
    "   OR EXISTS (SELECT 1 FROM lending.web_leads w WHERE w.phone = p) "
    "   OR EXISTS (SELECT 1 FROM lending.call_dispositions d "
    "              WHERE d.phone = p AND d.call_source = 'ghl' "
    "                AND (d.direction = 'outbound' OR d.direction IS NULL) "
    "                AND d.call_ended_at > :since)"
)


def ghl_calling_phones(db, phones: Sequence[str], *, now: Optional[datetime] = None) -> set[str]:
    """The subset of ``phones`` (already normalized) that GHL owns. One query for the whole list."""
    if not phones:
        return set()
    since = (now or datetime.now(timezone.utc)) - timedelta(days=GHL_CALLING_LOOKBACK_DAYS)
    return {row[0] for row in db.execute(_GHL_CALLING, {"phones": list(phones), "since": since})}
