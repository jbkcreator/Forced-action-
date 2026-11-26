"""R1 0.6: exhausted is permanent for calling lists.

A phone that reached the total attempt history cap (``MAX_ATTEMPTS_TOTAL`` attempts in
``ATTEMPT_HISTORY_BUSINESS_DAYS`` business days) is marked in ``lending.exhausted_phones``. The mark stays after the
attempts age out of the window, so the phone never reloads from a later list (the spec: never recycle exhausted
contacts). Only a new inbound clears it: a new LendingFlow lead or a website form for that phone. Attempts made before
the clear no longer count, so the person who just asked to hear from us can be called again.

``exhausted_phones`` is the one rule every gate reads (the list-build gate, the dial-time gates, and 1.6's queue
count through its ``blocks=``). Reads never write; marks are written by the after-call hook and the dependency watch.
Does not commit: the caller owns the transaction.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence

from sqlalchemy import text

from config.lending_compliance import ATTEMPT_HISTORY_BUSINESS_DAYS, MAX_ATTEMPTS_TOTAL
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

CLEARED_BY_LENDINGFLOW = "new_lendingflow_lead"
CLEARED_BY_WEBSITE = "new_website_lead"

# Outbound attempts in the window, counted only after the phone's last clear (a new inbound starts it over).
_WINDOW_COUNTS = text("""
    SELECT d.phone, count(*) AS attempts
      FROM lending.call_dispositions d
      LEFT JOIN lending.exhausted_phones x ON x.phone = d.phone
     WHERE d.phone = ANY(:phones)
       AND (d.direction = 'outbound' OR d.direction IS NULL)
       AND d.call_ended_at > GREATEST(CAST(:since AS timestamptz), COALESCE(x.cleared_at, CAST(:since AS timestamptz)))
       AND d.call_ended_at <= :now
     GROUP BY d.phone
""")
_MARKED = text("SELECT phone FROM lending.exhausted_phones WHERE phone = ANY(:phones) AND cleared_at IS NULL")
_MARK = text("""
    INSERT INTO lending.exhausted_phones (phone, exhausted_at, attempts, updated_at)
    VALUES (:phone, :now, :attempts, :now)
    ON CONFLICT (phone) DO UPDATE
       SET exhausted_at = CASE WHEN lending.exhausted_phones.cleared_at IS NULL
                               THEN lending.exhausted_phones.exhausted_at ELSE EXCLUDED.exhausted_at END,
           attempts = EXCLUDED.attempts, cleared_at = NULL, cleared_reason = NULL, updated_at = EXCLUDED.updated_at
""")
_CLEAR = text("""
    UPDATE lending.exhausted_phones SET cleared_at = :now, cleared_reason = :reason, updated_at = :now
     WHERE phone = :phone AND cleared_at IS NULL
""")
# Every phone over the cap right now (the dependency watch marks them, so a call recorded without the after-call
# hook still leaves its mark before the attempts age out).
_OVER_CAP_NOW = text("""
    SELECT d.phone, count(*) AS attempts
      FROM lending.call_dispositions d
      LEFT JOIN lending.exhausted_phones x ON x.phone = d.phone
     WHERE d.phone IS NOT NULL
       AND (d.direction = 'outbound' OR d.direction IS NULL)
       AND d.call_ended_at > GREATEST(CAST(:since AS timestamptz), COALESCE(x.cleared_at, CAST(:since AS timestamptz)))
       AND d.call_ended_at <= :now
       AND (x.phone IS NULL OR x.cleared_at IS NOT NULL)
     GROUP BY d.phone
    HAVING count(*) >= :cap
""")


def business_days_ago(now: datetime, business_days: int) -> datetime:
    """``now`` minus N business days, skipping Sat/Sun: a rolling compliance window, not a holiday calendar."""
    remaining = business_days
    cursor = now
    while remaining > 0:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:
            remaining -= 1
    return cursor


def over_cap(counts: dict[str, int]) -> dict[str, int]:
    return {phone: n for phone, n in counts.items() if n >= MAX_ATTEMPTS_TOTAL}


def window_counts(db, phones: Sequence[str], now: datetime) -> dict[str, int]:
    if not phones:
        return {}
    since = business_days_ago(now, ATTEMPT_HISTORY_BUSINESS_DAYS)
    rows = db.execute(_WINDOW_COUNTS, {"phones": list(phones), "since": since, "now": now}).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def marked(db, phones: Sequence[str]) -> set[str]:
    if not phones:
        return set()
    return {r[0] for r in db.execute(_MARKED, {"phones": list(phones)}).fetchall()}


def exhausted_phones(db, phones: Sequence[str], now: Optional[datetime] = None) -> set[str]:
    """The phones among ``phones`` that are exhausted: over the cap in the window (since their last clear), or
    marked and not cleared since. Read-only."""
    phones = [p for p in phones if p]
    if not phones:
        return set()
    now = now or datetime.now(timezone.utc)
    return set(over_cap(window_counts(db, phones, now))) | marked(db, phones)


def is_exhausted(db, phone: str, now: Optional[datetime] = None) -> bool:
    normalized = normalize_phone(phone) if phone else None
    return bool(normalized) and normalized in exhausted_phones(db, [normalized], now)


def mark(db, counts: dict[str, int], now: Optional[datetime] = None) -> int:
    """Mark these phones exhausted (``counts`` = attempts that put each over the cap). Returns how many."""
    now = now or datetime.now(timezone.utc)
    for phone, attempts in counts.items():
        db.execute(_MARK, {"phone": phone, "now": now, "attempts": attempts})
    return len(counts)


def mark_one(db, phone: str, now: Optional[datetime] = None) -> bool:
    """The after-call hook: mark one phone that just reached the cap. In a savepoint and never raises (the dependency
    watch marks it on its next pass if this fails). Returns True when written."""
    now = now or datetime.now(timezone.utc)
    try:
        with db.begin_nested():
            mark(db, {phone: window_counts(db, [phone], now).get(phone, MAX_ATTEMPTS_TOTAL)}, now)
    except Exception as exc:  # class only: SQL errors embed bound parameters (the phone)
        logger.error("[exhausted] mark failed (%s); the next dependency watch pass marks it", type(exc).__name__)
        return False
    return True


def mark_over_cap(db, now: Optional[datetime] = None) -> int:
    """Mark every phone over the cap right now that is not marked yet (the dependency watch). Returns how many."""
    now = now or datetime.now(timezone.utc)
    since = business_days_ago(now, ATTEMPT_HISTORY_BUSINESS_DAYS)
    rows = db.execute(_OVER_CAP_NOW, {"since": since, "now": now, "cap": MAX_ATTEMPTS_TOTAL}).fetchall()
    return mark(db, {r[0]: int(r[1]) for r in rows}, now)


def clear_for_new_inbound(db, phone: Optional[str], reason: str, now: Optional[datetime] = None) -> bool:
    """A new LendingFlow lead or website form for ``phone``: the person asked to hear from us, so the exhausted mark
    is lifted and earlier attempts stop counting. Runs in a savepoint and never raises, so a lead is never lost to it.
    Returns True when a mark was cleared."""
    normalized = normalize_phone(phone) if phone else None
    if not normalized:
        return False
    try:
        with db.begin_nested():
            cleared = db.execute(_CLEAR, {"phone": normalized, "now": now or datetime.now(timezone.utc),
                                          "reason": reason}).rowcount
    except Exception as exc:  # class only: SQL errors embed bound parameters (the phone)
        logger.error("[exhausted] clear failed (%s); the phone stays exhausted", type(exc).__name__)
        return False
    if cleared:
        logger.info("[exhausted] mark cleared by a new inbound (%s)", reason)
    return bool(cleared)


def phones_of(records: Iterable[dict]) -> list[str]:
    return [p for p in (normalize_phone(r.get("normalized_phone") or r.get("phone") or "") for r in records) if p]
