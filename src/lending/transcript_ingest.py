"""T-12 audio fallback: after-call transcript ingestion in its own worker (``transcript_worker``).

BatchDialer offers no live audio stream (no WebSocket in its API); the closest thing is the
call transcript it exposes once a call ends (``GET /cdrs/{id}/transcription``). Each worker
tick takes a small batch of answered, finished calls still pending, fetches the transcript and
runs the existing call extraction.

- Not ready yet / fetch error: ``transcript_attempted_at`` is stamped and the call waits
  ``TRANSCRIPT_RETRY_SECONDS``; never-tried calls go first, so stuck calls cannot block new ones.
- Rate limited (HTTP 429): the batch stops at once.
- AI spending paused (vendor cost pause): the transcript is still fetched, and the call is marked
  ``deferred`` without a Claude call; deferred calls are extracted once the pause ends.
- Temporary API error (connection, timeout, rate limit, 5xx): ``deferred`` too, retried after
  ``TRANSCRIPT_RETRY_SECONDS`` up to ``TRANSCRIPT_MAX_EXTRACT_ATTEMPTS`` times, then ``failed``.
- Invalid extraction output: ``failed`` and never retried (each try is a paid Claude call).
- Still pending ``TRANSCRIPT_MAX_WAIT_SECONDS`` after the call ended: marked ``missing``.

The Claude call runs outside the savepoint that stores the result, so the usage record of a billed
answer survives when that answer is rejected. The transcript worker's advisory lock guarantees one
consumer, so no row locks are held across the HTTP fetch and the Claude call.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, Protocol, Sequence

from sqlalchemy import text

from config.lending_dispositions import (
    TRANSCRIPT_DEFERRED,
    TRANSCRIPT_DONE,
    TRANSCRIPT_EXTRACT_RETRY,
    TRANSCRIPT_FAILED,
    TRANSCRIPT_FETCH_FAILED,
    TRANSCRIPT_MAX_EXTRACT_ATTEMPTS,
    TRANSCRIPT_MAX_WAIT_SECONDS,
    TRANSCRIPT_MISSING,
    TRANSCRIPT_NOT_READY,
    TRANSCRIPT_PAUSED,
    TRANSCRIPT_PENDING_PREDICATE,
    TRANSCRIPT_RATE_LIMITED,
    TRANSCRIPT_RETRY_SECONDS,
)
from src.lending.dialer_port import DialerRequestError
from src.lending.models import LENDING_SCHEMA
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

_TABLE = f"{LENDING_SCHEMA}.call_dispositions"
_HTTP_TOO_MANY_REQUESTS = 429


@dataclass(frozen=True)
class PendingCall:
    dialer_call_id: str
    phone: Optional[str]
    call_ended_at: datetime


class TranscriptSource(Protocol):
    def call_transcript(self, call_record_id: str) -> Optional[str]: ...


class ExtractFields(Protocol):
    def __call__(self, transcript: str, *, today: date, db) -> object: ...


class SaveExtraction(Protocol):
    def __call__(self, db, *, dialer_call_id: str, phone: Optional[str], extraction: object,
                 extracted_at: datetime) -> None: ...


class CallStore(Protocol):
    def extraction_paused(self) -> bool: ...
    def extract_and_mark_done(self, call: PendingCall, transcript: str, *, now: datetime) -> None: ...
    def mark_failed(self, dialer_call_id: str) -> None: ...
    def mark_deferred(self, dialer_call_id: str, *, now: datetime, count_attempt: bool) -> None: ...
    def mark_attempted(self, dialer_call_ids: Sequence[str], *, now: datetime) -> None: ...


def is_temporary_api_error(exc: BaseException) -> bool:
    """Errors worth a bounded retry: the request produced no billed answer."""
    import anthropic

    if isinstance(exc, (anthropic.APIConnectionError, anthropic.RateLimitError, anthropic.InternalServerError)):
        return True  # APITimeoutError is an APIConnectionError
    return isinstance(exc, anthropic.APIStatusError) and exc.status_code >= 500


def extraction_paused(db) -> bool:
    """True while a vendor cost pause blocks the call-extraction Claude task."""
    from src.lending.call_extraction import EXTRACTION_TASK_TYPE
    from src.services.vendor_cost_attribution import resolve_pause_target
    from src.services.vendor_cost_pause_service import get_active_pause

    target = resolve_pause_target(task_type=EXTRACTION_TASK_TYPE)
    return bool(target and get_active_pause(db, "claude", target))


def _cutoff(now: datetime) -> datetime:
    return now - timedelta(seconds=TRANSCRIPT_MAX_WAIT_SECONDS)


def expire_stale(db, *, now: datetime) -> int:
    """Mark answered calls past the wait cap as missing, in one statement."""
    result = db.execute(text(
        f"UPDATE {_TABLE} SET transcript_status = :missing, updated_at = now() "
        f"WHERE {TRANSCRIPT_PENDING_PREDICATE} AND call_ended_at < :cutoff"
    ), {"missing": TRANSCRIPT_MISSING, "cutoff": _cutoff(now)})
    return int(result.rowcount or 0)


def fetch_pending(db, *, now: datetime, limit: int, include_deferred: bool = True) -> list[PendingCall]:
    """Pending calls inside the wait window plus (unless paused) deferred calls, never-tried first,
    skipping ones tried just now. Deferred calls have no wait window: their transcript already arrived."""
    rows = db.execute(text(
        f"SELECT dialer_call_id, phone, call_ended_at FROM {_TABLE} "
        f"WHERE (({TRANSCRIPT_PENDING_PREDICATE} AND call_ended_at IS NOT NULL AND call_ended_at >= :cutoff) "
        "OR (:include_deferred AND transcript_status = :deferred)) "
        "AND (transcript_attempted_at IS NULL OR transcript_attempted_at < :retry_before) "
        "ORDER BY transcript_attempted_at NULLS FIRST, call_ended_at LIMIT :limit"
    ), {"cutoff": _cutoff(now), "retry_before": now - timedelta(seconds=TRANSCRIPT_RETRY_SECONDS),
        "deferred": TRANSCRIPT_DEFERRED, "include_deferred": include_deferred, "limit": limit}).all()
    return [PendingCall(str(r[0]), normalize_phone(r[1] or "") or None, r[2]) for r in rows]


def _set_status(db, dialer_call_id: str, status: str) -> None:
    db.execute(text(
        f"UPDATE {_TABLE} SET transcript_status = :status, updated_at = now() WHERE dialer_call_id = :id"
    ), {"status": status, "id": dialer_call_id})


class DbCallStore:
    """Writes through ``db``. Each finished call commits on its own (a deliberate exception to
    batch writes): an extraction is a paid Claude call, and a later error in the batch must never
    roll it back and cause it to be billed again. The Claude call runs before the savepoint, so its
    usage record is never rolled back with a rejected answer."""

    def __init__(self, db, *, extract: ExtractFields, save: SaveExtraction) -> None:
        self._db, self._extract, self._save = db, extract, save

    def extraction_paused(self) -> bool:
        return extraction_paused(self._db)

    def extract_and_mark_done(self, call: PendingCall, transcript: str, *, now: datetime) -> None:
        from src.lending.first_contact import LENDING_TIMEZONE

        call_day = call.call_ended_at.astimezone(LENDING_TIMEZONE).date()
        extraction = self._extract(transcript, today=call_day, db=self._db)
        with self._db.begin_nested():
            self._save(self._db, dialer_call_id=call.dialer_call_id, phone=call.phone,
                       extraction=extraction, extracted_at=now)
            _set_status(self._db, call.dialer_call_id, TRANSCRIPT_DONE)
        self._db.commit()

    def mark_failed(self, dialer_call_id: str) -> None:
        _set_status(self._db, dialer_call_id, TRANSCRIPT_FAILED)
        self._db.commit()  # also keeps the usage record of a billed answer that was rejected

    def mark_deferred(self, dialer_call_id: str, *, now: datetime, count_attempt: bool) -> None:
        """Wait for a later cycle; the last allowed temporary error marks the call failed."""
        self._db.execute(text(
            f"UPDATE {_TABLE} SET transcript_attempts = transcript_attempts + :inc, "
            "transcript_status = CASE WHEN transcript_attempts + :inc >= :max THEN :failed ELSE :deferred END, "
            "transcript_attempted_at = :now, updated_at = now() WHERE dialer_call_id = :id"
        ), {"inc": 1 if count_attempt else 0, "max": TRANSCRIPT_MAX_EXTRACT_ATTEMPTS, "failed": TRANSCRIPT_FAILED,
            "deferred": TRANSCRIPT_DEFERRED, "now": now, "id": dialer_call_id})
        self._db.commit()

    def mark_attempted(self, dialer_call_ids: Sequence[str], *, now: datetime) -> None:
        if not dialer_call_ids:
            return
        self._db.execute(text(
            f"UPDATE {_TABLE} SET transcript_attempted_at = :now, updated_at = now() "
            "WHERE dialer_call_id = ANY(:ids)"
        ), {"now": now, "ids": list(dialer_call_ids)})


def _extract_one(call: PendingCall, transcript: str, store: CallStore, *, now: datetime,
                 counts: Counter) -> bool:
    """Extract one call; returns True when a vendor cost pause is now active."""
    try:
        store.extract_and_mark_done(call, transcript, now=now)
    except Exception as exc:  # class only: the transcript is borrower speech
        if is_temporary_api_error(exc):
            logger.warning("[transcript-ingest] temporary extraction error for call %s (%s); deferred",
                           call.dialer_call_id, type(exc).__name__)
            store.mark_deferred(call.dialer_call_id, now=now, count_attempt=True)
            counts[TRANSCRIPT_EXTRACT_RETRY] += 1
            return False
        if store.extraction_paused():  # the pause started during this batch
            store.mark_deferred(call.dialer_call_id, now=now, count_attempt=False)
            counts[TRANSCRIPT_PAUSED] += 1
            return True
        logger.error("[transcript-ingest] extraction failed for call %s (%s); marked failed",
                     call.dialer_call_id, type(exc).__name__)
        store.mark_failed(call.dialer_call_id)
        counts[TRANSCRIPT_FAILED] += 1
        return False
    counts[TRANSCRIPT_DONE] += 1
    return False


def ingest_calls(calls: Sequence[PendingCall], dialer: TranscriptSource, store: CallStore, *,
                 now: datetime) -> Counter:
    """Fetch and extract each call's transcript; only a rate limit stops the batch."""
    counts: Counter = Counter()
    attempted: list[str] = []
    paused = store.extraction_paused() if calls else False
    for call in calls:
        try:
            transcript = dialer.call_transcript(call.dialer_call_id)
        except DialerRequestError as exc:
            attempted.append(call.dialer_call_id)
            if exc.status == _HTTP_TOO_MANY_REQUESTS:
                logger.warning("[transcript-ingest] rate limited by the dialer; stopping this batch")
                counts[TRANSCRIPT_RATE_LIMITED] += 1
                break
            logger.warning("[transcript-ingest] fetch failed for call %s (status %s)", call.dialer_call_id, exc.status)
            counts[TRANSCRIPT_FETCH_FAILED] += 1
            continue
        if not transcript:
            attempted.append(call.dialer_call_id)
            counts[TRANSCRIPT_NOT_READY] += 1
            continue
        if paused:
            store.mark_deferred(call.dialer_call_id, now=now, count_attempt=False)
            counts[TRANSCRIPT_PAUSED] += 1
            continue
        paused = _extract_one(call, transcript, store, now=now, counts=counts)
    store.mark_attempted(attempted, now=now)
    return counts


def run_transcript_ingest(db, dialer: TranscriptSource, *, now: datetime, limit: int,
                          extract: Optional[ExtractFields] = None,
                          save: Optional[SaveExtraction] = None) -> Counter:
    """One worker cycle: expire stale calls, then ingest one batch. The caller commits the rest."""
    if extract is None:
        from src.lending.call_extraction import extract_call_fields as extract
    if save is None:
        from src.lending.call_extraction_store import save_call_extraction as save

    counts: Counter = Counter()
    expired = expire_stale(db, now=now)
    if expired:
        counts[TRANSCRIPT_MISSING] += expired
    calls = fetch_pending(db, now=now, limit=limit, include_deferred=not extraction_paused(db))
    db.commit()
    counts += ingest_calls(calls, dialer, DbCallStore(db, extract=extract, save=save), now=now)
    return counts
