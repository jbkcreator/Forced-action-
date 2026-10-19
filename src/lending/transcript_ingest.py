"""T-12 audio fallback: after-call transcript ingestion inside the CDR poller.

BatchDialer offers no live audio stream (no WebSocket in its API); the closest thing is the
call transcript it exposes once a call ends (``GET /cdrs/{id}/transcription``). Each poller
tick takes a small batch of answered, finished calls with no ``transcript_status`` yet,
fetches the transcript and runs the existing call extraction. A transcript not ready yet is
retried on the next tick; a call still without one ``TRANSCRIPT_MAX_WAIT_SECONDS`` after it
ended is marked ``missing`` and never fetched again.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional, Protocol, Sequence

from sqlalchemy import text

from config.lending_dispositions import (
    TRANSCRIPT_DONE,
    TRANSCRIPT_MAX_WAIT_SECONDS,
    TRANSCRIPT_MISSING,
)
from src.lending.dialer_port import DialerRequestError

logger = logging.getLogger(__name__)

NOT_READY = "not_ready"
FETCH_FAILED = "fetch_failed"
EXTRACT_FAILED = "extract_failed"


@dataclass(frozen=True)
class PendingCall:
    dialer_call_id: str
    phone: Optional[str]
    call_ended_at: datetime


class TranscriptSource(Protocol):
    def call_transcript(self, call_record_id: str) -> Optional[str]: ...


Extract = Callable[..., object]
Mark = Callable[[str, str], None]


def _cutoff(now: datetime) -> datetime:
    return now - timedelta(seconds=TRANSCRIPT_MAX_WAIT_SECONDS)


def expire_stale(db, *, now: datetime) -> int:
    """Mark answered calls past the wait cap as missing, in one statement."""
    result = db.execute(text(
        "UPDATE lending.call_dispositions SET transcript_status = :missing, updated_at = now() "
        "WHERE transcript_status IS NULL AND talk_duration_sec > 0 AND call_ended_at < :cutoff"
    ), {"missing": TRANSCRIPT_MISSING, "cutoff": _cutoff(now)})
    return int(result.rowcount or 0)


def fetch_pending(db, *, now: datetime, limit: int) -> list[PendingCall]:
    """Answered, finished calls inside the wait window with no transcript status, oldest first."""
    rows = db.execute(text(
        "SELECT dialer_call_id, phone, call_ended_at FROM lending.call_dispositions "
        "WHERE transcript_status IS NULL AND talk_duration_sec > 0 "
        "AND call_ended_at IS NOT NULL AND call_ended_at >= :cutoff "
        "ORDER BY call_ended_at LIMIT :limit FOR UPDATE SKIP LOCKED"
    ), {"cutoff": _cutoff(now), "limit": limit}).all()
    return [PendingCall(str(r[0]), r[1], r[2]) for r in rows]


def mark_status(db, dialer_call_id: str, status: str) -> None:
    db.execute(text(
        "UPDATE lending.call_dispositions SET transcript_status = :status, updated_at = now() "
        "WHERE dialer_call_id = :id"
    ), {"status": status, "id": dialer_call_id})


def ingest_calls(
    calls: Sequence[PendingCall],
    dialer: TranscriptSource,
    *,
    now: datetime,
    extract: Extract,
    mark: Mark,
) -> Counter:
    """Fetch and extract each call's transcript; one failure never stops the batch."""
    counts: Counter = Counter()
    cutoff = _cutoff(now)
    for call in calls:
        if call.call_ended_at < cutoff:
            mark(call.dialer_call_id, TRANSCRIPT_MISSING)
            counts[TRANSCRIPT_MISSING] += 1
            continue
        try:
            transcript = dialer.call_transcript(call.dialer_call_id)
        except DialerRequestError as exc:
            logger.warning("[transcript-ingest] fetch failed for call %s (status %s)", call.dialer_call_id, exc.status)
            counts[FETCH_FAILED] += 1
            continue
        if not transcript:
            counts[NOT_READY] += 1
            continue
        try:
            extract(dialer_call_id=call.dialer_call_id, phone=call.phone, transcript=transcript, extracted_at=now)
        except Exception as exc:  # class only: the transcript is borrower speech
            logger.error("[transcript-ingest] extraction failed for call %s (%s)", call.dialer_call_id, type(exc).__name__)
            counts[EXTRACT_FAILED] += 1
            continue
        mark(call.dialer_call_id, TRANSCRIPT_DONE)
        counts[TRANSCRIPT_DONE] += 1
    return counts


def run_transcript_ingest(db, dialer: TranscriptSource, *, now: datetime, limit: int) -> Counter:
    """One poller tick: expire stale calls, then ingest one batch. The caller commits."""
    from src.lending.call_extraction_store import extract_and_store_call

    counts: Counter = Counter()
    expired = expire_stale(db, now=now)
    if expired:
        counts[TRANSCRIPT_MISSING] += expired
    counts += ingest_calls(
        fetch_pending(db, now=now, limit=limit), dialer, now=now,
        extract=lambda **kw: extract_and_store_call(db, **kw),
        mark=lambda call_id, status: mark_status(db, call_id, status),
    )
    return counts
