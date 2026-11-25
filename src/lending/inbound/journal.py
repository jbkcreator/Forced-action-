"""Inbound journal (R1 0.5): save every webhook body raw first, then run its handler once.

Usage, in the module that owns the handler (once, at import):

    from src.lending.inbound import journal

    def handle_stage(db, raw: dict) -> Any:      # idempotent; runs inside the journal's transaction, never commits
        ...

    journal.register_source("ghl_stage", handle_stage, label="GHL stage change")

and in the endpoint, after its own 401 / 4xx checks and only while ``journal.journal_enabled()``:

    result = journal.ingest("ghl_stage", dedup_key, body)   # raises only when the raw body could not be saved -> 5xx
    return {"status": result.outcome}                       # processed | duplicate | failed: all answer 200

``ingest`` (1) inserts the raw body into ``lending.inbound_events`` and commits; a repeat of (source, dedup_key)
is a duplicate and its handler is skipped once the first one ran, unless ``rerun_duplicates`` (ghl-stage);
(2) runs the handler in a second transaction that also marks the row processed; (3) when the handler raises, the
body goes to ``webhook_failures`` (the single DLQ, retried every 5 minutes through the same handler) and the row is
marked failed. A row left ``received`` (worker killed between the steps) is run by ``reclaim_stuck`` from the
5-minute ``src.lending.webhook_failures`` cron. Logs carry source, ids and exception classes only: never the body.

Add the module that registers a source to ``HANDLER_MODULES`` so the cron process registers it too.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal, Optional, Protocol

from sqlalchemy import text

from config.settings import get_settings
from src.lending import webhook_failures
from src.lending.db import lending_session

logger = logging.getLogger(__name__)

Handler = Callable[[Any, dict[str, Any]], Any]
Outcome = Literal["processed", "duplicate", "failed"]
Reporter = Callable[[str, dict[str, Any], BaseException], Optional[int]]  # webhook_failures.report_webhook_failure

# Modules that call register_source at import (T3-T6 add theirs); imported by load_handlers() in the cron process.
HANDLER_MODULES: tuple[str, ...] = (
    "src.lending.instantly_webhook",
    "src.lending.booking_webhook",
    "src.lending.inbound.lendingflow",
    "src.lending.reply_webhook",
)

STUCK_AFTER_SECONDS = 120
RECLAIM_BATCH_SIZE = 50
MAX_SOURCE_LENGTH = 40  # webhook_failures.endpoint is String(40)
MAX_DEDUP_KEY_LENGTH = 255

_HANDLERS: dict[str, Handler] = {}


@dataclass(frozen=True)
class IngestResult:
    event_id: int
    outcome: Outcome
    value: Any = None  # what the handler returned (None for a duplicate or a failure)
    failure_id: Optional[int] = None  # the webhook_failures row when the handler failed and was saved


def journal_enabled() -> bool:
    return bool(get_settings().lending_inbound_journal_enabled)


def register_source(source: str, handler: Handler, *, label: str, resend_hint: Optional[str] = None) -> None:
    """Make ``source`` ingestible and replayable: the 5-minute retry runs the same handler and, once it
    succeeds, marks the journal row processed. ``label`` names the event in ops alerts ("LendingFlow lead");
    ``resend_hint`` says what ops does by hand when it is lost (default: re-run the GHL workflow)."""
    if not source or len(source) > MAX_SOURCE_LENGTH:
        raise ValueError(f"source must be 1-{MAX_SOURCE_LENGTH} characters")
    _HANDLERS[source] = handler
    webhook_failures.ENDPOINT_HANDLERS[source] = handler
    webhook_failures.ENDPOINT_LABELS[source] = label
    if resend_hint:
        webhook_failures.ENDPOINT_RESEND_HINTS[source] = resend_hint
    webhook_failures.ON_RESOLVED[source] = mark_processed_for_failure


def load_handlers() -> None:
    for module in HANDLER_MODULES:
        importlib.import_module(module)


# ---- storage --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class EventRow:
    id: int
    source: str
    status: str
    raw: dict[str, Any]


class JournalTx(Protocol):
    db: Any

    def lock(self, event_id: int, *, skip_locked: bool = False) -> Optional[EventRow]: ...

    def mark_processed(self, event_id: int) -> None: ...


class JournalStore(Protocol):
    def insert_raw(self, source: str, dedup_key: str, raw: dict[str, Any]) -> Optional[int]: ...

    def find_id(self, source: str, dedup_key: str) -> int: ...

    def transaction(self) -> Any: ...  # context manager yielding a JournalTx; commits on a clean exit

    def mark_failed(self, event_id: int, failure_id: Optional[int], error_class: str) -> None: ...

    def stuck_ids(self, *, older_than_seconds: int, max_attempts: int, limit: int) -> list[int]: ...


_INSERT = text(
    "INSERT INTO lending.inbound_events (source, dedup_key, raw) VALUES (:source, :key, CAST(:raw AS jsonb)) "
    "ON CONFLICT (source, dedup_key) DO NOTHING RETURNING id"
)
_FIND = text("SELECT id FROM lending.inbound_events WHERE source = :source AND dedup_key = :key")
_LOCK = text("SELECT id, source, status, raw FROM lending.inbound_events WHERE id = :id FOR UPDATE")
_LOCK_SKIP = text("SELECT id, source, status, raw FROM lending.inbound_events WHERE id = :id FOR UPDATE SKIP LOCKED")
_PROCESSED = text(
    "UPDATE lending.inbound_events SET status = 'processed', processed_at = now(), attempts = attempts + 1 "
    "WHERE id = :id"
)
# Not saved to the DLQ (failure_id NULL): the row stays 'received' so reclaim_stuck runs it again.
_FAILED = text(
    "UPDATE lending.inbound_events SET attempts = attempts + 1, last_error_class = :error, "
    "status = CASE WHEN CAST(:failure_id AS integer) IS NULL THEN status ELSE 'failed' END, "
    "failure_id = COALESCE(CAST(:failure_id AS integer), failure_id) WHERE id = :id"
)
_STUCK = text(
    "SELECT id FROM lending.inbound_events WHERE status = 'received' "
    "AND received_at < now() - make_interval(secs => :seconds) AND attempts < :max_attempts "
    "ORDER BY received_at LIMIT :limit"
)
_PROCESSED_BY_FAILURE = text(
    "UPDATE lending.inbound_events SET status = 'processed', processed_at = now() "
    "WHERE failure_id = :failure_id AND status <> 'processed'"
)


class _SqlTx:
    def __init__(self, db):
        self.db = db

    def lock(self, event_id: int, *, skip_locked: bool = False) -> Optional[EventRow]:
        row = self.db.execute(_LOCK_SKIP if skip_locked else _LOCK, {"id": event_id}).first()
        if row is None:
            return None
        raw = row.raw if isinstance(row.raw, dict) else json.loads(row.raw or "{}")
        return EventRow(id=row.id, source=row.source, status=row.status, raw=raw)

    def mark_processed(self, event_id: int) -> None:
        self.db.execute(_PROCESSED, {"id": event_id})


class SqlJournalStore:
    """``lending.inbound_events`` through the lending database; every call is its own transaction."""

    def __init__(self, session_factory: Callable = lending_session):
        self._session = session_factory

    def insert_raw(self, source: str, dedup_key: str, raw: dict[str, Any]) -> Optional[int]:
        with self._session() as db:
            event_id = db.execute(_INSERT, {"source": source, "key": dedup_key,
                                            "raw": json.dumps(raw, default=str)}).scalar()
        return int(event_id) if event_id is not None else None

    def find_id(self, source: str, dedup_key: str) -> int:
        with self._session() as db:
            return int(db.execute(_FIND, {"source": source, "key": dedup_key}).scalar_one())

    @contextmanager
    def transaction(self) -> Iterator[_SqlTx]:
        with self._session() as db:
            yield _SqlTx(db)

    def mark_failed(self, event_id: int, failure_id: Optional[int], error_class: str) -> None:
        with self._session() as db:
            db.execute(_FAILED, {"id": event_id, "failure_id": failure_id, "error": error_class})

    def stuck_ids(self, *, older_than_seconds: int, max_attempts: int, limit: int) -> list[int]:
        with self._session() as db:
            return [int(r.id) for r in db.execute(_STUCK, {"seconds": older_than_seconds,
                                                          "max_attempts": max_attempts, "limit": limit}).all()]


def mark_processed_for_failure(db, failure_id: int) -> None:
    """ON_RESOLVED hook: a webhook_failures replay that succeeded closes its journal row in the same savepoint."""
    db.execute(_PROCESSED_BY_FAILURE, {"failure_id": failure_id})


# ---- the journal ----------------------------------------------------------------------------------------------

def _normalize_key(dedup_key: str) -> str:
    key = str(dedup_key)
    if not key:
        raise ValueError("dedup_key is required")
    if len(key) > MAX_DEDUP_KEY_LENGTH:
        return "sha256:" + hashlib.sha256(key.encode("utf-8")).hexdigest()
    return key


def _handler_for(source: str) -> Handler:
    try:
        return _HANDLERS[source]
    except KeyError:
        raise LookupError(f"inbound source {source!r} is not registered") from None


def ingest(source: str, dedup_key: str, raw: dict[str, Any], *, rerun_duplicates: bool = False,
           store: Optional[JournalStore] = None,
           report: Optional[Reporter] = None) -> IngestResult:
    """Save ``raw`` under (source, dedup_key), then run the source's handler once. Raises only when the raw
    body could not be saved (the endpoint answers 5xx so the sender delivers it again)."""
    _handler_for(source)  # an unregistered source is a programming error: fail before saving anything
    store = store or SqlJournalStore()
    report = report or webhook_failures.report_webhook_failure  # looked up per call so tests can patch it
    key = _normalize_key(dedup_key)
    try:
        event_id = store.insert_raw(source, key, raw)
        is_new = event_id is not None
        if event_id is None:
            event_id = store.find_id(source, key)
    except Exception as exc:  # class only: SQL errors embed bound parameters (the body)
        logger.error("[lending-inbound] %s raw save failed: %s", source, type(exc).__name__)
        raise
    if not is_new:
        logger.info("[lending-inbound] %s #%s delivered again", source, event_id)
    return _process(store, event_id, rerun=rerun_duplicates, skip_locked=False, report=report)


def _process(store: JournalStore, event_id: int, *, rerun: bool, skip_locked: bool,
             report: Reporter) -> IngestResult:
    """Lock the row, run its handler and mark it processed in one transaction. A row already processed (or
    failed and owned by the retry), or held by another worker when ``skip_locked``, is a duplicate."""
    source, raw = "", {}
    try:
        with store.transaction() as tx:
            row = tx.lock(event_id, skip_locked=skip_locked)
            if row is None or (row.status != "received" and not rerun):
                return IngestResult(event_id=event_id, outcome="duplicate")
            source, raw = row.source, row.raw
            value = _handler_for(source)(tx.db, raw)
            tx.mark_processed(event_id)
    except Exception as exc:  # class only
        return _record_failure(store, source, event_id, raw, exc, report)
    logger.info("[lending-inbound] %s #%s processed", source, event_id)
    return IngestResult(event_id=event_id, outcome="processed", value=value)


def _record_failure(store: JournalStore, source: str, event_id: int, raw: dict[str, Any], exc: BaseException,
                    report: Reporter) -> IngestResult:
    error = type(exc).__name__
    logger.error("[lending-inbound] %s #%s handler failed: %s", source, event_id, error)
    failure_id = report(source, raw, exc) if raw else None
    try:
        store.mark_failed(event_id, failure_id, error)
    except Exception as mark_exc:  # the row stays 'received'; reclaim_stuck runs it again
        logger.error("[lending-inbound] %s #%s could not be marked failed: %s", source, event_id,
                     type(mark_exc).__name__)
    return IngestResult(event_id=event_id, outcome="failed", failure_id=failure_id)


@dataclass
class ReclaimResult:
    processed: int = 0
    failed: int = 0
    skipped: int = 0  # taken by another worker, finished meanwhile, or no handler registered


def reclaim_stuck(*, store: Optional[JournalStore] = None,
                  report: Optional[Reporter] = None) -> ReclaimResult:
    """Run each row still ``received`` after STUCK_AFTER_SECONDS (its worker died between save and process)
    once through the normal path. A row another worker holds is skipped (FOR UPDATE SKIP LOCKED)."""
    store = store or SqlJournalStore()
    report = report or webhook_failures.report_webhook_failure
    result = ReclaimResult()
    for event_id in store.stuck_ids(older_than_seconds=STUCK_AFTER_SECONDS,
                                    max_attempts=webhook_failures.MAX_RETRY_ATTEMPTS, limit=RECLAIM_BATCH_SIZE):
        outcome = _process(store, event_id, rerun=False, skip_locked=True, report=report).outcome
        if outcome == "processed":
            result.processed += 1
        elif outcome == "failed":
            result.failed += 1
        else:
            result.skipped += 1
    return result

