"""
Enrichment event consumer (ADR 0016).

Drains gold_lead_scored events from the Lifecycle event bus, batches them by
county_id, and calls run_cascade() for each batch.

Design decisions:
  - Thread-safe buffer deduped by property_id — scoring bursts naturally
    aggregate one wave, giving Tracerfy a real batch.
  - Flush on size OR timer (whichever fires first).
  - A flush failure logs and continues — never crashes the listener thread.
  - Durability: Redis Pub/Sub is transient; the nightly reconciliation batch
    (run_enrichment.py 07:30) is the backstop for dropped events (ADR 0016).

Wired from src/agents/events/ingestion.py::run_forever().
Routed from src/agents/supervisor.py::dispatch_event() for gold_lead_scored.
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class EnrichmentBatcher:
    """
    Thread-safe buffer that aggregates gold_lead_scored event payloads and
    flushes them to run_cascade() in batches.

    Flush triggers (whichever fires first):
      1. Buffer size >= flush_size
      2. flush_seconds since the first un-flushed item arrived

    Timer thread calls _check_timer_flush() every second; stopped cleanly
    via stop().
    """

    def __init__(self, flush_size: int = 200, flush_seconds: int = 120) -> None:
        self._flush_size    = flush_size
        self._flush_seconds = flush_seconds

        # buffer: property_id (str) → event payload dict
        self._buffer: Dict[str, Dict[str, Any]] = {}
        self._first_item_time: Optional[float]  = None
        self._lock = threading.Lock()

        self._stop_event = threading.Event()
        self._timer_thread = threading.Thread(
            target=self._timer_loop,
            daemon=True,
            name="enrichment-batcher-timer",
        )

    def start(self, stop_event: Optional[threading.Event] = None) -> None:
        if stop_event is not None:
            self._stop_event = stop_event
        self._timer_thread.start()
        logger.info(
            "[EnrichmentBatcher] started (flush_size=%d, flush_seconds=%d)",
            self._flush_size, self._flush_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()
        self._timer_thread.join(timeout=5.0)
        self._flush()  # drain on shutdown
        logger.info("[EnrichmentBatcher] stopped")

    def add(self, payload: Dict[str, Any]) -> None:
        """Add a gold_lead_scored event payload; dedups by property_id."""
        pid = str(payload.get("property_id", ""))
        if not pid:
            logger.warning("[EnrichmentBatcher] received payload with no property_id: %s", payload)
            return

        import time
        with self._lock:
            self._buffer[pid] = payload
            if self._first_item_time is None:
                self._first_item_time = time.monotonic()
            should_flush = len(self._buffer) >= self._flush_size

        if should_flush:
            logger.info(
                "[EnrichmentBatcher] size flush triggered (buffer=%d)", len(self._buffer)
            )
            self._flush()

    def _timer_loop(self) -> None:
        import time
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=1.0)
            with self._lock:
                if (
                    self._buffer
                    and self._first_item_time is not None
                    and (time.monotonic() - self._first_item_time) >= self._flush_seconds
                ):
                    should_flush = True
                else:
                    should_flush = False
            if should_flush:
                logger.info("[EnrichmentBatcher] timer flush triggered")
                self._flush()

    def _flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            batch = dict(self._buffer)
            self._buffer.clear()
            self._first_item_time = None

        logger.info("[EnrichmentBatcher] flushing %d events", len(batch))
        try:
            self._run_cascade_for_batch(list(batch.values()))
        except Exception as exc:
            logger.error("[EnrichmentBatcher] cascade flush failed: %s", exc, exc_info=True)

    def _run_cascade_for_batch(self, payloads: list) -> None:
        """
        Resolve owner_ids from property_ids, group by county_id, route each
        county's batch through EnrichmentRouter (Task 6.2) instead of calling
        run_cascade() directly — gates paid-provider spend against the
        rolling spend/revenue ratio before this, the third real call site
        for the cascade (easy to miss since it's the Lifecycle agent runtime, a
        separate process from the two cron-based callers).
        """
        from sqlalchemy import text as sa_text
        from src.core.database import get_db_context
        from src.services.enrichment_router import EnrichmentRouter, LeadRecord

        property_ids = [int(p["property_id"]) for p in payloads]

        # Resolve owner_id and confirm county_id from DB (payload county is advisory)
        with get_db_context() as session:
            rows = session.execute(sa_text("""
                SELECT o.id AS owner_id, o.county_id, o.property_id
                FROM owners o
                WHERE o.property_id = ANY(:pids)
            """), {"pids": property_ids}).fetchall()

        # Group by county_id → list of rows
        by_county: dict[str, list] = defaultdict(list)
        for row in rows:
            cid = row.county_id or "hillsborough"
            by_county[cid].append(row)

        total_hits = 0
        total_cost = 0
        router = EnrichmentRouter()
        for county_id, county_rows in by_county.items():
            logger.info(
                "[EnrichmentBatcher] cascade county=%s owners=%d",
                county_id, len(county_rows),
            )
            lead_records = [
                LeadRecord(property_id=r.property_id, owner_id=r.owner_id, county_id=county_id)
                for r in county_rows
            ]
            try:
                with get_db_context() as session:
                    batch_result = router.fetch_contact_profiles_batch(lead_records, session)
                    session.commit()
                if batch_result["cascade_stats"] is not None:
                    total_hits += batch_result["cascade_stats"].hits
                    total_cost += batch_result["cascade_stats"].total_cost_cents
                else:
                    total_hits += sum(1 for r in batch_result["free_results"].values() if r["found"])
            except Exception as exc:
                logger.error(
                    "[EnrichmentBatcher] cascade failed county=%s: %s",
                    county_id, exc, exc_info=True,
                )

        logger.info(
            "[EnrichmentBatcher] flush done: hits=%d cost_cents=%d",
            total_hits, total_cost,
        )


# Module-level singleton — initialized by run_forever() in ingestion.py.
_batcher: Optional[EnrichmentBatcher] = None


def get_batcher() -> Optional[EnrichmentBatcher]:
    return _batcher


def init_batcher(stop_event: Optional[threading.Event] = None) -> EnrichmentBatcher:
    global _batcher
    from config.settings import get_settings
    settings = get_settings()
    _batcher = EnrichmentBatcher(
        flush_size=settings.enrichment_batch_flush_size,
        flush_seconds=settings.enrichment_batch_flush_seconds,
    )
    _batcher.start(stop_event=stop_event)
    return _batcher
