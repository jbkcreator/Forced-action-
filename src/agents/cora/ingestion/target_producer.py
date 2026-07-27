"""
Target-to-draft pipeline, C2 — the "target.ready" producer.

Sources ONLY Cell #1 (config/cora_cell_grid.py's cell_1_founder_tier_blitz),
the one cell config/cora_cell_grid.py's own docstring says is "exercised
end-to-end at launch" via Hunter's whale_ranking.get_ranked_whales(). Cell
#2 (auction fast-follow) and Cell #3 (win-back) need their own source
queries that don't exist yet in a directly-callable form — deferred, per
the plan, not built here.

An in-process periodic thread, NOT a cron entry — scripts/cron/crontab.txt
is off-limits on this branch (a separate, unreviewed rename branch touches
it). run_periodic() is meant to be started as a background thread from
src.agents.cora.__main__ alongside the worker loop, same process.
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from src.agents.cora import fallback_ranking, queue, store
from src.agents.cora.tools.read_tools import (
    get_buyer_entity_by_opportunity_thread_id,
    get_contact_channel,
    get_ranked_whales,
)

logger = logging.getLogger(__name__)

CELL_ID = "cell_1_founder_tier_blitz"
DEFAULT_INTERVAL_SECONDS = 15 * 60


def _facts_for(ranked_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    observed_at = store.now().isoformat()
    facts = [
        {
            "fact_key": "total_purchase_count",
            "value": ranked_row.get("total_purchase_count"),
            "source_ref": "hunter_whale_ranking",
            "observed_at": observed_at,
            "freshness_class": "whale_snapshot",
        },
        {
            "fact_key": "total_cash_volume",
            "value": str(ranked_row.get("total_cash_volume")),
            "source_ref": "hunter_whale_ranking",
            "observed_at": observed_at,
            "freshness_class": "whale_snapshot",
        },
    ]
    if ranked_row.get("why_now"):
        facts.append({
            "fact_key": "why_now",
            "value": ranked_row["why_now"],
            "source_ref": "hunter_whale_ranking",
            "observed_at": observed_at,
            "freshness_class": "auction_event",
        })
    return facts


def _idempotency_key(opportunity_thread_id: str) -> str:
    # Bucketed by day: at most one target.ready per thread per calendar day
    # regardless of how often the periodic sweep runs, so a 15-min interval
    # doesn't repeatedly re-queue the same still-qualifying whale.
    content_hash = hashlib.sha256(f"{opportunity_thread_id}:{CELL_ID}:{date.today().isoformat()}".encode()).hexdigest()[:16]
    return queue.make_idempotency_key("target.ready", opportunity_thread_id, content_hash)


def produce_targets(db: Session, limit: int = 25, county_id: Optional[str] = None) -> List[str]:
    """Returns the opportunity_thread_ids actually published this pass (skips ones with an active draft already)."""
    ranked = get_ranked_whales(db, limit=limit, county_id=county_id)
    scored = fallback_ranking.rank_targets(ranked)

    produced: List[str] = []
    for row in scored:
        thread_id = row.get("opportunity_thread_id")
        if not thread_id:
            continue
        if store.has_duplicate_actionable_draft(thread_id, CELL_ID):
            continue

        buyer_entity = get_buyer_entity_by_opportunity_thread_id(db, thread_id)
        if buyer_entity is None:
            logger.warning("target_producer: opportunity_thread_id=%s no longer resolvable — skipping", thread_id)
            continue

        contact = get_contact_channel(db, buyer_entity["id"])
        payload = {
            "buyer_entity": buyer_entity,
            "cell_id": CELL_ID,
            "facts_used": _facts_for(row),
            "contact_email": contact.get("email"),
            "contact_phone": contact.get("phone"),
        }
        message_id = queue.publish("target.ready", payload, idempotency_key=_idempotency_key(thread_id))
        if message_id is not None:
            produced.append(thread_id)

    logger.info("target_producer: produced %d target.ready event(s) out of %d ranked", len(produced), len(scored))
    return produced


def run_periodic(stop_event: threading.Event, interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> None:
    from src.core.database import get_db_context

    logger.info("target_producer: starting periodic sweep every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            with get_db_context() as db:
                produce_targets(db)
        except Exception:  # noqa: BLE001
            logger.exception("target_producer: sweep failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("target_producer: stopped")
