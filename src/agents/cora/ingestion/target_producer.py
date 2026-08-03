"""
Target-to-draft pipeline, C2 — the "target.ready" producer.

Sources the founder_tier_blitz cell (config/cora_cell_grid.py) via Hunter's
whale_ranking.get_ranked_whales(), and the auction_fast_follow cell via
read_tools.get_recent_auction_fast_follow_whales() — a read-only reuse of
src.connectors.whale_auction_fast_follow's own detection vocabulary, never
a re-trigger of that connector's write path.

The win_back cell lives in its own module, ingestion/win_back_producer.py,
not here — its population (lapsed subscribers, per
src.services.reactivation_eligibility + src.tasks.reactivation_scheduler's
existing tier3_winback cohort) is subscriber-shaped, not buyer-entity-shaped,
and needs a synthetic opportunity_thread_id + a cross-system double-
messaging guard against src.agents.graphs.reactivation.py's own live
outreach to the same population — different enough machinery that bolting
it onto this file's buyer_entity-shaped helpers would obscure both.

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
    get_recent_auction_fast_follow_whales,
)
from src.services import venture_ladder

logger = logging.getLogger(__name__)

FOUNDER_TIER_BLITZ_CELL_ID = "founder_tier_blitz"
AUCTION_FAST_FOLLOW_CELL_ID = "auction_fast_follow"
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
    if ranked_row.get("latest_auction_deed_date"):
        facts.append({
            "fact_key": "latest_auction_deed_date",
            "value": str(ranked_row["latest_auction_deed_date"]),
            "source_ref": "hunter_whale_auction_fast_follow",
            "observed_at": observed_at,
            "freshness_class": "auction_event",
        })
    return facts


def _idempotency_key(cell_id: str, opportunity_thread_id: str) -> str:
    # Bucketed by day: at most one target.ready per thread+cell per calendar
    # day regardless of how often the periodic sweep runs, so a 15-min
    # interval doesn't repeatedly re-queue the same still-qualifying target.
    content_hash = hashlib.sha256(f"{opportunity_thread_id}:{cell_id}:{date.today().isoformat()}".encode()).hexdigest()[:16]
    return queue.make_idempotency_key("target.ready", opportunity_thread_id, content_hash)


def _produce_from_rows(db: Session, cell_id: str, scored: List[Dict[str, Any]]) -> List[str]:
    produced: List[str] = []
    for row in scored:
        thread_id = row.get("opportunity_thread_id")
        if not thread_id:
            continue
        if store.has_duplicate_actionable_draft(db, thread_id, cell_id):
            continue

        buyer_entity = get_buyer_entity_by_opportunity_thread_id(db, thread_id)
        if buyer_entity is None:
            logger.warning("target_producer: opportunity_thread_id=%s no longer resolvable — skipping", thread_id)
            continue

        contact = get_contact_channel(db, buyer_entity["id"])
        payload = {
            "buyer_entity": buyer_entity,
            "cell_id": cell_id,
            "facts_used": _facts_for(row),
            "contact_email": contact.get("email"),
            "contact_phone": contact.get("phone"),
            # Attached per-row from the target's OWN county, not the call's
            # county_id filter — a fleet-wide sweep (county_id=None) can return
            # targets belonging to different ventures in the same pass, and
            # outreach.py's persist node trusts this rather than falling back to
            # venture #1's key (see store.venture_key_for_county's docstring on
            # why that silent default is a production bug, not a cosmetic one).
            "venture_key": store.venture_key_for_county(db, buyer_entity.get("county_id")),
        }
        message_id = queue.publish("target.ready", payload, idempotency_key=_idempotency_key(cell_id, thread_id))
        if message_id is not None:
            produced.append(thread_id)
    return produced


def _cell_production_limit(db: Session, county_id: Optional[str], cell_id: str, limit: int) -> tuple[int, int]:
    """`limit`, scaled by this call's venture's recorded cell-level auto-double
    (src/services/venture_ladder.py:cell_production_multipliers). Returns
    (scaled_limit, multiplier) so callers can log what happened.

    Scoped by the call's OWN county_id filter, not per-row: a single call is
    one venture's sweep when county_id is given, and DEFAULT_VENTURE_KEY (the
    pre-CL3 single-venture behaviour) when it is not — a mixed fleet-wide batch
    has no single multiplier to apply.

    Wrapped in a savepoint and defaults to no scaling (1x) on any failure —
    e.g. the CL4 migration (venture_ladder_events) not yet applied in this
    environment. A bare try/except without begin_nested() would leave the
    surrounding transaction aborted for every statement after it (the ranked-
    whales query included), so a missing CL4 table would silently stop target
    production rather than just skip the multiplier.
    """
    try:
        with db.begin_nested():
            venture_key = store.venture_key_for_county(db, county_id)
            multiplier = venture_ladder.cell_production_multipliers(db, venture_key).get(cell_id, 1)
        return limit * multiplier, multiplier
    except Exception:  # noqa: BLE001 — a missing multiplier must never block target production
        logger.warning(
            "target_producer: could not resolve the cell production multiplier for "
            "cell=%s county_id=%s — producing at the unscaled limit (%d)",
            cell_id, county_id, limit, exc_info=True,
        )
        return limit, 1


def produce_targets(db: Session, limit: int = 25, county_id: Optional[str] = None) -> List[str]:
    """founder_tier_blitz cell. Returns the opportunity_thread_ids actually published this pass (skips ones with an active draft already)."""
    scaled_limit, multiplier = _cell_production_limit(db, county_id, FOUNDER_TIER_BLITZ_CELL_ID, limit)
    ranked = get_ranked_whales(db, limit=scaled_limit, county_id=county_id)
    scored = fallback_ranking.rank_targets(ranked)
    produced = _produce_from_rows(db, FOUNDER_TIER_BLITZ_CELL_ID, scored)
    logger.info(
        "target_producer: cell=%s produced %d target.ready event(s) out of %d ranked "
        "(limit=%d, cell multiplier=%dx)",
        FOUNDER_TIER_BLITZ_CELL_ID, len(produced), len(scored), scaled_limit, multiplier,
    )
    return produced


def produce_auction_fast_follow_targets(
    db: Session, limit: int = 25, county_id: Optional[str] = None, lookback_days: int = 7,
) -> List[str]:
    """auction_fast_follow cell. Read-only; never triggers whale_auction_fast_follow.py's own write path."""
    scaled_limit, multiplier = _cell_production_limit(db, county_id, AUCTION_FAST_FOLLOW_CELL_ID, limit)
    rows = get_recent_auction_fast_follow_whales(db, lookback_days=lookback_days, limit=scaled_limit, county_id=county_id)
    scored = fallback_ranking.rank_targets(rows)
    produced = _produce_from_rows(db, AUCTION_FAST_FOLLOW_CELL_ID, scored)
    logger.info(
        "target_producer: cell=%s produced %d target.ready event(s) out of %d candidates "
        "(limit=%d, cell multiplier=%dx)",
        AUCTION_FAST_FOLLOW_CELL_ID, len(produced), len(scored), scaled_limit, multiplier,
    )
    return produced


def run_periodic(stop_event: threading.Event, interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> None:
    from src.core.database import get_db_context

    logger.info("target_producer: starting periodic sweep every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            with get_db_context() as db:
                produce_targets(db)
        except Exception:  # noqa: BLE001
            logger.exception("target_producer: founder_tier_blitz sweep failed — will retry next interval")
        try:
            with get_db_context() as db:
                produce_auction_fast_follow_targets(db)
        except Exception:  # noqa: BLE001
            logger.exception("target_producer: auction_fast_follow sweep failed — will retry next interval")
        try:
            from src.agents.cora.ingestion.win_back_producer import produce_win_back_targets
            with get_db_context() as db:
                produce_win_back_targets(db)
        except Exception:  # noqa: BLE001
            logger.exception("target_producer: win_back sweep failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("target_producer: stopped")
