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

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.venture_ladder import AUTO_DOUBLE_CELL_MAX_MULTIPLIER
from src.agents.cora import fallback_ranking, queue, store
from src.agents.cora.tools.read_tools import (
    get_buyer_entity_by_opportunity_thread_id,
    get_contact_channel,
    get_ranked_whales,
    get_recent_auction_fast_follow_whales,
)
from src.agents.contracts import hunter_to_cora
from src.agents.hunter.gating import UNVERIFIED_FLOOR
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
        handoff_input = {
            **row,
            "entity_type": buyer_entity.get("entity_type"),
            "contact_channel": "phone" if contact.get("phone") else ("email" if contact.get("email") else "none"),
            "contact_confidence": contact.get("contact_confidence") or 0,
        }
        try:
            handoff = hunter_to_cora.validate_handoff(handoff_input)
        except ValidationError as exc:
            errors = [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()]
            hunter_to_cora.reject_handoff(db, handoff_input, errors)
            logger.warning("target_producer: thread_id=%s Hunter->Cora handoff rejected: %s", thread_id, errors)
            continue
        if not hunter_to_cora.is_handoff_citable(handoff):
            # is_handoff_citable() fails on either a low confidence_score OR a
            # stale freshness_class (spec §1.1.8) -- report whichever
            # actually failed rather than always blaming confidence, so the
            # handoff_rejections audit row reflects the real reason.
            reasons = []
            if handoff.confidence_score < UNVERIFIED_FLOOR:
                reasons.append(f"confidence_score: {handoff.confidence_score} < {UNVERIFIED_FLOOR} (UNVERIFIED_FLOOR)")
            if handoff.freshness_class == "stale":
                reasons.append("freshness_class: 'stale' -- spec §1.1.8 (stale gold is barred)")
            hunter_to_cora.reject_handoff(db, handoff_input, reasons)
            logger.warning(
                "target_producer: thread_id=%s not citable (confidence=%d freshness=%s) — not surfaced to Cora",
                thread_id, handoff.confidence_score, handoff.freshness_class,
            )
            continue

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


def _cell_multiplier(db: Session, venture_key: str, cell_id: str) -> float:
    """This venture's recorded cell-level auto-double multiplier for `cell_id`
    (src/services/venture_ladder.py:cell_production_multipliers), defaulting
    to 1x on any failure — e.g. the CL4 migration (venture_ladder_events) not
    yet applied in this environment.

    If the cell is currently throttled (LEARN-v2.2 Layer 3), the base
    multiplier is scaled down to THROTTLE_FLOOR_PCT % of normal so the cell
    keeps a thin evidence stream rather than going dark entirely.

    Wrapped in a savepoint: a bare try/except without begin_nested() would
    leave the surrounding transaction aborted for every statement after it
    (the ranked-whales query included), so a missing CL4 table would silently
    stop target production rather than just skip the multiplier.
    """
    from config.cell_allocation import THROTTLE_FLOOR_PCT
    from src.services.cell_allocation import cell_is_throttled

    try:
        with db.begin_nested():
            base: float = venture_ladder.cell_production_multipliers(db, venture_key).get(cell_id, 1)
            if cell_is_throttled(db, venture_key, cell_id):
                floor = THROTTLE_FLOOR_PCT / 100.0
                throttled = max(base * floor, floor)
                logger.info(
                    "target_producer: cell %s/%s is throttled — multiplier %.2f → %.2f",
                    venture_key, cell_id, base, throttled,
                )
                return throttled
            return base
    except Exception:  # noqa: BLE001 — a missing multiplier must never block target production
        logger.warning(
            "target_producer: could not resolve the cell production multiplier for "
            "venture=%s cell=%s — using 1x", venture_key, cell_id, exc_info=True,
        )
        return 1


def _cell_production_limit(db: Session, county_id: Optional[str], cell_id: str, limit: int) -> tuple[float, float]:
    """`limit`, scaled by the ONE venture this call is scoped to.

    Only correct when `county_id` scopes the call to a single venture: the
    scheduled sweep (county_id=None) has no single venture to read a
    multiplier for, and must go through _truncate_scored_rows_per_venture()
    instead, which applies each represented venture's OWN multiplier rather
    than reading DEFAULT_VENTURE_KEY's and calling it done.
    """
    venture_key = store.venture_key_for_county(db, county_id)
    multiplier = _cell_multiplier(db, venture_key, cell_id)
    return limit * multiplier, multiplier


def _truncate_scored_rows_per_venture(
    db: Session, cell_id: str, default_limit: int, scored: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Keep up to `default_limit * multiplier` rows PER VENTURE, in ranked
    order, instead of one multiplier applied to the whole (possibly
    multi-venture) fleet-wide batch.

    The scheduled sweep (run_periodic(), --produce-targets) calls the
    producers with county_id=None — a single shared ranked pool spanning
    every venture's buyer entities, with no per-venture segregation in the
    SQL LIMIT. Reading DEFAULT_VENTURE_KEY's multiplier and applying it to
    that whole pool (the pre-fix behaviour) meant a non-default venture's
    recorded cell auto-double was never read at all. This truncates
    per-venture AFTER ranking instead — each row's venture is resolved from
    its OWN county_id (never the call's), exactly like _produce_from_rows'
    per-row venture_key attribution. `scored` must already be over-fetched
    (see produce_targets/produce_auction_fast_follow_targets) so a
    higher-multiplier venture's rows are not lost to the SQL-level LIMIT
    before this ever sees them.
    """
    venture_cache: Dict[Optional[str], str] = {}
    cap_cache: Dict[str, int] = {}
    kept_count: Dict[str, int] = {}
    kept: List[Dict[str, Any]] = []

    for row in scored:
        county_id = row.get("county_id")
        venture_key = venture_cache.get(county_id)
        if venture_key is None:
            venture_key = store.venture_key_for_county(db, county_id)
            venture_cache[county_id] = venture_key

        cap = cap_cache.get(venture_key)
        if cap is None:
            cap = default_limit * _cell_multiplier(db, venture_key, cell_id)
            cap_cache[venture_key] = cap

        if kept_count.get(venture_key, 0) < cap:
            kept.append(row)
            kept_count[venture_key] = kept_count.get(venture_key, 0) + 1

    return kept


def _fleet_fetch_size(db: Session, cell_id: str, limit: int) -> int:
    """SQL LIMIT for a fleet-wide sweep = sum of every active venture's (limit × multiplier).

    Guarantees the result set is large enough that _truncate_scored_rows_per_venture
    can satisfy each venture's full cap regardless of ranking interleave.
    Falls back to limit × AUTO_DOUBLE_CELL_MAX_MULTIPLIER on any DB error.
    # ponytail: one SELECT + N savepoint reads; upgrade to a single aggregating
    # query if active-venture count grows large enough to matter.
    """
    try:
        rows = db.execute(
            text("SELECT venture_key FROM ventures WHERE is_active = true")
        ).fetchall()
        total = sum(_cell_multiplier(db, r.venture_key, cell_id) for r in rows)
        return max(limit, limit * total)
    except Exception:  # noqa: BLE001
        logger.warning(
            "target_producer: could not compute fleet fetch size for cell=%s — falling back to %dx",
            cell_id, AUTO_DOUBLE_CELL_MAX_MULTIPLIER, exc_info=True,
        )
        return limit * AUTO_DOUBLE_CELL_MAX_MULTIPLIER


def produce_targets(db: Session, limit: int = 25, county_id: Optional[str] = None) -> List[str]:
    """founder_tier_blitz cell. Returns the opportunity_thread_ids actually published this pass (skips ones with an active draft already)."""
    if county_id is not None:
        scaled_limit, multiplier = _cell_production_limit(db, county_id, FOUNDER_TIER_BLITZ_CELL_ID, limit)
        ranked = get_ranked_whales(db, limit=scaled_limit, county_id=county_id)
        scored = fallback_ranking.rank_targets(ranked)
    else:
        # Fleet-wide: over-fetch by the largest possible per-venture
        # multiplier, then truncate per-venture below — see
        # _truncate_scored_rows_per_venture's docstring.
        ranked = get_ranked_whales(db, limit=_fleet_fetch_size(db, FOUNDER_TIER_BLITZ_CELL_ID, limit), county_id=None)
        scored = _truncate_scored_rows_per_venture(
            db, FOUNDER_TIER_BLITZ_CELL_ID, limit, fallback_ranking.rank_targets(ranked)
        )

    produced = _produce_from_rows(db, FOUNDER_TIER_BLITZ_CELL_ID, scored)
    logger.info(
        "target_producer: cell=%s produced %d target.ready event(s) out of %d ranked",
        FOUNDER_TIER_BLITZ_CELL_ID, len(produced), len(scored),
    )
    return produced


def produce_auction_fast_follow_targets(
    db: Session, limit: int = 25, county_id: Optional[str] = None, lookback_days: int = 7,
) -> List[str]:
    """auction_fast_follow cell. Read-only; never triggers whale_auction_fast_follow.py's own write path."""
    if county_id is not None:
        scaled_limit, multiplier = _cell_production_limit(db, county_id, AUCTION_FAST_FOLLOW_CELL_ID, limit)
        rows = get_recent_auction_fast_follow_whales(
            db, lookback_days=lookback_days, limit=scaled_limit, county_id=county_id
        )
        scored = fallback_ranking.rank_targets(rows)
    else:
        rows = get_recent_auction_fast_follow_whales(
            db, lookback_days=lookback_days, limit=_fleet_fetch_size(db, AUCTION_FAST_FOLLOW_CELL_ID, limit), county_id=None
        )
        scored = _truncate_scored_rows_per_venture(
            db, AUCTION_FAST_FOLLOW_CELL_ID, limit, fallback_ranking.rank_targets(rows)
        )

    produced = _produce_from_rows(db, AUCTION_FAST_FOLLOW_CELL_ID, scored)
    logger.info(
        "target_producer: cell=%s produced %d target.ready event(s) out of %d candidates",
        AUCTION_FAST_FOLLOW_CELL_ID, len(produced), len(scored),
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
