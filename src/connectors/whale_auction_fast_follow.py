"""
Whale auction fast-follow (HUNTER-02, W2).

A fresh auction win (foreclosure/tax-deed) surfaces as a distressed-
acquisition deed shortly after the sale — the same detection signal
src/connectors/deed_flip_outcomes.py already uses (a subsequent deed on the
property, never case-status text, per ADR 0022): reuses its DISTRESSED_KEYWORDS
vocabulary directly rather than duplicating it.

This does NOT redo entity resolution — H2.7's incremental sweep
(buyer_entity_resolution.run_incremental) already resolves new deeds into
buyer_entities on its own schedule. This connector's job is narrower: when a
distressed-acquisition deed is ALREADY resolved to an entity, re-check that
one entity's whale status immediately, rather than waiting for the next
scheduled full refresh_whale_flags() sweep over the whole table.

Also classifies each rescored entity NEW-BUYER vs REPEAT-BUYER (HUNTER-05,
per the constitution's standing run #2: "winners resolved -> deed-history
matched -> NEW-BUYER / REPEAT-BUYER -> whale-scored"), based on
total_purchase_count == 1 (this auction win is their only recorded
purchase) vs >1. Relies on total_purchase_count already reflecting this
deed -- true when this connector runs after the nightly sweep's portfolio
aggregation step, per crontab.txt's stagger (08:30 sweep, 08:35 fast-follow);
this connector does not re-aggregate itself.

Latency SLA (Hunter's constitution, standing run #2): must reach Cora's
queue within 24h of the event. A deed still unresolved after
STALE_LATENCY_DAYS is logged as a stale-run warning, not silently retried
forever — H2.7 running more often, not this connector, is the actual fix
for a stale queue.

Deliberately plain logging (matching src/tasks/stripe_reconcile.py's
convention), not wired into ScraperRunStats -- that registry's source_type
values are constrained by a CHECK constraint scoped to the existing
scraper/connector catalog; adding a Hunter entry there is a separate,
optional monitoring improvement, not required for this to be correct.

Usage:
    PYTHONPATH=. python -m src.connectors.whale_auction_fast_follow --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.hunter.kill_switch import hunter_halted
from src.connectors.deed_flip_outcomes import DISTRESSED_KEYWORDS
from src.services.whale_detection import refresh_whale_flags

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 2              # re-score window: deeds recorded within roughly the last 48h
STALE_LATENCY_DAYS = 3         # >72h unresolved = flag as stale, per the constitution's latency SLA
STALE_CHECK_WINDOW_DAYS = 10   # how far back to keep EXAMINING deeds for staleness

# STALE_CHECK_WINDOW_DAYS must stay wider than STALE_LATENCY_DAYS -- a deed
# only becomes "stale" once it's older than STALE_LATENCY_DAYS, so if the
# query window were narrower than (or equal to) that threshold, a stale deed
# would already have aged out of the query before it could ever be flagged.
# Confirmed as a real bug via testing: with both windows equal, the stale
# path never fired on a deliberately-constructed stale test case.
assert STALE_CHECK_WINDOW_DAYS > STALE_LATENCY_DAYS


def _find_recent_distressed_acquisitions(
    session: Session, county_id: str, since: date,
) -> list[tuple[int, date]]:
    """Distressed-acquisition deeds recorded since `since`, for this county —
    same DISTRESSED_KEYWORDS vocabulary deed_flip_outcomes.py already uses."""
    keyword_clauses = " OR ".join(f"LOWER(deed_type) LIKE :kw{i}" for i in range(len(DISTRESSED_KEYWORDS)))
    params: dict = {f"kw{i}": f"%{kw}%" for i, kw in enumerate(DISTRESSED_KEYWORDS)}
    params.update({"county_id": county_id, "since": since})
    rows = session.execute(
        text(f"""
            SELECT id, record_date FROM deeds
            WHERE county_id = :county_id
              AND record_date >= :since
              AND deed_type IS NOT NULL
              AND ({keyword_clauses})
            ORDER BY id
        """),
        params,
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _classify_new_vs_repeat(session: Session, entity_ids: list[int]) -> dict[int, str]:
    """NEW-BUYER if total_purchase_count == 1 (this is their only recorded
    purchase), REPEAT-BUYER otherwise. One batched query, not a query per
    entity in the loop that calls this."""
    if not entity_ids:
        return {}
    rows = session.execute(
        text("SELECT id, total_purchase_count FROM buyer_entities WHERE id = ANY(:ids)"),
        {"ids": entity_ids},
    ).fetchall()
    return {r.id: ("NEW-BUYER" if r.total_purchase_count == 1 else "REPEAT-BUYER") for r in rows}


def run_whale_fast_follow(session: Session, county_id: str) -> dict:
    """
    Re-score whale status for any entity that just picked up a fresh
    distressed-acquisition deed. Returns a summary dict for logging/tests.
    """
    # Checked first -- this is a cron-triggered writer (refresh_whale_flags
    # mutates buyer_entities), so an active "STOP Hunter" override must halt
    # it before any read/write, same as the nightly sweep and the backfill
    # script.
    if hunter_halted():
        logger.warning(
            "whale_auction_fast_follow[%s]: Hunter kill switch active -- skipping, no DB mutation.",
            county_id,
        )
        return {"examined": 0, "rescored_entities": 0, "new_buyers": 0, "repeat_buyers": 0,
                "classifications": {}, "stale": 0, "halted": True}

    # Examine the WIDER window so a deed can still be seen once it's past
    # STALE_LATENCY_DAYS -- otherwise it ages out of the query before the
    # staleness check below ever runs on it.
    since = date.today() - timedelta(days=STALE_CHECK_WINDOW_DAYS)
    deeds = _find_recent_distressed_acquisitions(session, county_id, since)

    entity_ids: set[int] = set()
    stale = 0

    for deed_id, record_date in deeds:
        age_days = (date.today() - record_date).days
        entity_id = session.execute(
            text("SELECT buyer_entity_id FROM buyer_entity_links WHERE source_table = 'deeds' AND source_id = :id"),
            {"id": deed_id},
        ).scalar()
        if entity_id is None:
            if age_days > STALE_LATENCY_DAYS:
                stale += 1
            continue
        if age_days <= LOOKBACK_DAYS:
            entity_ids.add(entity_id)
        # else: resolved, but older than the re-score window -- a prior run
        # already re-scored it; nothing new to do here.

    classifications: dict[int, str] = {}
    if entity_ids:
        classifications = _classify_new_vs_repeat(session, list(entity_ids))
        refresh_whale_flags(session, entity_ids=list(entity_ids))

    new_buyers = sum(1 for c in classifications.values() if c == "NEW-BUYER")
    repeat_buyers = sum(1 for c in classifications.values() if c == "REPEAT-BUYER")

    if stale:
        logger.warning(
            "whale_auction_fast_follow[%s]: %d distressed-acquisition deed(s) still unresolved "
            "past the %d-day latency SLA -- H2.7's incremental sweep needs to run more often, "
            "not this connector retrying.",
            county_id, stale, STALE_LATENCY_DAYS,
        )

    logger.info(
        "whale_auction_fast_follow[%s]: examined %d deed(s), re-scored %d entit(y/ies) "
        "(%d new-buyer, %d repeat-buyer), %d stale.",
        county_id, len(deeds), len(entity_ids), new_buyers, repeat_buyers, stale,
    )
    return {
        "examined": len(deeds),
        "rescored_entities": len(entity_ids),
        "new_buyers": new_buyers,
        "repeat_buyers": repeat_buyers,
        "classifications": classifications,
        "stale": stale,
    }


if __name__ == "__main__":
    from src.core.database import get_db_context

    parser = argparse.ArgumentParser()
    parser.add_argument("--county-id", default="hillsborough")
    args = parser.parse_args()

    with get_db_context() as db_session:
        run_whale_fast_follow(db_session, args.county_id)
