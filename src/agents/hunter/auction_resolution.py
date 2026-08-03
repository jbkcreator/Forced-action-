"""
Hunter — direct auction-winner-to-buyer-entity resolution (HUNTER-05).

Scoped to TaxDeedAuction.sold_to only -- Foreclosure.sold_to carries no
identity to resolve (see the scraper prompt in
src/scrappers/foreclosures/foreclosure_engine.py: it's a fixed 2-category
label, "Plaintiff" or "3rd Party Bidder", never a real bidder name). This is
the permanent design, not a stopgap -- foreclosure winners are served
indefinitely by the existing grantee-on-subsequent-deed proxy
(whale_auction_fast_follow.py).

No fuzzy name-only matching: TaxDeedAuction has no buyer mailing address to
corroborate a fuzzy score, and buyer_entity_resolution.score_candidate_pair's
own docstring documents real wrongly-auto-matched pairs from name similarity
alone that were only caught by an address-disagreement gate this data can't
use. Resolution is therefore exact-normalized-name-only:
  - exactly one existing buyer_entities row matches -> attach (verified)
  - more than one matches -> leave unresolved (ambiguous), never guess
  - none match -> create ONE new low-confidence provisional entity, with its
    canonical_name stored in NORMALIZED form so a later appearance of the
    same winner (this run or a future one) finds it via the same exact-match
    step -- this is what prevents the same LLC re-winning auctions from
    spawning a new entity every time. Every auction row in the current batch
    sharing one normalized name is resolved together in one decision, which
    also prevents intra-batch duplicates.

Tracks buyer_resolution_status separately from "processed" -- reaching
'provisional' satisfies the <24h processing SLA without asserting the
winner's identity is verified; see find_stale_unresolved_auctions.

Usage:
    PYTHONPATH=. python -m src.agents.hunter.auction_resolution --county-id hillsborough
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import NamedTuple, Optional

from sqlalchemy import insert, text
from sqlalchemy.orm import Session

from src.agents.hunter.gating import UNVERIFIED_FLOOR, verification_status
from src.agents.hunter.kill_switch import hunter_halted
from src.core.models import BuyerEntity, BuyerEntityLink
from src.loaders.base import BaseLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Below gating.UNVERIFIED_FLOOR -- a provisional entity created from an
# auction win alone must never surface in a Lifecycle draft until corroborated.
PROVISIONAL_CONFIDENCE = 50
assert PROVISIONAL_CONFIDENCE < UNVERIFIED_FLOOR

STALE_LATENCY_DAYS = 1          # >24h unresolved = flag as stale, per Hunter's constitution
STALE_CHECK_WINDOW_DAYS = 10    # must stay wider than STALE_LATENCY_DAYS, same reasoning as
                                 # whale_auction_fast_follow.py's own assert on this pair
assert STALE_CHECK_WINDOW_DAYS > STALE_LATENCY_DAYS

_LLC_TOKENS = ("LLC", "INC", "INCORPORATED", "CORP", "CORPORATION")
_TRUST_TOKENS = ("TRUST",)


class AuctionWin(NamedTuple):
    id: int
    sold_to: str
    normalized_name: str


def _guess_entity_type(normalized_name: str) -> str:
    """Best-effort legal-structure guess from the winner's name text alone --
    a tax deed auction result carries no owner_type hint the way an owners
    row does. Defaults to 'Individual', the same fallback
    buyer_entity_resolution.entity_type_for_cluster uses for deed-only
    clusters with no type hint of their own."""
    tokens = set(normalized_name.split())
    if tokens & set(_LLC_TOKENS):
        return "LLC"
    if tokens & set(_TRUST_TOKENS):
        return "Trust"
    return "Individual"


def _fetch_unresolved_wins(session: Session, county_id: str) -> list[AuctionWin]:
    rows = session.execute(
        text("""
            SELECT id, sold_to
            FROM tax_deed_auctions
            WHERE county_id = :county_id
              AND sold_to IS NOT NULL
              AND buyer_resolution_status IS NULL
        """),
        {"county_id": county_id},
    ).fetchall()
    wins = []
    for r in rows:
        normalized = BaseLoader.normalize_owner_name(r.sold_to)
        if normalized:
            wins.append(AuctionWin(id=r.id, sold_to=r.sold_to, normalized_name=normalized))
    return wins


def _load_existing_normalized_index(session: Session) -> dict[str, list[int]]:
    """normalized canonical_name -> every buyer_entities.id sharing it.
    Loads the full id/canonical_name set -- the same thing
    buyer_entity_resolution.load_existing_entity_candidates already does for
    every incremental run, an established pattern in this codebase, not new
    overhead introduced here."""
    rows = session.execute(text("SELECT id, canonical_name FROM buyer_entities")).fetchall()
    index: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        normalized = BaseLoader.normalize_owner_name(r.canonical_name)
        if normalized:
            index[normalized].append(r.id)
    return index


def resolve_tax_deed_winners(session: Session, county_id: str) -> dict:
    """
    Resolve every unprocessed TaxDeedAuction.sold_to for county_id against
    buyer_entities. Returns a summary dict for logging/tests.
    """
    if hunter_halted():
        logger.warning("auction_resolution[%s]: Hunter kill switch active -- skipping, no DB mutation.", county_id)
        return {"examined": 0, "verified": 0, "provisional": 0, "ambiguous": 0, "halted": True}

    wins = _fetch_unresolved_wins(session, county_id)
    if not wins:
        return {"examined": 0, "verified": 0, "provisional": 0, "ambiguous": 0, "halted": False}

    existing_index = _load_existing_normalized_index(session)

    wins_by_name: dict[str, list[AuctionWin]] = defaultdict(list)
    for win in wins:
        wins_by_name[win.normalized_name].append(win)

    status_by_auction_id: dict[int, str] = {}
    verified = provisional = ambiguous = 0

    for normalized_name, name_wins in wins_by_name.items():
        auction_ids = [w.id for w in name_wins]
        matches = existing_index.get(normalized_name, [])

        if len(matches) == 1:
            entity_id = matches[0]
            # One link per auction row sharing this name, not just the first
            # -- every win must stay traceable to its own source row (the
            # BuyerEntityLink uniqueness is on (source_table, source_id), so
            # a distinct auction id per row is required, not optional).
            session.execute(insert(BuyerEntityLink).values([
                {
                    "buyer_entity_id": entity_id, "source_table": "tax_deed_auctions", "source_id": win.id,
                    "match_confidence": 100, "match_method": "exact_name_only",
                }
                for win in name_wins
            ]))
            for aid in auction_ids:
                status_by_auction_id[aid] = "verified"
            verified += len(auction_ids)

        elif len(matches) > 1:
            logger.warning(
                "auction_resolution[%s]: %r matches %d existing entities (ids=%s) -- ambiguous, not guessing.",
                county_id, name_wins[0].sold_to, len(matches), matches,
            )
            for aid in auction_ids:
                status_by_auction_id[aid] = "ambiguous"
            ambiguous += len(auction_ids)

        else:
            entity = BuyerEntity(
                canonical_name=normalized_name,
                entity_type=_guess_entity_type(normalized_name),
                confidence_score=PROVISIONAL_CONFIDENCE,
                verification_status=verification_status(PROVISIONAL_CONFIDENCE),
                county_id=county_id,
            )
            session.add(entity)
            session.flush()  # need entity.id for the links below
            session.execute(insert(BuyerEntityLink).values([
                {
                    "buyer_entity_id": entity.id, "source_table": "tax_deed_auctions", "source_id": win.id,
                    "match_confidence": PROVISIONAL_CONFIDENCE, "match_method": "auction_name_only_unverified",
                }
                for win in name_wins
            ]))
            existing_index[normalized_name].append(entity.id)  # dedup within this same run too
            for aid in auction_ids:
                status_by_auction_id[aid] = "provisional"
            provisional += len(auction_ids)

    session.execute(
        text("UPDATE tax_deed_auctions SET buyer_resolution_status = :status WHERE id = :id"),
        [{"id": aid, "status": status} for aid, status in status_by_auction_id.items()],
    )
    session.commit()

    logger.info(
        "auction_resolution[%s]: examined %d win(s) -- %d verified, %d provisional, %d ambiguous.",
        county_id, len(wins), verified, provisional, ambiguous,
    )
    return {"examined": len(wins), "verified": verified, "provisional": provisional, "ambiguous": ambiguous, "halted": False}


def find_stale_unresolved_auctions(session: Session, county_id: str) -> list[dict]:
    """
    Auction wins still unprocessed (buyer_resolution_status IS NULL) past the
    24h processing SLA -- mirrors whale_auction_fast_follow.py's
    STALE_LATENCY_DAYS/STALE_CHECK_WINDOW_DAYS pattern, including its
    wider-check-window-than-latency-floor requirement (a real bug caught
    there when the two were equal -- a stale row aged out of the query
    before the staleness check ever ran on it). Reaching 'provisional' or
    'ambiguous' satisfies this SLA -- only a still-NULL status counts as stale.
    """
    since = date.today() - timedelta(days=STALE_CHECK_WINDOW_DAYS)
    rows = session.execute(
        text("""
            SELECT id, auction_date, sold_to
            FROM tax_deed_auctions
            WHERE county_id = :county_id
              AND sold_to IS NOT NULL
              AND buyer_resolution_status IS NULL
              AND auction_date >= :since
        """),
        {"county_id": county_id, "since": since},
    ).fetchall()

    stale = [
        {"id": r.id, "auction_date": r.auction_date, "sold_to": r.sold_to}
        for r in rows
        if (date.today() - r.auction_date).days > STALE_LATENCY_DAYS
    ]
    if stale:
        logger.warning(
            "auction_resolution[%s]: %d tax deed auction win(s) still unresolved past the %d-day "
            "processing SLA -- resolve_tax_deed_winners needs to run more often, not this check retrying.",
            county_id, len(stale), STALE_LATENCY_DAYS,
        )
    return stale


def main() -> int:
    from src.core.database import get_db_context

    parser = argparse.ArgumentParser(description="Resolve tax deed auction winners to buyer entities")
    parser.add_argument("--county-id", default="hillsborough")
    args = parser.parse_args()

    with get_db_context() as session:
        stats = resolve_tax_deed_winners(session, args.county_id)
        stale = find_stale_unresolved_auctions(session, args.county_id)
    print({**stats, "stale": len(stale)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
