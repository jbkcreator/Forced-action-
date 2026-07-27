"""
Cora's cell-grid taxonomy — offer x avenue x angle.

Static config, same shape/spirit as config/revenue_ladder.py's REVENUE_LADDER:
a plain list of dicts, no DB table. A cell is a stable, addressable
combination of (offer, avenue, angle) that every draft is tagged with.
CELL_GRID is the lookup Cora's validation/drafting code resolves cell_id
against — the taxonomy is the single source of truth, nothing re-declares
these strings elsewhere.

Only cell_1_founder_tier_blitz is exercised end-to-end at launch (Hunter's
whale_ranking.get_ranked_whales() feeds it directly). cell_2/cell_3 entries
exist and validate now so the taxonomy doesn't need reshaping when their
producers (auction fast-follow, win-back sweep) come online.
"""
from __future__ import annotations

from typing import Optional, TypedDict


class Cell(TypedDict):
    cell_id: str
    offer: str
    avenue: str
    angle: str
    label: str
    source_cell: Optional[str]  # which launch cell (Cell #1/#2/#3) this belongs to


OFFERS = (
    "core_subscription",
    "lead_packs",
    "insurance_distress_pack",
    "bankruptcy_alert",
    "hard_money_intro",
    "founder_tier",
)

AVENUES = (
    "flippers",
    "buy_and_hold",
    "wholesalers",
    "lender_types",
)

ANGLES = (
    "scarcity_seat_number",       # founder-tier numbered-seat framing
    "why_now_catalyst",           # fresh filing/lien/auction date framing
    "portfolio_recognition",      # "we noticed your N purchases" framing
    "auction_congrats",           # Cell #2 fast-follow framing
    "win_back_offer",             # Cell #3 lapsed/abandoned framing
)

CELL_GRID: dict[str, Cell] = {
    "cell_1_founder_tier_blitz": {
        "cell_id": "cell_1_founder_tier_blitz",
        "offer": "founder_tier",
        "avenue": "flippers",
        "angle": "scarcity_seat_number",
        "label": "Founder-Tier Blitz — numbered seats, whale entities",
        "source_cell": "cell_1",
    },
    "cell_1_founder_tier_blitz_bh": {
        "cell_id": "cell_1_founder_tier_blitz_bh",
        "offer": "founder_tier",
        "avenue": "buy_and_hold",
        "angle": "portfolio_recognition",
        "label": "Founder-Tier Blitz — numbered seats, buy-and-hold whales",
        "source_cell": "cell_1",
    },
    "cell_2_auction_fast_follow": {
        "cell_id": "cell_2_auction_fast_follow",
        "offer": "core_subscription",
        "avenue": "flippers",
        "angle": "auction_congrats",
        "label": "Auction-winner fast-follow — congrats + next month's auctions",
        "source_cell": "cell_2",
    },
    "cell_3_win_back": {
        "cell_id": "cell_3_win_back",
        "offer": "core_subscription",
        "avenue": "wholesalers",
        "angle": "win_back_offer",
        "label": "Win-back sweep — lapsed subs / abandoned checkouts",
        "source_cell": "cell_3",
    },
    "hard_money_intro_lenders": {
        "cell_id": "hard_money_intro_lenders",
        "offer": "hard_money_intro",
        "avenue": "lender_types",
        "angle": "why_now_catalyst",
        "label": "Hard-money intro — lead handoff only, no fee mechanics (RESPA-gated)",
        "source_cell": None,
    },
}


def get_cell(cell_id: str) -> Optional[Cell]:
    return CELL_GRID.get(cell_id)


def is_valid_cell(cell_id: str) -> bool:
    return cell_id in CELL_GRID
