"""
Cora's cell-grid taxonomy — offer x avenue x angle.

Static config, same shape/spirit as config/revenue_ladder.py's REVENUE_LADDER:
a plain list of dicts, no DB table. A cell is a stable, addressable
combination of (offer, avenue, angle) that every draft is tagged with.
CELL_GRID is the lookup Cora's validation/drafting code resolves cell_id
against — the taxonomy is the single source of truth, nothing re-declares
these strings elsewhere.

Cell IDs are named for what they are, not for their position in the
originating brief (that document numbered them Cell #1/#2/#3 — a section
label, not a naming scheme).

Only founder_tier_blitz is exercised end-to-end at launch (Hunter's
whale_ranking.get_ranked_whales() feeds it directly). auction_fast_follow/
win_back entries exist and validate now so the taxonomy doesn't need
reshaping when their producers come online.
"""
from __future__ import annotations

from typing import Optional, TypedDict


class Cell(TypedDict):
    cell_id: str
    offer: str
    avenue: str
    angle: str
    label: str
    launch_group: Optional[str]  # groups avenue variants of the same launch cohort, e.g. founder_tier_blitz + founder_tier_blitz_bh


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
    "auction_congrats",           # auction-winner fast-follow framing
    "win_back_offer",             # lapsed/abandoned win-back framing
    "post_call_recap",            # follow-up after a completed call
)

CELL_GRID: dict[str, Cell] = {
    "founder_tier_blitz": {
        "cell_id": "founder_tier_blitz",
        "offer": "founder_tier",
        "avenue": "flippers",
        "angle": "scarcity_seat_number",
        "label": "Founder-Tier Blitz — numbered seats, whale entities",
        "launch_group": "founder_tier_blitz",
    },
    "founder_tier_blitz_bh": {
        "cell_id": "founder_tier_blitz_bh",
        "offer": "founder_tier",
        "avenue": "buy_and_hold",
        "angle": "portfolio_recognition",
        "label": "Founder-Tier Blitz — numbered seats, buy-and-hold whales",
        "launch_group": "founder_tier_blitz",
    },
    "auction_fast_follow": {
        "cell_id": "auction_fast_follow",
        "offer": "core_subscription",
        "avenue": "flippers",
        "angle": "auction_congrats",
        "label": "Auction-winner fast-follow — congrats + next month's auctions",
        "launch_group": "auction_fast_follow",
    },
    "win_back": {
        "cell_id": "win_back",
        "offer": "core_subscription",
        "avenue": "wholesalers",
        "angle": "win_back_offer",
        "label": "Win-back sweep — lapsed subs / abandoned checkouts",
        "launch_group": "win_back",
    },
    "hard_money_intro_lenders": {
        "cell_id": "hard_money_intro_lenders",
        "offer": "hard_money_intro",
        "avenue": "lender_types",
        "angle": "why_now_catalyst",
        "label": "Hard-money intro — lead handoff only, no fee mechanics (RESPA-gated)",
        "launch_group": None,
    },
    "post_call_recap": {
        "cell_id": "post_call_recap",
        "offer": "core_subscription",
        "avenue": "flippers",
        "angle": "post_call_recap",
        "label": "Post-call recap — follow-up draft after a completed call, offer TBD per-call context",
        "launch_group": None,
    },
}


def get_cell(cell_id: str) -> Optional[Cell]:
    return CELL_GRID.get(cell_id)


def is_valid_cell(cell_id: str) -> bool:
    return cell_id in CELL_GRID
