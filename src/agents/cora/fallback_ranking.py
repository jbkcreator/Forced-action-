"""
Deterministic fallback ranking — stands in for REVINT I1 until that ships.
No LLM involved. Swappable later with no signature change (contracts.py
documents the seam).

Score components, each normalized to [0, 1] then weighted:
  - whale_score: total_purchase_count and total_cash_volume from
    src.services.whale_ranking.get_ranked_whales() output.
  - catalyst_freshness: derived from acquisition_velocity / whale_flagged_at
    recency — a fresher "why_now" catalyst ranks higher.
  - auction_recency: decays over ~14 days from latest_auction_deed_date, when
    present (Cell #2 rows only — Cell #1 rows have no auction event, so this
    contributes 0.0 for them, same as before Cell #2 existed).
  - purchase_activity: total_purchase_count alone (distinct from cash volume).
  - estimated_value: total_cash_volume, log-scaled so a single huge outlier
    doesn't dominate the ranking.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_WEIGHTS = {
    "whale_score": 0.30,
    "catalyst_freshness": 0.25,
    "auction_recency": 0.10,
    "purchase_activity": 0.15,
    "estimated_value": 0.20,
}


def _normalize_count(count: Optional[int], cap: int = 10) -> float:
    if not count:
        return 0.0
    return min(count, cap) / cap


def _normalize_value_log(cents_or_dollars: Optional[float], cap: float = 5_000_000) -> float:
    if not cents_or_dollars or cents_or_dollars <= 0:
        return 0.0
    return min(math.log10(1 + cents_or_dollars) / math.log10(1 + cap), 1.0)


def _catalyst_freshness(whale_flagged_at: Optional[str]) -> float:
    if not whale_flagged_at:
        return 0.0
    try:
        flagged = datetime.fromisoformat(str(whale_flagged_at))
        if flagged.tzinfo is None:
            flagged = flagged.replace(tzinfo=timezone.utc)
    except ValueError:
        return 0.0
    days_ago = (datetime.now(timezone.utc) - flagged).days
    if days_ago < 0:
        return 1.0
    return max(0.0, 1.0 - (days_ago / 90))  # decays to 0 over ~90 days


def _auction_recency(latest_auction_deed_date: Optional[str]) -> float:
    if not latest_auction_deed_date:
        return 0.0
    try:
        deed_date = datetime.fromisoformat(str(latest_auction_deed_date))
        if deed_date.tzinfo is None:
            deed_date = deed_date.replace(tzinfo=timezone.utc)
    except ValueError:
        return 0.0
    days_ago = (datetime.now(timezone.utc) - deed_date).days
    if days_ago < 0:
        return 1.0
    return max(0.0, 1.0 - (days_ago / 14))  # decays to 0 over ~14 days — a fast-follow signal, not a slow one


def score_ranked_whale(ranked_whale: Dict[str, Any]) -> float:
    """ranked_whale is one dict from whale_ranking.get_ranked_whales()'s output,
    or from read_tools.get_recent_auction_fast_follow_whales() (Cell #2)."""
    whale_score = _normalize_count(ranked_whale.get("total_purchase_count"), cap=10)
    catalyst = _catalyst_freshness(ranked_whale.get("whale_flagged_at"))
    auction_recency = _auction_recency(ranked_whale.get("latest_auction_deed_date"))
    purchase_activity = _normalize_count(ranked_whale.get("total_purchase_count"), cap=20)
    estimated_value = _normalize_value_log(ranked_whale.get("total_cash_volume"))

    return (
        _WEIGHTS["whale_score"] * whale_score
        + _WEIGHTS["catalyst_freshness"] * catalyst
        + _WEIGHTS["auction_recency"] * auction_recency
        + _WEIGHTS["purchase_activity"] * purchase_activity
        + _WEIGHTS["estimated_value"] * estimated_value
    )


def rank_targets(ranked_whales: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """Attaches fallback_score to each row and sorts descending."""
    scored = []
    for rw in ranked_whales:
        row = dict(rw)
        row["fallback_score"] = score_ranked_whale(rw)
        scored.append(row)
    scored.sort(key=lambda r: r["fallback_score"], reverse=True)
    return scored
