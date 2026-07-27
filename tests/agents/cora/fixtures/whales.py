"""
10 seeded whale-shaped BuyerEntity fixtures — the exact dict shape
src.agents.cora.tools.read_tools.get_buyer_entity_by_opportunity_thread_id()
returns (never a real DB row; Cora's tests never require Postgres to
exercise C1/C2's drafting logic itself — only validation.py's suppression
check needs a real `db`, via the `fresh_db` fixture from tests/conftest.py).

WHALES[8] and WHALES[9] are deliberately edge-case: one below Hunter's
UNVERIFIED_FLOOR (70), one with no whale_flagged_at at all.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


def _whale(
    n: int,
    *,
    confidence_score: int = 90,
    total_purchase_count: int = 10,
    total_cash_volume: float = 2_000_000.0,
    is_whale: bool = True,
    whale_flagged_at: Optional[str] = None,
    entity_type: str = "LLC",
) -> Dict[str, Any]:
    return {
        "id": 90000 + n,
        "canonical_name": f"CORA TEST WHALE {n} LLC",
        "entity_type": entity_type,
        "primary_mailing_address": f"{100 + n} TEST AVE, TAMPA, FL, 33602",
        "confidence_score": confidence_score,
        "verification_status": "verified" if confidence_score >= 70 else "unverified",
        "total_purchase_count": total_purchase_count,
        "total_cash_volume": total_cash_volume,
        "is_whale": is_whale,
        "whale_flagged_at": whale_flagged_at,
        "opportunity_thread_id": f"OPP-TEST-{n:04d}",
        "county_id": "hillsborough",
    }


_NOW = datetime.now(timezone.utc)

WHALES: List[Dict[str, Any]] = [
    _whale(1, confidence_score=95, total_purchase_count=34, total_cash_volume=10_794_680.75,
           whale_flagged_at=_NOW.isoformat()),
    _whale(2, confidence_score=90, total_purchase_count=12, total_cash_volume=3_200_000.0,
           whale_flagged_at=(_NOW - timedelta(days=5)).isoformat()),
    _whale(3, confidence_score=85, total_purchase_count=8, total_cash_volume=1_500_000.0,
           whale_flagged_at=(_NOW - timedelta(days=20)).isoformat()),
    _whale(4, confidence_score=100, total_purchase_count=26, total_cash_volume=7_000_000.0,
           whale_flagged_at=(_NOW - timedelta(days=1)).isoformat(), entity_type="Trust"),
    _whale(5, confidence_score=75, total_purchase_count=5, total_cash_volume=900_000.0,
           whale_flagged_at=(_NOW - timedelta(days=45)).isoformat()),
    _whale(6, confidence_score=88, total_purchase_count=15, total_cash_volume=4_100_000.0,
           whale_flagged_at=(_NOW - timedelta(days=10)).isoformat(), entity_type="Corp"),
    _whale(7, confidence_score=92, total_purchase_count=20, total_cash_volume=5_500_000.0,
           whale_flagged_at=(_NOW - timedelta(days=3)).isoformat()),
    _whale(8, confidence_score=80, total_purchase_count=9, total_cash_volume=1_100_000.0,
           whale_flagged_at=(_NOW - timedelta(days=60)).isoformat()),
    # Below Hunter's UNVERIFIED_FLOOR (70) — acceptance item #8.
    _whale(9, confidence_score=40, total_purchase_count=3, total_cash_volume=250_000.0,
           whale_flagged_at=_NOW.isoformat()),
    # No catalyst timestamp at all — exercises _catalyst_freshness()'s None branch.
    _whale(10, confidence_score=91, total_purchase_count=11, total_cash_volume=2_800_000.0,
           whale_flagged_at=None),
]


def facts_for(whale: Dict[str, Any], *, stale: bool = False, empty: bool = False) -> List[Dict[str, Any]]:
    """Real-shaped facts_used entries (fact_key/value/source_ref/observed_at/freshness_class)."""
    if empty:
        return []
    observed_at = (_NOW - timedelta(days=30)).isoformat() if stale else _NOW.isoformat()
    return [
        {
            "fact_key": "total_purchase_count",
            "value": whale["total_purchase_count"],
            "source_ref": "hunter_whale_ranking",
            "observed_at": observed_at,
            "freshness_class": "whale_snapshot",
        },
        {
            "fact_key": "total_cash_volume",
            "value": str(whale["total_cash_volume"]),
            "source_ref": "hunter_whale_ranking",
            "observed_at": observed_at,
            "freshness_class": "whale_snapshot",
        },
    ]
