"""Tests for src/services/entitlement_service.py — cached tier resolution."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.entitlement_service import TIER_RANK, get_account_tier


def test_tier_rank_ordering():
    assert TIER_RANK["starter"] < TIER_RANK["investor_pro"] < TIER_RANK["founder"]


def test_get_account_tier_cache_hit_skips_db():
    db = MagicMock()
    with patch("src.services.entitlement_service.redis_available", return_value=True), \
         patch("src.services.entitlement_service.rget", return_value="investor_pro"):
        tier = get_account_tier(db, "11111111-1111-1111-1111-111111111111")
    assert tier == "investor_pro"
    db.execute.assert_not_called()


def test_get_account_tier_cache_miss_reads_db_and_populates_cache():
    db = MagicMock()
    row = MagicMock()
    row.tier = "founder"
    db.execute.return_value.fetchone.return_value = row
    with patch("src.services.entitlement_service.redis_available", return_value=True), \
         patch("src.services.entitlement_service.rget", return_value=None), \
         patch("src.services.entitlement_service.rset") as mock_rset:
        tier = get_account_tier(db, "11111111-1111-1111-1111-111111111111")
    assert tier == "founder"
    mock_rset.assert_called_once()


def test_get_account_tier_redis_down_falls_back_to_db():
    db = MagicMock()
    row = MagicMock()
    row.tier = "starter"
    db.execute.return_value.fetchone.return_value = row
    with patch("src.services.entitlement_service.redis_available", return_value=False):
        tier = get_account_tier(db, "11111111-1111-1111-1111-111111111111")
    assert tier == "starter"
    db.execute.assert_called_once()


def test_get_account_tier_no_plan_returns_none():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    with patch("src.services.entitlement_service.redis_available", return_value=False):
        tier = get_account_tier(db, "11111111-1111-1111-1111-111111111111")
    assert tier is None
