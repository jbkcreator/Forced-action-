"""Tests for src/middleware/tier_gate.py — the centralized entitlement gate."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from src.middleware.tier_gate import require_tier


def _account(tier: str = "starter"):
    row = MagicMock()
    row.account_id = "11111111-1111-1111-1111-111111111111"
    return row, tier


def test_unknown_min_tier_raises_at_setup_time():
    with pytest.raises(ValueError):
        require_tier("not_a_real_tier")


def test_starter_account_blocked_from_investor_pro_surface():
    account, tier = _account("starter")
    db = MagicMock()
    gate = require_tier("investor_pro")
    with patch("src.middleware.tier_gate.get_account_tier", return_value=tier):
        with pytest.raises(HTTPException) as exc:
            gate(account=account, db=db)
    assert exc.value.status_code == 403
    assert exc.value.detail == "Your plan does not include access to this feature"


def test_investor_pro_account_passes_investor_pro_gate():
    account, tier = _account("investor_pro")
    db = MagicMock()
    gate = require_tier("investor_pro")
    with patch("src.middleware.tier_gate.get_account_tier", return_value=tier):
        result = gate(account=account, db=db)
    assert result is account


def test_founder_account_passes_a_lower_gate():
    account, tier = _account("founder")
    db = MagicMock()
    gate = require_tier("starter")
    with patch("src.middleware.tier_gate.get_account_tier", return_value=tier):
        result = gate(account=account, db=db)
    assert result is account


def test_account_with_no_plan_is_blocked():
    account, _ = _account("starter")
    db = MagicMock()
    gate = require_tier("starter")
    with patch("src.middleware.tier_gate.get_account_tier", return_value=None):
        with pytest.raises(HTTPException) as exc:
            gate(account=account, db=db)
    assert exc.value.status_code == 403
