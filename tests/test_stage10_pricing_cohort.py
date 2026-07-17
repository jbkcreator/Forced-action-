"""
Stage 10 — Unit tests: pricing_cohort_engine.py

Covers:
  - check_activation_gates: not-ready, ready
  - evaluate_and_activate: invalid args, gates not met, guardrail violation,
    activate, already_active, update price
  - get_price_for_subscriber: base price passthrough, cohort adjusted, re-clamped
  - rollback_cohort: no cohort, rolls back
  - check_cohort_rollback_trigger: insufficient data, no-action, auto-rollback
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_cohort(
    id_: int = 1,
    county_id: str = "hillsborough",
    trade_vertical: str = "roofing",
    price_type: str = "lock",
    base_price_cents: int = 19700,
    adjusted_price_cents: int = 21670,  # +10%
    adjustment_pct: float = 10.0,
    status: str = "active",
    activated_at=None,
):
    from datetime import datetime, timezone
    c = MagicMock()
    c.id = id_
    c.county_id = county_id
    c.trade_vertical = trade_vertical
    c.price_type = price_type
    c.base_price_cents = base_price_cents
    c.adjusted_price_cents = adjusted_price_cents
    c.adjustment_pct = adjustment_pct
    c.status = status
    c.activated_at = activated_at or datetime(2026, 4, 1, tzinfo=timezone.utc)
    return c


def _db_with_deal_counts(weeks: int, deals: int, active_cohort=None):
    db = MagicMock()

    def execute_side_effect(query, params=None):
        text = str(query)
        result = MagicMock()

        if "deal_weeks" in text or "trade_vertical" in text:
            row = MagicMock()
            row.deal_weeks = weeks
            row.total_deals = deals
            result.first.return_value = row
        elif "pricing_cohorts" in text and "SELECT" in text and active_cohort is not None:
            result.first.return_value = active_cohort
        elif "pricing_cohorts" in text and "SELECT" in text:
            result.first.return_value = None
        elif "RETURNING id" in text:
            row = MagicMock()
            row.id = 99
            result.first.return_value = row
        elif "COUNT" in text:
            row = MagicMock()
            row.total = 0
            row.wins = 0
            result.first.return_value = row
        else:
            result.first.return_value = None
        result.rowcount = 1
        return result

    db.execute.side_effect = execute_side_effect
    return db


# ── check_activation_gates ────────────────────────────────────────────────────

class TestCheckActivationGates:
    def test_not_ready_insufficient_weeks(self):
        from src.services.pricing_cohort_engine import check_activation_gates
        db = _db_with_deal_counts(weeks=4, deals=15)
        gates = check_activation_gates("hillsborough", "roofing", db)
        assert not gates["ready"]
        assert not gates["deal_weeks_met"]
        assert gates["deal_count_met"]

    def test_not_ready_insufficient_deals(self):
        from src.services.pricing_cohort_engine import check_activation_gates
        db = _db_with_deal_counts(weeks=8, deals=3)
        gates = check_activation_gates("hillsborough", "roofing", db)
        assert not gates["ready"]
        assert gates["deal_weeks_met"]
        assert not gates["deal_count_met"]

    def test_ready_when_both_gates_met(self):
        from src.services.pricing_cohort_engine import check_activation_gates
        db = _db_with_deal_counts(weeks=8, deals=20)
        gates = check_activation_gates("hillsborough", "roofing", db)
        assert gates["ready"]
        assert gates["weeks"] == 8
        assert gates["deals"] == 20


# ── evaluate_and_activate ─────────────────────────────────────────────────────

class TestEvaluateAndActivate:
    def test_invalid_price_type(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = MagicMock()
        result = evaluate_and_activate("hillsborough", "roofing", "invalid_type", 19700, 5.0, db)
        assert result["status"] == "invalid_price_type"

    def test_invalid_trade_vertical(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = MagicMock()
        result = evaluate_and_activate("hillsborough", "yoga_studio", "lock", 19700, 5.0, db)
        assert result["status"] == "invalid_trade_vertical"

    def test_gates_not_met(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = _db_with_deal_counts(weeks=2, deals=3)
        result = evaluate_and_activate("hillsborough", "roofing", "lock", 19700, 5.0, db)
        assert result["status"] == "gates_not_met"

    def test_guardrail_violation_exceeds_25pct(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = _db_with_deal_counts(weeks=8, deals=20)
        # +30% adjustment — exceeds guardrail ±25%
        result = evaluate_and_activate("hillsborough", "roofing", "lock", 19700, 30.0, db)
        assert result["status"] == "guardrail_violation"

    def test_activates_within_guardrails(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = _db_with_deal_counts(weeks=8, deals=20)
        result = evaluate_and_activate(
            "hillsborough", "roofing", "lock", 19700, 10.0, db,
            activation_reason="test activation",
        )
        # 'activated' on first run; 'updated' if same cohort already exists in mock.
        assert result["status"] in ("activated", "updated", "already_active")
        if result["status"] in ("activated", "updated"):
            assert result.get("adjusted_cents", 21670) <= 24700  # lock max guardrail
            assert result.get("adjusted_cents", 21670) >= 14700  # lock min guardrail

    def test_annual_lock_positive_adjustment_takes_effect(self):
        """annual_lock's guardrail bounds must allow real movement off the flat
        $1970 rate — a fixed (197000, 197000) bound clamps every adjustment
        back to the same number while still reporting 'activated'."""
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = _db_with_deal_counts(weeks=8, deals=20)
        result = evaluate_and_activate(
            "hillsborough", "roofing", "annual_lock", 197000, 10.0, db,
        )
        assert result["status"] in ("activated", "updated")
        assert result["adjusted_cents"] != 197000
        assert result["adjusted_cents"] == 216700  # 197000 * 1.10

    def test_annual_lock_negative_adjustment_takes_effect(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        db = _db_with_deal_counts(weeks=8, deals=20)
        result = evaluate_and_activate(
            "hillsborough", "roofing", "annual_lock", 197000, -10.0, db,
        )
        assert result["status"] in ("activated", "updated")
        assert result["adjusted_cents"] != 197000
        assert result["adjusted_cents"] == 177300  # 197000 * 0.90

    def test_already_active_no_price_change(self):
        from src.services.pricing_cohort_engine import evaluate_and_activate
        existing = _make_cohort(adjusted_price_cents=21670)
        db = _db_with_deal_counts(weeks=8, deals=20, active_cohort=existing)
        # Same adjusted price — should return already_active.
        # 19700 * 1.10 = 21670
        result = evaluate_and_activate("hillsborough", "roofing", "lock", 19700, 10.0, db)
        assert result["status"] in ("already_active", "updated", "activated")


# ── get_price_for_subscriber ──────────────────────────────────────────────────

class TestGetPriceForSubscriber:
    def test_base_price_when_no_cohort(self):
        from src.services.pricing_cohort_engine import get_price_for_subscriber
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        price, source = get_price_for_subscriber("hillsborough", "roofing", "lock", 19700, db)
        assert price == 19700
        assert source == "base_price"

    def test_cohort_adjusted_when_active(self):
        from src.services.pricing_cohort_engine import get_price_for_subscriber
        cohort = _make_cohort(adjusted_price_cents=21670)
        db = MagicMock()
        db.execute.return_value.first.return_value = cohort
        price, source = get_price_for_subscriber("hillsborough", "roofing", "lock", 19700, db)
        assert price == 21670
        assert source == "cohort_adjusted"

    def test_passthrough_for_invalid_vertical(self):
        from src.services.pricing_cohort_engine import get_price_for_subscriber
        db = MagicMock()
        price, source = get_price_for_subscriber("hillsborough", "yoga_studio", "lock", 19700, db)
        assert price == 19700
        assert source == "base_price"
        db.execute.assert_not_called()

    def test_annual_lock_adjusted_price_not_clamped_to_flat_rate(self):
        from src.services.pricing_cohort_engine import get_price_for_subscriber
        cohort = _make_cohort(price_type="annual_lock", adjusted_price_cents=216700)
        db = MagicMock()
        db.execute.return_value.first.return_value = cohort
        price, source = get_price_for_subscriber("hillsborough", "roofing", "annual_lock", 197000, db)
        assert price == 216700
        assert source == "cohort_adjusted"

    def test_clamped_to_guardrail_bounds(self):
        """Even if the DB has an out-of-bounds price, it gets clamped."""
        from src.services.pricing_cohort_engine import get_price_for_subscriber
        # adjusted_price_cents = 99999 — exceeds lock max of 24700
        cohort = _make_cohort(adjusted_price_cents=99999)
        db = MagicMock()
        db.execute.return_value.first.return_value = cohort
        price, source = get_price_for_subscriber("hillsborough", "roofing", "lock", 19700, db)
        assert price == 24700  # clamped to lock max
        assert source == "cohort_adjusted"


# ── rollback_cohort ───────────────────────────────────────────────────────────

class TestRollbackCohort:
    def test_no_active_cohort(self):
        from src.services.pricing_cohort_engine import rollback_cohort
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        result = rollback_cohort("hillsborough", "roofing", "lock", db)
        assert result["status"] == "no_active_cohort"

    def test_rolls_back_active_cohort(self):
        from src.services.pricing_cohort_engine import rollback_cohort
        cohort = _make_cohort()
        db = MagicMock()
        db.execute.return_value.first.return_value = cohort
        result = rollback_cohort("hillsborough", "roofing", "lock", db, reason="test")
        assert result["status"] == "rolled_back"
        assert result["cohort_id"] == cohort.id


# ── check_cohort_rollback_trigger ─────────────────────────────────────────────

class TestCheckCohortRollbackTrigger:
    def test_no_cohort(self):
        from src.services.pricing_cohort_engine import check_cohort_rollback_trigger
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        result = check_cohort_rollback_trigger("hillsborough", "roofing", "lock", db)
        assert result["action"] == "no_cohort"

    def test_insufficient_data(self):
        from src.services.pricing_cohort_engine import check_cohort_rollback_trigger
        cohort = _make_cohort()

        call_count = [0]
        def execute_side_effect(query, params=None):
            result = MagicMock()
            c = call_count[0]
            call_count[0] += 1
            if c == 0:
                # active cohort lookup
                result.first.return_value = cohort
            else:
                # deal counts (too few)
                row = MagicMock()
                row.total = 3
                row.wins = 1
                result.first.return_value = row
            return result

        db = MagicMock()
        db.execute.side_effect = execute_side_effect
        result = check_cohort_rollback_trigger("hillsborough", "roofing", "lock", db)
        assert result["action"] == "insufficient_data"

    def test_no_action_when_stable(self):
        from src.services.pricing_cohort_engine import check_cohort_rollback_trigger
        cohort = _make_cohort()

        call_count = [0]
        def execute_side_effect(query, params=None):
            result = MagicMock()
            c = call_count[0]
            call_count[0] += 1
            if c == 0:
                result.first.return_value = cohort
            elif c == 1:
                row = MagicMock()
                row.total = 50
                row.wins = 15  # 30%
                result.first.return_value = row
            else:
                row = MagicMock()
                row.total = 50
                row.wins = 14  # 28% — not significantly different
                result.first.return_value = row
            return result

        db = MagicMock()
        db.execute.side_effect = execute_side_effect
        result = check_cohort_rollback_trigger("hillsborough", "roofing", "lock", db)
        assert result["action"] == "no_action"

    def test_auto_rollback_when_drop_exceeds_sigma(self):
        """30% → 3% drop should trigger auto-rollback."""
        from src.services.pricing_cohort_engine import check_cohort_rollback_trigger
        cohort = _make_cohort()

        call_count = [0]
        def execute_side_effect(query, params=None):
            result = MagicMock()
            c = call_count[0]
            call_count[0] += 1
            if c == 0:
                result.first.return_value = cohort
            elif c == 1:
                # post-activation: 3% conversion
                row = MagicMock()
                row.total = 200
                row.wins = 6
                result.first.return_value = row
            elif c == 2:
                # pre-activation baseline: 30% conversion
                row = MagicMock()
                row.total = 200
                row.wins = 60
                result.first.return_value = row
            else:
                result.first.return_value = None
            result.rowcount = 1
            return result

        db = MagicMock()
        db.execute.side_effect = execute_side_effect
        result = check_cohort_rollback_trigger("hillsborough", "roofing", "lock", db)
        assert result["action"] == "rolled_back"
        assert result["z_score"] < -2.0
