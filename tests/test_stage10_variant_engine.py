"""
Stage 10 — Unit tests: variant_engine.py

Covers:
  - Deterministic assignment and traffic cap
  - record_send / record_outcome
  - check_and_retire: not-ready, insufficient-sigma, idempotent retire, retire+replace
  - check_replacement_performance: still-proving, promote, revert
  - check_sigma_rollback: no-action, rollback triggered
  - promote_winner (self-healing hook)
"""

from __future__ import annotations

import math
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_test(
    id_: int = 1,
    sequence_name: str = "fomo_v1",
    traffic_pct: int = 10,
    status: str = "active",
    a_sends: int = 0, a_convs: int = 0, a_replies: int = 0, a_status: str = "active",
    b_sends: int = 0, b_convs: int = 0, b_replies: int = 0, b_status: str = "active",
    c_sends: int = 0, c_convs: int = 0, c_replies: int = 0, c_status: str = "active",
    proving_slot: str | None = None,
    proving_baseline_conv_rate: float | None = None,
):
    t = MagicMock()
    t.id = id_
    t.sequence_name = sequence_name
    t.traffic_pct = traffic_pct
    t.status = status

    t.slot_a_body = "Body A"
    t.slot_a_sends = a_sends
    t.slot_a_conversions = a_convs
    t.slot_a_replies = a_replies
    t.slot_a_status = a_status
    t.slot_a_retired_at = None

    t.slot_b_body = "Body B"
    t.slot_b_sends = b_sends
    t.slot_b_conversions = b_convs
    t.slot_b_replies = b_replies
    t.slot_b_status = b_status
    t.slot_b_retired_at = None

    t.slot_c_body = "Body C"
    t.slot_c_sends = c_sends
    t.slot_c_conversions = c_convs
    t.slot_c_replies = c_replies
    t.slot_c_status = c_status
    t.slot_c_retired_at = None

    t.proving_slot = proving_slot
    t.proving_baseline_conv_rate = proving_baseline_conv_rate
    t.proving_started_at = None
    return t


def _mock_db(test: Any | None, *, retirement_exists: bool = False):
    """Return a mock Session wired to return the given test row."""
    db = MagicMock()
    scalar_result = MagicMock()
    scalar_result.first.return_value = test

    first_result = MagicMock()
    first_result.first.return_value = (MagicMock() if retirement_exists else None)

    execute_result = MagicMock()
    execute_result.first.side_effect = [test, (MagicMock() if retirement_exists else None)]
    execute_result.fetchall.return_value = []
    execute_result.rowcount = 0 if retirement_exists else 1

    db.execute.return_value = execute_result
    return db


# ── assignment ────────────────────────────────────────────────────────────────

class TestAssignVariant:
    def test_returns_none_when_no_test(self):
        from src.services.variant_engine import assign_variant
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        assert assign_variant(1, "nonexistent", db) is None

    def test_returns_none_when_hash_outside_cap(self):
        """Subscriber hash >= traffic_pct → excluded."""
        from src.services.variant_engine import assign_variant, _hash_slot
        test = _make_test(traffic_pct=1)
        db = MagicMock()
        db.execute.return_value.first.return_value = test

        # With traffic_pct=1, only h<1 passes. Most subscribers should get None.
        results = [assign_variant(i, "fomo_v1", db) for i in range(200)]
        nones = [r for r in results if r is None]
        assert len(nones) > 150, "Expected most subscribers outside 1% cap"

    def test_deterministic_for_same_subscriber(self):
        """Same subscriber always gets the same (slot, body) tuple."""
        from src.services.variant_engine import assign_variant
        test = _make_test(traffic_pct=100)
        db = MagicMock()
        db.execute.return_value.first.return_value = test

        r1 = assign_variant(42, "fomo_v1", db)
        r2 = assign_variant(42, "fomo_v1", db)
        assert r1 == r2

    def test_all_three_slots_reachable(self):
        """With enough subscribers and full traffic, all 3 slots are assigned."""
        from src.services.variant_engine import assign_variant
        test = _make_test(traffic_pct=100)
        db = MagicMock()
        db.execute.return_value.first.return_value = test

        slots_seen = set()
        for i in range(300):
            result = assign_variant(i, "fomo_v1", db)
            if result:
                slots_seen.add(result[0])
        assert slots_seen == {"a", "b", "c"}

    def test_retired_slot_routes_to_active(self):
        """Subscriber assigned to retired slot gets rerouted to active slot."""
        from src.services.variant_engine import assign_variant
        # Make slot 'b' retired; subscriber 1 hashes to 'b'.
        test = _make_test(traffic_pct=100, b_status="retired")
        db = MagicMock()
        db.execute.return_value.first.return_value = test

        # Find a subscriber that would normally hash to 'b'.
        for i in range(1, 1000):
            h = int(__import__("hashlib").md5(f"fomo_v1{i}".encode()).hexdigest(), 16) % 100
            if h < 100 and h % 3 == 1:  # hashes to 'b'
                result = assign_variant(i, "fomo_v1", db)
                assert result is not None
                assert result[0] in ("a", "c")  # never 'b'
                break


# ── record_send / record_outcome ──────────────────────────────────────────────

class TestRecordSendOutcome:
    def test_record_send_calls_update(self):
        from src.services.variant_engine import record_send
        db = MagicMock()
        record_send("fomo_v1", "a", db)
        db.execute.assert_called_once()
        # Verify the SQL clause text contains the slot column reference.
        clause = db.execute.call_args[0][0]
        assert "slot_a_sends" in str(clause)

    def test_record_send_invalid_slot_is_noop(self):
        from src.services.variant_engine import record_send
        db = MagicMock()
        record_send("fomo_v1", "z", db)
        db.execute.assert_not_called()

    def test_record_outcome_converted(self):
        from src.services.variant_engine import record_outcome
        db = MagicMock()
        record_outcome("fomo_v1", "b", "converted", db)
        db.execute.assert_called_once()
        clause = db.execute.call_args[0][0]
        assert "slot_b_conversions" in str(clause)

    def test_record_outcome_invalid_is_noop(self):
        from src.services.variant_engine import record_outcome
        db = MagicMock()
        record_outcome("fomo_v1", "a", "bounced", db)  # 'bounced' not valid
        db.execute.assert_not_called()


# ── check_and_retire ─────────────────────────────────────────────────────────

class TestCheckAndRetire:
    def test_no_test_returns_no_test(self):
        from src.services.variant_engine import check_and_retire
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        result = check_and_retire("missing_seq", db)
        assert result["status"] == "no_test"

    def test_not_ready_when_fewer_than_2_slots_qualify(self):
        from src.services.variant_engine import check_and_retire
        # Only slot A has 200+ sends; B and C have fewer.
        test = _make_test(
            a_sends=300, a_convs=90,
            b_sends=10, b_convs=3,
            c_sends=5, c_convs=1,
        )
        db = MagicMock()
        db.execute.return_value.first.return_value = test
        result = check_and_retire("fomo_v1", db)
        assert result["status"] == "not_ready"

    def test_not_significantly_worse_when_z_above_threshold(self):
        """When z-score is above -2.0, no retirement triggered."""
        from src.services.variant_engine import check_and_retire
        # Both slots have similar rates; z will be near 0.
        test = _make_test(
            a_sends=300, a_convs=90,   # 30%
            b_sends=300, b_convs=87,   # 29%
            c_sends=300, c_convs=93,   # 31%
        )
        db = MagicMock()
        db.execute.return_value.first.return_value = test
        result = check_and_retire("fomo_v1", db)
        assert result["status"] == "not_significantly_worse"

    def test_retires_loser_and_generates_replacement(self):
        """Clear loser should be retired and Haiku replacement installed."""
        from src.services.variant_engine import check_and_retire
        # Slot A: 5% conv (loser), B: 30% (winner), C: 28%
        test = _make_test(
            a_sends=300, a_convs=15,   # 5%
            b_sends=300, b_convs=90,   # 30%
            c_sends=300, c_convs=84,   # 28%
        )
        execute_results = MagicMock()
        # First call: get_test; second: idempotency check (None = not yet logged)
        execute_results.first.side_effect = [test, None]
        execute_results.rowcount = 1

        db = MagicMock()
        db.execute.return_value = execute_results

        with patch(
            "src.services.variant_engine._generate_replacement",
            return_value="New Haiku SMS body",
        ) as mock_gen:
            result = check_and_retire("fomo_v1", db)

        assert result["status"] == "retired"
        assert result["slot"] == "a"
        assert result["z_score"] < -2.0
        mock_gen.assert_called_once()

    def test_idempotent_on_retry(self):
        """Second call with same test returns already_retired without double-writes."""
        from src.services.variant_engine import check_and_retire
        test = _make_test(
            a_sends=300, a_convs=15,
            b_sends=300, b_convs=90,
            c_sends=300, c_convs=84,
        )
        execute_results = MagicMock()
        # Idempotency check returns a row (already logged).
        execute_results.first.side_effect = [test, MagicMock()]
        execute_results.rowcount = 0

        db = MagicMock()
        db.execute.return_value = execute_results

        result = check_and_retire("fomo_v1", db)
        assert result["status"] == "already_retired"


# ── check_replacement_performance ────────────────────────────────────────────

class TestCheckReplacementPerformance:
    def test_no_proving_cycle(self):
        from src.services.variant_engine import check_replacement_performance
        test = _make_test(proving_slot=None)
        db = MagicMock()
        db.execute.return_value.first.return_value = test
        result = check_replacement_performance("fomo_v1", db)
        assert result["status"] == "no_proving_cycle"

    def test_still_proving_when_sends_below_threshold(self):
        from src.services.variant_engine import check_replacement_performance
        test = _make_test(
            proving_slot="a",
            proving_baseline_conv_rate=0.05,
            a_sends=100,
            a_convs=10,
        )
        db = MagicMock()
        db.execute.return_value.first.return_value = test
        result = check_replacement_performance("fomo_v1", db)
        assert result["status"] == "still_proving"
        assert result["sends"] == 100

    def test_promotes_when_replacement_beats_baseline(self):
        from src.services.variant_engine import check_replacement_performance
        # Replacement: 200 sends, 60 conversions = 30%; baseline was 5%
        test = _make_test(
            proving_slot="a",
            proving_baseline_conv_rate=0.05,
            a_sends=200,
            a_convs=60,
            b_sends=300, b_convs=90,  # winner used for fallback
        )
        execute_results = MagicMock()
        execute_results.first.side_effect = [test, None]
        execute_results.rowcount = 1

        db = MagicMock()
        db.execute.return_value = execute_results

        result = check_replacement_performance("fomo_v1", db)
        assert result["status"] == "promoted"
        assert result["replacement_rate"] == pytest.approx(0.30, rel=1e-3)

    def test_reverts_when_replacement_loses(self):
        from src.services.variant_engine import check_replacement_performance
        # Replacement: 200 sends, 4 conversions = 2%; baseline was 5%
        test = _make_test(
            proving_slot="a",
            proving_baseline_conv_rate=0.05,
            a_sends=200,
            a_convs=4,
            b_sends=300, b_convs=90,
            b_status="active",
        )
        execute_results = MagicMock()
        execute_results.first.side_effect = [test, None]
        execute_results.rowcount = 1

        db = MagicMock()
        db.execute.return_value = execute_results

        result = check_replacement_performance("fomo_v1", db)
        assert result["status"] == "reverted"
        assert result["replacement_rate"] < result["baseline_rate"]


# ── check_sigma_rollback ──────────────────────────────────────────────────────

class TestCheckSigmaRollback:
    def test_no_test(self):
        from src.services.variant_engine import check_sigma_rollback
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        result = check_sigma_rollback("fomo_v1", db)
        assert result["status"] == "no_test"

    def test_insufficient_active_slots(self):
        from src.services.variant_engine import check_sigma_rollback
        # Only one slot has enough sends.
        test = _make_test(a_sends=300, a_convs=90, b_sends=5, c_sends=5)
        db = MagicMock()
        db.execute.return_value.first.return_value = test
        result = check_sigma_rollback("fomo_v1", db)
        assert result["status"] == "insufficient_data"

    def test_no_action_when_slots_similar(self):
        from src.services.variant_engine import check_sigma_rollback
        test = _make_test(
            a_sends=300, a_convs=90,   # 30%
            b_sends=300, b_convs=87,   # 29%
            c_sends=300, c_convs=93,   # 31%
        )
        execute_results = MagicMock()
        execute_results.first.return_value = test
        execute_results.rowcount = 1

        db = MagicMock()
        db.execute.return_value = execute_results

        result = check_sigma_rollback("fomo_v1", db)
        assert result["status"] == "checked"
        assert result["paused"] == []

    def test_pauses_slot_below_two_sigma(self):
        from src.services.variant_engine import check_sigma_rollback
        # Slot A: 3%, B: 30%, C: 28% — A should trigger rollback
        test = _make_test(
            a_sends=300, a_convs=9,    # 3%
            b_sends=300, b_convs=90,   # 30%
            c_sends=300, c_convs=84,   # 28%
        )
        execute_results = MagicMock()
        execute_results.first.return_value = test
        execute_results.rowcount = 1

        db = MagicMock()
        db.execute.return_value = execute_results

        result = check_sigma_rollback("fomo_v1", db)
        assert result["status"] == "checked"
        assert any(p["slot"] == "a" for p in result["paused"])
        assert result["paused"][0]["z_score"] < -2.0


# ── promote_winner (self-healing hook) ────────────────────────────────────────

class TestPromoteWinner:
    def test_no_test(self):
        from src.services.variant_engine import promote_winner
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        result = promote_winner("fomo_v1", db)
        assert result["status"] == "no_test"

    def test_promotes_best_pauses_worst(self):
        from src.services.variant_engine import promote_winner
        test = _make_test(
            a_sends=300, a_convs=9,    # 3% — loser
            b_sends=300, b_convs=90,   # 30% — winner
            c_sends=300, c_convs=84,   # 28%
        )
        execute_results = MagicMock()
        execute_results.first.side_effect = [test, None]
        execute_results.rowcount = 1

        db = MagicMock()
        db.execute.return_value = execute_results

        result = promote_winner("fomo_v1", db)
        assert result["status"] == "promoted"
        assert result["paused_slot"] == "a"
        assert result["winner_slot"] == "b"

    def test_idempotent_on_second_call(self):
        from src.services.variant_engine import promote_winner
        test = _make_test(
            a_sends=300, a_convs=9,
            b_sends=300, b_convs=90,
            c_sends=300, c_convs=84,
        )
        execute_results = MagicMock()
        # Second call: idempotency key already exists (rowcount=0).
        execute_results.first.side_effect = [test, MagicMock()]
        execute_results.rowcount = 0

        db = MagicMock()
        db.execute.return_value = execute_results

        result = promote_winner("fomo_v1", db)
        assert result["status"] == "already_promoted"
