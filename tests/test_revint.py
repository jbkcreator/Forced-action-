"""
Critical-path tests for REVINT-v2.2 (I1–I4).

Covers:
  1. OpportunityScore — per-action uniqueness
  2. OpportunityScore — actuals calibration (cold-start vs rolling mean)
  3. NBRA queue ordering and automated-action exclusion
  4. Offer recommendation rules (auction winner, whale, default fallback)
  5. PriceAssignment — band validation and RESPA gate
  6. Vertical Autopilot — dim5 and legal gate
  7. Probe loop — presell gate and package generation
"""

from __future__ import annotations

import statistics
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from src.core.models import (
    OpportunityScore,
    OpportunityScoreHistory,
    RevenueType,
    VerticalCandidatePacket,
    VerticalProbe,
    VerticalVerdict,
    PriceAssignment,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_score(
    db,
    thread_id: str,
    source_action_type: str | None = "email_outreach",
    segment: str = "default",
    buyer_entity_id: int = 1,
    revenue_type: RevenueType = RevenueType.SUBSCRIPTION,
    expected_rgp_cents: int = 50_000,
    is_automated: bool = False,
) -> OpportunityScore:
    from src.services.opportunity_score import get_or_create_score
    return get_or_create_score(
        db=db,
        buyer_entity_id=buyer_entity_id,
        opportunity_thread_id=thread_id,
        segment=segment,
        revenue_type=revenue_type,
        expected_revenue_cents=expected_rgp_cents + 5_000,
        expected_retained_gross_profit_cents=expected_rgp_cents,
        source_action_type=source_action_type,
        is_automated=is_automated,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. OpportunityScore — per-action uniqueness
# ─────────────────────────────────────────────────────────────────────────────

class TestOpportunityScoreUniqueness:
    def test_different_action_type_creates_two_rows(self, fresh_db):
        thread = "OPP-2026-00001"
        s1 = _make_score(fresh_db, thread, source_action_type="email_outreach")
        s2 = _make_score(fresh_db, thread, source_action_type="sms_outreach")
        assert s1.id != s2.id
        assert s1.source_action_type == "email_outreach"
        assert s2.source_action_type == "sms_outreach"

    def test_same_action_type_returns_existing_row(self, fresh_db):
        thread = "OPP-2026-00002"
        s1 = _make_score(fresh_db, thread, source_action_type="email_outreach")
        s2 = _make_score(fresh_db, thread, source_action_type="email_outreach")
        assert s1.id == s2.id


# ─────────────────────────────────────────────────────────────────────────────
# 2. OpportunityScore — actuals calibration
# ─────────────────────────────────────────────────────────────────────────────

class TestOpportunityScoreActuals:
    def test_cold_start_priors_with_few_history_rows(self, fresh_db):
        from src.services.opportunity_score import recalculate_score, COLD_START_PRIORS

        thread = "OPP-2026-00010"
        score = _make_score(fresh_db, thread, segment="default")
        # Insert only 5 history rows — below ACTUALS_MIN_SAMPLES=10
        for _ in range(5):
            h = OpportunityScoreHistory(
                opportunity_score_id=score.id,
                opportunity_thread_id=thread,
                snapshot_at=datetime.now(timezone.utc),
                p_reply=0.99,
                p_close=0.99,
                time_to_cash_days=1,
                nbra_score=999.0,
            )
            fresh_db.add(h)
        fresh_db.flush()

        refreshed = recalculate_score(fresh_db, score.id, reason="test")
        priors = COLD_START_PRIORS["default"]
        assert float(refreshed.p_reply) == pytest.approx(priors["p_reply"])
        assert float(refreshed.p_close) == pytest.approx(priors["p_close"])
        assert refreshed.time_to_cash_days == priors["time_to_cash_days"]

    def test_uses_mean_of_history_with_enough_rows(self, fresh_db):
        from src.services.opportunity_score import recalculate_score

        thread = "OPP-2026-00011"
        score = _make_score(fresh_db, thread, segment="default")

        p_reply_vals = [0.10 + i * 0.01 for i in range(10)]
        p_close_vals = [0.05 + i * 0.005 for i in range(10)]
        ttc_vals = [15 + i for i in range(10)]

        for i in range(10):
            h = OpportunityScoreHistory(
                opportunity_score_id=score.id,
                opportunity_thread_id=thread,
                snapshot_at=datetime.now(timezone.utc),
                p_reply=p_reply_vals[i],
                p_close=p_close_vals[i],
                time_to_cash_days=ttc_vals[i],
                nbra_score=100.0,
            )
            fresh_db.add(h)
        fresh_db.flush()

        refreshed = recalculate_score(fresh_db, score.id, reason="actuals_test")
        expected_p_reply = statistics.mean(p_reply_vals)
        expected_p_close = statistics.mean(p_close_vals)
        expected_ttc = round(statistics.mean(ttc_vals))

        assert float(refreshed.p_reply) == pytest.approx(expected_p_reply, rel=1e-3)
        assert float(refreshed.p_close) == pytest.approx(expected_p_close, rel=1e-3)
        assert refreshed.time_to_cash_days == expected_ttc


# ─────────────────────────────────────────────────────────────────────────────
# 3. NBRA queue
# ─────────────────────────────────────────────────────────────────────────────

class TestNBRAQueue:
    def test_ranked_queue_ordered_by_nbra_desc(self, fresh_db):
        from src.services.nbra_engine import get_ranked_queue

        thread = "OPP-2026-00020"
        # Create 3 scores with different rgp (→ different nbra_score) for different action types
        s_low = _make_score(fresh_db, thread, source_action_type="follow_up",   expected_rgp_cents=10_000)
        s_mid = _make_score(fresh_db, thread, source_action_type="sms_outreach", expected_rgp_cents=30_000)
        s_high = _make_score(fresh_db, thread, source_action_type="email_outreach", expected_rgp_cents=50_000)
        fresh_db.flush()

        queue = get_ranked_queue(fresh_db)
        ids_in_queue = [s.id for s in queue]
        assert s_high.id in ids_in_queue
        assert s_mid.id in ids_in_queue
        assert s_low.id in ids_in_queue

        # Verify descending order by nbra_score
        nbra_values = [float(s.nbra_score) for s in queue if s.id in {s_low.id, s_mid.id, s_high.id}]
        assert nbra_values == sorted(nbra_values, reverse=True)

    def test_automated_scores_excluded_from_ranked_queue(self, fresh_db):
        from src.services.nbra_engine import get_ranked_queue

        thread = "OPP-2026-00021"
        manual = _make_score(fresh_db, thread, source_action_type="call_outreach", is_automated=False)
        auto = _make_score(fresh_db, thread, source_action_type="relay_auto", is_automated=True)
        fresh_db.flush()

        queue = get_ranked_queue(fresh_db)
        ids = [s.id for s in queue]
        assert manual.id in ids
        assert auto.id not in ids

    def test_get_automated_actions_returns_only_automated(self, fresh_db):
        from src.services.nbra_engine import get_automated_actions

        thread = "OPP-2026-00022"
        manual = _make_score(fresh_db, thread, source_action_type="proposal_send", is_automated=False)
        auto = _make_score(fresh_db, thread, source_action_type="standing_order", is_automated=True)
        fresh_db.flush()

        actions = get_automated_actions(fresh_db)
        ids = [s.id for s in actions]
        assert auto.id in ids
        assert manual.id not in ids


# ─────────────────────────────────────────────────────────────────────────────
# 4. Offer recommendation
# ─────────────────────────────────────────────────────────────────────────────

class TestRecommendOffer:
    def test_auction_winner_returns_single_zip_pack(self):
        from src.agents.cora.contracts import recommend_offer

        entity = {
            "is_whale": False,
            "is_auction_winner": True,
            "entity_links": [{"source_table": "auction_records", "source_id": 1}],
        }
        rec = recommend_offer(entity)
        assert rec["offer"] == "single_ZIP_pack"
        assert rec["matched_rule_id"] == "auction_winner_zip_pack"

    def test_whale_beats_auction_winner(self):
        from src.agents.cora.contracts import recommend_offer

        entity = {
            "is_whale": True,
            "is_auction_winner": True,
            "entity_links": [{"source_table": "auction_records", "source_id": 1}],
        }
        rec = recommend_offer(entity)
        assert rec["offer"] == "founder_tier"
        assert rec["rule_priority"] == 1

    def test_empty_signals_returns_core_subscription_default(self):
        from src.agents.cora.contracts import recommend_offer

        rec = recommend_offer({})
        assert rec["offer"] == "core_subscription"
        assert rec["matched_rule_id"] == "default_core_sub"
        assert rec["rule_priority"] == 99


# ─────────────────────────────────────────────────────────────────────────────
# 5. PriceAssignment — band validation
# ─────────────────────────────────────────────────────────────────────────────

class TestPriceAssignment:
    def test_price_within_band_creates_validated_record(self, fresh_db):
        import src.services.price_assignment as pa_mod
        from src.services.price_assignment import assign_price, PRICE_BANDS

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = True
        try:
            band = PRICE_BANDS["core_subscription"]
            mid_price = (band["floor"] + band["ceiling"]) // 2
            result = assign_price("OPP-2026-00030", "core_subscription", mid_price, fresh_db)
            assert result.band_validated is True
            assert result.assigned_price_cents == mid_price
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig

    def test_price_outside_band_raises(self, fresh_db):
        import src.services.price_assignment as pa_mod
        from src.services.price_assignment import assign_price, PRICE_BANDS

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = True
        try:
            band = PRICE_BANDS["core_subscription"]
            over_ceiling = band["ceiling"] + 1
            with pytest.raises(ValueError, match="outside band"):
                assign_price("OPP-2026-00031", "core_subscription", over_ceiling, fresh_db)
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig

    def test_respa_gated_offer_raises(self, fresh_db):
        from src.services.price_assignment import assign_price

        with pytest.raises(ValueError, match="RESPA-gated"):
            assign_price("OPP-2026-00032", "hard_money_intro", 10000, fresh_db)

    def test_flag_disabled_always_uses_floor(self, fresh_db):
        import src.services.price_assignment as pa_mod
        from src.services.price_assignment import assign_price, PRICE_BANDS

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = False
        try:
            band = PRICE_BANDS["core_subscription"]
            result = assign_price("OPP-2026-00033", "core_subscription", band["ceiling"], fresh_db)
            assert result.assigned_price_cents == band["floor"]
            assert result.band_validated is True
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig


# ─────────────────────────────────────────────────────────────────────────────
# 5b. get_price_variant — thread-keyed arm selection (REVINT-I3 review fix)
# ─────────────────────────────────────────────────────────────────────────────

class TestPriceVariant:
    def _make_test(self, db, test_name: str, traffic_pct: int = 100) -> "AbTest":
        from src.core.models import AbTest

        test = AbTest(
            test_name=test_name,
            variant_a={},
            variant_b={},
            traffic_pct=traffic_pct,
            status="active",
            offer="core_subscription",
            control_price_cents=19700,
            test_price_cents=24700,
        )
        db.add(test)
        db.flush()
        return test

    def test_flag_disabled_returns_control_with_no_assignment(self, fresh_db):
        import src.services.ab_engine as ab_mod
        import src.services.price_assignment as pa_mod
        from src.services.ab_engine import get_price_variant

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = False
        try:
            test = self._make_test(fresh_db, "price_variant_flag_off")
            result = get_price_variant("core_subscription", test.id, "OPP-2026-00040", fresh_db)
            assert result == {"arm": "control", "price_cents": 19700, "ab_assignment_id": None}
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig

    def test_deterministic_assignment_is_stable_across_calls(self, fresh_db):
        import src.services.price_assignment as pa_mod
        from src.services.ab_engine import get_price_variant

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = True
        try:
            test = self._make_test(fresh_db, "price_variant_stable")
            first = get_price_variant("core_subscription", test.id, "OPP-2026-00041", fresh_db)
            second = get_price_variant("core_subscription", test.id, "OPP-2026-00041", fresh_db)
            assert first == second
            assert first["ab_assignment_id"] is not None

            from sqlalchemy import select
            from src.core.models import AbAssignment
            rows = fresh_db.execute(
                select(AbAssignment).where(
                    AbAssignment.test_id == test.id,
                    AbAssignment.opportunity_thread_id == "OPP-2026-00041",
                )
            ).scalars().all()
            assert len(rows) == 1  # second call reused the existing row, didn't duplicate it
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig

    def test_both_arms_reachable_with_correct_prices_and_real_assignment_ids(self, fresh_db):
        """At traffic_pct=100 every thread is in-test; across enough distinct
        threads both the control and test arm must appear, each carrying its
        own price and a real, persisted AbAssignment id — the exact defect
        the review flagged (100% of prospects silently got control)."""
        import src.services.price_assignment as pa_mod
        from src.services.ab_engine import get_price_variant

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = True
        try:
            test = self._make_test(fresh_db, "price_variant_both_arms")
            arms_seen = set()
            for i in range(20):
                thread_id = f"OPP-2026-001{i:02d}"
                result = get_price_variant("core_subscription", test.id, thread_id, fresh_db)
                arms_seen.add(result["arm"])
                assert result["ab_assignment_id"] is not None
                if result["arm"] == "test":
                    assert result["price_cents"] == 24700
                else:
                    assert result["price_cents"] == 19700

            assert arms_seen == {"control", "test"}, (
                f"expected both arms to appear across 20 threads at traffic_pct=100, got only {arms_seen}"
            )
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig

    def test_end_to_end_feeds_assign_price(self, fresh_db):
        """get_price_variant()'s ab_assignment_id must round-trip correctly
        into assign_price()'s PriceAssignment row."""
        import src.services.price_assignment as pa_mod
        from src.services.ab_engine import get_price_variant
        from src.services.price_assignment import assign_price

        orig = pa_mod.PRICE_BAND_TESTING_ENABLED
        pa_mod.PRICE_BAND_TESTING_ENABLED = True
        try:
            test = self._make_test(fresh_db, "price_variant_e2e")
            variant = get_price_variant("core_subscription", test.id, "OPP-2026-00099", fresh_db)

            assignment = assign_price(
                "OPP-2026-00099", "core_subscription", variant["price_cents"], fresh_db,
                ab_assignment_id=variant["ab_assignment_id"],
            )
            assert assignment.assigned_price_cents == variant["price_cents"]
            assert assignment.ab_assignment_id == variant["ab_assignment_id"]
        finally:
            pa_mod.PRICE_BAND_TESTING_ENABLED = orig


# ─────────────────────────────────────────────────────────────────────────────
# 6. Vertical Autopilot — dim5 and legal gate
# ─────────────────────────────────────────────────────────────────────────────

class TestVerticalAutopilot:
    def test_dim5_requires_both_money_and_urgency(self):
        from src.services.vertical_autopilot import evaluate_dim5

        both = {"prior_purchases": 5, "auction_date": "2026-08-01"}
        assert evaluate_dim5(both) == 1

    def test_dim5_money_only_returns_zero(self):
        from src.services.vertical_autopilot import evaluate_dim5

        money_only = {"prior_purchases": 5}
        assert evaluate_dim5(money_only) == 0

    def test_dim5_urgency_only_returns_zero(self):
        from src.services.vertical_autopilot import evaluate_dim5

        urgency_only = {"auction_date": "2026-08-01"}
        assert evaluate_dim5(urgency_only) == 0

    def test_legal_status_allowlisted_vertical_approved(self):
        from src.services.vertical_autopilot import check_legal_status

        status, eligible = check_legal_status("tax_lien")
        assert status == "approved"
        assert eligible is True

    def test_legal_status_unknown_vertical_pending_review(self):
        from src.services.vertical_autopilot import check_legal_status

        status, eligible = check_legal_status("unknown_vertical_xyz")
        assert status == "pending_review"
        assert eligible is False


# ─────────────────────────────────────────────────────────────────────────────
# 7. Probe loop — presell gate and package generation
# ─────────────────────────────────────────────────────────────────────────────

class TestProbeLoop:
    def _make_eligible_packet(self, db) -> VerticalCandidatePacket:
        packet = VerticalCandidatePacket(
            vertical_name="tax_lien",
            dim1_score=1,
            dim2_score=1,
            dim3_score=1,
            dim4_score=1,
            dim5_score=1,
            dim6_score=1,
            total_score=6,
            legal_status="approved",
            eligible_for_probe=True,
            status="candidate",
            created_at=datetime.now(timezone.utc),
        )
        db.add(packet)
        db.flush()
        return packet

    def _stub_sends_winning(self, probe, db) -> None:
        """Simulates >8% reply rate (18/20 = 90%)."""
        probe.sends_count = 20
        probe.reply_count = 18
        probe.completion_receipt = True

    def test_probe_win_presell_confirmed_false(self, fresh_db):
        from src.services.vertical_autopilot import run_probe

        packet = self._make_eligible_packet(fresh_db)

        with patch("src.services.vertical_autopilot._execute_sends", side_effect=self._stub_sends_winning), \
             patch("src.services.vertical_autopilot._run_compliance_preflight", return_value=True):
            probe = run_probe(packet.id, fresh_db)

        assert probe.status == "completed"
        assert float(probe.reply_rate) > 0.08

        # verdict should exist and presell_confirmed starts False
        from sqlalchemy import select
        verdict = fresh_db.execute(
            select(VerticalVerdict).where(VerticalVerdict.vertical_probe_id == probe.id)
        ).scalar_one()
        assert verdict.verdict == "won"
        assert verdict.presell_confirmed is False

    def test_confirm_presell_sets_flag(self, fresh_db):
        from src.services.vertical_autopilot import run_probe, confirm_presell

        packet = self._make_eligible_packet(fresh_db)

        with patch("src.services.vertical_autopilot._execute_sends", side_effect=self._stub_sends_winning), \
             patch("src.services.vertical_autopilot._run_compliance_preflight", return_value=True):
            probe = run_probe(packet.id, fresh_db)

        from sqlalchemy import select
        verdict = fresh_db.execute(
            select(VerticalVerdict).where(VerticalVerdict.vertical_probe_id == probe.id)
        ).scalar_one()

        updated = confirm_presell(verdict.id, fresh_db)
        assert updated.presell_confirmed is True

    def test_won_verdict_package_generated(self, fresh_db):
        from src.services.vertical_autopilot import run_probe

        packet = self._make_eligible_packet(fresh_db)

        with patch("src.services.vertical_autopilot._execute_sends", side_effect=self._stub_sends_winning), \
             patch("src.services.vertical_autopilot._run_compliance_preflight", return_value=True):
            probe = run_probe(packet.id, fresh_db)

        from sqlalchemy import select
        verdict = fresh_db.execute(
            select(VerticalVerdict).where(VerticalVerdict.vertical_probe_id == probe.id)
        ).scalar_one()
        assert verdict.package_generated is True
        assert verdict.package_id is not None

    def test_run_probe_fails_closed_without_stub(self, fresh_db):
        """Review fix: a real caller (no monkeypatched _execute_sends) must
        never fabricate a completed/"won" verdict — it should raise instead."""
        from src.services.vertical_autopilot import run_probe

        packet = self._make_eligible_packet(fresh_db)

        with patch("src.services.vertical_autopilot._run_compliance_preflight", return_value=True):
            with pytest.raises(NotImplementedError):
                run_probe(packet.id, fresh_db)

        from sqlalchemy import select
        verdict = fresh_db.execute(
            select(VerticalVerdict).where(VerticalVerdict.vertical_candidate_packet_id == packet.id)
        ).scalar_one_or_none()
        assert verdict is None
