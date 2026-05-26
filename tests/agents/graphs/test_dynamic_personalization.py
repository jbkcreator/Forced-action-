"""
Dynamic SMS personalization tests — fa038.

Verifies that the same FOMO campaign produces materially different prompt
context and fallback copy across distinct trade/county/behavior/score
combinations, and that send_sms receives a personalization_context with all
required fields.

All external I/O (DB, Claude, SMS dispatch) is mocked.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict
from unittest.mock import MagicMock, patch, call

import pytest

from src.agents.context_utils import (
    build_personalization_fields,
    county_display_name,
    days_to_recency_band,
    score_to_band,
)
from src.agents.graphs.fomo import _node_build_compose_context


# ─────────────────────────────────────────────────────────────────────────────
# Part 1 — unit tests for context_utils helpers
# ─────────────────────────────────────────────────────────────────────────────

class TestScoreToBand:
    def test_very_high(self):
        assert score_to_band(100) == "very_high"
        assert score_to_band(80) == "very_high"

    def test_high(self):
        assert score_to_band(79) == "high"
        assert score_to_band(60) == "high"

    def test_medium(self):
        assert score_to_band(59) == "medium"
        assert score_to_band(30) == "medium"

    def test_low(self):
        assert score_to_band(29) == "low"
        assert score_to_band(0) == "low"


class TestDaysToRecencyBand:
    def test_same_day(self):
        assert days_to_recency_band(0) == "same_day"

    def test_recent(self):
        assert days_to_recency_band(1) == "recent_1_3_days"
        assert days_to_recency_band(3) == "recent_1_3_days"

    def test_cooling(self):
        assert days_to_recency_band(4) == "cooling_4_7_days"
        assert days_to_recency_band(7) == "cooling_4_7_days"

    def test_stale(self):
        assert days_to_recency_band(8) == "stale_8_plus_days"
        assert days_to_recency_band(30) == "stale_8_plus_days"

    def test_unknown(self):
        assert days_to_recency_band(None) == "unknown"


class TestCountyDisplayName:
    def test_known_county(self):
        assert county_display_name("hillsborough") == "Hillsborough County"
        assert county_display_name("pinellas") == "Pinellas County"
        assert county_display_name("pasco") == "Pasco County"

    def test_unknown_county(self):
        # Unknown IDs get title-cased
        result = county_display_name("some_new_county")
        assert "county" in result.lower() or "Some New County" in result

    def test_none_county(self):
        assert county_display_name(None) == "your county"
        assert county_display_name("") == "your county"


class TestBuildPersonalizationFields:
    def test_all_fields_present(self):
        profile = {"county_id": "hillsborough", "vertical": "roofing"}
        segment_data = {
            "segment": "high_intent",
            "revenue_signal_score": 85,
            "revenue_signal_band": "very_high",
            "last_significant_action_at": None,
        }
        result = build_personalization_fields(profile, segment_data, 85)

        assert result["county_id"] == "hillsborough"
        assert result["county_name"] == "Hillsborough County"
        assert result["behavioral_segment"] == "high_intent"
        assert result["revenue_signal_score"] == 85
        assert result["revenue_signal_score_band"] == "very_high"
        assert "last_action_recency_band" in result
        assert "days_since_last_action" in result

    def test_falls_back_to_computed_band_when_db_band_missing(self):
        profile = {}
        segment_data = {"segment": "engaged", "revenue_signal_score": 65, "revenue_signal_band": None}
        result = build_personalization_fields(profile, segment_data, 65)
        assert result["revenue_signal_score_band"] == "high"

    def test_empty_segment_data_safe(self):
        result = build_personalization_fields({}, {}, 0)
        assert result["behavioral_segment"] == "unknown"
        assert result["revenue_signal_score_band"] == "low"
        assert result["last_action_recency_band"] == "unknown"
        assert result["county_name"] == "your county"


# ─────────────────────────────────────────────────────────────────────────────
# Part 2 — FOMO context builder produces different context per combination
# ─────────────────────────────────────────────────────────────────────────────

def _fomo_state(
    *,
    vertical: str,
    county_id: str,
    segment: str,
    score: int,
    recency_band: str = "same_day",
    zip_code: str = "33647",
) -> dict:
    """Build a FOMOState-shaped dict for direct node testing."""
    from datetime import datetime, timezone, timedelta

    # Pick a last_significant_action_at that matches the recency band
    _recency_to_days = {
        "same_day": 0,
        "recent_1_3_days": 2,
        "cooling_4_7_days": 5,
        "stale_8_plus_days": 12,
        "unknown": None,
    }
    days = _recency_to_days.get(recency_band)
    if days is not None:
        ts = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    else:
        ts = None

    return {
        "decision_id": str(uuid.uuid4()),
        "subscriber_id": 1,
        "event_payload": {
            "zip_code": zip_code,
            "vertical": vertical,
            "lead_tier": "Gold",
        },
        "subscriber_profile": {
            "id": 1,
            "name": "Test User",
            "vertical": vertical,
            "county_id": county_id,
            "tier": "starter",
            "status": "active",
        },
        "zip_activity": {"active_viewers": 7, "messages_last_24h": 2},
        "competition_status": {"is_locked": False, "active_wallet_users_in_vertical": 3},
        "segment_data": {
            "segment": segment,
            "revenue_signal_score": score,
            "revenue_signal_band": score_to_band(score),
            "last_significant_action_at": ts,
            "revenue_signal_last_action": "wallet_credit_spent",
            "classified_at": datetime.now(timezone.utc).isoformat(),
            "reason": f"{segment}:test",
        },
        "revenue_signal_score": score,
        "action_allowed": True,
        "use_fallback": False,
    }


# Combination A: roofing + Hillsborough + very_high score + same_day activity
COMBO_A = _fomo_state(
    vertical="roofing",
    county_id="hillsborough",
    segment="high_intent",
    score=87,
    recency_band="same_day",
)

# Combination B: public_adjusters + Pinellas + medium score + cooling activity
COMBO_B = _fomo_state(
    vertical="public_adjusters",
    county_id="pinellas",
    segment="at_risk",
    score=45,
    recency_band="cooling_4_7_days",
)

# Combination C: attorneys + Pasco + low score + stale activity
COMBO_C = _fomo_state(
    vertical="attorneys",
    county_id="pasco",
    segment="churned",
    score=12,
    recency_band="stale_8_plus_days",
)


def _run_build_context(state: dict) -> dict:
    """
    Run _node_build_compose_context with loader patched so no filesystem I/O
    occurs. Returns the _render_context from the node output.
    """
    fake_system = "SYSTEM_PROMPT"
    fake_user = "USER_PROMPT"

    with patch(
        "src.agents.graphs.fomo.render_for_subscriber_auto",
        return_value=(fake_system, fake_user, None, None),
    ), patch(
        "src.agents.graphs.fomo.render_fallback_body",
        return_value="FALLBACK_BODY",
    ):
        output = _node_build_compose_context(state)

    return output.get("_render_context", {})


class TestFomoContextBuilderPersonalization:
    def test_combo_a_roofing_hillsborough_very_high(self):
        ctx = _run_build_context(COMBO_A)
        assert ctx["vertical"] == "roofing"
        assert ctx["county_id"] == "hillsborough"
        assert ctx["county_name"] == "Hillsborough County"
        assert ctx["behavioral_segment"] == "high_intent"
        assert ctx["revenue_signal_score"] == 87
        assert ctx["revenue_signal_score_band"] == "very_high"
        assert ctx["last_action_recency_band"] == "same_day"
        assert ctx["prompt_version"] == "fomo_v2"

    def test_combo_b_pa_pinellas_medium_cooling(self):
        ctx = _run_build_context(COMBO_B)
        assert ctx["vertical"] == "public_adjusters"
        assert ctx["county_id"] == "pinellas"
        assert ctx["county_name"] == "Pinellas County"
        assert ctx["behavioral_segment"] == "at_risk"
        assert ctx["revenue_signal_score"] == 45
        assert ctx["revenue_signal_score_band"] == "medium"
        assert ctx["last_action_recency_band"] == "cooling_4_7_days"

    def test_combo_c_attorneys_pasco_low_stale(self):
        ctx = _run_build_context(COMBO_C)
        assert ctx["vertical"] == "attorneys"
        assert ctx["county_id"] == "pasco"
        assert ctx["county_name"] == "Pasco County"
        assert ctx["behavioral_segment"] == "churned"
        assert ctx["revenue_signal_score"] == 12
        assert ctx["revenue_signal_score_band"] == "low"
        assert ctx["last_action_recency_band"] == "stale_8_plus_days"

    def test_all_three_contexts_are_materially_different(self):
        """Verify the key differentiating fields differ across all 3 combinations."""
        ctx_a = _run_build_context(COMBO_A)
        ctx_b = _run_build_context(COMBO_B)
        ctx_c = _run_build_context(COMBO_C)

        # Verticals must all differ
        assert len({ctx_a["vertical"], ctx_b["vertical"], ctx_c["vertical"]}) == 3

        # Counties must all differ
        assert len({ctx_a["county_id"], ctx_b["county_id"], ctx_c["county_id"]}) == 3

        # Score bands must all differ
        assert len({
            ctx_a["revenue_signal_score_band"],
            ctx_b["revenue_signal_score_band"],
            ctx_c["revenue_signal_score_band"],
        }) == 3

        # Recency bands must all differ
        assert len({
            ctx_a["last_action_recency_band"],
            ctx_b["last_action_recency_band"],
            ctx_c["last_action_recency_band"],
        }) == 3

        # Segments must all differ
        assert len({
            ctx_a["behavioral_segment"],
            ctx_b["behavioral_segment"],
            ctx_c["behavioral_segment"],
        }) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Part 3 — fallback body differs across verticals
# ─────────────────────────────────────────────────────────────────────────────

class TestFallbackVariation:
    def test_roofing_fallback_differs_from_default(self):
        from src.agents.prompts.loader import render_fallback_body

        roofing_ctx = {
            "vertical": "roofing",
            "first_name": "Mike",
            "zip_code": "33647",
            "active_lead_count": 8,
            "unlock_link": "https://app.forcedaction.io/feed/1",
        }
        default_ctx = {**roofing_ctx, "vertical": "wholesalers"}

        roofing_body = render_fallback_body("fomo", roofing_ctx)
        default_body = render_fallback_body("fomo", default_ctx)

        # Roofing fallback should mention something specific to roofing
        assert roofing_body != default_body
        assert "roofer" in roofing_body.lower() or "storm" in roofing_body.lower() or "roofing" in roofing_body.lower()

    def test_attorneys_fallback_differs_from_default(self):
        from src.agents.prompts.loader import render_fallback_body

        attorney_ctx = {
            "vertical": "attorneys",
            "first_name": "Sarah",
            "zip_code": "34638",
            "active_lead_count": 5,
            "unlock_link": "https://app.forcedaction.io/feed/2",
        }
        default_ctx = {**attorney_ctx, "vertical": "wholesalers"}

        attorney_body = render_fallback_body("fomo", attorney_ctx)
        default_body = render_fallback_body("fomo", default_ctx)

        assert attorney_body != default_body
        assert "attorney" in attorney_body.lower() or "probate" in attorney_body.lower()

    def test_pa_fallback_differs_from_default(self):
        from src.agents.prompts.loader import render_fallback_body

        pa_ctx = {
            "vertical": "public_adjusters",
            "first_name": "John",
            "zip_code": "33701",
            "active_lead_count": 4,
            "unlock_link": "https://app.forcedaction.io/feed/3",
        }
        default_ctx = {**pa_ctx, "vertical": "wholesalers"}

        pa_body = render_fallback_body("fomo", pa_ctx)
        default_body = render_fallback_body("fomo", default_ctx)

        assert pa_body != default_body
        assert "PA" in pa_body or "claim" in pa_body.lower() or "storm" in pa_body.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Part 4 — send_sms receives personalization_context with all required fields
# ─────────────────────────────────────────────────────────────────────────────

class TestSendSmsPersonalizationLogging:
    def test_send_sms_receives_personalization_context(self):
        """
        Run the full FOMO graph with Claude and DB mocked. Verify that send_sms
        is called with a personalization_context that includes all fa038 fields.
        """
        from src.agents.graphs.fomo import run_fomo

        fake_claude = {
            "text": "Mike, a roofer just took a Gold lead in 33647. 7 remain. [link]",
            "model": "haiku",
            "input_tokens": 100,
            "output_tokens": 30,
            "cost_usd": 0.0001,
        }

        captured_personalization = {}

        def _fake_send_sms(**kwargs):
            captured_personalization.update(kwargs.get("personalization_context") or {})
            return {"sent": True, "reason": "ok", "message_outcome_id": 1}

        patches = [
            patch("src.agents.graphs.fomo.get_subscriber_profile",
                  return_value={
                      "id": 1, "name": "Mike", "vertical": "roofing",
                      "county_id": "hillsborough", "tier": "starter",
                      "status": "active", "has_saved_card": False,
                  }),
            patch("src.agents.graphs.fomo.get_zip_activity",
                  return_value={"active_viewers": 7, "messages_last_24h": 2}),
            patch("src.agents.graphs.fomo.get_competition_status",
                  return_value={"is_locked": False, "active_wallet_users_in_vertical": 3}),
            patch("src.agents.graphs.fomo.get_segment_and_score",
                  return_value={
                      "segment": "high_intent",
                      "revenue_signal_score": 87,
                      "revenue_signal_band": "very_high",
                      "last_significant_action_at": None,
                      "revenue_signal_last_action": None,
                      "classified_at": None,
                      "reason": "high_intent:rss=87",
                  }),
            patch("src.agents.graphs.fomo.run_decision_hierarchy",
                  return_value={
                      "action_allowed": True,
                      "use_fallback": False,
                      "kill_switch_color": "green",
                      "revenue_signal_score": 87,
                      "segment": "high_intent",
                  }),
            patch("src.agents.graphs.fomo.get_cached_metric", return_value=None),
            # render_for_subscriber_auto opens its own DB session for A/B variant
            # lookup — mock it directly to avoid DB calls in this unit test.
            patch("src.agents.graphs.fomo.render_for_subscriber_auto",
                  return_value=("SYS_PROMPT", "USR_PROMPT", None, None)),
            patch("src.agents.graphs.fomo.render_fallback_body",
                  return_value="FALLBACK"),
            patch("src.agents.subgraphs.compose_and_send.call_claude_with_usage",
                  return_value=fake_claude),
            patch("src.agents.subgraphs.compose_and_send.compliance_check",
                  return_value={"can_send": True, "reason": "ok"}),
            patch("src.agents.subgraphs.compose_and_send.send_sms",
                  side_effect=_fake_send_sms),
            patch("src.agents.subgraphs.compose_and_send.log_decision"),
            patch("src.agents.subgraphs.compose_and_send.budget_check",
                  return_value={"allowed": True, "reason": "ok"}),
        ]

        for p in patches:
            p.start()
        try:
            result = run_fomo(
                event_payload={
                    "zip_code": "33647",
                    "vertical": "roofing",
                    "lead_tier": "Gold",
                },
                subscriber_id=1,
            )
        finally:
            for p in patches:
                p.stop()

        assert result["terminal_status"] == "completed"
        assert result["sent"] is True

        # Verify all fa038 personalization fields were passed to send_sms
        assert captured_personalization["vertical"] == "roofing"
        assert captured_personalization["county_id"] == "hillsborough"
        assert captured_personalization["county_name"] == "Hillsborough County"
        assert captured_personalization["behavioral_segment"] == "high_intent"
        assert captured_personalization["revenue_signal_score"] == 87
        assert captured_personalization["revenue_signal_score_band"] == "very_high"
        assert "last_action_recency_band" in captured_personalization
        assert captured_personalization["prompt_version"] == "fomo_v2"


# ─────────────────────────────────────────────────────────────────────────────
# Part 5 — Abandonment context builder also includes personalization fields
# ─────────────────────────────────────────────────────────────────────────────

class TestAbandonmentPersonalizationContext:
    def test_wave1_context_includes_personalization(self):
        """
        Directly run _wave1_build_context and verify the render context
        contains all fa038 fields.
        """
        from src.agents.graphs.abandonment import _wave1_build_context

        state = {
            "decision_id": str(uuid.uuid4()),
            "subscriber_id": 2,
            "event_payload": {
                "zip_code": "33701",
                "vertical": "public_adjusters",
                "minutes_elapsed": 12,
                "wall_countdown_minutes": 3,
            },
            "subscriber_profile": {
                "id": 2,
                "name": "John",
                "vertical": "public_adjusters",
                "county_id": "pinellas",
                "tier": "starter",
                "status": "active",
            },
            "zip_activity": {"active_viewers": 4},
            "segment_data": {
                "segment": "at_risk",
                "revenue_signal_score": 40,
                "revenue_signal_band": "medium",
                "last_significant_action_at": None,
                "revenue_signal_last_action": None,
            },
            "wallet_state": {},
            "recent_messages": [],
            "action_allowed": True,
            "use_fallback": False,
        }

        with patch("src.agents.graphs.abandonment.render_for_subscriber_auto",
                   return_value=("SYS", "USR", None, None)), \
             patch("src.agents.graphs.abandonment.render_fallback_body",
                   return_value="FALLBACK"):
            output = _wave1_build_context(state)

        ctx = output.get("_render_context", {})
        assert ctx["vertical"] == "public_adjusters"
        assert ctx["county_name"] == "Pinellas County"
        assert ctx["behavioral_segment"] == "at_risk"
        assert ctx["revenue_signal_score_band"] == "medium"
        assert ctx["prompt_version"] == "abandonment_w1_v2"
