"""R1 0.9 Seam C: auto-send judge rules — pure, no DB.

Tests are worked examples: expected values come from the spec thresholds and rule tables in
0.9-GRILL-DECISIONS.md D11, not from the code recomputing them.
"""
from __future__ import annotations

import pytest

from src.lending.shadow.judge import CardFeatures, predict_baseline, predict_v1

# ── Baseline judge ────────────────────────────────────────────────────────────

def _base(
    *,
    total: int = 0,
    edit: int = 0,
    reject: int = 0,
    template: str = "LF-D2-SMS-v1",
) -> CardFeatures:
    return CardFeatures(
        template_version=template,
        past_total=total,
        past_edit=edit,
        past_reject=reject,
    )


class TestBaselineJudge:
    def test_thin_history_gives_approve_with_no_confidence(self):
        p = predict_baseline(_base(total=4, edit=0, reject=0))
        assert p.decision == "approve"
        assert p.confidence is None

    def test_zero_history_also_thin(self):
        p = predict_baseline(_base(total=0))
        assert p.decision == "approve"
        assert p.confidence is None

    def test_exactly_at_threshold_is_thin(self):
        # 4 past decisions = thin (need ≥ 5)
        assert predict_baseline(_base(total=4)).confidence is None

    def test_exactly_5_history_uses_rates(self):
        # 5 decisions, 0 edits, 0 rejects → approve with confidence
        p = predict_baseline(_base(total=5, edit=0, reject=0))
        assert p.decision == "approve"
        assert p.confidence is not None

    def test_reject_rate_at_20_percent_predicts_reject(self):
        # 10 total, 2 rejected = 20 % → reject
        p = predict_baseline(_base(total=10, reject=2, edit=0))
        assert p.decision == "reject"
        assert p.confidence is not None

    def test_reject_rate_below_20_percent_does_not_reject(self):
        # 10 total, 1 rejected = 10 % < 20 %
        p = predict_baseline(_base(total=10, reject=1, edit=0))
        assert p.decision != "reject"

    def test_edit_rate_at_10_percent_predicts_edit(self):
        # 10 total, 1 edited = 10 % → edit
        p = predict_baseline(_base(total=10, edit=1, reject=0))
        assert p.decision == "edit"
        assert p.confidence is not None

    def test_edit_rate_below_10_percent_does_not_edit(self):
        # 20 total, 1 edited = 5 % < 10 %
        p = predict_baseline(_base(total=20, edit=1, reject=0))
        assert p.decision == "approve"

    def test_reject_threshold_takes_priority_over_edit(self):
        # both thresholds met; reject wins
        p = predict_baseline(_base(total=10, edit=1, reject=2))
        assert p.decision == "reject"

    def test_confidence_is_bounded_to_1(self):
        # very high reject rate should not produce confidence > 1
        p = predict_baseline(_base(total=5, reject=5, edit=0))
        assert p.confidence is not None
        assert p.confidence <= 1.0

    def test_all_approved_cleanly_gives_approve_with_confidence(self):
        p = predict_baseline(_base(total=25, edit=0, reject=0))
        assert p.decision == "approve"
        assert p.confidence is not None and p.confidence > 0


# ── V1 judge — card signals ──────────────────────────────────────────────────

def _v1(**changes) -> CardFeatures:
    """A clean first-touch card the baseline approves, with no red flag, plus the given changes."""
    clean = dict(template_version="LF-D2-SMS-v1", past_total=20, past_edit=0, past_reject=0,
                 is_first_touch=True, hours_since_last_message=None, body_length=80)
    return CardFeatures(**{**clean, **changes})


class TestV1Judge:
    def test_clean_card_keeps_baseline_approve(self):
        assert predict_v1(_v1()).decision == "approve"

    @pytest.mark.parametrize("signal", [
        {"borrower_replied_since_last_send": True},
        {"from_agent_job": True, "agent_job_held": True},
        {"prev_draft_edited": True},
        {"is_first_touch": False, "hours_since_last_message": 3.0},
        {"has_link": True},
        {"body_length": 10},
        {"missing_first_name": True},
    ])
    def test_each_red_flag_downgrades_approve_to_edit(self, signal):
        p = predict_v1(_v1(**signal))
        assert p.decision == "edit"
        assert p.confidence == 0.25

    def test_agent_job_that_was_never_held_is_no_flag(self):
        assert predict_v1(_v1(from_agent_job=True, agent_job_held=False)).decision == "approve"

    def test_follow_up_after_a_day_is_no_flag(self):
        assert predict_v1(_v1(is_first_touch=False, hours_since_last_message=30.0)).decision == "approve"

    def test_link_on_a_follow_up_is_no_flag(self):
        assert predict_v1(_v1(is_first_touch=False, hours_since_last_message=48.0, has_link=True)).decision == "approve"

    def test_body_length_exactly_20_is_no_flag(self):
        assert predict_v1(_v1(body_length=20)).decision == "approve"

    def test_two_flags_give_confidence_half(self):
        p = predict_v1(_v1(borrower_replied_since_last_send=True, prev_draft_edited=True))
        assert (p.decision, p.confidence) == ("edit", 0.5)

    def test_reject_baseline_is_never_upgraded(self):
        assert predict_v1(_v1(past_total=10, past_reject=2)).decision == "reject"

    def test_edit_baseline_with_a_flag_stays_edit_with_baseline_confidence(self):
        # 10 total, 1 edited = 10% → baseline edit at confidence 1.0; v1 keeps the baseline
        p = predict_v1(_v1(past_total=10, past_edit=1, prev_draft_edited=True))
        assert (p.decision, p.confidence) == ("edit", 1.0)
