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

def _v1_clean() -> CardFeatures:
    """A clean card that the baseline would approve with no signals fired."""
    return CardFeatures(
        template_version="LF-D2-SMS-v1",
        past_total=20,
        past_edit=0,
        past_reject=0,
        borrower_replied_since_draft=False,
        agent_job_verdict="unknown",
        hold_history_count=0,
        prev_draft_edited=False,
        is_first_touch=True,
        body_length=80,
        has_link=False,
        missing_first_name=False,
    )


class TestV1Judge:
    def test_clean_card_keeps_baseline_approve(self):
        p = predict_v1(_v1_clean())
        assert p.decision == "approve"

    def test_agent_job_fail_predicts_reject(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "agent_job_verdict": "fail"})
        p = predict_v1(f)
        assert p.decision == "reject"
        assert p.confidence == 0.90

    def test_agent_job_fail_overrides_even_edit_baseline(self):
        # Edit baseline + fail verdict → reject wins
        f = CardFeatures(
            template_version="LF-D2-SMS-v1",
            past_total=10, past_edit=1, past_reject=0,  # edit baseline
            agent_job_verdict="fail",
            borrower_replied_since_draft=False,
            hold_history_count=0, prev_draft_edited=False, is_first_touch=True,
            body_length=80, has_link=False, missing_first_name=False,
        )
        assert predict_v1(f).decision == "reject"

    def test_borrower_replied_downgrades_approve_to_edit(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "borrower_replied_since_draft": True})
        assert predict_v1(f).decision == "edit"

    def test_hold_history_2_downgrades_to_edit(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "hold_history_count": 2})
        assert predict_v1(f).decision == "edit"

    def test_hold_history_1_does_not_downgrade(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "hold_history_count": 1})
        assert predict_v1(f).decision == "approve"

    def test_prev_draft_edited_downgrades_to_edit(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "prev_draft_edited": True})
        assert predict_v1(f).decision == "edit"

    def test_short_body_downgrades_to_edit(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "body_length": 10})
        assert predict_v1(f).decision == "edit"

    def test_body_length_exactly_20_does_not_fire(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "body_length": 20})
        assert predict_v1(f).decision == "approve"

    def test_missing_first_name_downgrades_to_edit(self):
        f = CardFeatures(**{**_v1_clean().__dict__, "missing_first_name": True})
        assert predict_v1(f).decision == "edit"

    def test_reject_baseline_not_upgraded_by_good_signals(self):
        # Reject baseline with no bad signals → still reject
        f = CardFeatures(
            template_version="LF-D2-SMS-v1",
            past_total=10, past_edit=0, past_reject=2,  # reject baseline
            agent_job_verdict="pass",
            borrower_replied_since_draft=False, hold_history_count=0,
            prev_draft_edited=False, is_first_touch=True, body_length=100,
            has_link=False, missing_first_name=False,
        )
        assert predict_v1(f).decision == "reject"

    def test_confidence_on_edit_signal_is_proportional_to_signal_count(self):
        # 2 signals → confidence = 0.50
        f = CardFeatures(**{**_v1_clean().__dict__, "borrower_replied_since_draft": True, "hold_history_count": 2})
        p = predict_v1(f)
        assert p.decision == "edit"
        assert p.confidence == 0.50
