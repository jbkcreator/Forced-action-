"""
Regression: which win size gets which prompt (E3).

The merge of the testimonial ask into the referral email was scoped to BIG
wins only. An earlier revision deleted the unconditional referral-prompt call
from record_outcome_side_effects and replaced it with a big-win-gated social
proof email, which silently removed referral prompts from every win under
$10k. These tests pin both halves so that can't recur:

  small win -> plain referral prompt (as before)
  big win   -> no plain prompt; the merged testimonial+referral email instead
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.services.deal_outcome_effects import record_outcome_side_effects


class _FakeDb:
    """Minimal session stand-in — begin_nested() must be a context manager."""

    @contextmanager
    def begin_nested(self):
        yield


def _outcome(**overrides):
    payload = {
        "id": 99,
        "subscriber_id": 7,
        "property_id": None,
        "deal_amount": None,
        "deal_size_bucket": "5_10k",
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _subscriber():
    return SimpleNamespace(id=7, email="winner@example.com", name="Winner")


@contextmanager
def _isolate_other_effects():
    """Stub the unrelated side-effects so only referral routing is exercised."""
    with patch("src.services.lifecycle_suppression.create_suppression"), \
         patch("src.services.win_graphic.generate", return_value=None), \
         patch("src.services.win_autopsy.record_win_autopsy"), \
         patch("src.services.attribution_service.record_conversion_attribution"), \
         patch("src.tasks.annual_push._push_annual_offer", return_value=False):
        yield


def test_small_win_still_gets_the_plain_referral_prompt():
    mock_prompt = MagicMock()
    with _isolate_other_effects(), \
         patch("src.services.referral_prompt_service.maybe_send_referral_prompt", mock_prompt):
        record_outcome_side_effects(
            _outcome(deal_amount=8000, deal_size_bucket="5_10k"), _subscriber(), _FakeDb()
        )
    mock_prompt.assert_called_once()
    assert mock_prompt.call_args.kwargs["trigger_type"] == "deal_win"


def test_big_win_does_not_get_the_plain_referral_prompt():
    """Big wins get the merged testimonial+referral email instead — one email, not two."""
    mock_prompt = MagicMock()
    with _isolate_other_effects(), \
         patch("src.services.referral_prompt_service.maybe_send_referral_prompt", mock_prompt):
        record_outcome_side_effects(
            _outcome(deal_amount=15000, deal_size_bucket="10_25k"), _subscriber(), _FakeDb()
        )
    mock_prompt.assert_not_called()


def test_skip_outcome_gets_no_referral_prompt():
    mock_prompt = MagicMock()
    with _isolate_other_effects(), \
         patch("src.services.referral_prompt_service.maybe_send_referral_prompt", mock_prompt):
        record_outcome_side_effects(
            _outcome(deal_size_bucket="skip"), _subscriber(), _FakeDb()
        )
    mock_prompt.assert_not_called()


def test_ownerless_outcome_is_a_no_op():
    mock_prompt = MagicMock()
    with _isolate_other_effects(), \
         patch("src.services.referral_prompt_service.maybe_send_referral_prompt", mock_prompt):
        result = record_outcome_side_effects(
            _outcome(subscriber_id=None), None, _FakeDb()
        )
    mock_prompt.assert_not_called()
    assert result == {"graphic_url": None, "annual_offered": False}
