"""tests/agents/reply_concierge/test_backflip_email_parser.py

No real Backflip notification email samples exist yet (Q7/Q8/Q13 still
open per the client clarifications doc) -- these fixtures are an assumed,
reasonable format. Recalibrate the moment real samples arrive; the parser
is isolated into this one module specifically so that recalibration never
touches stage_monitor.py, fa_max_file_state.py, or the poller wiring.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.agents.reply_concierge.backflip_email_parser import parse_backflip_notification


def _mock_llm_response(payload: dict):
    """Builds a fake anthropic.Anthropic().messages.create() return value."""
    block = MagicMock()
    block.text = json.dumps(payload)
    response = MagicMock()
    response.content = [block]
    return response


def test_stage_change_under_review():
    subject = "Application BF-10293 — Now Under Review"
    body = "Your application BF-10293 has moved to Under Review."
    result = parse_backflip_notification(subject, body)
    assert result is not None
    assert result.event_type == "stage_change"
    assert result.stage == "under_review"
    assert result.backflip_ref == "BF-10293"


def test_stage_change_conditional_approval():
    subject = "BF-55521: Conditional Approval Issued"
    body = "Conditional approval has been issued for BF-55521."
    result = parse_backflip_notification(subject, body)
    assert result.stage == "conditional_approval"
    assert result.backflip_ref == "BF-55521"


def test_document_request():
    subject = "Action needed on BF-77812: Documents requested"
    body = "We need the following document: Bank Statement (last 2 months)."
    result = parse_backflip_notification(subject, body)
    assert result.event_type == "document_request"
    assert result.document_name == "Bank Statement (last 2 months)"
    assert result.backflip_ref == "BF-77812"


def test_cleared_to_close():
    subject = "BF-33012 is Cleared to Close"
    body = "Congratulations — BF-33012 has been cleared to close."
    result = parse_backflip_notification(subject, body)
    assert result.stage == "cleared_to_close"


def test_terms_email():
    subject = "Term Sheet Ready — BF-90011"
    body = (
        "Term sheet for BF-90011: Loan amount $250,000.00, term 12 months."
    )
    result = parse_backflip_notification(subject, body)
    assert result.event_type == "terms"
    assert result.loan_amount_cents == 25_000_000
    assert result.maturity_months == 12
    assert result.backflip_ref == "BF-90011"


def test_stage_change_matches_non_bf_ref_format():
    """The ref pattern must not assume Backflip's real format -- it's
    unconfirmed (Q7/Q8/Q13). This uses our own test data's actual shape
    (letters immediately followed by digits, no hyphen)."""
    subject = "Application b1234 — Now Under Review"
    body = "Your application b1234 has moved to Under Review."
    result = parse_backflip_notification(subject, body)
    assert result is not None
    assert result.event_type == "stage_change"
    assert result.backflip_ref == "b1234"


def test_stage_change_matches_app_prefixed_ref_format():
    subject = "APP-1: Conditional Approval Issued"
    body = "Conditional approval has been issued for APP-1."
    result = parse_backflip_notification(subject, body)
    assert result is not None
    assert result.backflip_ref == "APP-1"
    assert result.stage == "conditional_approval"


def test_unrecognized_email_returns_none():
    result = parse_backflip_notification("Weekly newsletter", "Nothing relevant here.")
    assert result is None


def test_missing_backflip_ref_returns_none():
    result = parse_backflip_notification("Now Under Review", "Your file has moved to Under Review.")
    assert result is None


class TestLLMFallback:
    """The LLM fallback only fires when the regex path returns None."""

    def test_regex_match_never_calls_llm(self):
        with patch("anthropic.Anthropic") as mock_anthropic:
            result = parse_backflip_notification(
                "Application BF-10293 — Now Under Review",
                "Your application BF-10293 has moved to Under Review.",
            )
        assert result.stage == "under_review"
        mock_anthropic.assert_not_called()

    def test_llm_fallback_extracts_stage_change_in_unrecognized_format(self):
        payload = {
            "is_backflip_notification": True, "event_type": "stage_change",
            "backflip_ref": "APP-99182", "stage": "docs_requested",
            "document_name": None, "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification(
                "Your file needs attention", "We're waiting on something from you for APP-99182.",
            )
        assert result is not None
        assert result.event_type == "stage_change"
        assert result.stage == "docs_requested"
        assert result.backflip_ref == "APP-99182"

    def test_llm_fallback_extracts_document_request(self):
        payload = {
            "is_backflip_notification": True, "event_type": "document_request",
            "backflip_ref": "APP-1", "stage": None,
            "document_name": "Voided check", "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("Re: your file APP-1", "Please send a voided check.")
        assert result.event_type == "document_request"
        assert result.document_name == "Voided check"

    def test_llm_fallback_extracts_terms(self):
        payload = {
            "is_backflip_notification": True, "event_type": "terms",
            "backflip_ref": "APP-1", "stage": None, "document_name": None,
            "loan_amount_cents": 30_000_000, "maturity_months": 18,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("APP-1 approved", "Your terms are ready.")
        assert result.event_type == "terms"
        assert result.loan_amount_cents == 30_000_000
        assert result.maturity_months == 18

    def test_llm_says_not_a_backflip_email_returns_none(self):
        payload = {
            "is_backflip_notification": False, "event_type": None, "backflip_ref": None,
            "stage": None, "document_name": None, "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("Weekly digest", "Nothing about a loan file here.")
        assert result is None

    def test_llm_returns_stage_outside_known_set_is_rejected(self):
        """Safety guard: an LLM-invented stage name must never reach the caller,
        mirroring src/loaders/llm_matcher.py's own candidate-set validation."""
        payload = {
            "is_backflip_notification": True, "event_type": "stage_change",
            "backflip_ref": "APP-1", "stage": "pending_secondary_review",
            "document_name": None, "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("Update on APP-1", "Some unusual status text.")
        assert result is None

    def test_llm_returns_malformed_json_degrades_to_none(self):
        block = MagicMock()
        block.text = "not valid json at all"
        response = MagicMock()
        response.content = [block]
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = response
            result = parse_backflip_notification("Subject", "Body text with no ref.")
        assert result is None

    def test_llm_api_call_raising_degrades_to_none(self):
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.side_effect = RuntimeError("API down")
            result = parse_backflip_notification("Subject", "Body text with no ref.")
        assert result is None

    def test_llm_missing_backflip_ref_returns_none(self):
        payload = {
            "is_backflip_notification": True, "event_type": "stage_change",
            "backflip_ref": None, "stage": "under_review",
            "document_name": None, "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("Subject", "Body with vague status update.")
        assert result is None

    def test_llm_wrong_typed_stage_list_instead_of_string_degrades_to_none(self):
        """LLM returns stage as list instead of string -- unhashable type
        error when checking membership in BACKFLIP_STAGE_KEYS (frozenset).
        Must degrade to None, not raise."""
        payload = {
            "is_backflip_notification": True, "event_type": "stage_change",
            "backflip_ref": "APP-1", "stage": ["under", "review"],
            "document_name": None, "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("Subject", "Body text.")
        assert result is None

    def test_llm_wrong_typed_event_type_list_instead_of_string_degrades_to_none(self):
        """LLM returns event_type as list instead of string -- unhashable
        type error when checking membership in _EVENT_TYPES. Must degrade to None."""
        payload = {
            "is_backflip_notification": True, "event_type": ["stage", "change"],
            "backflip_ref": "APP-1", "stage": "under_review",
            "document_name": None, "loan_amount_cents": None, "maturity_months": None,
        }
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = _mock_llm_response(payload)
            result = parse_backflip_notification("Subject", "Body text.")
        assert result is None

    def test_llm_loan_amount_as_infinity_degrades_to_none(self):
        """LLM returns loan_amount_cents as float('inf') -- OverflowError when
        int() tries to convert. isinstance(float('inf'), (int, float)) is True,
        so this bypasses the isinstance guard and tries int(). Must degrade to None."""
        # Create JSON with Infinity which json.loads() accepts
        block = MagicMock()
        block.text = '{"is_backflip_notification": true, "event_type": "terms", "backflip_ref": "APP-1", "stage": null, "document_name": null, "loan_amount_cents": Infinity, "maturity_months": null}'
        response = MagicMock()
        response.content = [block]
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = response
            result = parse_backflip_notification("Subject", "Body text.")
        assert result is None

    def test_llm_maturity_months_as_invalid_float_degrades_to_none(self):
        """LLM returns maturity_months as float('inf') -- OverflowError when
        int() tries to convert. Must degrade to None, not raise."""
        import json
        block = MagicMock()
        # Manually create JSON with float('inf') which json.loads() accepts
        block.text = '{"is_backflip_notification": true, "event_type": "terms", "backflip_ref": "APP-1", "stage": null, "document_name": null, "loan_amount_cents": null, "maturity_months": Infinity}'
        response = MagicMock()
        response.content = [block]
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_anthropic.return_value.messages.create.return_value = response
            result = parse_backflip_notification("Subject", "Body text.")
        assert result is None
