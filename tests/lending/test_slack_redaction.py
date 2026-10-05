"""A borrower's SSN, account or card number is never posted to Slack (no borrower financial data, ever)."""
from __future__ import annotations

import pytest

from src.lending.reply_guard import ReplyEvent, format_slack, redact_financial_digits


@pytest.mark.parametrize("raw", [
    "my SSN is 123-45-6789 what is your rate",
    "ssn 123 45 6789",
    "account 123456789012 rate?",
    "card 4111 1111 1111 1111",
    "ssn is 123456789",
])
def test_long_digit_runs_are_masked(raw):
    masked = redact_financial_digits(raw)
    assert "[redacted]" in masked
    assert not any(run in masked for run in ("123-45-6789", "123 45 6789", "123456789", "4111"))


@pytest.mark.parametrize("raw", ["I need $300,000 at 12 points", "close by 10/15/2026", "call 3 or 4 times", "zip 33601"])
def test_ordinary_numbers_are_left_alone(raw):
    assert redact_financial_digits(raw) == raw


def test_the_slack_post_never_contains_an_ssn():
    event = ReplyEvent(message_id="m1", direction="inbound", body="my SSN is 123-45-6789, what is your rate?",
                       contact_id="c1", first_name="Marcus", phone="+18135550147", handoff=False)
    message = format_slack("rate_terms_handoff", event)
    assert "123-45-6789" not in message and "6789" not in message and "[redacted]" in message
    assert "what is your rate" in message
