"""Where the lending GHL webhooks find a key: top level, then ``customData`` (a GHL webhook action's key/value body),
then ``contact`` (GHL's default nested payload)."""
from __future__ import annotations

import pytest

from src.lending.ghl_consent_events import field, message_tokens


@pytest.mark.parametrize("body", [
    {"stage_name": "Funded"},
    {"customData": {"stage_name": "Funded"}},
    {"contact": {"stage_name": "Funded"}},
])
def test_a_key_is_found_in_each_place(body):
    assert field(body, "stage_name") == "Funded"


def test_the_top_level_wins_then_custom_data_then_contact():
    body = {"phone": "top", "customData": {"phone": "custom", "email": "custom@example.com"},
            "contact": {"phone": "contact", "email": "contact@example.com", "first_name": "Ana"}}
    assert field(body, "phone") == "top"
    assert field(body, "email") == "custom@example.com"
    assert field(body, "first_name") == "Ana"


@pytest.mark.parametrize("body", [
    {"customData": {"stage_name": ""}, "contact": {"stage_name": "Held"}},  # an empty value falls through
    {"customData": "not-a-dict", "contact": {"stage_name": "Held"}},
])
def test_empty_or_malformed_custom_data_falls_through(body):
    assert field(body, "stage_name") == "Held"


def test_missing_everywhere_is_none():
    assert field({"customData": {}, "contact": {}}, "opportunity_id") is None


def test_a_number_is_returned_as_text():
    assert field({"customData": {"monetary_value": 1500}}, "monetary_value") == "1500"


def test_the_reply_text_is_read_from_custom_data():
    assert message_tokens({"customData": {"message": "STOP please"}}) == ["stop", "please"]
