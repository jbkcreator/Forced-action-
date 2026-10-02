"""WP-GL-9 wording, borrower-name rules and template choice (no DB)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.lending_text_back import MAX_TEXT_CHARS, TEMPLATES
from src.lending.text_back import PendingText, choose_template, first_name_of, format_number, render_body

NUMBER = "+18135550100"


def _item(**over) -> PendingText:
    base = dict(event_id=1, call_id="c1", phone="+18135558601", ended_at=datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc),
                property_address="123 Main St, Tampa FL 33602", queue="verified_maturity", caller_name="Alex",
                borrower_name="Sam Jones", county="Hillsborough")
    base.update(over)
    return PendingText(**base)


def test_the_three_client_approved_texts_are_unchanged():
    assert TEMPLATES["maturity"] == ("{greeting}, it's {caller} with Next Deal Lending. Sorry I missed you. "
                                     "I was calling about the loan on {property}. "
                                     "Call or text me back at {number} when it suits you. Reply STOP to opt out.")
    assert TEMPLATES["deal_drop"] == ("{greeting}, {caller} from Next Deal Lending here. Just tried you about {property}. "
                                      "We help investors in {county} fund their next deal. Text back if you'd like to chat. "
                                      "Reply STOP to opt out.")
    assert TEMPLATES["general"] == ("{greeting}, this is {caller} with Next Deal Lending. Sorry we missed each other. "
                                    "Reply here or call {number} whenever works. Reply STOP to opt out.")


@pytest.mark.parametrize("over,expected", [
    ({}, "maturity"),
    ({"queue": "transaction_ready"}, "deal_drop"),
    ({"queue": "builders"}, "deal_drop"),
    ({"queue": "nurture"}, "general"),
    ({"queue": None}, "general"),
    ({"queue": "verified_maturity", "property_address": None}, "general"),
    ({"queue": "builders", "county": None}, "general"),
    ({"queue": "builders", "property_address": "  "}, "general"),
])
def test_template_follows_the_queue_and_falls_back_to_general_without_an_address_or_county(over, expected):
    assert choose_template(_item(**over)) == expected


@pytest.mark.parametrize("name,expected", [
    ("Samuel Jones", "Samuel"), ("SAM JONES", "Sam"), ("mary-ann lee", "Mary-Ann"), ("McDonald Lee", "McDonald"),
    ("Acme Holdings LLC", None), ("Smith Family Trust", None), ("J. Smith", None), ("  ", None), (None, None), ("X", None),
])
def test_first_name_is_a_real_first_name_or_nothing(name, expected):
    assert first_name_of(name) == expected


def test_maturity_text_renders_exactly():
    assert render_body(_item(), "maturity", number=NUMBER) == (
        "Hi Sam, it's Alex with Next Deal Lending. Sorry I missed you. I was calling about the loan on 123 Main St. "
        "Call or text me back at (813) 555-0100 when it suits you. Reply STOP to opt out.")


def test_no_first_name_or_caller_still_reads_naturally():
    body = render_body(_item(borrower_name="Acme Holdings LLC", caller_name=None), "general", number=NUMBER)
    assert body.startswith("Hi, this is our team with Next Deal Lending.")


def test_format_number_shows_a_us_number_and_passes_anything_else_through():
    assert format_number("+18135550100") == "(813) 555-0100"
    assert format_number("+442071838750") == "+442071838750"


@pytest.mark.parametrize("key", ["maturity", "deal_drop", "general"])
def test_worst_case_fields_keep_the_text_short_and_the_stop_language_whole(key):
    long = "x" * 300
    body = render_body(_item(property_address=long, county=long, caller_name=long, borrower_name=long + " Jones"), key, number=NUMBER)
    assert len(body) <= MAX_TEXT_CHARS and body.endswith("Reply STOP to opt out.")
