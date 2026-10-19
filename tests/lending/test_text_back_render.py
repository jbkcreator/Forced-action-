"""WP-GL-9 wording, borrower-name rules and template choice (no DB)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from config.lending_text_back import BOOKING_LINK, CAPS, MAX_TEXT_CHARS, TEMPLATES
from src.lending.text_back import PendingText, choose_template, first_name_of, format_number, render_body

NUMBER = "+18135550100"


def _item(**over) -> PendingText:
    base = dict(event_id=1, call_id="c1", phone="+18135558601", ended_at=datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc),
                property_address="123 Main St, Tampa FL 33602", queue="verified_maturity", caller_name="Alex",
                borrower_name="Sam Jones", county="Hillsborough")
    base.update(over)
    return PendingText(**base)


def test_the_three_texts_carry_no_company_name():
    assert TEMPLATES["maturity"] == ("{greeting}, it's {caller} with Josh Kantor, Loan Officer. Sorry I missed you. "
                                     "I was calling about the loan on {property}. "
                                     "Call or text me back at {number} when it suits you. "
                                     f"Book a time: {BOOKING_LINK} Reply STOP to opt out.")
    assert TEMPLATES["deal_drop"] == ("{greeting}, {caller} with Josh Kantor, Loan Officer here. Just tried you about {property}. "
                                      "We help investors in {county} fund their next deal. Text back if you'd like to chat. "
                                      f"Book a time: {BOOKING_LINK} Reply STOP to opt out.")
    assert TEMPLATES["general"] == ("{greeting}, this is {caller} with Josh Kantor, Loan Officer. Sorry we missed each other. "
                                    "Reply here or call {number} whenever works. "
                                    f"Book a time: {BOOKING_LINK} Reply STOP to opt out.")


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
    ("Bayshore Homes", None), ("Smith Construction", None), ("Tampa Bay Builders", None),
    ("Smith & Sons Roofing", None), ("Acme L.L.C.", None), ("Estate of John Smith", None),
    ("Smith, John", None), ("José Garcia", None),
])
def test_first_name_is_a_real_first_name_or_nothing(name, expected):
    assert first_name_of(name) == expected


def test_maturity_text_renders_exactly():
    assert render_body(_item(), "maturity", number=NUMBER) == (
        "Hi Sam, it's Alex with Josh Kantor, Loan Officer. Sorry I missed you. I was calling about the loan on 123 Main St. "
        "Call or text me back at (813) 555-0100 when it suits you. "
        f"Book a time: {BOOKING_LINK} Reply STOP to opt out.")


def test_no_first_name_or_caller_still_reads_naturally():
    body = render_body(_item(borrower_name="Acme Holdings LLC", caller_name=None), "general", number=NUMBER)
    assert body.startswith("Hi, this is our team with Josh Kantor, Loan Officer.")


def test_format_number_shows_a_us_number_and_passes_anything_else_through():
    assert format_number("+18135550100") == "(813) 555-0100"
    assert format_number("+442071838750") == "+442071838750"


@pytest.mark.parametrize("key", ["maturity", "deal_drop", "general"])
def test_worst_case_fields_keep_the_text_short_and_the_stop_language_whole(key):
    long = "x" * 300
    body = render_body(_item(property_address=long, county=long, caller_name=long, borrower_name=long + " Jones"), key, number=NUMBER)
    assert len(body) <= MAX_TEXT_CHARS and body.endswith("Reply STOP to opt out.")


def test_street_address_with_only_comma_and_city_falls_back_to_general():
    assert choose_template(_item(queue="verified_maturity", property_address=", Tampa FL")) == "general"


def test_render_body_raises_when_number_is_blank():
    with pytest.raises(ValueError, match="a texting number is required"):
        render_body(_item(), "general", number="")
    with pytest.raises(ValueError, match="a texting number is required"):
        render_body(_item(), "general", number=None)


def test_whitespace_is_collapsed_in_substituted_values():
    body = render_body(
        _item(property_address="123  Main\tSt, Tampa FL", caller_name="Alex\n  Kim", county="Hills\tborough"),
        "deal_drop",
        number=NUMBER
    )
    assert "123 Main St" in body
    assert "Alex Kim" in body
    assert "Hills borough" in body


def test_braces_in_data_render_literally_without_exception():
    body = render_body(
        _item(property_address="{0} {x} Street, Tampa FL", caller_name="Alex {caller}", county="Hill {county}"),
        "deal_drop",
        number=NUMBER
    )
    assert "{0} {x}" in body
    assert "Alex {caller}" in body
    assert "Hill {county}" in body


def test_deal_drop_text_renders_exactly():
    assert render_body(_item(queue="transaction_ready"), "deal_drop", number=NUMBER) == (
        "Hi Sam, Alex with Josh Kantor, Loan Officer here. Just tried you about 123 Main St. "
        "We help investors in Hillsborough fund their next deal. Text back if you'd like to chat. "
        f"Book a time: {BOOKING_LINK} Reply STOP to opt out.")


def test_general_text_renders_exactly():
    assert render_body(_item(queue="nurture"), "general", number=NUMBER) == (
        "Hi Sam, this is Alex with Josh Kantor, Loan Officer. Sorry we missed each other. "
        "Reply here or call (813) 555-0100 whenever works. "
        f"Book a time: {BOOKING_LINK} Reply STOP to opt out.")


@pytest.mark.parametrize("key", ["maturity", "deal_drop", "general"])
def test_every_text_carries_the_booking_link_before_the_stop_language(key):
    body = render_body(_item(), key, number=NUMBER)
    assert f"Book a time: {BOOKING_LINK} Reply STOP to opt out." in body


@pytest.mark.parametrize("key", ["maturity", "deal_drop", "general"])
def test_fields_at_their_caps_still_fit_with_the_booking_link(key):
    item = _item(property_address="x" * CAPS["property"], county="x" * CAPS["county"],
                 caller_name="x" * CAPS["caller"], borrower_name="x" * CAPS["first_name"] + " Jones")
    body = render_body(item, key, number=NUMBER)
    assert len(body) <= MAX_TEXT_CHARS and BOOKING_LINK in body and body.endswith("Reply STOP to opt out.")
