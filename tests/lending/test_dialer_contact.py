"""Pool record -> Aircall contact display mapping."""
from __future__ import annotations

from decimal import Decimal

from src.lending.dialer_contact import (
    INFORMATION_MAX_CHARS,
    NOT_AVAILABLE,
    dialer_fields,
    display_from_record,
)

RECORD = {
    "borrower_name": "  John   Smith ",
    "entity_name": "SMITH HOLDINGS LLC",
    "property_address": "123 Main St, Tampa, FL 33602",
    "estimated_loan_value": "$350,000",
    "recent_permit_details": "New single-family residence, issued 2026-08-14",
}


def test_full_record_maps_to_every_display_field():
    fields = dialer_fields(display_from_record(RECORD, "DESK_CONSTRUCTION"), email="John@Smith.com")
    assert fields.first_name == "John"
    assert fields.last_name == "Smith"
    assert fields.company_name == "SMITH HOLDINGS LLC"
    assert fields.email == "john@smith.com"
    assert fields.information.splitlines() == [
        "Property: 123 Main St, Tampa, FL 33602",
        "Est. loan value: $350,000",
        "Recent permit: New single-family residence, issued 2026-08-14",
        "Campaign: DESK_CONSTRUCTION",
    ]


def test_missing_values_are_shown_as_not_available():
    fields = dialer_fields(display_from_record({}, None))
    assert fields.first_name is None and fields.last_name is None and fields.company_name is None
    assert all(line.endswith(NOT_AVAILABLE) for line in fields.information.splitlines())


def test_single_word_name_is_kept_as_first_name():
    fields = dialer_fields(display_from_record({"borrower_name": "Madonna"}, None))
    assert (fields.first_name, fields.last_name) == ("Madonna", None)


def test_multi_word_first_name_keeps_last_word_as_last_name():
    fields = dialer_fields(display_from_record({"borrower_name": "Mary Ann van Dyke"}, None))
    assert (fields.first_name, fields.last_name) == ("Mary Ann van", "Dyke")


def test_loan_value_parsing():
    assert display_from_record({"estimated_loan_value": 250000}, None).estimated_loan_value == Decimal("250000")
    assert display_from_record({"estimated_loan_value": "1,250,000.50"}, None).estimated_loan_value == Decimal("1250000.50")
    assert display_from_record({"estimated_loan_value": "unknown"}, None).estimated_loan_value is None
    assert display_from_record({"estimated_loan_value": "-5"}, None).estimated_loan_value is None


def test_invalid_email_is_dropped():
    assert dialer_fields(display_from_record(RECORD, None), email="not-an-email").email is None


def test_information_is_bounded():
    long_record = {**RECORD, "recent_permit_details": "x" * 5000}
    information = dialer_fields(display_from_record(long_record, None)).information
    assert len(information) == INFORMATION_MAX_CHARS
