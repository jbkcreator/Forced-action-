"""Canonical US phone normalization — strict E.164 (+1XXXXXXXXXX)."""

from __future__ import annotations

import phonenumbers
from phonenumbers import NumberParseException


def normalize(raw: str | None) -> str | None:
    """
    Parse and format a US phone number to strict E.164 (+1XXXXXXXXXX).
    Returns None for unparseable, invalid, or non-US numbers.
    Idempotent: normalize(normalize(x)) == normalize(x).
    """
    if not raw:
        return None
    try:
        parsed = phonenumbers.parse(raw, "US")
    except NumberParseException:
        return None
    if not phonenumbers.is_valid_number(parsed):
        return None
    # Reject non-US country codes
    if parsed.country_code != 1:
        return None
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
