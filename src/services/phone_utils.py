"""Canonical US phone normalization — strict E.164 (+1XXXXXXXXXX)."""

from __future__ import annotations

import phonenumbers
from phonenumbers import NumberParseException


def _qa_allowlist() -> set[str]:
    """Return the QA test-phone allowlist (E.164), empty unless configured."""
    try:
        from config.settings import get_settings
        raw = get_settings().qa_test_phone_allowlist
    except Exception:
        return set()
    if not raw:
        return set()
    return {n.strip() for n in raw.split(",") if n.strip()}


def normalize(raw: str | None) -> str | None:
    """
    Parse and format a US phone number to strict E.164 (+1XXXXXXXXXX).
    Returns None for unparseable, invalid, or non-US numbers.
    Idempotent: normalize(normalize(x)) == normalize(x).
    """
    if not raw:
        return None

    # QA test exception: explicitly allowlisted numbers (incl. non-US) pass
    # through as valid E.164 for staging voice-drop tests. Controlled by
    # settings.qa_test_phone_allowlist (env QA_TEST_PHONE_ALLOWLIST), unset in
    # production so the strict US-only guard below is unchanged.
    allow = _qa_allowlist()
    if allow:
        try:
            intl = phonenumbers.parse(raw, None)  # raw must carry its +country code
            if phonenumbers.is_valid_number(intl):
                e164 = phonenumbers.format_number(intl, phonenumbers.PhoneNumberFormat.E164)
                if e164 in allow:
                    return e164
        except NumberParseException:
            pass

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
