"""Masking and escaping for text that leaves the process (Slack) or lands in an audit column."""
from __future__ import annotations

import re

# Same rule as the lending Slack alerts: runs of nine or more digits (phones, account numbers,
# SSNs) are never shown or stored in full.
_LONG_DIGITS = re.compile(r"\d(?:[ -]?\d){8,}")
_SEPARATORS = re.compile(r"[ -]")
# Email addresses keep their first character and domain, enough to tell two contacts apart.
_EMAIL = re.compile(r"\b([A-Za-z0-9])[A-Za-z0-9._%+-]*@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,})\b")


# A link whose last path segment is a long random token is a credential (an upload or partner link): anyone holding
# it can use it. 32+ URL-safe characters; ordinary ids and slugs are shorter or contain other characters.
_URL_TOKEN = re.compile(r"(https?://[^\s<>|]*/)[A-Za-z0-9_-]{32,}(?=$|[\s<>|)\]}.,;:!?'\"])")


def mask_url_tokens(value: str) -> str:
    """``"https://x.test/api/lending/docs/<43-char token>"`` -> ``"https://x.test/api/lending/docs/[…]"``."""
    return _URL_TOKEN.sub(r"\1[…]", value)


def mask_long_digit_runs(value: str) -> str:
    """``"call 727-436-9951"`` -> ``"call […9951]"``; shorter numbers are left alone."""
    return _LONG_DIGITS.sub(lambda match: f"[…{_SEPARATORS.sub('', match.group())[-4:]}]", value)


def mask_emails(value: str) -> str:
    """``"sam.lee@example.com"`` -> ``"s***@example.com"``."""
    return _EMAIL.sub(r"\1***@\2", value)


def mask_personal_data(value: str) -> str:
    """Long digit runs and email addresses, the two contact details never shown in full."""
    return mask_emails(mask_long_digit_runs(value))


def slack_escape(value: str) -> str:
    """Slack reads <!channel>, <!here> and <url|label> as live markup unless & < > are escaped."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def slack_safe(value: str) -> str:
    return slack_escape(mask_personal_data(value))
