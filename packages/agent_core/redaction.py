"""Masking and escaping for text that leaves the process (Slack) or lands in an audit column."""
from __future__ import annotations

import re

# Same rule as the lending Slack alerts: runs of nine or more digits (phones, account numbers,
# SSNs) are never shown or stored in full.
_LONG_DIGITS = re.compile(r"\d(?:[ -]?\d){8,}")
_SEPARATORS = re.compile(r"[ -]")


def mask_long_digit_runs(value: str) -> str:
    """``"call 727-436-9951"`` -> ``"call […9951]"``; shorter numbers are left alone."""
    return _LONG_DIGITS.sub(lambda match: f"[…{_SEPARATORS.sub('', match.group())[-4:]}]", value)


def slack_escape(value: str) -> str:
    """Slack reads <!channel>, <!here> and <url|label> as live markup unless & < > are escaped."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def slack_safe(value: str) -> str:
    return slack_escape(mask_long_digit_runs(value))
