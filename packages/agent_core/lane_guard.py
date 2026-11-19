"""Lane deny list: no outbound draft may name Backflip, its phone numbers or its domain.

Cora sends for Next Deal Lending only. The rules are fixed in code, not settings, so no prompt,
standing rule or environment value can switch them off. The send gate, the reviser and the relay
each run :func:`lane_violation` on the payload, so a draft is stopped whether it is new, revised
or already approved.
"""
from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import Any

from .tools import ToolInputError

LANE_BLOCK_PREFIX = "BACKFLIP_LANE"

# Zero or more separators people put inside a phone number: space ( ) . + -
_SEP = r"[\s().+\-]*"


def _phone_pattern(digits: str) -> str:
    """'6562096700' -> regex matching (656) 209-6700, 656.209.6700, +1 656 209 6700, 6562096700."""
    return r"(?<!\d)(?:1" + _SEP + r")?" + _SEP.join(digits) + r"(?!\d)"


# Checked in order, so a domain is reported as a domain even though it also contains the name.
DENY_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    # search() finds it inside www.backflip.com or josh@backflip.com, so no subdomain prefix is needed.
    ("blocked_domain", re.compile(r"backflip\.com\b", re.IGNORECASE)),
    ("blocked_number", re.compile(_phone_pattern("6562096700") + "|" + _phone_pattern("6562076500"))),
    ("blocked_name", re.compile(r"back[\s\-_]*flip", re.IGNORECASE)),
)


class LaneViolation(ToolInputError):
    """A draft broke the lane deny list. The message goes back to the model, so it carries no PII."""


def _strings(value: Any) -> Iterator[str]:
    """Every string nested anywhere in a payload (mappings, lists, tuples)."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def lane_violation(payload: Mapping[str, Any]) -> str | None:
    """Name of the first deny rule the payload breaks, or None when it is clean.

    Scans each string value on its own, so digits from two separate fields cannot join into a
    blocked number.
    """
    for value in _strings(payload):
        for rule_name, pattern in DENY_RULES:
            if pattern.search(value):
                return rule_name
    return None


def lane_block_reason(rule_name: str) -> str:
    """The reason stored on a blocked row, e.g. ``BACKFLIP_LANE: blocked_number``."""
    return f"{LANE_BLOCK_PREFIX}: {rule_name}"
