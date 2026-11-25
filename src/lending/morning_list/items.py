"""The morning list's items: ranking, padding, owner split and the Slack lines (pure: no database, no clock).

``build`` takes every candidate the readers found (all tiers, padding included) and returns the ranked list: tier
order from ``config.lending_morning_list.TIER``, the reader's ``sort_key`` inside a tier, one line per phone at its
highest tier, warm leads and padding only with a Mobile number, cut at ``size``. Padding (``COLD_TOP``) therefore
only fills what real work leaves. ``render_sections`` turns the list into the Josh and David blocks.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Iterable, Optional

from config.lending_morning_list import (
    BRIEF_MAX_CHARS, CALLBACK, JOSH, JOSH_CALLER_PREFIXES, MOBILE_LINE_TYPE, MOBILE_REQUIRED, OWNER, REASON_LABELS,
    TIER,
)
from packages.agent_core.briefing import BriefSection
from packages.agent_core.redaction import slack_safe


@dataclass(frozen=True)
class ListItem:
    ref: str                                  # source and id, e.g. "lf:12", "web:3", "loan:5", "dlr:99"
    reason: str                               # a config.lending_morning_list reason code
    name: Optional[str]
    phone: Optional[str]                      # phone_utils.normalize form
    line_type: Optional[str]                  # lending.contacts.line_type
    sort_key: tuple = ()                      # order inside the tier, ascending
    brief: str = ""
    email: Optional[str] = None
    email_source: Optional[str] = None        # "clay" when Clay found it
    entity_name: Optional[str] = None
    property_address: Optional[str] = None    # Clay LLC lookup input
    caller_name: Optional[str] = None         # callbacks: who logged the call
    received_at: Optional[datetime] = None    # warm leads: when the lead arrived
    owner: str = field(default="")

    @property
    def tier(self) -> int:
        return TIER[self.reason]

    @property
    def has_mobile(self) -> bool:
        return (self.line_type or "").strip().lower() == MOBILE_LINE_TYPE

    @property
    def first_name(self) -> Optional[str]:
        return (self.name or "").strip().split(" ")[0] or None

    @property
    def last4(self) -> str:
        digits = "".join(ch for ch in (self.phone or "") if ch.isdigit())
        return digits[-4:]


def _owner(item: ListItem) -> str:
    if item.reason == CALLBACK and (item.caller_name or "").strip().lower().startswith(JOSH_CALLER_PREFIXES):
        return JOSH
    return OWNER[item.reason]


def build(candidates: Iterable[ListItem], *, size: int) -> list[ListItem]:
    """The ranked list: tier, then sort key; one entry per phone (its highest tier); at most ``size``."""
    eligible = [c for c in candidates if c.has_mobile or c.reason not in MOBILE_REQUIRED]
    ranked = sorted(eligible, key=lambda c: (c.tier, c.sort_key))
    seen: set[str] = set()
    picked: list[ListItem] = []
    for candidate in ranked:
        person = candidate.phone or candidate.ref
        if person in seen:
            continue
        seen.add(person)
        picked.append(replace(candidate, owner=_owner(candidate)))
        if len(picked) >= size:
            break
    return picked


def _line(item: ListItem) -> str:
    flags = [f"…{item.last4}"] if item.last4 else []
    if not item.has_mobile:
        flags.append("no mobile")
    who = slack_safe(item.first_name or item.entity_name or "Lead") + (f" ({', '.join(flags)})" if flags else "")
    extras = [slack_safe(item.brief[:BRIEF_MAX_CHARS])] if item.brief else []
    if item.email_source == "clay":
        extras.append("email found")
    return f"• *{REASON_LABELS[item.reason]}*: {who}." + (f" {' '.join(extras)}" if extras else "")


SECTION_BUDGET = 2700  # under agent_core.briefing's 2,900-character cut, with room for the title


def _chunks(lines: list[str]) -> list[list[str]]:
    """Lines grouped so each Slack section stays under SECTION_BUDGET characters (none is ever cut)."""
    chunks: list[list[str]] = [[]]
    size = 0
    for line in lines:
        if chunks[-1] and size + len(line) + 1 > SECTION_BUDGET:
            chunks.append([])
            size = 0
        chunks[-1].append(line)
        size += len(line) + 1
    return chunks


def _sections(title: str, lines: list[str], empty: str) -> list[BriefSection]:
    parts = _chunks(lines)
    if len(parts) == 1:
        return [BriefSection(title, parts[0], empty)]
    return [BriefSection(f"{title} ({n} of {len(parts)})", part, empty) for n, part in enumerate(parts, 1)]


def render_sections(items: list[ListItem], *, david_user_id: str) -> list[BriefSection]:
    """Josh's block (stuck deals, deals, booked calls) then David's block (callbacks, new leads, top of queue); a
    block longer than one Slack section continues in the next one."""
    josh = [_line(i) for i in items if i.owner == JOSH]
    david_lines = [_line(i) for i in items if i.owner != JOSH]
    david = ([f"<@{david_user_id}>"] if david_user_id and david_lines else []) + david_lines
    return (_sections(f"Today's list for Josh: {len(josh)}", josh, "Nothing for Josh today.")
            + _sections(f"Today's list for David: {len(david_lines)}", david, "Nothing for David today."))
