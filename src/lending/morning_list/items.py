"""The morning list's items: ranking, padding, owner split and the Slack lines (pure: no database, no clock).

``build`` takes every candidate the readers found (all tiers, padding included) and returns the ranked list: tier
order from ``config.lending_morning_list.TIER``, the reader's ``sort_key`` inside a tier, one line per phone at its
highest tier, warm leads and padding only with a Mobile number, cut at ``size``. Padding (``COLD_TOP``) therefore
only fills what real work leaves.

Slack layout: ``render_main`` = Josh's items in full plus a one-line David summary (the main digest post);
``render_thread`` = David's items grouped by reason (callbacks, new leads, then the calling queue by list), posted as
replies under it. Each line links to the contact in GHL and BatchDialer when we hold the id.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import quote

from config.lending_morning_list import (
    ACTIVE_DEAL, BOOKED_TODAY, BRIEF_MAX_CHARS, CALLBACK, CLOSING_BLOCKER, COLD_TOP, JOSH, JOSH_CALLER_PREFIXES,
    KNOWN_NON_MOBILE_DROPPED, MOBILE_LINE_TYPE, MOBILE_REQUIRED, NEW_WARM_LEAD, OWNER, REASON_LABELS, TIER,
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
    group: Optional[str] = None               # calling queue: the list it is on (thread sub-heading)
    ghl_contact_id: Optional[str] = None
    dialer_contact_id: Optional[str] = None
    ghl_url: Optional[str] = None             # contact page in GHL
    dialer_url: Optional[str] = None          # contact page in BatchDialer
    owner: str = field(default="")

    @property
    def tier(self) -> int:
        return TIER[self.reason]

    @property
    def has_mobile(self) -> bool:
        return (self.line_type or "").strip().lower() == MOBILE_LINE_TYPE

    @property
    def line_type_known(self) -> bool:
        return bool((self.line_type or "").strip())

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
    eligible = [c for c in candidates if c.has_mobile or not (
        c.reason in MOBILE_REQUIRED or (c.reason in KNOWN_NON_MOBILE_DROPPED and c.line_type_known))]
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


ICONS = {CLOSING_BLOCKER: ":triangular_flag_on_post:", ACTIVE_DEAL: ":page_facing_up:", BOOKED_TODAY: ":calendar:",
         CALLBACK: ":telephone_receiver:", NEW_WARM_LEAD: ":star:", COLD_TOP: ":small_blue_diamond:"}
GROUP_TITLES = {CALLBACK: "Callbacks", NEW_WARM_LEAD: "New leads"}
SECTION_BUDGET = 2700  # under agent_core.briefing's 2,900-character cut, with room for the title
THREAD_MAX_BLOCKS = 45  # Slack allows 50 blocks a message


def _link(url: Optional[str], label: str) -> Optional[str]:
    return f"<{quote(url, safe=':/?=&#%')}|{label}>" if url else None


def _line(item: ListItem) -> str:
    """One item: icon, first name (or company) with last 4, the brief, then the GHL / BatchDialer links. Names and
    briefs are escaped; links are built from our own ids, never from borrower text."""
    flags = [f"…{item.last4}"] if item.last4 else []
    if not item.has_mobile:
        flags.append("no mobile" if item.line_type_known else "mobile not checked")
    who = f"*{slack_safe(item.first_name or item.entity_name or 'Lead')}*" + (f" ({', '.join(flags)})" if flags else "")
    parts = [slack_safe(item.brief[:BRIEF_MAX_CHARS])] if item.brief else []
    if item.email_source == "clay":
        parts.append("email found")
    parts += [p for p in (_link(item.ghl_url, "GHL ↗"), _link(item.dialer_url, "BatchDialer ↗")) if p]
    return f"{ICONS[item.reason]} {who}  " + " · ".join(parts)


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


def _groups(items: list[ListItem]) -> list[tuple[str, list[ListItem]]]:
    """David's items in thread order: callbacks, new leads, then the calling queue one list at a time."""
    groups: dict[str, list[ListItem]] = {}
    for item in items:
        title = GROUP_TITLES.get(item.reason) or (
            f"Top of queue · {item.group}" if item.group else REASON_LABELS[item.reason])
        groups.setdefault(title, []).append(item)
    return list(groups.items())


def render_main(items: list[ListItem], *, david_user_id: str) -> list[BriefSection]:
    """The main post's list part: Josh's items in full, then David's count with a pointer to the thread."""
    josh = [_line(i) for i in items if i.owner == JOSH]
    david = [i for i in items if i.owner != JOSH]
    counts = " · ".join(f"{title}: {len(group)}" for title, group in _merge_queue(_groups(david)))
    summary = ([f"<@{david_user_id}>"] if david_user_id and david else []) + (
        [counts, "Full list in the thread below :arrow_down:"] if david else [])
    return (_sections(f"Josh · {len(josh)}", josh, "Nothing for Josh today.")
            + [BriefSection(f"David · {len(david)}", summary, "Nothing for David today.")])


def _merge_queue(groups: list[tuple[str, list[ListItem]]]) -> list[tuple[str, list[ListItem]]]:
    """For the summary count: all calling-queue lists as one "top of queue" group."""
    merged: dict[str, list[ListItem]] = {}
    for title, group in groups:
        merged.setdefault("Top of queue" if title.startswith("Top of queue") else title, []).extend(group)
    return list(merged.items())


def render_thread(items: list[ListItem]) -> list[list[dict]]:
    """David's items as thread replies (Block Kit), grouped and split so no section or message is cut."""
    blocks: list[dict] = []
    for title, group in _groups([i for i in items if i.owner != JOSH]):
        for section in _sections(f"{title} · {len(group)}", [_line(i) for i in group], ""):
            body = "\n".join(section.lines)
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*{section.title}*\n{body}"}})
    return [blocks[n:n + THREAD_MAX_BLOCKS] for n in range(0, len(blocks), THREAD_MAX_BLOCKS)]
