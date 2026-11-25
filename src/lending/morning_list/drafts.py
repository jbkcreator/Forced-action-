"""R1 1.3 morning drafts (D8, D16, D18): one SMS (or email without text consent) per eligible list item, in Josh's
voice, held for approval in #approvals. Nothing here sends: ``submit_all`` hands each draft to ``SendGate.submit`` and
the relay runs every send-time check (consent, 8 AM-8 PM window, suppression, halt, recipient re-check).

Eligible: a new warm lead or a callback, more than 24 hours old for a warm lead (speed-to-lead owns the first day),
no outbound message from us in the last 3 days and no open draft for that phone. The writer (an LLM) sees the first
name and the one-line brief only, never a phone or email. A draft that fails, comes back empty, or quotes a number
(``reply_guard.quotes_numbers``: rates, points, fees, amounts) is replaced by the fixed template.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config.lending_morning_list import (
    CALLBACK, DRAFT_MIN_LEAD_AGE, DRAFT_REQUESTED_BY, DRAFT_TTL, DRAFTABLE, FALLBACK_EMAIL_BODY,
    FALLBACK_EMAIL_SUBJECT, FALLBACK_SMS, NEW_WARM_LEAD, NO_TOUCH_WINDOW, TEMPLATE_VERSION,
)
from packages.agent_core.send_gate import SendGate
from packages.agent_core.tools import EgressDraft, ToolContext
from src.lending.morning_list.items import ListItem
from src.lending.reply_guard import quotes_numbers

logger = logging.getLogger(__name__)

SMS_MAX_CHARS = 320
WRITER_VERSION = "morning_list_llm_v1"
_CONTACT_REF = {"lf": "lendingflow_lead", "web": "web_lead"}

# Phones we messaged (sent, or a draft still open) in the no-touch window, from Cora's drafts table.
_TOUCHED = text("""
    SELECT recipient_phone, status FROM lending.pending_actions
     WHERE recipient_phone IN :phones
       AND (status IN ('pending', 'approved', 'sending', 'held') OR (executed_at IS NOT NULL AND executed_at >= :since))
""").bindparams(bindparam("phones", expanding=True))
_OPEN = ("pending", "approved", "sending", "held")


@dataclass(frozen=True)
class DraftRequest:
    first_name: str
    brief: str
    channel: str  # "sms" or "email"
    reason: str


DraftWriter = Callable[[DraftRequest], Optional[str]]


@dataclass(frozen=True)
class DraftContext:
    now: datetime
    text_consent: Callable[[str], bool]
    recently_contacted: set[str]
    open_drafts: set[str]


@dataclass(frozen=True)
class MorningDraft:
    item: ListItem
    draft: EgressDraft
    key: str


def _first_name(item: ListItem) -> str:
    return (item.name or "").split(" ")[0].strip() or "there"


def _eligible(item: ListItem, ctx: DraftContext) -> bool:
    if item.reason not in DRAFTABLE or not item.phone:
        return False
    if item.phone in ctx.recently_contacted or item.phone in ctx.open_drafts:
        return False
    if item.reason == NEW_WARM_LEAD:
        return item.received_at is not None and ctx.now - item.received_at >= DRAFT_MIN_LEAD_AGE
    return item.reason == CALLBACK


def _written(writer: DraftWriter, request: DraftRequest) -> Optional[str]:
    try:
        body = (writer(request) or "").strip()
    except Exception as exc:  # any writer failure: the template goes instead
        logger.warning("[morning-list] draft writer failed (%s)", type(exc).__name__)
        return None
    if not body or quotes_numbers(body) or (request.channel == "sms" and len(body) > SMS_MAX_CHARS):
        return None
    return body


def _draft(item: ListItem, writer: DraftWriter, ctx: DraftContext) -> Optional[EgressDraft]:
    text_ok = ctx.text_consent(item.phone or "")
    if not text_ok and not item.email:
        return None
    channel = "sms" if text_ok else "email"
    first = _first_name(item)
    body = _written(writer, DraftRequest(first_name=first, brief=item.brief, channel=channel, reason=item.reason))
    version = WRITER_VERSION if body else TEMPLATE_VERSION
    contact_ref = None
    prefix, _, number = item.ref.partition(":")
    if prefix in _CONTACT_REF:
        contact_ref = f"{_CONTACT_REF[prefix]}:{number}"
    if text_ok:
        payload = {"to_phone": item.phone, "body": body or FALLBACK_SMS.format(first_name=first)}
        executor = "ghl_sms"
    else:
        payload = {"to_email": item.email, "to_phone": item.phone, "subject": FALLBACK_EMAIL_SUBJECT,
                   "body": body or FALLBACK_EMAIL_BODY.format(first_name=first)}
        executor = "ghl_email"
    payload["template_version"] = version
    summary = f"🌅 Morning follow-up · {version}\n{item.name or 'Lead'} (…{item.last4}): {item.brief}"[:300]
    return EgressDraft(channel=executor, payload=payload, summary=summary, recipient_phone=item.phone,
                       recipient_email=item.email, contact_ref=contact_ref, deal_ref=None)


def morning_drafts(items: Iterable[ListItem], writer: DraftWriter, ctx: DraftContext) -> list[MorningDraft]:
    """The drafts to hold for approval, in list order."""
    day = ctx.now.date().isoformat()
    out = []
    for item in items:
        if not _eligible(item, ctx):
            continue
        draft = _draft(item, writer, ctx)
        if draft is not None:
            out.append(MorningDraft(item=item, draft=draft, key=f"morning:{day}:{item.ref}"))
    return out


def touched_phones(db: Session, phones: list[str], now: datetime) -> tuple[set[str], set[str]]:
    """(phones messaged by us inside the no-touch window, phones with an open draft)."""
    if not phones:
        return set(), set()
    rows = db.execute(_TOUCHED, {"phones": phones, "since": now - NO_TOUCH_WINDOW}).mappings().all()
    return ({r["recipient_phone"] for r in rows},
            {r["recipient_phone"] for r in rows if r["status"] in _OPEN})


def submit_all(gate: SendGate, drafts: Iterable[MorningDraft], *, approvals_channel: str) -> int:
    """Hold each draft for approval; its key makes a repeat return the same card, never a second one."""
    count = 0
    for d in drafts:
        context = ToolContext(user_id=DRAFT_REQUESTED_BY, is_approver=False, channel=approvals_channel,
                              thread_ts="", tool_use_id=d.key)
        try:
            gate.submit(d.draft, tool_name=DRAFT_REQUESTED_BY, context=context, ttl=DRAFT_TTL)
            count += 1
        except Exception as exc:  # one refused draft never stops the rest; the line is still in the digest
            logger.error("[morning-list] draft %s not queued (%s)", d.item.ref, type(exc).__name__)
    return count


JOSH_VOICE = (
    "You write one short follow-up message from Josh at Next Deal Lending (private and hard-money loans for "
    "real estate investors), to a borrower who asked about a loan and has not talked to him yet. Plain, friendly, "
    "direct, first person, no emojis, no exclamation marks. Ask for a quick call today. Never state a rate, points, "
    "fee, amount, percentage or approval. Do not add an opt-out line. Return only the message text."
)


def anthropic_writer(messages_client, model: str) -> DraftWriter:
    """An LLM writer on Cora's Anthropic client. Input: first name, brief, channel; output: the message text."""
    def write(request: DraftRequest) -> Optional[str]:
        limit = "under 300 characters, SMS" if request.channel == "sms" else "3 to 5 sentences, email body"
        prompt = (f"Borrower first name: {request.first_name}\nWhat we know: {request.brief or 'a loan request'}\n"
                  f"Why now: {'they asked for a callback' if request.reason == CALLBACK else 'new lead, not reached'}\n"
                  f"Length: {limit}.")
        response = messages_client.create(model=model, max_tokens=300, system=JOSH_VOICE,
                                          messages=[{"role": "user", "content": prompt}])
        return "".join(getattr(block, "text", "") for block in response.content)
    return write
