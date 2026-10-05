"""WP-GL-10 reply agent safety net: route rate / terms handoffs to Slack and flag quoted numbers.

GoHighLevel's Conversation AI answers text replies and offers booking slots; this module is the part we
control. A GHL workflow posts each inbound reply (and each AI reply) to ``POST /webhooks/lending/ghl-reply``
(src/lending/reply_webhook.py), which calls ``handle_reply_event``:

  * inbound message that asks about a rate / terms (or that GHL marks as handed off) -> a Slack post to the
    replies channel so Josh answers within his one-business-hour window;
  * outbound AI message that contains a money amount / percentage / points -> a Slack alert (the agent must
    never quote numbers).

Everything else is a no-op. Each GHL message id is handled once: a redelivered webhook never posts twice,
and a post that failed is retried on redelivery. Logs carry ids and phone hashes only, never message text.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_reply_agent import (
    KIND_AI_HANDOFF,
    KIND_AI_QUOTED_NUMBERS,
    KIND_RATE_TERMS,
    KIND_RESCHEDULE,
    QUOTED_NUMBER_PATTERNS,
    RATE_TERMS_PHRASES,
    RATE_TERMS_WORDS,
    REPLY_HOURS_END,
    REPLY_HOURS_START,
    REPLY_WEEKDAYS,
    RESCHEDULE_PHRASES,
    SNIPPET_CHARS,
)
from config.settings import get_settings
from src.lending.compliance import phone_hash
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

TIMEZONE_NAME = "America/New_York"

SlackPoster = Callable[[str], None]

_QUOTED = re.compile("|".join(QUOTED_NUMBER_PATTERNS), re.IGNORECASE)
_WORDS = re.compile(r"[a-z]+")
# A run of nine or more digits (an SSN, an account or card number), allowing spaces or dashes between digits.
_LONG_DIGITS = re.compile(r"\d(?:[ -]?\d){8,}")


def _normalized(message: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9 ]+", " ", message.lower()).split())


def asks_rate_or_terms(message: str) -> bool:
    """True when an inbound message asks about price, rate, points, fees or loan terms."""
    if "%" in message or "$" in message:
        return True
    norm = _normalized(message)
    if any(phrase in norm for phrase in RATE_TERMS_PHRASES):
        return True
    return any(word in RATE_TERMS_WORDS for word in _WORDS.findall(message.lower()))


def asks_to_reschedule(message: str) -> bool:
    """True when an inbound message asks to move or cancel the booked call."""
    norm = _normalized(message)
    return any(phrase in norm for phrase in RESCHEDULE_PHRASES)


def quotes_numbers(reply: str) -> bool:
    """True when an AI reply states a money amount, percentage or points."""
    return bool(_QUOTED.search(reply))


@dataclass(frozen=True)
class ReplyEvent:
    message_id: str
    direction: str            # "inbound" | "outbound"
    body: str
    contact_id: Optional[str]
    first_name: Optional[str]
    phone: Optional[str]
    handoff: bool             # GHL says the AI handed this conversation to a human
    from_user: bool = False   # an outbound message typed by a person in GHL (Josh), not the AI or a workflow


def _pick(body: Mapping[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        node: Any = body
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        if node not in (None, ""):
            return node
    return None


def parse_event(body: Mapping[str, Any]) -> Optional[ReplyEvent]:
    """Pull the fields out of a GHL workflow webhook; None when there is no message id or text.
    UNVERIFIED field names: they follow GHL's public reference, not a captured payload."""
    message_id = _pick(body, ("messageId",), ("message", "id"), ("id",))
    text_body = _pick(body, ("body",), ("message", "body"), ("text",), ("message",))
    if not message_id or not isinstance(text_body, str) or not text_body.strip():
        return None
    direction = str(_pick(body, ("direction",), ("message", "direction")) or "inbound").lower()
    phone = _pick(body, ("phone",), ("contact", "phone"))
    return ReplyEvent(
        message_id=str(message_id),
        direction="outbound" if direction.startswith("out") else "inbound",
        body=text_body.strip(),
        contact_id=str(_pick(body, ("contactId",), ("contact", "id")) or "") or None,
        first_name=str(_pick(body, ("firstName",), ("contact", "firstName")) or "") or None,
        phone=normalize(str(phone)) if phone else None,
        handoff=bool(_pick(body, ("handoff",), ("aiHandoff",), ("humanHandoff",))),
        from_user=bool(_pick(body, ("userId",), ("user", "id"), ("message", "userId"))),
    )


def classify(event: ReplyEvent) -> Optional[str]:
    """The kind of Slack post this event needs, or None."""
    if event.direction == "outbound":
        if event.from_user:
            return None  # Josh may quote numbers; only the AI is held to the no-numbers rule
        return KIND_AI_QUOTED_NUMBERS if quotes_numbers(event.body) else None
    if asks_rate_or_terms(event.body):
        return KIND_RATE_TERMS
    if asks_to_reschedule(event.body):
        return KIND_RESCHEDULE
    return KIND_AI_HANDOFF if event.handoff else None


def in_reply_hours(moment: datetime) -> bool:
    """True inside Josh's answering hours (Mon-Fri 9:00 AM - 7:15 PM ET)."""
    local = moment.astimezone(ZoneInfo(TIMEZONE_NAME))
    return local.weekday() in REPLY_WEEKDAYS and REPLY_HOURS_START <= (local.hour, local.minute) < REPLY_HOURS_END


def redact_financial_digits(value: str) -> str:
    """Mask runs of nine or more digits so a borrower who texts an SSN or account number never has it posted to Slack."""
    return _LONG_DIGITS.sub("[redacted]", value)


def _slack_safe(value: str) -> str:
    """Slack treats <!channel>, <!here> and <url|label> as live markup unless & < > are escaped."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_slack(kind: str, event: ReplyEvent, *, now: Optional[datetime] = None) -> str:
    who = _slack_safe(event.first_name or "Unknown contact")
    tail = f" (…{event.phone[-4:]})" if event.phone else ""
    snippet = _slack_safe(redact_financial_digits(event.body)[:SNIPPET_CHARS]) + ("…" if len(event.body) > SNIPPET_CHARS else "")
    ref = f"\nGHL contact: {_slack_safe(event.contact_id)}" if event.contact_id else ""
    if kind == KIND_RATE_TERMS:
        head = "*Rate / terms question: needs Josh* (reply within one business hour; nothing was quoted)"
    elif kind == KIND_RESCHEDULE:
        head = "*Reschedule request: Josh answers* (reply within one business hour; the old reminders stay until the appointment changes in GHL)"
    elif kind == KIND_AI_HANDOFF:
        head = "*The reply agent handed this conversation to Josh*"
    else:
        head = "*ALERT: the reply agent's message contains a number it must not quote*"
    after_hours = ""
    if kind in (KIND_RATE_TERMS, KIND_RESCHEDULE) and not in_reply_hours(now or datetime.now(timezone.utc)):
        after_hours = "\n_Received outside 9:00 AM - 7:15 PM ET, Mon-Fri: answer first thing next business morning._"
    return f"{head}\nFrom: {who}{tail}\n> {snippet}{ref}{after_hours}"


_CLAIM = text("""
    INSERT INTO lending.reply_handoffs (message_id, kind, contact_id, phone_hash)
    VALUES (:message_id, :kind, :contact_id, :phone_hash)
    ON CONFLICT (message_id) DO NOTHING
""")
_RESPONDED = text("""
    UPDATE lending.reply_handoffs SET responded_at = now()
     WHERE contact_id = :contact_id AND posted_at IS NOT NULL AND responded_at IS NULL
""")
_STATE = text("SELECT posted_at IS NOT NULL FROM lending.reply_handoffs WHERE message_id = :message_id")
_POSTED = text("UPDATE lending.reply_handoffs SET posted_at = now() WHERE message_id = :message_id")


def handle_reply_event(db, event: ReplyEvent, *, poster: Optional[SlackPoster]) -> str:
    """Returns "ignored", "duplicate", "posted" or "not_configured". Commits the claim before posting (so
    a failed post is retried on redelivery) and the post result after. Raises if the post itself fails."""
    if event.direction == "outbound" and event.from_user and event.contact_id:
        answered = db.execute(_RESPONDED, {"contact_id": event.contact_id}).rowcount
        db.commit()
        if answered:
            logger.info("[reply-guard] %d handoff(s) answered by a person contact=%s", answered, event.contact_id)
    kind = classify(event)
    if kind is None:
        return "ignored"
    db.execute(_CLAIM, {"message_id": event.message_id, "kind": kind, "contact_id": event.contact_id,
                        "phone_hash": phone_hash(event.phone)[:12] if event.phone else None})
    db.commit()
    if db.execute(_STATE, {"message_id": event.message_id}).scalar():
        return "duplicate"
    if poster is None:
        logger.error("[reply-guard] %s for message %s not posted: Slack channel or token is not configured",
                     kind, event.message_id)
        return "not_configured"
    poster(format_slack(kind, event))
    db.execute(_POSTED, {"message_id": event.message_id})
    db.commit()
    logger.info("[reply-guard] posted %s message=%s phone_hash=%s", kind, event.message_id,
                phone_hash(event.phone)[:12] if event.phone else "-")
    return "posted"


def slack_poster() -> Optional[SlackPoster]:
    """Posts to LENDING_REPLIES_CHANNEL with the Cora Lending bot token; None until both are set."""
    settings = get_settings()
    channel = settings.lending_replies_channel
    if not (channel and settings.lending_slack_bot_token):
        return None

    def post(message: str) -> None:
        from slack_sdk import WebClient

        WebClient(token=settings.lending_slack_bot_token.get_secret_value()).chat_postMessage(channel=channel, text=message)

    return post
