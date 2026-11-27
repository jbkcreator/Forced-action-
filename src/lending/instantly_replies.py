"""P1.3: replies to NDL's Instantly cold emails, from webhook to a triage post and a drafted answer.

Two halves:

* **Webhook** (``POST /webhooks/lending/instantly``, ``instantly_webhook.py``): ``record_event`` stores each
  ``reply_received`` once (keyed by Instantly's email id). A reply asking to stop, or Instantly's own
  ``lead_unsubscribed`` event, is suppressed at once through ``compliance.propagate_opt_out`` (channel email), the
  same path the opt-out mailbox uses: FA's email/SMS stores, ``lending.suppression_list``, every phone that email is
  on file with, the dialer, and GHL do-not-disturb (the opt-out poller syncs GHL within 15 s). No answer is ever
  drafted for it.
* **Cora's minute sweep** (``ReplyProcessor``): each new reply is classified (deal / objection / dead) and, for a
  deal or an objection, an answer is drafted onto an Approve / Revise / Reject card in #approvals (channel
  ``instantly_email``: an approved answer goes back inside the Instantly thread). The triage goes to the replies
  channel like the GHL reply handoffs, and into Cora's inbox. A reply the model reads as asking to stop (any wording
  the phrase check missed) is suppressed by the sweep through the same path, with no answer drafted. A dead reply gets
  the triage only, never a suppression.

Logs and inbox payloads carry ids, masked addresses and the intent only, never the reply text.

UNVERIFIED: Instantly's webhook field names follow its public reference, not a captured delivery; ``parse_event``
accepts the documented names and a few alternatives.
"""
from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import text

from config.lending_compliance import OptOutChannel
from config.lending_standing_sends import INSTANTLY_REPLY_TEMPLATE
from packages.agent_core.lane_guard import LaneViolation
from packages.agent_core.redaction import mask_emails
from src.lending.compliance import propagate_opt_out
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.inbox.unified_triage import (  # noqa: F401  (re-exported: the classifier lives in unified_triage)
    DRAFT_INTENTS,
    INTENT_LABELS,
    MODEL_INTENTS,
    STOP_INTENT,
    Triage,
    TriageError,
    _parse_triage,
    classify_prompt,
    classify_reply,
    new_reply_text,
    triage_line,
    with_booking_link,
)
from src.lending.web_leads import DeliveryError

logger = logging.getLogger(__name__)

EVENT_REPLY = "reply_received"
EVENT_UNSUBSCRIBED = "lead_unsubscribed"
HANDLED_EVENTS = frozenset({EVENT_REPLY, EVENT_UNSUBSCRIBED})
CHANNEL = "instantly_email"
TOOL_NAME = "draft_instantly_reply"
CLASSIFY_PROMPT = classify_prompt(CHANNEL)
SOURCE_REF_PREFIX = "instantly:"
MAX_ATTEMPTS = 5
# A STOP the webhook is still recording is left alone; the sweep retries one only after this long (well inside 60 s).
BATCH_SIZE = 10
# Phrases that always mean "stop contacting me", anywhere in the new part of the reply. Wording these miss is caught
# by the model (intent "stop") in Cora's sweep.
_STOP_PHRASES = re.compile(
    r"\b(?:unsubscribe|opt[\s-]?out|remove (?:me|us)|take (?:me|us) off|off (?:your|the|this) (?:\w+ )?list|"
    r"(?:stop|cease) (?:all |any )?(?:further |more )?(?:emailing|e-?mails?|contacting|contact|sending|messaging|"
    r"messages)|"
    r"(?:do not|don'?t) (?:email|e-mail|contact|message)|"
    r"(?:do not|don'?t) send (?:me |us )?(?:any ?more|more|further|another)|"
    r"(?:do not|don'?t) want (?:any ?more|more|further|your) (?:e-?mails?|messages)|"
    r"(?:remove|delete|take) (?:my|our) (?:\w+ ){0,3}(?:from|off) (?:your|the|this|that|any|all) (?:\w+ )?"
    r"(?:list|lists|mailing|database|records|system)|"
    r"no (?:more|further) (?:e-?mails?|messages|contact))\b",
    re.IGNORECASE,
)
# Words that could mean "stop" in wording the phrases above miss ("please remove", "this is spam", "leave us alone").
# The webhook treats any of them as a STOP on the spot (the safe default: missing an opt-out is the bigger risk), so a
# STOP never waits for Cora's sweep or the AI model; the model still sorts every other reply.
_POSSIBLE_STOP = re.compile(
    r"\b(?:cease(?: and desist| (?:all |any |further |more )?(?:contact|communication\w*|emailing|e-?mails?|"
    r"sending|messaging|calling|texting))|unsubscrib\w*|harass\w*|leave (?:me|us) alone|(?:please |pls )?stop (?:it|this|that|now)|"
    r"(?:remove|delete|take) (?:me|us|my|our) (?:\w+ )?(?:name|email|e-?mail|address|number|info|details|from|off)|"
    r"(?:this (?:is|was)|it'?s|reported? (?:as|you as)|mark(?:ed|ing)? (?:as|you as)) spam|spammer|"
    r"no longer (?:want|wish) to (?:receive|hear|get)|"
    r"never (?:email|contact|call|text|message)|not (?:email|contact) (?:me|us))\b", re.IGNORECASE)
# "don't call me", "never text me", "stop emailing me": a negated contact verb. It switches the callback guard below off,
# so "delete my info, don't call me" is still a stop even though it says "call me".
_NEGATED_CONTACT = re.compile(
    r"\b(?:don'?t|do not|never|stop|quit|no more)\s+(?:\w+\s+){0,2}(?:call|text|phone|ring|reach|contact|e-?mail|message)\w*\b",
    re.IGNORECASE)
# A lead asking us to call, text or reach them ("call me at my new number", "give me a call"). Wording that only *could*
# mean stop is not suppressed on the spot when the same reply asks for contact: it goes to triage, where the model
# decides (and a real stop is still suppressed by the sweep). Clear stop requests are not affected.
_ASKS_FOR_CONTACT = re.compile(
    r"\b(?:(?<!not )(?<!n't )(?<!never )(?:call|phone|text|ring|reach) (?:me|us)|give (?:me|us) a (?:call|ring)|"
    r"call(?:ing)? (?:me )?back|callback|my (?:new|updated|correct|current|cell|mobile|direct) (?:number|phone|cell))\b",
    re.IGNORECASE)
# A line that is only a stop word, with at most a short lead-in or tail ("STOP", "Please stop", "No thanks, stop",
# "Remove"), so "can't stop thinking about it" is not one. Checked word by word, not with a regex: the reply is text
# anyone can send, and a nested-quantifier pattern here backtracks exponentially (PR #428 re-review).
_STOP_WORDS = frozenset({"stop", "unsubscribe", "remove", "cancel", "quit", "end"})
_STOP_LEAD_INS = frozenset({"no", "nope", "thanks", "thank", "you", "please", "pls", "ok", "okay"})
_STOP_TAILS = frozenset({"please", "now", "it", "this", "that"})
_STOP_LINE_MAX_CHARS = 80
_STOP_LINE_MAX_WORDS = 8
_STOP_LINE_CHARS = re.compile(r"[a-z\s,.!-]+")
_WORDS = re.compile(r"[a-z]+")


@dataclass(frozen=True)
class InstantlyEvent:
    event_type: str
    lead_email: str
    dedup_key: str
    first_name: Optional[str] = None
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None
    email_account: Optional[str] = None
    reply_to_uuid: Optional[str] = None
    reply_subject: Optional[str] = None
    reply_text: str = ""


@dataclass(frozen=True)
class RecordResult:
    status: str  # "recorded", "duplicate" or "ignored"
    reply_id: Optional[int] = None
    stop_request: bool = False


def _pick(body: Mapping[str, Any], *keys: str) -> Optional[str]:
    """The first non-empty value among ``keys``; a dotted key reads one level down (``lead.email``)."""
    for key in keys:
        value: Any = body
        for part in key.split("."):
            value = value.get(part) if isinstance(value, Mapping) else None
        if value not in (None, "") and not isinstance(value, (Mapping, list)):
            return str(value).strip()
    return None


def is_stop_line(line: str) -> bool:
    """True for a line that is only a stop word, optionally after a lead-in ("no thanks", "please") and before one
    tail word ("please", "now"). Linear in the line length, and lines over 80 characters are never one."""
    lowered = line.strip().lower()
    if not lowered or len(lowered) > _STOP_LINE_MAX_CHARS or not _STOP_LINE_CHARS.fullmatch(lowered):
        return False
    words = _WORDS.findall(lowered)
    if len(words) > _STOP_LINE_MAX_WORDS:
        return False
    i = 0
    while i < len(words) and words[i] in _STOP_LEAD_INS:
        if (words[i] == "you") != (i > 0 and words[i - 1] == "thank"):  # "you" only in "thank you"
            return False
        i += 1
    if i == len(words) or words[i] not in _STOP_WORDS or (i > 0 and words[i - 1] == "thank"):
        return False
    tail = words[i + 1:]
    return not tail or (len(tail) == 1 and tail[0] in _STOP_TAILS)


def is_stop_request(reply: str) -> bool:
    own = new_reply_text(reply)
    return bool(_STOP_PHRASES.search(own) or any(is_stop_line(line) for line in own.splitlines()))


def could_mean_stop(reply: str) -> bool:
    """A stop request, or any wording that could be one. Decided in the webhook, before anything else runs."""
    if is_stop_request(reply):
        return True
    own = new_reply_text(reply)
    if not _POSSIBLE_STOP.search(own):
        return False
    return bool(_NEGATED_CONTACT.search(own)) or not _ASKS_FOR_CONTACT.search(own)


def parse_event(body: Mapping[str, Any]) -> Optional[InstantlyEvent]:
    """The event, or None when it is not one this module handles or carries no lead address."""
    event_type = (_pick(body, "event_type", "event", "type") or "").lower()
    if event_type not in HANDLED_EVENTS:
        return None
    email = (_pick(body, "lead_email", "email", "lead.email", "to_email") or "").lower()
    if "@" not in email:
        return None
    reply_text = _pick(body, "reply_text", "reply_text_snippet", "text", "body") or ""
    reply_to_uuid = _pick(body, "email_id", "reply_to_uuid", "id", "message_id")
    if event_type == EVENT_UNSUBSCRIBED:
        dedup = f"unsub:{hashlib.sha256(email.encode()).hexdigest()[:40]}"
    elif reply_to_uuid:
        dedup = f"reply:{reply_to_uuid}"
    else:
        seed = "|".join((email, _pick(body, "timestamp") or "", reply_text[:500]))
        dedup = f"reply:{hashlib.sha256(seed.encode()).hexdigest()[:40]}"
    return InstantlyEvent(
        event_type=event_type, lead_email=email, dedup_key=dedup,
        first_name=_pick(body, "firstName", "first_name", "lead_first_name", "lead.first_name", "lead.firstName"),
        campaign_id=_pick(body, "campaign_id", "campaign"), campaign_name=_pick(body, "campaign_name"),
        email_account=(_pick(body, "email_account", "eaccount", "from_account") or "").lower() or None,
        reply_to_uuid=reply_to_uuid, reply_subject=_pick(body, "reply_subject", "subject", "email_subject"),
        reply_text=reply_text,
    )


_INSERT = text("""
    INSERT INTO lending.instantly_replies
        (dedup_key, event_type, lead_email, first_name, campaign_id, campaign_name, email_account, reply_to_uuid,
         reply_subject, reply_text, stop_request, status, intent)
    VALUES (:dedup_key, :event_type, :lead_email, :first_name, :campaign_id, :campaign_name, :email_account,
            :reply_to_uuid, :reply_subject, :reply_text, :stop, :status, :intent)
    ON CONFLICT (dedup_key) DO NOTHING
    RETURNING id
""")


def record_event(db, event: InstantlyEvent) -> RecordResult:
    """Store the event once. Does not commit."""
    stop = event.event_type == EVENT_UNSUBSCRIBED or could_mean_stop(event.reply_text)
    reply_id = db.execute(_INSERT, {
        "dedup_key": event.dedup_key[:200], "event_type": event.event_type, "lead_email": event.lead_email[:320],
        "first_name": (event.first_name or None) and event.first_name[:100], "campaign_id": event.campaign_id,
        "campaign_name": (event.campaign_name or None) and event.campaign_name[:200],
        "email_account": event.email_account, "reply_to_uuid": event.reply_to_uuid,
        "reply_subject": (event.reply_subject or None) and event.reply_subject[:500], "reply_text": event.reply_text,
        "stop": stop, "status": "suppressed" if stop else "new", "intent": "unsubscribe" if stop else None,
    }).scalar()
    if reply_id is None:
        return RecordResult("duplicate")
    return RecordResult("recorded", int(reply_id), stop)


_REPLY_ROW = text("SELECT id, lead_email, dedup_key, suppressed_at FROM lending.instantly_replies WHERE id = :id")
_SUPPRESSED = text("""
    UPDATE lending.instantly_replies SET suppressed_at = now(), opt_out_event_id = :event_id, processed_at = now(),
           last_error = NULL
     WHERE id = :id AND suppressed_at IS NULL
""")


_EMAIL_SUPPRESSED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE email = :e)
        OR EXISTS (SELECT 1 FROM email_opt_outs WHERE email = :e)
""")


def suppress_reply(db, reply_id: int) -> Optional[int]:
    """Record the opt-out for a STOP reply or an unsubscribe; returns the opt-out event id (None when it was already
    recorded). Idempotent per Instantly event. Does not commit."""
    row = db.execute(_REPLY_ROW, {"id": reply_id}).mappings().first()
    if row is None or row["suppressed_at"] is not None:
        return None
    event_id = propagate_opt_out(db, email=row["lead_email"], source_ref=f"{SOURCE_REF_PREFIX}{row['dedup_key']}",
                                 actor="instantly", channel=OptOutChannel.EMAIL)
    db.execute(_SUPPRESSED, {"id": reply_id, "event_id": event_id})
    return event_id


def suppression_notice(email: str, event_id: Optional[int], *, via_unsubscribe_link: bool) -> str:
    how = "used Instantly's unsubscribe link" if via_unsubscribe_link else "replied asking to stop"
    return (f":no_entry: Instantly: {mask_emails(email)} {how}. Suppressed everywhere (email, texts, calls, the "
            f"dialer and GHL do-not-disturb). Opt-out event #{event_id}. No answer will be drafted.")


def queue_suppression_for_cora(db, reply_id: int, email: str, event_id: Optional[int], *, posted: bool) -> None:
    add_to_inbox_safely(
        db, event_type="instantly_opt_out", dedup_key=str(reply_id), target_channel="ops_notice",
        channel_posted=posted, summary=f"Instantly opt-out suppressed: {mask_emails(email)}",
        payload={"instantly_reply_id": reply_id, "email_masked": mask_emails(email), "opt_out_event_id": event_id},
    )


# ── Cora's sweep: classify, draft, triage ────────────────────────────────────

def triage_text(row: Mapping[str, Any], triage: Triage, action_id: Optional[int]) -> str:
    who = row.get("first_name") or "Unknown contact"
    campaign = f", campaign {row['campaign_name']}" if row.get("campaign_name") else ""
    if action_id:
        next_step = f"Drafted answer: approval card in #approvals (action {action_id})."
    elif triage.intent == "dead":
        next_step = "No answer drafted."
    elif triage.intent == "unclassified":
        next_step = ("Not classified and no answer drafted: read it in Instantly. If they asked to stop, opt them out "
                     "by hand.")
    else:
        next_step = "No answer drafted: answer it by hand in Instantly."
    line, _ = triage_line(channel="instantly_email", intent=triage.intent, who=who,
                          contact_hint=f"{mask_emails(row['lead_email'])}{campaign}", summary=triage.summary,
                          next_step=next_step)
    return line


_CLAIM = text("""
    SELECT id, lead_email, first_name, campaign_name, email_account, reply_to_uuid, reply_subject, reply_text,
           status, stop_request, intent, summary, action_id, attempts, dedup_key
      FROM lending.instantly_replies
     WHERE (status = 'new'
            OR (status = 'suppressed' AND suppressed_at IS NULL AND received_at < now() - interval '20 seconds'))
       AND attempts < :max_attempts
     ORDER BY received_at
     LIMIT :limit
       FOR UPDATE SKIP LOCKED
""")
_FAILED = text("""
    UPDATE lending.instantly_replies SET attempts = attempts + 1, last_error = :error,
           status = CASE WHEN attempts + 1 >= :max_attempts AND status = 'new' THEN 'failed' ELSE status END
     WHERE id = :id
""")
_MODEL_STOP = text("""
    UPDATE lending.instantly_replies SET status = 'suppressed', stop_request = true, intent = 'unsubscribe',
           summary = :summary
     WHERE id = :id
""")
_DONE = text("""
    UPDATE lending.instantly_replies SET status = 'done', intent = :intent, summary = :summary, action_id = :action_id,
           triage_posted_at = CASE WHEN :posted THEN now() END, processed_at = now(), last_error = :error
     WHERE id = :id
""")

Enqueue = Callable[..., int]  # PendingActionQueue.enqueue
PostCard = Callable[[int], bool]  # SendGate.post_card
Poster = Callable[[str], bool]  # posts to a team channel; False when unconfigured or refused
Blocker = Callable[[str], bool]  # adds an address to the Instantly block list; best effort


@dataclass
class SweepResult:
    triaged: int = 0
    drafted: int = 0
    suppressed: int = 0
    failed: int = 0


class ReplyProcessor:
    """One sweep's work on ``lending.instantly_replies``, run by Cora (``cora_agent.run_sweep``)."""

    def __init__(self, *, session_factory: Callable[[], Any], client: Any, model: str, enqueue: Enqueue,
                 post_card: PostCard, post_triage: Poster, post_notice: Poster,
                 rules: Callable[[], list[str]] = lambda: [],
                 block_address: Blocker = lambda email: False,
                 hand_off: Optional[Callable[[Mapping[str, Any]], Any]] = None) -> None:
        self._session_factory = session_factory
        self._client = client
        self._model = model
        self._enqueue = enqueue
        self._post_card = post_card
        self._post_triage = post_triage
        self._post_notice = post_notice
        self._rules = rules
        self._block_address = block_address
        self._hand_off = hand_off

    def run(self, limit: int = BATCH_SIZE) -> SweepResult:
        result = SweepResult()
        with self._session_factory() as db:
            rows = [dict(r) for r in db.execute(_CLAIM, {"max_attempts": MAX_ATTEMPTS, "limit": limit}).mappings()]
            for row in rows:
                if row["status"] == "suppressed":
                    self._retry_suppression(db, row, result)
                else:
                    self._triage(db, row, result)
                db.commit()
        return result

    def _fail(self, db, row: Mapping[str, Any], error: str, result: SweepResult) -> None:
        result.failed += 1
        db.execute(_FAILED, {"id": row["id"], "error": error[:200], "max_attempts": MAX_ATTEMPTS})
        logger.error("[instantly-replies] reply %s not processed (attempt %d): %s", row["id"], row["attempts"] + 1,
                     error)
        if row["attempts"] + 1 >= MAX_ATTEMPTS and row["status"] == "new":
            self._finish(db, row, Triage("unclassified", "Could not be classified automatically."), None, error,
                         result)

    def _retry_suppression(self, db, row: Mapping[str, Any], result: SweepResult) -> None:
        try:
            with db.begin_nested():
                event_id = suppress_reply(db, row["id"])
        except Exception as exc:  # class only: SQL errors embed the address
            self._fail(db, row, f"opt-out not recorded ({type(exc).__name__})", result)
            if row["attempts"] + 1 >= MAX_ATTEMPTS:
                posted = self._post_notice(
                    f":rotating_light: Instantly: an opt-out request from {mask_emails(row['lead_email'])} could not "
                    f"be recorded after {MAX_ATTEMPTS} attempts ({type(exc).__name__}, reply {row['id']}). It is NOT "
                    "suppressed: record it by hand.")
                add_to_inbox_safely(
                    db, event_type="instantly_opt_out_failed", dedup_key=str(row["id"]), target_channel="ops_notice",
                    channel_posted=posted, summary="An Instantly opt-out could not be recorded: record it by hand",
                    payload={"instantly_reply_id": row["id"], "email_masked": mask_emails(row["lead_email"]),
                             "error": type(exc).__name__})
            return
        result.suppressed += 1
        posted = self._post_notice(suppression_notice(row["lead_email"], event_id, via_unsubscribe_link=False))
        queue_suppression_for_cora(db, row["id"], row["lead_email"], event_id, posted=posted)
        try:
            self._block_address(row["lead_email"])  # so NDL's Instantly campaigns stop too, as the webhook does
        except Exception as exc:  # best effort: the opt-out is recorded in our stores either way
            logger.warning("[instantly-replies] reply %s not added to the Instantly block list (%s)", row["id"],
                           type(exc).__name__)

    def _triage(self, db, row: Mapping[str, Any], result: SweepResult) -> None:
        try:
            triage = classify_reply(self._client, self._model, subject=row["reply_subject"], reply=row["reply_text"],
                                    rules=tuple(self._rules()))
        except TriageError as exc:
            self._fail(db, row, str(exc), result)
            return
        except Exception as exc:  # model or network: retried next sweep
            self._fail(db, row, f"model call failed ({type(exc).__name__})", result)
            return
        if triage.intent == STOP_INTENT:
            self._suppress_model_stop(db, row, triage, result)
            return
        action_id = None
        error = None
        if triage.intent in DRAFT_INTENTS:
            if row["email_account"] and row["reply_to_uuid"]:
                try:
                    action_id = self._draft(row, triage)
                    result.drafted += 1
                except LaneViolation:  # blocked by the send gate's Backflip check: no card, the team sees the error
                    error = "the drafted answer names Backflip, so the send gate blocked it"
            else:
                error = "reply has no Instantly inbox or email id, so it cannot be answered in the thread"
        if triage.intent == "deal" and self._hand_off is not None:
            self._hand_off_deal(db, row)
        self._finish(db, row, triage, action_id, error, result)

    def _hand_off_deal(self, db, row: Mapping[str, Any]) -> None:
        """A Deal reply becomes a GHL contact and pipeline card. Best effort: a failure never stops the reply being
        triaged and answered; it is raised to Cora's inbox so the team adds the contact by hand."""
        try:
            with db.begin_nested():  # a failed read must not abort the sweep's transaction
                opted_out = bool(db.execute(_EMAIL_SUPPRESSED, {"e": row["lead_email"].strip().lower()}).scalar())
            if opted_out:
                logger.info("[instantly-replies] reply %s not handed to GHL: the address has opted out", row["id"])
                return
            if self._hand_off is not None:
                self._hand_off(row)
        except Exception as exc:  # the step name and status carry no address; the class alone for anything else
            detail = str(exc)[:120] if isinstance(exc, DeliveryError) else type(exc).__name__
            logger.error("[instantly-replies] reply %s not handed to GHL (%s)", row["id"], detail)
            add_to_inbox_safely(
                db, event_type="instantly_ghl_handoff_failed", dedup_key=str(row["id"]), target_channel="ops",
                summary=f"A Deal reply from {mask_emails(row['lead_email'])} may be only partly in GHL ({detail}): "
                        "check the contact, its tag and the pipeline card, and finish by hand",
                payload={"instantly_reply_id": row["id"], "email_masked": mask_emails(row["lead_email"]),
                         "error": detail})

    def _suppress_model_stop(self, db, row: Mapping[str, Any], triage: Triage, result: SweepResult) -> None:
        """A stop request in wording the phrase check missed: marked as a STOP first, so a failed opt-out is retried
        by the sweep like the webhook's, then suppressed through the same path. No answer is drafted."""
        db.execute(_MODEL_STOP, {"id": row["id"], "summary": triage.summary or None})
        logger.info("[instantly-replies] reply %s read as a stop request by the model", row["id"])
        self._retry_suppression(db, {**row, "status": "suppressed", "stop_request": True, "intent": "unsubscribe"},
                                result)

    def _draft(self, row: Mapping[str, Any], triage: Triage) -> int:
        who = row["first_name"] or mask_emails(row["lead_email"])
        action_id = self._enqueue(
            tool_name=TOOL_NAME, channel=CHANNEL,
            payload={"to_email": row["lead_email"], "subject": triage.subject, "body": triage.body,
                     "from_inbox": row["email_account"], "reply_to_uuid": row["reply_to_uuid"],
                     "template_version": INSTANTLY_REPLY_TEMPLATE},
            summary=f"Answer to {who}'s Instantly reply ({INTENT_LABELS[triage.intent]}): {triage.summary}"[:500],
            requested_by="instantly", recipient_email=row["lead_email"],
            contact_ref=f"instantly_reply:{row['id']}", idempotency_key=f"instantly_reply:{row['id']}",
        )
        self._post_card(action_id)  # a refused card is re-posted by the sweep's retry_missing_cards
        return action_id

    def _finish(self, db, row: Mapping[str, Any], triage: Triage, action_id: Optional[int], error: Optional[str],
                result: SweepResult) -> None:
        posted = self._post_triage(triage_text(row, triage, action_id))
        db.execute(_DONE, {"id": row["id"], "intent": triage.intent, "summary": triage.summary or None,
                           "action_id": action_id, "posted": posted, "error": error})
        add_to_inbox_safely(
            db, event_type="instantly_reply", dedup_key=str(row["id"]), target_channel="replies",
            channel_posted=posted,
            summary=f"Instantly reply ({INTENT_LABELS.get(triage.intent, triage.intent)}): "
                    f"{row['first_name'] or mask_emails(row['lead_email'])}",
            payload={"instantly_reply_id": row["id"], "intent": triage.intent, "action_id": action_id,
                     "first_name": row["first_name"], "email_masked": mask_emails(row["lead_email"]),
                     "campaign": row["campaign_name"]},
        )
        result.triaged += 1
        logger.info("[instantly-replies] reply %s triaged intent=%s action=%s posted=%s", row["id"], triage.intent,
                    action_id, posted)
