"""P2.1 Speed-to-Lead: an instant text, then Josh's phone (David's if he misses it), then Voice AI at 2 minutes.

Our code decides; GoHighLevel texts and dials. Right after a new lead reaches GHL (LendingFlow: the delivery
follow-up; website: the delivery itself) ``start`` runs every gate and adds the tags that start the GHL workflows
(``docs/lending/speed-to-lead.md``):

* **Text.** LendingFlow leads already get T-09's intro text at the push (``lendingflow_ghl``); the time it was tagged
  is copied here for the report. A website lead gets ``web-intro-ready`` when the text box was ticked and the number
  still has text consent (``consent.has_text_consent``: consent row, no opt-out), the switch
  ``LENDING_INTRO_SMS_ENABLED`` is on and it is 8am-8pm for the borrower. Outside the window the text is held and the
  web-lead sweep releases it (``release_held_web_intro``), up to TEXT_LAST_CHANCE after the form.
* **Targets** (``config/lending_speed_to_lead.py``): text 5 s, Josh and David 60 s, Voice AI 2 min. A miss is logged
  and shown on the timing report. The text is skipped more than TEXT_LAST_CHANCE (24 h) after the form.
* **Call cascade** (``LENDING_SPEED_TO_LEAD_ENABLED``), under the same rules as Josh's GHL call lists
  (``ghl_call_gate.blocked_reasons``: suppression, DNC, already reached in BatchDialer, attempt caps, the 8am-8pm
  calling window, emergency halt). A website lead also needs the call-consent box and a fresh DNC scrub (one Tracerfy
  credit, run here; LendingFlow leads are exempt, client decision). Then ``ndl-ai-voice-ok`` is set or cleared, and
  ``ndl-speed-to-lead`` is removed and re-added so GHL's tag trigger fires again for a returning contact.
* **Call queue.** A lead that may be called but not right now waits in the queue instead of being skipped: outside
  the callers' hours (``LENDING_ALARM_HOURS_START`` / ``_END``, Eastern, Mon-Fri), delayed (reached GHL more than
  FRESH_LEAD_WINDOW after the form), another cascade started less than CASCADE_SPACING ago, or a temporary gate
  reason (its own calling window, the attempt cap, a scrub not back yet). ``release_queue`` (minute job
  ``src.tasks.lending_speed_to_lead_queue``) starts one cascade at a time, fresh leads first, then the oldest, and
  re-checks every rule first: the gate, the lead already handled in GHL, and a live read of the lead's GHL calls (a
  failed read releases nothing). Both paths take one advisory lock (QUEUE_LOCK_KEY), so two cascades never start
  together. A lead still queued after QUEUE_MAX_AGE leaves the queue with a Slack line; a backlog and a morning
  summary are posted to the lead-alerts channel once a day.
* **Voice AI** (brief section 7: only on consented leads, discloses it is an AI assistant for Next Deal Lending). Our
  code only marks who may get it: a website lead whose AI-call consent box was ticked with the exact wording. It runs
  2 minutes after that lead's cascade starts, queued leads included. A LendingFlow lead has no recorded AI-call
  consent, so it never gets the tag (open question for Josh).

The lending emergency halt withholds every tag and the queue releases nothing while it is on. Nothing here ever fails
a delivery: errors are logged by lead id and the lead still reaches BatchDialer as usual (no hold: team decision,
Nov 13; a lead BatchDialer has reached is not released from the queue). Logs carry lead ids only, never a phone number.

Usage:
    PYTHONPATH=. python -m src.lending.speed_to_lead --report web:123     # timings for one lead
    PYTHONPATH=. python -m src.lending.speed_to_lead --recent 10          # the last 10 leads
"""
from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_compliance import REACHED_CODES, REACHED_GHL_STATUSES, REACHED_MIN_TALK_SECONDS
from config.lending_followup import CALL_SOURCE_GHL
from config.lending_speed_to_lead import (
    ACTIVE_CASCADE_MINUTES,
    CALL_BLOCKED,
    CALL_EXPIRED,
    CALL_FAILED,
    CALL_HALTED,
    CALL_INTERRUPTED,
    CALL_OFF,
    CALL_QUEUED,
    CALL_RELEASING,
    CALL_TAGGED,
    CALL_TARGET,
    CASCADE_SPACING,
    FRESH_LEAD_WINDOW,
    GHL_TAG_AI_VOICE_OK,
    GHL_TAG_SPEED_TO_LEAD,
    GHL_TAG_WEB_INTRO_READY,
    NOTICE_BACKLOG,
    NOTICE_MORNING,
    QUEUE_ALERT_BATCH,
    QUEUE_ALERT_DEPTH,
    QUEUE_ALERT_WAIT,
    QUEUE_DELAYED,
    QUEUE_GHL_FAILURES_PER_RUN,
    QUEUE_GHL_RETRY_AFTER,
    QUEUE_LOCK_KEY,
    QUEUE_MAX_AGE,
    QUEUE_OUTSIDE_HOURS,
    QUEUE_RECENT_CALL,
    QUEUE_RECHECK_AFTER,
    QUEUE_SCAN_LIMIT,
    QUEUE_TEMPORARY_REASONS,
    QUEUE_WAITING_TURN,
    RELEASE_STUCK_AFTER,
    REPORT_CALL_WINDOW_MINUTES,
    SCRUB_MAX_CREDITS_PER_LEAD,
    SCRUB_MAX_POLLS,
    SOURCE_LENDINGFLOW,
    SOURCE_WEB,
    SOURCES,
    TEXT_BY_LENDINGFLOW,
    TEXT_HELD,
    TEXT_NOT_ALLOWED,
    TEXT_OFF,
    TEXT_LAST_CHANCE,
    TEXT_TAGGED,
    TEXT_TARGET,
    TEXT_TOO_LATE,
    WEB_INTRO_RELEASE_BATCH,
)
from config.settings import get_settings
from src.lending.compliance import text_window_open
from src.lending.consent import has_text_consent
from src.lending.global_halt import is_globally_halted

logger = logging.getLogger(__name__)

SOURCE_LABELS = {SOURCE_LENDINGFLOW: "LendingFlow", SOURCE_WEB: "Website"}
NO_CALL_CONSENT = "NO_CALL_CONSENT"
NO_PHONE = "NO_PHONE"
EMERGENCY_STOP = "EMERGENCY_STOP"
HANDLED_IN_GHL = "HANDLED_IN_GHL"
GHL_CHECK_FAILED = "GHL_CHECK_FAILED"
LEAD_MISSING = "LEAD_MISSING"
GHL_TAG_FAILED = "GHL_TAG_FAILED"


class TagClient(Protocol):
    def add_tag(self, contact_id: str, tag: str) -> None: ...
    def remove_tag(self, contact_id: str, tag: str) -> None: ...


Scrubber = Callable[[list[str]], list[dict]]


@dataclass(frozen=True)
class Outcome:
    source: str
    lead_id: int
    text_status: str
    call_status: str
    call_block_reason: Optional[str] = None
    voice_eligible: bool = False
    queue_reason: Optional[str] = None


_LEAD = {
    SOURCE_LENDINGFLOW: text(
        "SELECT id, phone, email, property_state, property_zip, received_at, ghl_contact_id, suppressed, "
        "handled_in_ghl_at, intro_sms_tagged_at, false AS sms_consent, false AS call_consent, "
        "false AS ai_call_consent, false AS ai_call_consent_text_matches FROM lending.lendingflow_leads WHERE id = :id"),
    SOURCE_WEB: text(
        "SELECT id, phone, email, property_state, NULL AS property_zip, received_at, ghl_contact_id, suppressed, "
        "handled_in_ghl_at, NULL AS intro_sms_tagged_at, sms_consent, call_consent, ai_call_consent, "
        "COALESCE(ai_call_consent_text_matches, false) AS ai_call_consent_text_matches "
        "FROM lending.web_leads WHERE id = :id"),
}
_CLAIM = text("""
    INSERT INTO lending.speed_to_lead (source, lead_id, ghl_contact_id, received_at, started_at)
    VALUES (:source, :lead_id, :contact_id, :received_at, :now)
    ON CONFLICT (source, lead_id) DO NOTHING
    RETURNING id
""")
# The fresh path marks the cascade as starting (with its start time, so the spacing sees it) before any tag is sent.
_MARK_STARTING = text("UPDATE lending.speed_to_lead SET call_status = :starting, call_tagged_at = :at WHERE id = :id")


class ClaimStore(Protocol):
    """Where ``start`` records that a lead was taken and that its cascade is starting. Both writes must be durable
    before any GHL tag is sent, so a delivery transaction that later fails to commit can never let a retry ring the
    lead a second time."""

    def claim(self, params: Mapping[str, Any]) -> Optional[int]: ...
    def mark_starting(self, row_id: int, at: datetime) -> None: ...


class SameSessionClaims:
    """Writes in the caller's session (tests on a rolled-back session). Not durable on its own."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def claim(self, params: Mapping[str, Any]) -> Optional[int]:
        return self.db.execute(_CLAIM, dict(params)).scalar()

    def mark_starting(self, row_id: int, at: datetime) -> None:
        self.db.execute(_MARK_STARTING, {"starting": CALL_RELEASING, "at": at, "id": row_id})


class OwnSessionClaims:
    """Live: each write commits at once in its own short session, independent of the delivery transaction."""

    def __init__(self, sessions: Optional[Callable[[], Any]] = None) -> None:
        self._sessions = sessions

    def _session(self) -> Any:
        if self._sessions is not None:
            return self._sessions()
        from src.lending.db import lending_session

        return lending_session()

    def claim(self, params: Mapping[str, Any]) -> Optional[int]:
        with self._session() as session:
            row_id = session.execute(_CLAIM, dict(params)).scalar()
            session.commit()
            return row_id

    def mark_starting(self, row_id: int, at: datetime) -> None:
        with self._session() as session:
            session.execute(_MARK_STARTING, {"starting": CALL_RELEASING, "at": at, "id": row_id})
            session.commit()


_RECORD = text("""
    UPDATE lending.speed_to_lead
       SET text_status = :text_status, text_tagged_at = :text_tagged_at, call_status = :call_status,
           call_block_reason = :call_block_reason, call_tagged_at = :call_tagged_at, voice_eligible = :voice,
           queued_at = :queued_at, queue_reason = :queue_reason
     WHERE id = :id
""")


def intro_sms_on() -> bool:
    """LENDING_INTRO_SMS_ENABLED, the greeting field the workflow reads (as for LendingFlow's T-09), and the carrier
    (10DLC) approval LENDING_SMS_CARRIER_APPROVED that every lending text waits for (project rule: no SMS before
    carrier approval is confirmed)."""
    settings = get_settings()
    return bool(settings.lending_intro_sms_enabled and settings.lending_ghl_cf_intro_name
                and settings.lending_sms_carrier_approved)


def callers_hours_open(now: datetime) -> bool:
    """Josh's and David's hours: LENDING_ALARM_HOURS_START to _END, Eastern, on the alarm business days (the same
    hours the uncalled-lead alarms use)."""
    from src.lending.uncalled_alarm_worker import OPEN, alarm_hours

    return alarm_hours(now) == OPEN


def callers_hours_start(now: datetime) -> datetime:
    """Today's opening of the callers' hours (Eastern), as an aware datetime."""
    from config.lending_reminders import TIMEZONE
    from src.lending.uncalled_alarm_worker import working_hours

    return datetime.combine(now.astimezone(TIMEZONE).date(), working_hours()[0], tzinfo=TIMEZONE)


def voice_eligible(lead: Mapping[str, Any]) -> bool:
    """A recorded, unrevoked AI-call consent: the website form's AI call box ticked with exactly the approved wording
    (``config/lending_web.AI_CALL_CONSENT_TEXT``), on a number that is not suppressed. LendingFlow leads have no
    AI-call consent record, so never. The Voice AI only follows the Josh/David cascade, which needs the call box."""
    return bool(lead["ai_call_consent"] and lead["ai_call_consent_text_matches"] and not lead["suppressed"])


def _web_text(db, lead: Mapping[str, Any], tags: TagClient, *, halted: bool, now: datetime) -> tuple[str, Optional[datetime]]:
    if not intro_sms_on():
        return TEXT_OFF, None
    if now - lead["received_at"] > TEXT_LAST_CHANCE:
        return TEXT_TOO_LATE, None
    if halted or not lead["sms_consent"] or lead["suppressed"] or not has_text_consent(db, lead["phone"]):
        return TEXT_NOT_ALLOWED, None
    if not text_window_open(lead["phone"], property_state=lead["property_state"], now=now):
        logger.info("[speed-to-lead] web intro text held for the send window lead=%s", lead["id"])
        return TEXT_HELD, None
    tags.add_tag(lead["ghl_contact_id"], GHL_TAG_WEB_INTRO_READY)
    return TEXT_TAGGED, now


def _call_block_reason(db, source: str, lead: Mapping[str, Any], *, halted: bool, now: datetime,
                       scrubber: Optional[Scrubber]) -> Optional[str]:
    """Why the cascade may not call this lead now, or None. Fails closed: a failed scrub leaves the number
    unscrubbed, which the gate then refuses."""
    from src.lending.ghl_call_gate import blocked_reasons

    if halted:
        return EMERGENCY_STOP
    if not lead["phone"]:
        return NO_PHONE
    if lead["handled_in_ghl_at"] is not None:
        return HANDLED_IN_GHL
    if source == SOURCE_WEB:
        if not lead["call_consent"]:
            return NO_CALL_CONSENT
        if scrubber is not None:
            from src.lending.ghl_list_scrub import list_scrub

            try:
                with db.begin_nested():
                    list_scrub(db, [lead["phone"]], scrubber=scrubber, max_credits=SCRUB_MAX_CREDITS_PER_LEAD, now=now)
            except Exception as exc:  # class only: vendor errors can echo the number
                logger.error("[speed-to-lead] DNC scrub failed lead=%s: %s", lead["id"], type(exc).__name__)
    # The halt was already answered above; False only skips the gate's own repeat of that check.
    return blocked_reasons(db, [lead["phone"]], now=now, halted=False).get(lead["phone"])


def _start_cascade(lead: Mapping[str, Any], tags: TagClient, *, voice: bool) -> None:
    contact_id = lead["ghl_contact_id"]
    # The voice tag first: the workflow reads it at the 2-minute step, and a contact that no longer has consent
    # must not keep an older tag.
    if voice:
        tags.add_tag(contact_id, GHL_TAG_AI_VOICE_OK)
    else:
        tags.remove_tag(contact_id, GHL_TAG_AI_VOICE_OK)
    tags.remove_tag(contact_id, GHL_TAG_SPEED_TO_LEAD)  # GHL starts a workflow only when the tag is newly added
    tags.add_tag(contact_id, GHL_TAG_SPEED_TO_LEAD)


@dataclass(frozen=True)
class CallStep:
    status: str
    block_reason: Optional[str] = None
    tagged_at: Optional[datetime] = None
    voice: bool = False
    queue_reason: Optional[str] = None


_TRY_LOCK = text("SELECT pg_try_advisory_xact_lock(:key)")
_MUST_WAIT = text("""
    SELECT EXISTS (SELECT 1 FROM lending.speed_to_lead WHERE call_tagged_at > :spacing_since)
        OR EXISTS (SELECT 1 FROM lending.speed_to_lead
                    WHERE call_status = 'queued' AND received_at > :fresh_since
                      AND queue_reason IN ('waiting_turn', 'outside_hours'))
""")
_CASCADE_RUNNING = text("SELECT EXISTS (SELECT 1 FROM lending.speed_to_lead WHERE call_tagged_at > :spacing_since)")


Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fixed(at: datetime) -> Clock:
    return lambda: at


def try_queue_lock(db) -> bool:
    """The one lock the fresh-lead start and the queue worker share, held until this transaction ends, so the next
    holder sees the cascade just started (its call_tagged_at) and keeps CASCADE_SPACING. Never waits: a delivery
    batch can hold it for minutes, and a lead that cannot get it simply waits its turn in the queue."""
    return bool(db.execute(_TRY_LOCK, {"key": QUEUE_LOCK_KEY}).scalar())


def _gate(db, source: str, lead: Mapping[str, Any], *, halted: bool, now: datetime,
          scrubber: Optional[Scrubber]) -> Optional[CallStep]:
    """None when the lead may be called now; else the step that ends here (blocked for good, or queued for a
    temporary reason, which the queue re-checks)."""
    reason = _call_block_reason(db, source, lead, halted=halted, now=now, scrubber=scrubber)
    if reason is None:
        return None
    if reason in QUEUE_TEMPORARY_REASONS or reason == EMERGENCY_STOP:  # the halt is lifted later: wait, like a queued lead
        return CallStep(CALL_QUEUED, queue_reason=reason)
    logger.info("[speed-to-lead] no call cascade lead=%s:%s reason=%s", source, lead["id"], reason)
    return CallStep(CALL_BLOCKED, block_reason=reason)


def _call(db, source: str, lead: Mapping[str, Any], tags: TagClient, *, halted: bool, now: datetime,
          scrubber: Optional[Scrubber], clock: Clock, before_tags: Callable[[datetime], None]) -> CallStep:
    """A fresh lead: start the cascade now when every rule allows it and no other cascade is due first; otherwise
    queue it (or block it for good)."""
    if not get_settings().lending_speed_to_lead_enabled:
        return CallStep(CALL_OFF)
    gated = _gate(db, source, lead, halted=halted, now=now, scrubber=scrubber)
    if gated is not None:
        return gated
    if now - lead["received_at"] > FRESH_LEAD_WINDOW:
        return CallStep(CALL_QUEUED, queue_reason=QUEUE_DELAYED)
    if not callers_hours_open(now):
        return CallStep(CALL_QUEUED, queue_reason=QUEUE_OUTSIDE_HOURS)
    if not try_queue_lock(db):
        return CallStep(CALL_QUEUED, queue_reason=QUEUE_WAITING_TURN)
    at = clock()  # after the scrub and the lock: the spacing is measured from now, not from the arrival
    if db.execute(_MUST_WAIT, {"spacing_since": at - CASCADE_SPACING,
                               "fresh_since": at - FRESH_LEAD_WINDOW}).scalar():
        return CallStep(CALL_QUEUED, queue_reason=QUEUE_WAITING_TURN)
    before_tags(at)  # durable before any tag: a failed commit of the delivery can never lead to a second cascade
    return _begin(source, lead, tags, now=at)


def _begin(source: str, lead: Mapping[str, Any], tags: TagClient, *, now: datetime) -> CallStep:
    """Add the tags that start the GHL cascade, then undo them if the emergency halt landed meanwhile."""
    voice = voice_eligible(lead)
    try:
        _start_cascade(lead, tags, voice=voice)
    except Exception as exc:  # class only
        logger.error("[speed-to-lead] GHL tag failed, no call cascade lead=%s:%s: %s", source, lead["id"],
                     type(exc).__name__)
        return CallStep(CALL_FAILED, block_reason=type(exc).__name__[:60], voice=voice)
    if is_globally_halted():
        # The halt landed after the check above but before this row is committed, so its own cleanup
        # (stop_active_cascades) cannot see this lead: undo the tags here.
        try:
            _stop_cascade(lead, tags)
        except Exception as exc:  # class only
            logger.error("[speed-to-lead] could not undo the cascade after the halt lead=%s:%s: %s", source,
                         lead["id"], type(exc).__name__)
            return CallStep(CALL_TAGGED, tagged_at=now, voice=voice)  # left tagged: the halt's cleanup sees it
        logger.warning("[speed-to-lead] emergency halt set while starting, cascade undone lead=%s:%s", source,
                       lead["id"])
        return CallStep(CALL_HALTED, block_reason=EMERGENCY_STOP, voice=voice)
    logger.info("[speed-to-lead] call cascade started lead=%s:%s voice_ai=%s", source, lead["id"], voice)
    return CallStep(CALL_TAGGED, tagged_at=now, voice=voice)


def _stop_cascade(lead: Mapping[str, Any], tags: TagClient) -> None:
    tags.remove_tag(lead["ghl_contact_id"], GHL_TAG_AI_VOICE_OK)
    tags.remove_tag(lead["ghl_contact_id"], GHL_TAG_SPEED_TO_LEAD)


Notifier = Callable[[str], bool]  # posts to the lead-alerts channel (ops_alerts.post_ops_alert)
ContactUrl = Callable[[str], Optional[str]]


def _log_target_misses(source: str, lead: Mapping[str, Any], text_at: Optional[datetime],
                       call_at: Optional[datetime]) -> None:
    for step, at, target in (("text", text_at, TEXT_TARGET), ("call", call_at, CALL_TARGET)):
        if at is not None and at - lead["received_at"] > target:
            logger.warning("[speed-to-lead] %s missed its %ss target lead=%s:%s (+%.0fs)", step,
                           int(target.total_seconds()), source, lead["id"], (at - lead["received_at"]).total_seconds())


def _post(notify: Optional[Notifier], message: str, what: str) -> bool:
    """Best effort and never raises: a failed post never rolls back a queue decision."""
    if notify is None:
        return False
    try:
        posted = bool(notify(message))
    except Exception as exc:  # class only
        logger.error("[speed-to-lead] %s post failed: %s", what, type(exc).__name__)
        return False
    if not posted:
        logger.error("[speed-to-lead] %s post not sent", what)
    return posted


def start(db, source: str, lead_id: int, *, tags: TagClient, scrubber: Optional[Scrubber] = None,
          now: Optional[datetime] = None, clock: Optional[Clock] = None,
          claims: Optional[ClaimStore] = None) -> Optional[Outcome]:
    """Run the speed-to-lead step once for a lead that has just reached GHL. Returns None when the lead is unknown,
    has no GHL contact yet, or was already handled (a retried delivery). A lead that may be called but not right now
    is queued for ``release_queue``. Does not commit; when the cascade could start, the queue lock is held until the
    caller's transaction ends. ``now`` fixes the time for tests; otherwise the clock is read again after the lock.
    ``claims`` is where the lead is claimed and marked as starting; the live step passes ``OwnSessionClaims`` so both
    are committed before any tag is sent (default: the caller's session)."""
    if source not in SOURCES:
        raise ValueError(f"unknown speed-to-lead source {source!r}")
    clock = clock or (_fixed(now) if now is not None else _utcnow)
    now = clock()
    lead = db.execute(_LEAD[source], {"id": lead_id}).mappings().first()
    if lead is None or not lead["ghl_contact_id"]:
        return None
    claims = claims or SameSessionClaims(db)
    row_id = claims.claim({"source": source, "lead_id": lead_id, "contact_id": lead["ghl_contact_id"],
                           "received_at": lead["received_at"], "now": now})
    if row_id is None:
        return None
    halted = is_globally_halted()
    if source == SOURCE_LENDINGFLOW:
        text_status, text_at = TEXT_BY_LENDINGFLOW, lead["intro_sms_tagged_at"]
    else:
        try:
            text_status, text_at = _web_text(db, lead, tags, halted=halted, now=now)
        except Exception as exc:  # class only
            logger.error("[speed-to-lead] web intro tag failed lead=%s: %s", lead_id, type(exc).__name__)
            text_status, text_at = TEXT_NOT_ALLOWED, None
    call = _call(db, source, lead, tags, halted=halted, now=now, scrubber=scrubber, clock=clock,
                 before_tags=lambda at: claims.mark_starting(row_id, at))
    queued = call.status == CALL_QUEUED
    db.execute(_RECORD, {"id": row_id, "text_status": text_status, "text_tagged_at": text_at,
                         "call_status": call.status, "call_block_reason": call.block_reason,
                         "call_tagged_at": call.tagged_at, "voice": call.voice,
                         "queued_at": now if queued else None, "queue_reason": call.queue_reason})
    if queued:
        logger.info("[speed-to-lead] call queued lead=%s:%s reason=%s", source, lead_id, call.queue_reason)
    _log_target_misses(source, lead, text_at, call.tagged_at)
    return Outcome(source, lead_id, text_status, call.status, call.block_reason, call.voice, call.queue_reason)


def start_safely(db, source: str, lead_id: int, *, tags: Optional[TagClient],
                 scrubber: Optional[Scrubber] = None, claims: Optional[ClaimStore] = None) -> Optional[Outcome]:
    """``start`` inside a savepoint; never raises, so the lead's delivery is never affected."""
    if tags is None:
        return None
    try:
        with db.begin_nested():
            return start(db, source, lead_id, tags=tags, scrubber=scrubber, claims=claims)
    except Exception as exc:  # class only
        logger.error("[speed-to-lead] step failed lead=%s:%s: %s", source, lead_id, type(exc).__name__)
        return None


def live_tag_client() -> Optional[TagClient]:
    """The Next Deal Lending GHL account's tag calls, or None when it is not configured."""
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_call_gate import GhlCallLists

    account = lending_ghl_account()
    return GhlCallLists(account) if account is not None else None


def live_scrubber() -> Scrubber:
    """Tracerfy, waiting at most SCRUB_MAX_POLLS polls so a slow scrub cannot push the call past 60 seconds."""
    from src.lending.compliance import tracerfy_scrub

    def scrub(phones: list[str]) -> list[dict]:
        return tracerfy_scrub(phones, max_polls=SCRUB_MAX_POLLS)

    return scrub


AfterDelivery = Callable[[Any, int], None]


def live_step(source: str) -> Optional[AfterDelivery]:
    """The step the live delivery paths run after a lead reaches GHL, or None when there is nothing to do: GHL not
    configured, or neither the call cascade nor (for website leads) the intro text is switched on."""
    settings = get_settings()
    if not (settings.lending_speed_to_lead_enabled or (source == SOURCE_WEB and intro_sms_on())):
        return None
    tags = live_tag_client()
    if tags is None:
        return None
    scrubber = live_scrubber() if source == SOURCE_WEB else None

    def step(db: Any, lead_id: int) -> None:
        start_safely(db, source, lead_id, tags=tags, scrubber=scrubber, claims=OwnSessionClaims())

    return step


def live_contact_url() -> ContactUrl:
    from src.lending.lead_arrival_alert import ghl_contact_url

    settings = get_settings()

    def contact_url(contact_id: str) -> Optional[str]:
        return ghl_contact_url(settings.lending_ghl_app_url, settings.lending_ghl_location_id, contact_id)

    return contact_url


_HELD_WEB = text("""
    SELECT s.id AS row_id, w.id, w.phone, w.property_state, w.ghl_contact_id, w.suppressed, w.sms_consent
      FROM lending.speed_to_lead s JOIN lending.web_leads w ON w.id = s.lead_id
     WHERE s.source = 'web' AND s.text_status = :held AND s.received_at > :cutoff
     ORDER BY s.received_at
     LIMIT :limit
       FOR UPDATE OF s SKIP LOCKED
""")
_TEXT_DONE = text("UPDATE lending.speed_to_lead SET text_status = :status, text_tagged_at = :at WHERE id = :id")


def release_held_web_intro(db, tags: Optional[TagClient], *, now: Optional[datetime] = None) -> int:
    """Web-lead sweep: tag the website intro texts held for the send window once it opens. Re-checks consent (an
    opt-out since arrival withholds it for good). Returns how many were tagged. Does not commit."""
    if tags is None or not intro_sms_on() or is_globally_halted():
        return 0
    now = now or datetime.now(timezone.utc)
    tagged = 0
    rows = db.execute(_HELD_WEB, {"held": TEXT_HELD, "limit": WEB_INTRO_RELEASE_BATCH,
                                  "cutoff": now - TEXT_LAST_CHANCE}).mappings().all()
    for lead in rows:
        if lead["suppressed"] or not lead["sms_consent"] or not has_text_consent(db, lead["phone"]):
            db.execute(_TEXT_DONE, {"status": TEXT_NOT_ALLOWED, "at": None, "id": lead["row_id"]})
            continue
        if not text_window_open(lead["phone"], property_state=lead["property_state"], now=now):
            continue
        try:
            tags.add_tag(lead["ghl_contact_id"], GHL_TAG_WEB_INTRO_READY)
        except Exception as exc:  # class only; retried next sweep
            logger.error("[speed-to-lead] held web intro tag failed lead=%s: %s", lead["id"], type(exc).__name__)
            continue
        db.execute(_TEXT_DONE, {"status": TEXT_TAGGED, "at": now, "id": lead["row_id"]})
        tagged += 1
    if tagged:
        logger.info("[speed-to-lead] released %d held web intro text(s)", tagged)
    return tagged


# ── the call queue ───────────────────────────────────────────────────────────

@dataclass
class QueueRun:
    expired: int = 0
    released: int = 0
    blocked: int = 0
    waiting: int = 0
    ghl_check_failed: int = 0
    interrupted: int = 0
    switched_off: int = 0


_EXPIRE = text("""
    UPDATE lending.speed_to_lead SET call_status = :expired, released_at = :now
     WHERE call_status = 'queued' AND received_at < :cutoff
""")
_INTERRUPTED = text("""
    UPDATE lending.speed_to_lead SET call_status = :interrupted, call_block_reason = 'RELEASE_INTERRUPTED'
     WHERE call_status = :releasing AND COALESCE(released_at, call_tagged_at) < :stuck_before
""")
_SWITCHED_OFF = text("""
    UPDATE lending.speed_to_lead SET call_status = :off, call_block_reason = 'SWITCHED_OFF', released_at = :now
     WHERE call_status = 'queued'
""")
_UNALERTED = text("""
    SELECT s.id, s.source, s.lead_id, s.call_status, s.ghl_contact_id,
           COALESCE(NULLIF(btrim(concat_ws(' ', l.first_name, l.last_name)), ''), NULLIF(btrim(w.name), '')) AS name
      FROM lending.speed_to_lead s
      LEFT JOIN lending.lendingflow_leads l ON s.source = 'lendingflow' AND l.id = s.lead_id
      LEFT JOIN lending.web_leads w ON s.source = 'web' AND w.id = s.lead_id
     WHERE s.call_status IN (:expired, :interrupted) AND s.alerted_at IS NULL
     ORDER BY s.id LIMIT :limit
""")
_ALERTED = text("UPDATE lending.speed_to_lead SET alerted_at = :now WHERE id = ANY(:ids)")
_CANDIDATES = text("""
    SELECT id AS row_id, source, lead_id FROM lending.speed_to_lead
     WHERE call_status = 'queued' AND (recheck_at IS NULL OR recheck_at <= :now)
     ORDER BY (received_at > :fresh_since) DESC, received_at, id
     LIMIT :limit
""")
_STILL_QUEUED = text("SELECT 1 FROM lending.speed_to_lead WHERE id = :id AND call_status = 'queued' FOR UPDATE")
_RECHECK_LATER = text("""
    UPDATE lending.speed_to_lead SET queue_reason = :reason, recheck_at = :recheck_at
     WHERE id = :id AND call_status = 'queued'
""")
_END_QUEUED = text("""
    UPDATE lending.speed_to_lead SET call_status = :status, call_block_reason = :reason, released_at = :now
     WHERE id = :id AND call_status = 'queued'
""")
# Committed before any tag is sent: the claim's call_tagged_at keeps the spacing for the next holder of the lock, and a
# crash before the outcome is written leaves the lead 'releasing' (never rung twice; reported as interrupted).
_CLAIM_RELEASE = text("""
    UPDATE lending.speed_to_lead SET call_status = :releasing, call_tagged_at = :now, released_at = :now
     WHERE id = :id AND call_status = 'queued'
""")
_RELEASE = text("""
    UPDATE lending.speed_to_lead SET call_status = :status, call_tagged_at = :tagged_at, voice_eligible = :voice
     WHERE id = :id AND call_status = :releasing
""")
# The tags did not go on (GHL error) or the halt landed: back in the queue. call_tagged_at is kept: a tag call that
# timed out on our side may still have started the workflow in GHL, so the spacing keeps counting from the claim.
_BACK_IN_QUEUE = text("""
    UPDATE lending.speed_to_lead
       SET call_status = 'queued', queue_reason = :reason, released_at = NULL, recheck_at = :recheck_at
     WHERE id = :id AND call_status = :releasing
""")
_CONNECTED_STORED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.call_dispositions d
                    WHERE d.phone = :phone AND d.call_source = :ghl AND d.call_started_at >= :since
                      AND (d.disposition = ANY(:codes)
                           OR (d.disposition IS NULL AND d.disposition_raw = ANY(:connected)
                               AND COALESCE(d.talk_duration_sec, 0) >= :min_talk)))
""")
_RECENT_CALL_STORED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.call_dispositions
                    WHERE phone = :phone AND COALESCE(call_started_at, call_ended_at) >= :since)
""")
_QUEUE_STATS = text("SELECT count(*) AS waiting, min(received_at) AS oldest, min(queued_at) AS first_queued "
                    "FROM lending.speed_to_lead WHERE call_status = 'queued'")
_CLAIM_NOTICE = text("INSERT INTO lending.speed_to_lead_notices (kind, day, posted_at) VALUES (:kind, :day, :now) "
                     "ON CONFLICT DO NOTHING RETURNING kind")

CONNECTED, RECENTLY_CALLED = "connected", "recently_called"


def _ghl_call_check(db, read_calls: CallReader, lead: Mapping[str, Any], now: datetime) -> Optional[str]:
    """Read the lead's GHL calls live and say whether one since the lead arrived got through (CONNECTED, the shared
    "reached" rule in config.lending_compliance) or any call was placed in the last QUEUE_RECENT_CALL
    (RECENTLY_CALLED); "" when neither. Decided from the messages read, so a failed store cannot hide a call; the
    calls are also stored (best effort) as the dialer's live check does. Raises when the read fails."""
    from src.lending.ghl_call_import import CALL_DIRECTIONS, ImportStats, _store_contact
    from src.lending.ghl_calls import call_duration_seconds, call_state, message_time

    messages = read_calls(lead["ghl_contact_id"])
    recent_since = now - QUEUE_RECENT_CALL
    connected = recent = False
    for message in messages:
        at = message_time(message)
        if at is None or at < lead["received_at"]:
            continue
        if (f"ghl:{call_state(message)}" in REACHED_GHL_STATUSES
                and (call_duration_seconds(message) or 0) >= REACHED_MIN_TALK_SECONDS):
            connected = True
        recent = recent or at >= recent_since
    _store_contact(db, lambda contact_id: messages, lead["ghl_contact_id"], lead["phone"], lead["received_at"],
                   ImportStats(), CALL_DIRECTIONS)
    connected = connected or bool(db.execute(_CONNECTED_STORED, {
        "phone": lead["phone"], "ghl": CALL_SOURCE_GHL, "since": lead["received_at"], "codes": list(REACHED_CODES),
        "connected": list(REACHED_GHL_STATUSES), "min_talk": REACHED_MIN_TALK_SECONDS}).scalar())
    if connected:
        return CONNECTED
    # Any call to the number (BatchDialer CDRs too) in the last few minutes: never ring on top of it.
    recent = recent or bool(db.execute(_RECENT_CALL_STORED, {"phone": lead["phone"], "since": recent_since}).scalar())
    return RECENTLY_CALLED if recent else ""


Unset = Callable[[str], None]  # reports a post skipped for unset settings (ops_alerts / slack_channels.report_unset)


def _alert_left_queue(db, notify: Optional[Notifier], contact_url: Optional[ContactUrl], now: datetime,
                      unset: Optional[Unset] = None) -> None:
    """One post per run for leads that left the queue without a cascade (expired, or a release interrupted by a
    crash), up to QUEUE_ALERT_BATCH. Marked alerted only once the post went out, so a failed post is retried next
    minute; test leads are marked without a post. Without a channel (``notify`` None) the skip is reported through
    ``unset`` and the rows are marked, so they are not flushed as stale lines once the channel is set."""
    from src.lending.slack_names import name_and_ref
    from src.lending.slack_test_filter import hidden_lendingflow_ids, hidden_web_lead_ids, log_skipped

    rows = db.execute(_UNALERTED, {"expired": CALL_EXPIRED, "interrupted": CALL_INTERRUPTED,
                                   "limit": QUEUE_ALERT_BATCH}).mappings().all()
    if not rows:
        return
    if notify is None:
        if unset is not None:
            unset("speed-to-lead left-queue alert")
        db.execute(_ALERTED, {"now": now, "ids": [r["id"] for r in rows]})
        db.commit()
        return
    hidden = {(SOURCE_LENDINGFLOW, i) for i in hidden_lendingflow_ids(db, [r["lead_id"] for r in rows
                                                                          if r["source"] == SOURCE_LENDINGFLOW])}
    hidden |= {(SOURCE_WEB, i) for i in hidden_web_lead_ids(db, [r["lead_id"] for r in rows
                                                                 if r["source"] == SOURCE_WEB])}
    lines = []
    for r in rows:
        if (r["source"], r["lead_id"]) in hidden:
            log_skipped("speed-to-lead queue alert", f"{r['source']} lead {r['lead_id']}")
            continue
        url = contact_url(r["ghl_contact_id"]) if contact_url is not None and r["ghl_contact_id"] else None
        why = (f"waited {int(QUEUE_MAX_AGE.total_seconds() // 3600)} h" if r["call_status"] == CALL_EXPIRED
               else "its release was interrupted")
        who = name_and_ref(r["name"], f"{SOURCE_LABELS[r['source']]} lead {r['lead_id']}")
        lines.append(f"• {who}: {why}" + (f" <{url}|Open in GHL>" if url else ""))
    if lines and not _post(notify, ":hourglass: *Speed-to-lead queue:* left the queue without an instant call "
                                   "(still on the normal call list):\n" + "\n".join(lines), "left-queue"):
        db.rollback()
        return
    db.execute(_ALERTED, {"now": now, "ids": [r["id"] for r in rows]})
    db.commit()


def _housekeeping(db, now: datetime, run: QueueRun) -> None:
    run.expired = db.execute(_EXPIRE, {"expired": CALL_EXPIRED, "now": now, "cutoff": now - QUEUE_MAX_AGE}).rowcount
    run.interrupted = db.execute(_INTERRUPTED, {"interrupted": CALL_INTERRUPTED, "releasing": CALL_RELEASING,
                                                "stuck_before": now - RELEASE_STUCK_AFTER}).rowcount
    db.commit()
    if run.expired:
        logger.warning("[speed-to-lead] %d queued call(s) expired after %s", run.expired, QUEUE_MAX_AGE)
    if run.interrupted:
        logger.error("[speed-to-lead] %d release(s) interrupted before the outcome was written; posted to the "
                     "lead-alerts channel", run.interrupted)


def _claim_notice(db, kind: str, now: datetime) -> bool:
    from config.lending_reminders import TIMEZONE

    return db.execute(_CLAIM_NOTICE, {"kind": kind, "day": now.astimezone(TIMEZONE).date(), "now": now}).first() \
        is not None


def _post_notice(db, notify: Optional[Notifier], kind: str, message: Optional[str], now: datetime,
                 unset: Optional[Unset]) -> None:
    """Once per Eastern day per kind. A notice with nothing to say is recorded without a post; a post that fails is
    rolled back, so the next minute tries again. Without a channel the skip is reported once and recorded."""
    if not _claim_notice(db, kind, now):
        db.rollback()
        return
    if message is not None and notify is None:
        if unset is not None:
            unset(f"speed-to-lead queue {kind} notice")
        db.commit()
        return
    if message is None or _post(notify, message, f"queue {kind}"):
        db.commit()
    else:
        db.rollback()


def _notices(db, notify: Optional[Notifier], now: datetime, unset: Optional[Unset] = None) -> None:
    from config.lending_reminders import TIMEZONE

    stats = db.execute(_QUEUE_STATS).mappings().one()
    waiting = int(stats["waiting"])
    first = stats["oldest"].astimezone(TIMEZONE) if stats["oldest"] is not None else None
    oldest = f"oldest from {first:%a} {first.hour % 12 or 12}:{first:%M %p} ET" if first is not None else ""
    _post_notice(db, notify, NOTICE_MORNING,
                 f":sunrise: *Speed-to-lead queue:* {waiting} lead(s) waiting ({oldest}). They are rung one at a "
                 f"time, fresh leads first." if waiting else None, now, unset)
    if not waiting:
        return
    waited = now - max(stats["first_queued"] or now, callers_hours_start(now))
    if waiting >= QUEUE_ALERT_DEPTH or waited >= QUEUE_ALERT_WAIT:
        _post_notice(db, notify, NOTICE_BACKLOG,
                     f":rotating_light: *Speed-to-lead queue backlog:* {waiting} lead(s) waiting, the longest for "
                     f"{int(waited.total_seconds() // 60)} min of calling hours ({oldest}). Leads are rung one every "
                     f"{int(CASCADE_SPACING.total_seconds() // 60)} min.", now, unset)


def _end(db, row_id: int, reason: str, now: datetime, run: QueueRun, lead_ref: str) -> None:
    db.execute(_END_QUEUED, {"status": CALL_BLOCKED, "reason": reason, "now": now, "id": row_id})
    db.commit()
    run.blocked += 1
    logger.info("[speed-to-lead] queued call ended lead=%s reason=%s", lead_ref, reason)


def _later(db, row_id: int, reason: str, recheck_at: datetime, run: QueueRun) -> None:
    db.execute(_RECHECK_LATER, {"reason": reason, "recheck_at": recheck_at, "id": row_id})
    db.commit()
    run.waiting += 1


# _try_release outcomes: keep scanning the queue, or stop this run.
NEXT, STOP = False, True


def _try_release(db, row: Mapping[str, Any], *, tags: TagClient, scrubber: Optional[Scrubber], read_calls: CallReader,
                 clock: Clock, run: QueueRun) -> bool:
    """Re-check one queued lead and start its cascade when every rule allows it. STOP when a cascade started or
    nothing more may start this run (the lock is held elsewhere, a cascade is running, the halt, a GHL tag error)."""
    source, ref = row["source"], f"{row['source']}:{row['lead_id']}"
    now = clock()
    lead = db.execute(_LEAD[source], {"id": row["lead_id"]}).mappings().first()
    if lead is None or not lead["ghl_contact_id"]:
        _end(db, row["row_id"], LEAD_MISSING, now, run, ref)
        return NEXT
    gated = _gate(db, source, lead, halted=False, now=now, scrubber=scrubber)
    if gated is not None and gated.status == CALL_BLOCKED:
        _end(db, row["row_id"], gated.block_reason or "", now, run, ref)
        return NEXT
    if gated is not None:
        _later(db, row["row_id"], gated.queue_reason or "", now + QUEUE_RECHECK_AFTER, run)
        return NEXT
    db.commit()  # keep a scrub the gate just paid for, whatever the GHL read below does
    try:
        calls = _ghl_call_check(db, read_calls, lead, now)
    except Exception as exc:  # class only: GHL errors can echo request detail. Fail closed for this lead only.
        db.rollback()
        logger.error("[speed-to-lead] GHL call check failed lead=%s: %s", ref, type(exc).__name__)
        run.ghl_check_failed += 1
        _later(db, row["row_id"], GHL_CHECK_FAILED, now + QUEUE_GHL_RETRY_AFTER, run)
        return STOP if run.ghl_check_failed >= QUEUE_GHL_FAILURES_PER_RUN else NEXT
    db.commit()
    if calls == CONNECTED:
        _end(db, row["row_id"], HANDLED_IN_GHL, now, run, ref)
        return NEXT
    if calls == RECENTLY_CALLED:
        _later(db, row["row_id"], RECENTLY_CALLED.upper(), now + QUEUE_RECHECK_AFTER, run)
        return NEXT
    if not try_queue_lock(db):
        db.commit()
        return STOP
    at = clock()  # measured after the lock: the spacing and the alarm clock start at the real release time
    if db.execute(_STILL_QUEUED, {"id": row["row_id"]}).first() is None:
        db.commit()
        return NEXT
    if is_globally_halted() or db.execute(_CASCADE_RUNNING, {"spacing_since": at - CASCADE_SPACING}).scalar():
        db.commit()
        return STOP
    db.execute(_CLAIM_RELEASE, {"releasing": CALL_RELEASING, "now": at, "id": row["row_id"]})
    db.commit()
    step = _begin(source, lead, tags, now=at)
    if step.status == CALL_TAGGED:
        db.execute(_RELEASE, {"status": CALL_TAGGED, "tagged_at": step.tagged_at, "voice": step.voice,
                              "releasing": CALL_RELEASING, "id": row["row_id"]})
        db.commit()
        run.released += 1
        logger.info("[speed-to-lead] queued call released lead=%s waited=%.0fs", ref,
                    (at - lead["received_at"]).total_seconds())
        return STOP
    reason = step.block_reason if step.status == CALL_HALTED else GHL_TAG_FAILED
    # QUEUE_RECHECK_AFTER, not the short GHL retry: if GHL did start the workflow, its calls show up meanwhile and the
    # recent-call check keeps the lead from being rung again.
    db.execute(_BACK_IN_QUEUE, {"reason": reason, "recheck_at": at + QUEUE_RECHECK_AFTER,
                                "releasing": CALL_RELEASING, "id": row["row_id"]})
    db.commit()
    run.waiting += 1
    return STOP


def release_queue(db, *, tags: Optional[TagClient], scrubber: Optional[Scrubber], read_calls: Optional[CallReader],
                  notify: Optional[Notifier] = None, contact_url: Optional[ContactUrl] = None,
                  now: Optional[datetime] = None, clock: Optional[Clock] = None,
                  unset: Optional[Unset] = None) -> QueueRun:
    """The minute job (one instance at a time: ``src.tasks.lending_speed_to_lead_queue``). Expires leads queued past
    QUEUE_MAX_AGE and reports interrupted releases; with speed-to-lead switched off, ends every queued lead (the
    alarms and the morning list then treat them as ordinary leads). Inside the callers' hours and while no cascade
    started in the last CASCADE_SPACING, releases at most one queued lead (fresh first, then oldest) after re-checking
    every rule. Posts the once-a-day morning summary and backlog alert. Fails closed: no GHL tag client or call
    reader, the emergency halt, or a failed GHL read releases nothing. ``notify`` None means the lead-alerts channel is
    not configured: each skipped post is reported through ``unset`` instead. Commits as it goes."""
    clock = clock or (_fixed(now) if now is not None else _utcnow)
    start_at = clock()
    run = QueueRun()
    _housekeeping(db, start_at, run)
    if not get_settings().lending_speed_to_lead_enabled:
        run.switched_off = db.execute(_SWITCHED_OFF, {"off": CALL_OFF, "now": start_at}).rowcount
        db.commit()
        if run.switched_off:
            logger.warning("[speed-to-lead] switched off: %d queued lead(s) left the queue", run.switched_off)
        _alert_left_queue(db, notify, contact_url, start_at, unset)
        return run
    _alert_left_queue(db, notify, contact_url, start_at, unset)
    if not callers_hours_open(start_at):
        return run
    _notices(db, notify, start_at, unset)
    if tags is None or read_calls is None:
        logger.warning("[speed-to-lead] queue: GHL is not configured, nothing released")
        return run
    if is_globally_halted():
        return run
    if db.execute(_CASCADE_RUNNING, {"spacing_since": start_at - CASCADE_SPACING}).scalar():
        return run
    candidates = db.execute(_CANDIDATES, {"now": start_at, "fresh_since": start_at - FRESH_LEAD_WINDOW,
                                          "limit": QUEUE_SCAN_LIMIT}).mappings().all()
    db.commit()
    for row in candidates:
        try:
            if _try_release(db, row, tags=tags, scrubber=scrubber, read_calls=read_calls, clock=clock, run=run):
                break
        except Exception as exc:  # class only: SQL errors embed bound params (phones). Skip this lead, try the next.
            db.rollback()
            logger.error("[speed-to-lead] queue release crashed lead=%s:%s: %s", row["source"], row["lead_id"],
                         type(exc).__name__)
            db.execute(_RECHECK_LATER, {"reason": "ERROR", "recheck_at": clock() + QUEUE_RECHECK_AFTER,
                                        "id": row["row_id"]})
            db.commit()
    return run


# ── emergency halt: stop cascades already running in GHL ─────────────────────

_ACTIVE = text("""
    SELECT id, source, lead_id, ghl_contact_id FROM lending.speed_to_lead
     WHERE call_status = :tagged AND call_tagged_at > :since AND ghl_contact_id IS NOT NULL
     ORDER BY call_tagged_at
       FOR UPDATE SKIP LOCKED
""")
_HALTED = text("UPDATE lending.speed_to_lead SET call_status = :halted WHERE id = :id")


def stop_active_cascades(db, tags: TagClient, *, now: Optional[datetime] = None) -> int:
    """Emergency halt: remove the cascade and Voice AI tags from every lead whose cascade started in the last
    ACTIVE_CASCADE_MINUTES. The GHL workflow re-checks ``ndl-speed-to-lead`` before David's call and the Voice AI
    step, so those stop; a phone already ringing cannot be recalled. Returns how many were stopped. Does not
    commit; a lead whose tags could not be removed stays ``tagged`` and is logged."""
    now = now or datetime.now(timezone.utc)
    stopped = 0
    rows = db.execute(_ACTIVE, {"tagged": CALL_TAGGED,
                                "since": now - timedelta(minutes=ACTIVE_CASCADE_MINUTES)}).mappings().all()
    for row in rows:
        try:
            _stop_cascade(row, tags)
        except Exception as exc:  # class only
            logger.error("[speed-to-lead] halt could not stop the cascade lead=%s:%s: %s", row["source"],
                         row["lead_id"], type(exc).__name__)
            continue
        db.execute(_HALTED, {"halted": CALL_HALTED, "id": row["id"]})
        stopped += 1
    logger.warning("[speed-to-lead] emergency halt stopped %d running call cascade(s)", stopped)
    return stopped


def stop_active_cascades_live() -> Optional[int]:
    """The emergency halt's hook: None when speed-to-lead is off or GHL is not configured, else how many running
    cascades were stopped. Never raises."""
    if not get_settings().lending_speed_to_lead_enabled:
        return None
    try:
        tags = live_tag_client()
        if tags is None:
            return None
        from src.lending.db import lending_session

        with lending_session() as db:
            return stop_active_cascades(db, tags)
    except Exception as exc:  # class only: never let this get in the way of the halt
        logger.error("[speed-to-lead] halt hook failed: %s", type(exc).__name__)
        return None


# ── timing report for Josh ────────────────────────────────────────────────────

CallReader = Callable[[str], list[dict]]  # contact id -> GHL call messages (ghl_calls.call_messages_reader)

_REPORT_ROWS = text("""
    SELECT s.*, COALESCE(l.phone, w.phone) AS phone
      FROM lending.speed_to_lead s
      LEFT JOIN lending.lendingflow_leads l ON s.source = 'lendingflow' AND l.id = s.lead_id
      LEFT JOIN lending.web_leads w ON s.source = 'web' AND w.id = s.lead_id
     WHERE (CAST(:source AS text) IS NULL OR (s.source = :source AND s.lead_id = :lead_id))
     ORDER BY s.received_at DESC
     LIMIT :limit
""")
_GHL_CALLS = text("""
    SELECT call_started_at, direction, caller_name, talk_duration_sec, disposition, disposition_raw
      FROM lending.call_dispositions
     WHERE call_source = 'ghl' AND phone = :phone AND call_started_at >= :start AND call_started_at < :end
     ORDER BY call_started_at
""")


def _seconds(start: datetime, at: Optional[datetime]) -> str:
    return f"+{(at - start).total_seconds():.0f}s" if at is not None else "-"


def _target(start: datetime, at: Optional[datetime], target: timedelta) -> str:
    return f" (target {int(target.total_seconds())}s missed)" if at is not None and at - start > target else ""


def _queue_note(start: datetime, row: Mapping[str, Any]) -> str:
    if row.get("queued_at") is None:
        return ""
    return f"(queued {_seconds(start, row['queued_at'])}: {row.get('queue_reason') or '?'}, " \
           f"left the queue {_seconds(start, row.get('released_at'))}) "


def _live_call_lines(read_calls: CallReader, contact_id: str, start_at: datetime, end_at: datetime) -> list[str]:
    from src.lending.ghl_calls import call_duration_seconds, call_state, message_time

    in_window: list[tuple[datetime, dict]] = []
    for message in read_calls(contact_id):
        at = message_time(message)
        if at is not None and start_at <= at < end_at:
            in_window.append((at, message))
    in_window.sort(key=lambda timed: timed[0])
    return [f"    GHL call {_seconds(start_at, at)} {m.get('direction') or '?'} {call_state(m) or '?'} "
            f"talk {call_duration_seconds(m) or 0}s" for at, m in in_window]


def _stored_call_lines(db, phone: str, start_at: datetime, end_at: datetime) -> list[str]:
    return [f"    GHL call {_seconds(start_at, call['call_started_at'])} {call['direction'] or '?'} "
            f"{call['caller_name'] or '?'} talk {call['talk_duration_sec'] or 0}s "
            f"result {call['disposition'] or call['disposition_raw'] or '-'}"
            for call in db.execute(_GHL_CALLS, {"phone": phone, "start": start_at, "end": end_at}).mappings()]


def report_lines(db, *, source: Optional[str] = None, lead_id: Optional[int] = None, limit: int = 1,
                 read_calls: Optional[CallReader] = None) -> list[str]:
    """Readable timings per lead: arrival, intro-text tag, call-cascade tag and the GHL calls in the
    REPORT_CALL_WINDOW_MINUTES after arrival, read live from GHL when ``read_calls`` is given (read-only), else from
    the stored GHL call import. Phones are never printed."""
    lines: list[str] = []
    rows = db.execute(_REPORT_ROWS, {"source": source, "lead_id": lead_id, "limit": limit}).mappings().all()
    for row in rows:
        start_at = row["received_at"]
        end_at = start_at + timedelta(minutes=REPORT_CALL_WINDOW_MINUTES)
        lines.append(
            f"{row['source']}:{row['lead_id']} arrived {start_at:%Y-%m-%d %H:%M:%S %Z} | text {row['text_status']} "
            f"{_seconds(start_at, row['text_tagged_at'])}{_target(start_at, row['text_tagged_at'], TEXT_TARGET)} "
            f"| call cascade {row['call_status']}"
            f"{' (' + row['call_block_reason'] + ')' if row['call_block_reason'] else ''} "
            f"{_seconds(start_at, row['call_tagged_at'])}{_target(start_at, row['call_tagged_at'], CALL_TARGET)} "
            f"{_queue_note(start_at, row)}| Voice AI allowed: {'yes' if row['voice_eligible'] else 'no'}")
        if read_calls is not None and row["ghl_contact_id"]:
            try:
                lines.extend(_live_call_lines(read_calls, row["ghl_contact_id"], start_at, end_at))
                continue
            except Exception as exc:  # class only: fall back to the stored calls
                logger.warning("[speed-to-lead] live GHL call read failed lead=%s:%s (%s); using stored calls",
                               row["source"], row["lead_id"], type(exc).__name__)
        if row["phone"]:
            lines.extend(_stored_call_lines(db, row["phone"], start_at, end_at))
    return lines


def live_call_reader() -> Optional[CallReader]:
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_calls import call_messages_reader

    account = lending_ghl_account()
    return call_messages_reader(account) if account is not None else None


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="P2.1 speed-to-lead timings")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--report", metavar="SOURCE:LEAD_ID", help="one lead, e.g. web:123 or lendingflow:45")
    group.add_argument("--recent", type=int, metavar="N", help="the last N leads")
    args = parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    from src.lending.db import lending_session

    source, lead_id, limit = None, None, args.recent or 1
    if args.report:
        source, _, raw_id = args.report.partition(":")
        if source not in SOURCES or not raw_id.isdigit():
            parser.error("--report takes SOURCE:LEAD_ID with SOURCE lendingflow or web")
        lead_id = int(raw_id)
    with lending_session() as db:
        lines = report_lines(db, source=source, lead_id=lead_id, limit=limit, read_calls=live_call_reader())
    for line in lines or ["No speed-to-lead rows."]:
        logger.info("%s", line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
