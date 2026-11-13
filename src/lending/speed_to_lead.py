"""P2.1 Speed-to-Lead: an instant text, then Josh's phone (David's if he misses it), then Voice AI at 2 minutes.

Our code decides; GoHighLevel texts and dials. Right after a new lead reaches GHL (LendingFlow: the delivery
follow-up; website: the delivery itself) ``start`` runs every gate and adds the tags that start the GHL workflows
(``docs/lending/speed-to-lead.md``):

* **Text.** LendingFlow leads already get T-09's intro text at the push (``lendingflow_ghl``); the time it was tagged
  is copied here for the report. A website lead gets ``web-intro-ready`` when the text box was ticked and the number
  still has text consent (``consent.has_text_consent``: consent row, no opt-out), the switch
  ``LENDING_INTRO_SMS_ENABLED`` is on and it is 8am-8pm for the borrower. Outside the window the text is held and the
  web-lead sweep releases it (``release_held_web_intro``), up to TEXT_LAST_CHANCE after the form.
* **Targets and last chances** (``config/lending_speed_to_lead.py``). Each step has the client's target (a miss is
  logged and shown on the timing report) and a last chance, after which the step is skipped: text 5 s / 24 h, Josh
  and David 60 s / 5 min, Voice AI 2 min / 5 min. A lead too late for calls gets a "late lead, call now" post in the
  lead-alerts channel and goes to the normal call queue.
* **Call cascade** (``LENDING_SPEED_TO_LEAD_ENABLED``). Only for a lead that reached GHL within CALL_LAST_CHANCE
  and that may be called now under the same rules as Josh's GHL call lists (``ghl_call_gate.blocked_reasons``:
  suppression, DNC, already reached, attempt caps, the 8am-8pm calling window, emergency halt). A website lead also
  needs the call-consent box and a fresh DNC scrub (one Tracerfy credit, run here; LendingFlow leads are exempt,
  client decision). Then ``ndl-ai-voice-ok`` is set or cleared, and ``ndl-speed-to-lead`` is removed and re-added so
  GHL's tag trigger fires again for a returning contact.
* **Voice AI** (brief section 7: only on consented leads, discloses it is an AI assistant for Next Deal Lending). Our
  code only marks who may get it: a website lead whose call-consent box was ticked with the exact consent text, and
  only when the Josh/David step ran early enough that the AI call starts inside AI_LAST_CHANCE. A LendingFlow lead has
  no recorded call consent, so it never gets the tag (open question for Josh).

The lending emergency halt withholds every tag. Nothing here ever fails a delivery: errors are logged by lead id and
the lead still reaches BatchDialer as usual (no hold: team decision, Nov 13). Logs carry lead ids only, never a phone number.

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

from config.lending_speed_to_lead import (
    ACTIVE_CASCADE_MINUTES,
    CALL_BLOCKED,
    CALL_FAILED,
    CALL_HALTED,
    CALL_OFF,
    CALL_STALE,
    CALL_TAGGED,
    AI_LAST_CHANCE,
    AI_VOICE_AFTER_SECONDS,
    CALL_LAST_CHANCE,
    CALL_TARGET,
    GHL_TAG_AI_VOICE_OK,
    GHL_TAG_SPEED_TO_LEAD,
    GHL_TAG_WEB_INTRO_READY,
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


_LEAD = {
    SOURCE_LENDINGFLOW: text(
        "SELECT id, phone, email, property_state, property_zip, received_at, ghl_contact_id, suppressed, "
        "intro_sms_tagged_at, false AS sms_consent, false AS call_consent, false AS call_consent_text_matches "
        "FROM lending.lendingflow_leads WHERE id = :id"),
    SOURCE_WEB: text(
        "SELECT id, phone, email, property_state, NULL AS property_zip, received_at, ghl_contact_id, suppressed, "
        "NULL AS intro_sms_tagged_at, sms_consent, call_consent, "
        "COALESCE(call_consent_text_matches, false) AS call_consent_text_matches "
        "FROM lending.web_leads WHERE id = :id"),
}
_CLAIM = text("""
    INSERT INTO lending.speed_to_lead (source, lead_id, ghl_contact_id, received_at, started_at)
    VALUES (:source, :lead_id, :contact_id, :received_at, :now)
    ON CONFLICT (source, lead_id) DO NOTHING
    RETURNING id
""")
_RECORD = text("""
    UPDATE lending.speed_to_lead
       SET text_status = :text_status, text_tagged_at = :text_tagged_at, call_status = :call_status,
           call_block_reason = :call_block_reason, call_tagged_at = :call_tagged_at, voice_eligible = :voice
     WHERE id = :id
""")


def intro_sms_on() -> bool:
    """LENDING_INTRO_SMS_ENABLED, the greeting field the workflow reads (as for LendingFlow's T-09), and the carrier
    (10DLC) approval LENDING_SMS_CARRIER_APPROVED that every lending text waits for (project rule: no SMS before
    carrier approval is confirmed)."""
    settings = get_settings()
    return bool(settings.lending_intro_sms_enabled and settings.lending_ghl_cf_intro_name
                and settings.lending_sms_carrier_approved)


def voice_in_time(received_at: datetime, now: datetime) -> bool:
    """The Voice AI step starts AI_VOICE_AFTER_SECONDS after the cascade; it only makes sense inside AI_LAST_CHANCE."""
    return now - received_at + timedelta(seconds=AI_VOICE_AFTER_SECONDS) <= AI_LAST_CHANCE


def voice_eligible(lead: Mapping[str, Any]) -> bool:
    """A recorded, unrevoked call consent: the website call box ticked with the exact consent text, on a number that
    is not suppressed. LendingFlow leads have no call-consent record, so never."""
    return bool(lead["call_consent"] and lead["call_consent_text_matches"] and not lead["suppressed"])


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


def _call(db, source: str, lead: Mapping[str, Any], tags: TagClient, *, halted: bool, now: datetime,
          scrubber: Optional[Scrubber]) -> tuple[str, Optional[str], Optional[datetime], bool]:
    if not get_settings().lending_speed_to_lead_enabled:
        return CALL_OFF, None, None, False
    if now - lead["received_at"] > CALL_LAST_CHANCE:
        return CALL_STALE, None, None, False
    reason = _call_block_reason(db, source, lead, halted=halted, now=now, scrubber=scrubber)
    if reason:
        logger.info("[speed-to-lead] no call cascade lead=%s:%s reason=%s", source, lead["id"], reason)
        return CALL_BLOCKED, reason, None, False
    voice = voice_eligible(lead) and voice_in_time(lead["received_at"], now)
    try:
        _start_cascade(lead, tags, voice=voice)
    except Exception as exc:  # class only
        logger.error("[speed-to-lead] GHL tag failed, no call cascade lead=%s:%s: %s", source, lead["id"],
                     type(exc).__name__)
        return CALL_FAILED, type(exc).__name__[:60], None, voice
    if is_globally_halted():
        # The halt landed after the check above but before this row is committed, so its own cleanup
        # (stop_active_cascades) cannot see this lead: undo the tags here.
        try:
            _stop_cascade(lead, tags)
        except Exception as exc:  # class only
            logger.error("[speed-to-lead] could not undo the cascade after the halt lead=%s:%s: %s", source,
                         lead["id"], type(exc).__name__)
            return CALL_TAGGED, None, now, voice  # left tagged, so the halt's cleanup or a re-run sees it
        logger.warning("[speed-to-lead] emergency halt set while starting, cascade undone lead=%s:%s", source,
                       lead["id"])
        return CALL_HALTED, EMERGENCY_STOP, None, voice
    logger.info("[speed-to-lead] call cascade started lead=%s:%s voice_ai=%s", source, lead["id"], voice)
    return CALL_TAGGED, None, now, voice


def _stop_cascade(lead: Mapping[str, Any], tags: TagClient) -> None:
    tags.remove_tag(lead["ghl_contact_id"], GHL_TAG_AI_VOICE_OK)
    tags.remove_tag(lead["ghl_contact_id"], GHL_TAG_SPEED_TO_LEAD)


Notifier = Callable[[str], bool]  # posts to the lead-alerts channel (ops_alerts.post_ops_alert)


def late_lead_text(source: str, lead: Mapping[str, Any], now: datetime, contact_url: Optional[str]) -> str:
    minutes = int((now - lead["received_at"]).total_seconds() // 60)
    link = f"<{contact_url}|Open in GHL to call>" if contact_url else "Open it in GHL to call."
    return (f":hourglass_flowing_sand: *Late lead, call now:* {SOURCE_LABELS[source]} lead {lead['id']} reached GHL "
            f"{minutes} min after the form, so no instant call was made (last chance is "
            f"{int(CALL_LAST_CHANCE.total_seconds() // 60)} min). {link}")


def _log_target_misses(source: str, lead: Mapping[str, Any], text_at: Optional[datetime],
                       call_at: Optional[datetime]) -> None:
    for step, at, target in (("text", text_at, TEXT_TARGET), ("call", call_at, CALL_TARGET)):
        if at is not None and at - lead["received_at"] > target:
            logger.warning("[speed-to-lead] %s missed its %ss target lead=%s:%s (+%.0fs)", step,
                           int(target.total_seconds()), source, lead["id"], (at - lead["received_at"]).total_seconds())


def _post_late_lead(notify: Notifier, contact_url: Optional[Callable[[str], Optional[str]]], source: str,
                    lead: Mapping[str, Any], now: datetime) -> None:
    """Best effort and never raises: a failed post must not roll back this lead's record (the next sweep would then
    treat the lead as new and post it again)."""
    try:
        url = contact_url(lead["ghl_contact_id"]) if contact_url is not None else None
        posted = notify(late_lead_text(source, lead, now, url))
    except Exception as exc:  # class only
        logger.error("[speed-to-lead] late-lead post failed lead=%s:%s: %s", source, lead["id"], type(exc).__name__)
        return
    if not posted:
        logger.error("[speed-to-lead] late-lead post not sent lead=%s:%s", source, lead["id"])


def start(db, source: str, lead_id: int, *, tags: TagClient, scrubber: Optional[Scrubber] = None,
          notify: Optional[Notifier] = None, contact_url: Optional[Callable[[str], Optional[str]]] = None,
          now: Optional[datetime] = None) -> Optional[Outcome]:
    """Run the speed-to-lead step once for a lead that has just reached GHL. Returns None when the lead is unknown,
    has no GHL contact yet, or was already handled (a retried delivery). A lead past the call last chance is posted
    once through ``notify`` as a late lead. Does not commit."""
    if source not in SOURCES:
        raise ValueError(f"unknown speed-to-lead source {source!r}")
    now = now or datetime.now(timezone.utc)
    lead = db.execute(_LEAD[source], {"id": lead_id}).mappings().first()
    if lead is None or not lead["ghl_contact_id"]:
        return None
    row_id = db.execute(_CLAIM, {"source": source, "lead_id": lead_id, "contact_id": lead["ghl_contact_id"],
                                 "received_at": lead["received_at"], "now": now}).scalar()
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
    call_status, reason, call_at, voice = _call(db, source, lead, tags, halted=halted, now=now, scrubber=scrubber)
    db.execute(_RECORD, {"id": row_id, "text_status": text_status, "text_tagged_at": text_at, "call_status": call_status,
                         "call_block_reason": reason, "call_tagged_at": call_at, "voice": voice})
    _log_target_misses(source, lead, text_at, call_at)
    if call_status == CALL_STALE and notify is not None:
        _post_late_lead(notify, contact_url, source, lead, now)
    return Outcome(source, lead_id, text_status, call_status, reason, voice)


def start_safely(db, source: str, lead_id: int, *, tags: Optional[TagClient], scrubber: Optional[Scrubber] = None,
                 notify: Optional[Notifier] = None,
                 contact_url: Optional[Callable[[str], Optional[str]]] = None) -> Optional[Outcome]:
    """``start`` inside a savepoint; never raises, so the lead's delivery is never affected."""
    if tags is None:
        return None
    try:
        with db.begin_nested():
            return start(db, source, lead_id, tags=tags, scrubber=scrubber, notify=notify, contact_url=contact_url)
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
    from src.lending.lead_arrival_alert import ghl_contact_url
    from src.lending.ops_alerts import post_ops_alert

    def contact_url(contact_id: str) -> Optional[str]:
        return ghl_contact_url(settings.lending_ghl_app_url, settings.lending_ghl_location_id, contact_id)

    def step(db: Any, lead_id: int) -> None:
        start_safely(db, source, lead_id, tags=tags, scrubber=scrubber,
                     notify=post_ops_alert if settings.lending_speed_to_lead_enabled else None, contact_url=contact_url)

    return step


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
            f"| Voice AI allowed: {'yes' if row['voice_eligible'] else 'no'}")
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


def _live_call_reader() -> Optional[CallReader]:
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
        lines = report_lines(db, source=source, lead_id=lead_id, limit=limit, read_calls=_live_call_reader())
    for line in lines or ["No speed-to-lead rows."]:
        logger.info("%s", line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
