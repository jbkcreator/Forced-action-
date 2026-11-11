"""Decide what happens to one agent job before any draft exists: block it (with a receipt status), hold it (until
``not_before`` or the recipient's 8am-8pm window), or let it through on a rail (brief sections 6 and 7).

The rail: ``send_email`` to a contact tagged ``ndl-cold`` in GHL goes through NDL's Instantly (cold email, with a
CAN-SPAM footer); every other job goes through GHL (decision D15). The order of the checks is fixed: the first
failure decides. The cold-email cap counts Cora's cold emails to the address, sent or still awaiting a decision, so
two jobs can never both pass the cap while the first one waits for Josh.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Literal, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_agent_jobs import (
    COLD_EMAIL_CHANNEL,
    COLD_EMAIL_MAX_IN_WINDOW,
    COLD_EMAIL_MIN_GAP,
    COLD_EMAIL_WINDOW,
    HOURS_RECHECK,
    MESSAGE_JOB_TYPES,
)
from config.lending_compliance import CALL_WINDOW_END, CALL_WINDOW_START, DEFAULT_TZ, GHL_TAG_COLD_LEAD
from src.lending.agent_jobs.job import AgentJob
from src.lending.compliance import text_window_open
from src.lending.consent import has_text_consent
from src.lending.cora_tools import borrower_draft_problem
from src.lending.ghl_crm import GhlContact
from src.lending.lane_guard import backflip_hit

Rail = Literal["ghl", "instantly"]

_SUPPRESSED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE (phone = :p AND :p IS NOT NULL)
                                                              OR (email = :e AND :e IS NOT NULL))
        OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND :p IS NOT NULL AND do_not_contact)
        OR EXISTS (SELECT 1 FROM email_opt_outs WHERE email = :e AND :e IS NOT NULL)
""")
_COLD_EMAILS = text("""
    SELECT count(*) FILTER (WHERE COALESCE(executed_at, created_at) > :gap_since) AS recent,
           count(*) FILTER (WHERE COALESCE(executed_at, created_at) > :window_since) AS in_window
      FROM lending.pending_actions
     WHERE channel = :channel AND recipient_email = :e
       AND status IN ('pending', 'revising', 'approved', 'sending', 'sent')
""")


@dataclass(frozen=True)
class Verdict:
    outcome: Literal["go", "hold", "block"]
    status: Optional[str] = None
    reason: Optional[str] = None
    release_at: Optional[datetime] = None
    held_reason: Optional[Literal["hours", "not_before"]] = None
    rail: Optional[Rail] = None


def _block(status: str, reason: str) -> Verdict:
    return Verdict("block", status=status, reason=reason)


def rail_for(job: AgentJob, contact: GhlContact) -> Rail:
    return "instantly" if job.type == "send_email" and GHL_TAG_COLD_LEAD in contact.tags else "ghl"


def _eastern_window_open(now: datetime) -> bool:
    return CALL_WINDOW_START <= now.astimezone(ZoneInfo(DEFAULT_TZ)).time() < CALL_WINDOW_END


def _consent_problem(db, job: AgentJob, contact: GhlContact, rail: Rail) -> Optional[Verdict]:
    if db.execute(_SUPPRESSED, {"p": contact.phone, "e": contact.email}).scalar():
        return _block("blocked_consent", "the contact is opted out, suppressed or marked do-not-contact")
    if job.type == "send_sms":
        if not contact.phone:
            return _block("error", "the GHL contact has no phone number")
        if "SMS" in contact.dnd_channels:
            return _block("blocked_consent", "do-not-disturb is on for texts in GHL")
        if not has_text_consent(db, contact.phone):
            return _block("blocked_consent", "no text consent on record for this contact")
        return None
    if not contact.email:
        return _block("error", "the GHL contact has no email address")
    # Cold contacts carry GHL email do-not-disturb by design (the cold push sets it); their opt-outs are the
    # suppression tables above, so the GHL flag only gates warm (GHL) email.
    if rail == "ghl" and "Email" in contact.dnd_channels:
        return _block("blocked_consent", "do-not-disturb is on for email in GHL")
    return None


def _over_cold_cap(db, email: str, now: datetime) -> bool:
    row = db.execute(_COLD_EMAILS, {"channel": COLD_EMAIL_CHANNEL, "e": email, "gap_since": now - COLD_EMAIL_MIN_GAP,
                                    "window_since": now - COLD_EMAIL_WINDOW}).one()
    return bool(row.recent) or row.in_window >= COLD_EMAIL_MAX_IN_WINDOW


def check_job(db, job: AgentJob, contact: Optional[GhlContact], now: datetime, *,
              text_window: Callable[..., bool] = text_window_open, instantly_ready: bool = True) -> Verdict:
    if contact is None:
        return _block("error", "contact not found in NDL GHL")
    if job.lane != "NDL":
        return _block("blocked_lane", "the job is not in the NDL lane")
    if backflip_hit(job.body, job.subject, job.contact_label, job.reason, contact.phone, contact.email):
        return _block("blocked_lane", "the job mentions a Backflip name, number or domain")
    if job.expires_at <= now:
        return _block("expired", "the job expired before Cora could act on it")
    rail = rail_for(job, contact)
    is_message = job.type in MESSAGE_JOB_TYPES
    if is_message:
        if problem := _consent_problem(db, job, contact, rail):
            return problem
        if rail == "instantly" and not instantly_ready:
            return _block("error", "cold email rail not configured (Instantly key, campaign or footer address)")
        fields = {"body": job.body, **({"subject": job.subject} if job.subject else {})}
        if problem := borrower_draft_problem(fields):
            return _block("error", problem)
        if rail == "instantly" and _over_cold_cap(db, contact.email, now):
            return _block("blocked_cap", "cold email cap: at most once every 3 days and 4 times in 14 days")
    if job.not_before is not None and job.not_before > now:
        return Verdict("hold", release_at=job.not_before, held_reason="not_before", rail=rail)
    if is_message:
        window_open = text_window(contact.phone, now=now) if contact.phone else _eastern_window_open(now)
        if not window_open:
            return Verdict("hold", release_at=now + HOURS_RECHECK, held_reason="hours", rail=rail)
    return Verdict("go", rail=rail)
