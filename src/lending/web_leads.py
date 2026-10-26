"""nextdeallending.com lead form (WP-GL-11): save the submission, then deliver it to GoHighLevel.

Order matters: the row (with its consent evidence) is committed before GHL is called, so a GHL
outage never loses a lead. Delivery is one idempotent function used by both the request's
background task and the retry sweep. Nothing here sends anything to the visitor.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_web import (
    DEDUP_WINDOW_MINUTES,
    FIELD_MAX_LENGTHS,
    GHL_GIVE_UP_AFTER_HOURS,
    GHL_RETRY_BACKOFF_MINUTES,
    CALL_CONSENT_TEXT,
    MAX_LOAN_AMOUNT,
    SMS_CONSENT_TEXT,
    SWEEP_BATCH_SIZE,
)
from src.lending.consent import record_consent
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_YES = {"yes", "true", "on", "1"}
# "$250,000", "250000", "250k", "$1.5M": whole dollars, with an optional k or m suffix.
_LOAN_AMOUNT = re.compile(r"^\$?\s*(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?\s*([km])?$", re.IGNORECASE)
_AMOUNT_MULTIPLIER = {"k": 1_000, "m": 1_000_000}


class InvalidWebLead(ValueError):
    """The submission cannot become a lead (message is safe to show the visitor)."""


@dataclass(frozen=True)
class WebLeadInput:
    name: str
    phone: str
    email: Optional[str]
    property_city: Optional[str]
    deal_type: Optional[str]
    completed_projects_3y: Optional[str]
    loan_amount: Optional[int]  # whole dollars
    property_address: Optional[str]
    property_state: Optional[str]
    credit_band: Optional[str]
    sms_consent: bool  # box 1: text messages only
    call_consent: bool  # box 2: calls only
    deal_drop_optin: bool  # box 3: the weekly Deal Drop by email
    consent_text: Optional[str]  # the text box's label
    call_consent_text: Optional[str]
    page_url: Optional[str]
    ip_address: Optional[str]
    user_agent: Optional[str]


@dataclass(frozen=True)
class PushResult:
    contact_id: str
    pipeline_card: bool  # False when no new-lead stage is configured: contact only
    intro_ready_tagged: bool = False  # T-09: the push added the tag that starts the GHL intro-SMS workflow


class LeadSink(Protocol):
    """Where a stored lead is delivered. Live: GoHighLevel. Tests: a recording fake."""

    def push(self, lead: Mapping[str, Any]) -> PushResult: ...


class DeliveryError(Exception):
    """Delivery failed; the message is a short, PII-free reason for ``ghl_last_error``.

    ``config_error`` marks a rejected key or setup (HTTP 401/403): the lead is fine, the integration
    is not, so the failure does not use up one of the lead's attempts."""

    def __init__(self, message: str, *, config_error: bool = False) -> None:
        super().__init__(message)
        self.config_error = config_error


def _clean(value: Optional[str], field: str) -> Optional[str]:
    if value is None:
        return None
    cleaned = _CONTROL.sub(" ", str(value)).strip()
    return cleaned[: FIELD_MAX_LENGTHS[field]] or None


def _ticked(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in _YES


def _loan_amount(value: Optional[str]) -> Optional[int]:
    """Whole dollars from what the visitor typed. Blank is no answer; text that is not an amount, zero or an
    absurd figure is a typo the visitor can fix, so it is refused rather than stored as nothing."""
    cleaned = _clean(value, "loan_amount")
    if not cleaned:
        return None
    match = _LOAN_AMOUNT.match(cleaned)
    amount = 0
    if match:
        digits, fraction, suffix = match.groups()
        amount = int(float(digits.replace(",", "") + (fraction or "")) * _AMOUNT_MULTIPLIER.get((suffix or "").lower(), 1))
    if not 0 < amount <= MAX_LOAN_AMOUNT:
        raise InvalidWebLead("Please check the loan amount.")
    return amount


def _state(value: Optional[str]) -> Optional[str]:
    """The USPS code when the visitor typed or picked a recognisable state, else what they sent."""
    from src.lending.lendingflow import state_code  # lendingflow imports this module: import at call time

    cleaned = _clean(value, "property_state")
    return (state_code(cleaned) or cleaned) if cleaned else None


def build_input(form: Mapping[str, Optional[str]], *, ip_address: Optional[str], user_agent: Optional[str]) -> WebLeadInput:
    name = _clean(form.get("name"), "name")
    if not name:
        raise InvalidWebLead("Please add your name.")
    phone = normalize(form.get("phone"))
    if not phone:
        raise InvalidWebLead("Please add a valid mobile number.")
    email = _clean(form.get("email"), "email")
    if email:
        email = email.lower()
        if not _EMAIL.match(email):
            raise InvalidWebLead("Please check your email address.")
    deal_drop_optin = _ticked(form.get("deal_drop_optin"))
    if deal_drop_optin and not email:
        raise InvalidWebLead("Please add your email address to get the weekly Deal Drop.")
    return WebLeadInput(
        name=name,
        phone=phone,
        email=email,
        property_city=_clean(form.get("property_city"), "property_city"),
        deal_type=_clean(form.get("deal_type"), "deal_type"),
        completed_projects_3y=_clean(form.get("completed_projects_3y"), "completed_projects_3y"),
        loan_amount=_loan_amount(form.get("loan_amount")),
        property_address=_clean(form.get("property_address"), "property_address"),
        property_state=_state(form.get("property_state")),
        credit_band=_clean(form.get("credit_band"), "credit_band"),
        sms_consent=_ticked(form.get("sms_consent")),
        call_consent=_ticked(form.get("call_consent")),
        deal_drop_optin=deal_drop_optin,
        consent_text=_clean(form.get("consent_text"), "consent_text"),
        call_consent_text=_clean(form.get("call_consent_text"), "call_consent_text"),
        page_url=_clean(form.get("page_url"), "page_url"),
        ip_address=(str(ip_address).strip()[:45] or None) if ip_address else None,
        user_agent=_clean(user_agent, "user_agent"),
    )


def _suppression_reason(db, phone: str, email: Optional[str]) -> Optional[str]:
    """Same exclusions has_text_consent applies, so the lead's stored flag and the consent gate agree.
    Returns which gate fired (the opt-out list wins when both do), or None when the contact is clear."""
    row = db.execute(
        text("SELECT EXISTS (SELECT 1 FROM lending.suppression_list "
             "WHERE phone = :p OR (CAST(:e AS text) IS NOT NULL AND email = CAST(:e AS text))) AS on_list, "
             "EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND do_not_contact) AS dnc"),
        {"p": phone, "e": email},
    ).one()
    reason = "suppression_list" if row.on_list else "do_not_contact" if row.dnc else None
    if reason:
        logger.info("[lending-web] submission suppressed gate=%s", reason)
    return reason


def _recent_duplicate(db, data: WebLeadInput) -> Optional[int]:
    return db.execute(
        text("SELECT id FROM lending.web_leads WHERE phone = :p AND sms_consent = :c AND call_consent = :k "
             "AND deal_drop_optin = :d AND received_at > now() - make_interval(mins => :m) ORDER BY id DESC LIMIT 1"),
        {"p": data.phone, "c": data.sms_consent, "k": data.call_consent, "d": data.deal_drop_optin,
         "m": DEDUP_WINDOW_MINUTES},
    ).scalar()


def save_web_lead(db, data: WebLeadInput) -> tuple[int, bool]:
    """Insert the lead and, when the SMS box was ticked and the contact is not suppressed, the
    text-consent row. Returns (lead_id, created); a repeat inside the dedup window is not created
    again. The caller commits; until then a per-phone advisory lock makes simultaneous identical
    submissions queue up, so the second one sees the first and returns it instead of inserting."""
    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"lending_web_lead:{data.phone}"})
    existing = _recent_duplicate(db, data)
    if existing is not None:
        return int(existing), False
    reason = _suppression_reason(db, data.phone, data.email)
    suppressed = reason is not None
    lead_id = db.execute(
        text("INSERT INTO lending.web_leads (name, phone, email, property_city, deal_type, completed_projects_3y, "
             "loan_amount, property_address, property_state, credit_band, "
             "sms_consent, call_consent, deal_drop_optin, consent_text, consent_text_matches, "
             "call_consent_text, call_consent_text_matches, page_url, ip_address, user_agent, "
             "suppressed, suppression_reason) "
             "VALUES (:name, :phone, :email, :city, :deal, :projects, :amount, :address, :state, :band, "
             ":sms, :call, :drop, :ctext, :cmatch, :ktext, :kmatch, :url, :ip, :ua, :sup, :reason) "
             "RETURNING id"),
        {
            "name": data.name, "phone": data.phone, "email": data.email, "city": data.property_city,
            "deal": data.deal_type, "projects": data.completed_projects_3y,
            "amount": data.loan_amount, "address": data.property_address, "state": data.property_state,
            "band": data.credit_band,
            "sms": data.sms_consent, "call": data.call_consent, "drop": data.deal_drop_optin,
            "ctext": data.consent_text,
            "cmatch": None if data.consent_text is None else data.consent_text == SMS_CONSENT_TEXT,
            "ktext": data.call_consent_text,
            "kmatch": None if data.call_consent_text is None else data.call_consent_text == CALL_CONSENT_TEXT,
            "url": data.page_url, "ip": data.ip_address, "ua": data.user_agent, "sup": suppressed,
            "reason": reason,
        },
    ).scalar_one()
    if data.sms_consent and not suppressed:
        record_consent(db, data.phone, "web_form")
    logger.info("[lending-web] lead saved id=%s sms_consent=%s call_consent=%s deal_drop=%s suppressed=%s",
                lead_id, data.sms_consent, data.call_consent, data.deal_drop_optin, suppressed)
    return int(lead_id), True


def will_be_contacted(db, lead_id: int) -> bool:
    """True when the visitor ticked the text box or the call box and the number is not suppressed, i.e. the
    team may follow up by that channel. The form page shows its "we will call you back" line only for this case."""
    return bool(db.execute(
        text("SELECT (sms_consent OR call_consent) AND NOT suppressed FROM lending.web_leads WHERE id = :id"),
        {"id": lead_id},
    ).scalar())


def merge_into_existing(db, lead_id: int, data: WebLeadInput) -> bool:
    """A repeat submission inside the dedup window fills in details the stored lead lacks (an email
    added on the second try) and never overwrites one it has. Returns True when something was
    added; the lead is then queued for delivery again so GoHighLevel gets the new detail. Consent
    values are never touched: they stay exactly what the first submission recorded."""
    changed = db.execute(
        text("UPDATE lending.web_leads SET "
             "email = COALESCE(email, CAST(:email AS text)), "
             "property_city = COALESCE(property_city, CAST(:city AS text)), "
             "deal_type = COALESCE(deal_type, CAST(:deal AS text)), "
             "completed_projects_3y = COALESCE(completed_projects_3y, CAST(:projects AS text)), "
             "loan_amount = COALESCE(loan_amount, CAST(:amount AS bigint)), "
             "property_address = COALESCE(property_address, CAST(:address AS text)), "
             "property_state = COALESCE(property_state, CAST(:state AS text)), "
             "credit_band = COALESCE(credit_band, CAST(:band AS text)), "
             "ghl_status = 'pending', ghl_attempts = 0, ghl_last_error = NULL "
             "WHERE id = :id AND ("
             "(email IS NULL AND CAST(:email AS text) IS NOT NULL) OR "
             "(property_city IS NULL AND CAST(:city AS text) IS NOT NULL) OR "
             "(deal_type IS NULL AND CAST(:deal AS text) IS NOT NULL) OR "
             "(completed_projects_3y IS NULL AND CAST(:projects AS text) IS NOT NULL) OR "
             "(loan_amount IS NULL AND CAST(:amount AS bigint) IS NOT NULL) OR "
             "(property_address IS NULL AND CAST(:address AS text) IS NOT NULL) OR "
             "(property_state IS NULL AND CAST(:state AS text) IS NOT NULL) OR "
             "(credit_band IS NULL AND CAST(:band AS text) IS NOT NULL)) "
             "RETURNING id"),
        {"id": lead_id, "email": data.email, "city": data.property_city,
         "deal": data.deal_type, "projects": data.completed_projects_3y, "amount": data.loan_amount,
         "address": data.property_address, "state": data.property_state, "band": data.credit_band},
    ).first()
    if changed is not None:
        logger.info("[lending-web] repeat submission added details to lead id=%s", lead_id)
    return changed is not None


def _backoff_minutes_sql() -> str:
    """Minutes to wait after N failures, from config (ints only, never user input): 0 failures -> no
    wait, then the configured schedule, its last value repeating."""
    steps = GHL_RETRY_BACKOFF_MINUTES
    whens = " ".join(f"WHEN {n} THEN {int(minutes)}" for n, minutes in enumerate(steps[:-1], start=1))
    return f"CASE ghl_attempts WHEN 0 THEN 0 {whens} ELSE {int(steps[-1])} END"


_DELIVERABLE = (
    "SELECT id, name, phone, email, property_city, deal_type, completed_projects_3y, loan_amount, "
    "property_address, property_state, credit_band, sms_consent, call_consent, "
    "deal_drop_optin, suppressed, received_at, ghl_attempts FROM lending.web_leads "
    "WHERE ghl_status IN ('pending', 'failed') AND received_at > :oldest {extra} "
    "ORDER BY received_at LIMIT :limit FOR UPDATE SKIP LOCKED"
)


def deliver_pending(db, sink: Optional[LeadSink], *, lead_id: Optional[int] = None, now: Optional[datetime] = None) -> int:
    """Deliver stored leads to the sink; returns how many reached it. One lead (``lead_id``, used
    right after submit) or the retry backlog. The backlog is every lead not yet in GHL that is younger
    than GHL_GIVE_UP_AFTER_HOURS and past its backoff wait, so a long outage is retried for days, not
    minutes. With no sink (GHL not configured) leads stay pending and no attempt is counted, so
    configuring GHL later drains them. Never raises per lead."""
    now = now or datetime.now(timezone.utc)
    if sink is None:
        logger.warning("[lending-web] GHL is not configured: web leads stay pending")
        return 0
    params: dict[str, Any] = {"limit": SWEEP_BATCH_SIZE, "oldest": now - timedelta(hours=GHL_GIVE_UP_AFTER_HOURS)}
    if lead_id is not None:
        extra = "AND id = :lead_id"
        params["lead_id"] = lead_id
    else:
        extra = (f"AND (ghl_last_attempt_at IS NULL OR "
                 f"ghl_last_attempt_at < CAST(:now AS timestamptz) - make_interval(mins => {_backoff_minutes_sql()}))")
        params["now"] = now
    rows = db.execute(text(_DELIVERABLE.format(extra=extra)), params).mappings().all()
    delivered = 0
    for row in rows:
        delivered += _deliver_one(db, sink, dict(row), now)
    return delivered


def _deliver_one(db, sink: LeadSink, lead: dict[str, Any], now: datetime) -> int:
    try:
        result = sink.push(lead)
    except DeliveryError as exc:
        _record_failure(db, lead, str(exc)[:200], now, config_error=exc.config_error)
        return 0
    except Exception as exc:
        _record_failure(db, lead, f"unexpected {type(exc).__name__}", now)
        return 0
    db.execute(
        text("UPDATE lending.web_leads SET ghl_status = :s, ghl_contact_id = :c, ghl_attempts = ghl_attempts + 1, "
             "ghl_last_attempt_at = :now, ghl_synced_at = :now, ghl_last_error = NULL WHERE id = :id"),
        {"s": "synced" if result.pipeline_card else "contact_only", "c": result.contact_id, "now": now, "id": lead["id"]},
    )
    logger.info("[lending-web] lead delivered id=%s pipeline_card=%s", lead["id"], result.pipeline_card)
    return 1


def _record_failure(db, lead: dict[str, Any], reason: str, now: datetime, *, config_error: bool = False) -> None:
    """A rejected key or setup keeps the attempt count where it was: the lead is not at fault, and it
    is retried on every sweep until the integration is fixed."""
    attempts = int(lead["ghl_attempts"]) + (0 if config_error else 1)
    db.execute(
        text("UPDATE lending.web_leads SET ghl_status = 'failed', ghl_attempts = :a, ghl_last_attempt_at = :now, "
             "ghl_last_error = :err WHERE id = :id"),
        {"a": attempts, "now": now, "err": reason, "id": lead["id"]},
    )
    logger.log(logging.ERROR if config_error else logging.WARNING,
               "[lending-web] GHL delivery failed id=%s failures=%d config_error=%s reason=%s",
               lead["id"], attempts, config_error, reason)
