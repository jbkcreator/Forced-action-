"""T-11 LendingFlow intake: parse, dedupe, store, deliver to GoHighLevel, then follow up.

Order matters, as for web leads: the lead and its consent certificate are committed before GHL is
called, so a GHL outage never loses a paid lead. ``parse_lendingflow`` is the only function that
knows LendingFlow's payload shape. After the GHL contact exists the lead's
``LendingFlowLeadCreated`` event is emitted once and the T-07 pre-qual letter is queued.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_lendingflow import (
    GHL_GIVE_UP_AFTER_HOURS,
    GHL_RETRY_BACKOFF_MINUTES,
    LEAD_SOURCE,
    SWEEP_BATCH_SIZE,
)
from src.lending.contracts import LoanType
from src.lending.web_leads import DeliveryError, PushResult, _suppression_reason
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_BAND_RANGE = re.compile(r"^\s*(\d{3})\s*(?:-|to|–)\s*\d{3}\s*$", re.IGNORECASE)
_BAND_PLUS = re.compile(r"^\s*(\d{3})\s*\+\s*$")

_LOAN_TYPES = {
    "fix_and_flip": LoanType.FIX_AND_FLIP, "flip": LoanType.FIX_AND_FLIP,
    "ground_up": LoanType.GROUND_UP_CONSTRUCTION, "construction": LoanType.GROUND_UP_CONSTRUCTION,
    "ground_up_construction": LoanType.GROUND_UP_CONSTRUCTION, "new_construction": LoanType.GROUND_UP_CONSTRUCTION,
    "dscr": LoanType.DSCR_RENTAL, "rental": LoanType.DSCR_RENTAL, "dscr_rental": LoanType.DSCR_RENTAL,
    "bridge": LoanType.BRIDGE,
}
_LOAN_TYPE_SEPARATORS = re.compile(r"[\s\-/]+")

# LendingFlow's sample lead shows the full state name ("Florida"); lender rules use USPS codes.
_STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}
_STATE_CODE_SET = frozenset(_STATE_CODES.values())

# "$500K", "1M", "250,000", "$1.5M"
_MONEY = re.compile(r"\$?\s*(\d[\d,]*(?:\.\d+)?)\s*([km])?", re.IGNORECASE)
_MULTIPLIERS = {"k": 1_000, "m": 1_000_000}


class ParseError(ValueError):
    """The payload cannot become a lead. The message is for our logs only (never contains a payload
    value); the sender always gets one fixed 400 message."""


@dataclass(frozen=True)
class ParsedCertificate:
    raw: str
    certificate_id: Optional[str] = None
    url: Optional[str] = None
    consented_at: Optional[datetime] = None  # timestamp inside the certificate, when it carries one
    client_ip: Optional[str] = None
    source_url: Optional[str] = None
    disclosure_text: Optional[str] = None


@dataclass(frozen=True)
class ParsedLendingFlowLead:
    vendor_lead_id: str
    phone: str  # phone_utils.normalize
    email: Optional[str] = None  # lower-cased
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    credit_band: Optional[str] = None
    credit_band_min_fico: Optional[int] = None
    loan_amount: Optional[int] = None  # low end of the range: what the event and the pre-qual letter use
    loan_amount_range: Optional[str] = None  # as LendingFlow sent it, e.g. "$500K - $1M"
    loan_amount_min: Optional[int] = None
    loan_amount_max: Optional[int] = None
    loan_type: Optional[str] = None  # LoanType value
    property_state: Optional[str] = None
    property_address: Optional[str] = None
    property_city: Optional[str] = None
    property_zip: Optional[str] = None
    lead_source_campaign: Optional[str] = None  # LendingFlow's "Source", e.g. "60 Second Loan Match"
    submitted_at: Optional[datetime] = None  # when the borrower submitted on LendingFlow
    certificate: Optional[ParsedCertificate] = None


@dataclass(frozen=True)
class SaveResult:
    lead_id: int
    lead_uuid: str
    created: bool
    suppressed: bool = False


@dataclass(frozen=True)
class FollowUp:
    """What to do for a newly delivered lead once its transaction has committed."""
    lead: Mapping[str, Any]


class LeadSink(Protocol):
    def push(self, lead: Mapping[str, Any]) -> PushResult: ...


# --------------------------------------------------------------------------- parsing

def _str(payload: Mapping[str, Any], *names: str, limit: int = 200) -> Optional[str]:
    for name in names:
        value = payload.get(name)
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned:
            return cleaned[:limit]
    return None


def _credit_band_min_fico(raw: Optional[str]) -> Optional[int]:
    """Lower bound of a band: ``"680-719"`` -> 680, ``"720+"`` -> 720, anything else None.
    Stand-in until David / Josh confirm LendingFlow's real band values."""
    if not raw:
        return None
    match = _BAND_RANGE.match(raw) or _BAND_PLUS.match(raw)
    return int(match.group(1)) if match else None


def _money(text_value: str) -> Optional[int]:
    match = _MONEY.search(text_value)
    if not match:
        return None
    amount = float(match.group(1).replace(",", "")) * _MULTIPLIERS.get((match.group(2) or "").lower(), 1)
    return int(amount) if amount > 0 else None


def _loan_amount(value: Any) -> tuple[Optional[str], Optional[int], Optional[int]]:
    """``(raw, min, max)``. LendingFlow sends a band (``"$500K - $1M"``); a plain number is a band of one.
    ``"$1M+"`` has no max; ``"Under $100K"`` has no min. Anything unreadable is (raw, None, None)."""
    if value is None or value == "" or isinstance(value, bool):
        return None, None, None
    if isinstance(value, (int, float)):
        amount = int(value) if value > 0 else None
        return str(value), amount, amount
    raw = str(value).strip()[:40]
    lowered = raw.lower()
    parts = re.split(r"\s*(?:-|–|to)\s*", raw, maxsplit=1)
    if len(parts) == 2 and parts[0] and parts[1]:
        return raw, _money(parts[0]), _money(parts[1])
    amount = _money(raw)
    if lowered.startswith(("under", "below", "<", "less than", "up to")):
        return raw, None, amount
    if raw.endswith("+") or lowered.startswith(("over", "above", ">")):
        return raw, amount, None
    return raw, amount, amount


def _state_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    upper = value.strip().upper()
    if upper in _STATE_CODE_SET:
        return upper
    return _STATE_CODES.get(" ".join(value.lower().split()))


def _loan_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = _LOAN_TYPE_SEPARATORS.sub("_", value.lower().replace("&", "and")).strip("_")
    loan_type = _LOAN_TYPES.get(key)
    return loan_type.value if loan_type else None


def _parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_certificate(consent: Any) -> Optional[ParsedCertificate]:
    if consent in (None, "", {}):
        return None
    if isinstance(consent, str):
        return ParsedCertificate(raw=consent)
    if not isinstance(consent, dict):
        raise ParseError("consent must be an object or a string")
    raw = consent.get("raw")
    if not isinstance(raw, str) or not raw:
        raw = json.dumps(consent, ensure_ascii=False)
    return ParsedCertificate(
        raw=raw,  # verbatim: never stripped or re-encoded
        certificate_id=_str(consent, "certificate_id", limit=200),
        url=_str(consent, "certificate_url", limit=500),
        consented_at=_parse_time(consent.get("timestamp")),
        client_ip=_str(consent, "ip_address", limit=45),
        source_url=_str(consent, "page_url", limit=500),
        disclosure_text=consent.get("disclosure_text") if isinstance(consent.get("disclosure_text"), str) else None,
    )


def parse_lendingflow(payload: Any) -> ParsedLendingFlowLead:
    """The only function that knows LendingFlow's payload shape (Fake shape until David's schema lands)."""
    if not isinstance(payload, dict):
        raise ParseError("payload must be a JSON object")
    vendor_id = _str(payload, "lead_id", "vendor_lead_id", limit=100)
    if not vendor_id:
        raise ParseError("lead_id is required")
    phone = normalize(_str(payload, "phone", limit=40))
    if not phone:
        raise ParseError("a valid US phone number is required")
    email = _str(payload, "email", limit=255)
    if email:
        email = email.lower()
        if not _EMAIL.match(email):
            email = None  # a bad email never costs a paid lead; phone is the contact key
    amount_raw, amount_min, amount_max = _loan_amount(payload.get("loan_amount"))
    band = _str(payload, "credit_score_range", "credit_score", "credit_band", limit=40)
    return ParsedLendingFlowLead(
        vendor_lead_id=vendor_id,
        phone=phone,
        email=email,
        first_name=_str(payload, "first_name", limit=80),
        last_name=_str(payload, "last_name", limit=80),
        credit_band=band,
        credit_band_min_fico=_credit_band_min_fico(band),
        loan_amount=amount_min,
        loan_amount_range=amount_raw,
        loan_amount_min=amount_min,
        loan_amount_max=amount_max,
        loan_type=_loan_type(_str(payload, "loan_purpose", "loan_type", limit=60)),
        property_state=_state_code(_str(payload, "property_state", "state", limit=40)),
        property_address=_str(payload, "property_address", limit=200),
        property_city=_str(payload, "property_city", limit=80),
        property_zip=_str(payload, "property_zip", limit=10),
        lead_source_campaign=_str(payload, "source", "lead_source", limit=100),
        submitted_at=_parse_time(payload.get("submitted_at")),
        certificate=_parse_certificate(payload.get("consent")),
    )


def dedupe_hash(phone: str, email: Optional[str]) -> str:
    return hashlib.sha256(f"{phone}|{(email or '').strip().lower()}".encode()).hexdigest()


# --------------------------------------------------------------------------- storing

def _upsert_contact(db, phone: str, email: Optional[str]) -> int:
    return db.execute(
        text("INSERT INTO lending.contacts (phone, email) VALUES (:p, :e) "
             "ON CONFLICT (phone) DO UPDATE SET email = COALESCE(lending.contacts.email, EXCLUDED.email) RETURNING id"),
        {"p": phone, "e": email},
    ).scalar_one()


def store_certificate(db, lead_id: int, cert: ParsedCertificate, *, duplicate: bool, now: Optional[datetime] = None) -> None:
    now = now or datetime.now(timezone.utc)
    db.execute(
        text("INSERT INTO lending.lead_consent_certificates (lead_id, lead_source, received_at, raw_certificate, "
             "certificate_id, certificate_url, verified_at, verification_method, client_ip, source_url, "
             "tcpa_disclosure_text, is_duplicate_delivery) "
             "VALUES (:lead, :src, :now, :raw, :cid, :url, :verified, :method, :ip, :page, :tcpa, :dup)"),
        {"lead": lead_id, "src": LEAD_SOURCE, "now": now, "raw": cert.raw, "cid": cert.certificate_id,
         "url": cert.url, "verified": cert.consented_at or now,
         "method": "certificate_timestamp" if cert.consented_at else "receipt_only",
         "ip": cert.client_ip, "page": cert.source_url, "tcpa": cert.disclosure_text, "dup": duplicate},
    )


_INSERT_LEAD = (
    "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, contact_id, received_at, first_name, last_name, "
    "phone, email, credit_band, credit_band_min_fico, loan_amount, loan_amount_range, loan_amount_min, loan_amount_max, "
    "loan_type, property_state, property_address, property_city, property_zip, lead_source_campaign, submitted_at, "
    "consent_status, suppressed, suppression_reason, ghl_status, raw_payload) "
    "VALUES (:vendor, :hash, :contact, :now, :first, :last, :phone, :email, :band, :fico, :amount, :arange, :amin, :amax, "
    ":ltype, :state, :addr, :city, :zip, :campaign, :submitted, "
    ":consent, :suppressed, :reason, :ghl, CAST(:raw AS jsonb)) "
    "ON CONFLICT DO NOTHING RETURNING id, lead_uuid"
)


def _record_duplicate(db, parsed: ParsedLendingFlowLead, digest: str, now: datetime) -> Optional[SaveResult]:
    """Count a re-delivery against the existing lead (vendor-id match preferred) and keep its certificate.
    None when no lead matches either key."""
    existing = db.execute(
        text("UPDATE lending.lendingflow_leads SET duplicate_count = duplicate_count + 1, last_duplicate_at = :now "
             "WHERE id = (SELECT id FROM lending.lendingflow_leads WHERE vendor_lead_id = :v OR dedupe_hash = :h "
             "ORDER BY (vendor_lead_id = :v) DESC LIMIT 1) RETURNING id, lead_uuid"),
        {"v": parsed.vendor_lead_id, "h": digest, "now": now},
    ).one_or_none()
    if existing is None:
        return None
    if parsed.certificate:
        store_certificate(db, existing.id, parsed.certificate, duplicate=True, now=now)
    logger.info("[lendingflow] duplicate delivery lead=%s vendor=%s", existing.id, parsed.vendor_lead_id)
    return SaveResult(existing.id, str(existing.lead_uuid), created=False)


def save_lead(db, parsed: ParsedLendingFlowLead, raw_payload: Any, *, now: Optional[datetime] = None) -> SaveResult:
    """Insert the lead, or record a duplicate. A duplicate is detected before anything else is written,
    so it never touches lending.contacts. The two UNIQUE indexes still decide a concurrent race: the
    losing insert does nothing and is recorded as a duplicate. The caller commits."""
    now = now or datetime.now(timezone.utc)
    digest = dedupe_hash(parsed.phone, parsed.email)
    duplicate = _record_duplicate(db, parsed, digest, now)
    if duplicate is not None:
        return duplicate
    contact_id = _upsert_contact(db, parsed.phone, parsed.email)
    reason = _suppression_reason(db, parsed.phone, parsed.email)
    row = db.execute(text(_INSERT_LEAD), {
        "vendor": parsed.vendor_lead_id, "hash": digest, "contact": contact_id, "now": now,
        "first": parsed.first_name, "last": parsed.last_name, "phone": parsed.phone, "email": parsed.email,
        "band": parsed.credit_band, "fico": parsed.credit_band_min_fico, "amount": parsed.loan_amount,
        "ltype": parsed.loan_type, "state": parsed.property_state, "addr": parsed.property_address,
        "city": parsed.property_city, "zip": parsed.property_zip,
        "arange": parsed.loan_amount_range, "amin": parsed.loan_amount_min, "amax": parsed.loan_amount_max,
        "campaign": parsed.lead_source_campaign, "submitted": parsed.submitted_at,
        "consent": "present" if parsed.certificate else "missing",
        "suppressed": reason is not None, "reason": reason, "ghl": "skipped" if reason else "pending",
        "raw": json.dumps(raw_payload, ensure_ascii=False),
    }).one_or_none()
    if row is None:  # lost a concurrent race on one of the UNIQUE keys
        raced = _record_duplicate(db, parsed, digest, now)
        if raced is None:
            raise RuntimeError("lendingflow insert conflicted but no matching lead was found")
        return raced
    if parsed.certificate:
        store_certificate(db, row.id, parsed.certificate, duplicate=False, now=now)
    logger.info("[lendingflow] lead created id=%s vendor=%s suppressed=%s", row.id, parsed.vendor_lead_id, reason is not None)
    return SaveResult(row.id, str(row.lead_uuid), created=True, suppressed=reason is not None)


# --------------------------------------------------------------------------- delivery

def _backoff_minutes_sql() -> str:
    steps = GHL_RETRY_BACKOFF_MINUTES
    whens = " ".join(f"WHEN {n} THEN {int(minutes)}" for n, minutes in enumerate(steps[:-1], start=1))
    return f"CASE ghl_attempts WHEN 0 THEN 0 {whens} ELSE {int(steps[-1])} END"


_DELIVERABLE = (
    "SELECT id, lead_uuid, vendor_lead_id, first_name, last_name, phone, email, credit_band, credit_band_min_fico, "
    "loan_amount, loan_type, property_state, lead_source_campaign, consent_status, received_at, ghl_attempts "
    "FROM lending.lendingflow_leads "
    "WHERE ghl_status IN ('pending', 'failed') AND NOT suppressed AND received_at > :oldest {extra} "
    "ORDER BY received_at LIMIT :limit FOR UPDATE SKIP LOCKED"
)


def deliver_pending(db, sink: Optional[LeadSink], *, lead_id: Optional[int] = None,
                    now: Optional[datetime] = None) -> list[FollowUp]:
    """Push stored leads to GHL. Returns a follow-up for each lead that reached GHL and whose
    ``LendingFlowLeadCreated`` event was claimed here (once per lead). Never raises per lead; with
    no sink, leads stay pending and no attempt is counted."""
    now = now or datetime.now(timezone.utc)
    if sink is None:
        logger.warning("[lendingflow] GHL is not configured: leads stay pending")
        return []
    params: dict[str, Any] = {"limit": SWEEP_BATCH_SIZE, "oldest": now - timedelta(hours=GHL_GIVE_UP_AFTER_HOURS)}
    if lead_id is not None:
        extra = "AND id = :lead_id"
        params["lead_id"] = lead_id
    else:
        extra = (f"AND (ghl_last_attempt_at IS NULL OR "
                 f"ghl_last_attempt_at < CAST(:now AS timestamptz) - make_interval(mins => {_backoff_minutes_sql()}))")
        params["now"] = now
    rows = db.execute(text(_DELIVERABLE.format(extra=extra)), params).mappings().all()
    followups: list[FollowUp] = []
    for row in rows:
        lead = dict(row)
        if _deliver_one(db, sink, lead, now) and _claim_event(db, lead["id"], now):
            followups.append(FollowUp(lead))
    return followups


def _deliver_one(db, sink: LeadSink, lead: dict[str, Any], now: datetime) -> bool:
    try:
        result = sink.push(lead)
    except DeliveryError as exc:
        _record_failure(db, lead, str(exc)[:200], now, config_error=exc.config_error)
        return False
    except Exception as exc:
        _record_failure(db, lead, f"unexpected {type(exc).__name__}", now)
        return False
    lead["ghl_contact_id"] = result.contact_id
    db.execute(
        text("UPDATE lending.lendingflow_leads SET ghl_status = :s, ghl_contact_id = :c, ghl_attempts = ghl_attempts + 1, "
             "ghl_last_attempt_at = :now, ghl_synced_at = :now, ghl_last_error = NULL WHERE id = :id"),
        {"s": "synced" if result.pipeline_card else "contact_only", "c": result.contact_id, "now": now, "id": lead["id"]},
    )
    logger.info("[lendingflow] lead delivered id=%s pipeline_card=%s", lead["id"], result.pipeline_card)
    return True


def _record_failure(db, lead: dict[str, Any], reason: str, now: datetime, *, config_error: bool = False) -> None:
    attempts = int(lead["ghl_attempts"]) + (0 if config_error else 1)
    db.execute(
        text("UPDATE lending.lendingflow_leads SET ghl_status = 'failed', ghl_attempts = :a, ghl_last_attempt_at = :now, "
             "ghl_last_error = :err WHERE id = :id"),
        {"a": attempts, "now": now, "err": reason, "id": lead["id"]},
    )
    logger.log(logging.ERROR if config_error else logging.WARNING,
               "[lendingflow] GHL delivery failed id=%s failures=%d config_error=%s reason=%s",
               lead["id"], attempts, config_error, reason)


def _claim_event(db, lead_id: int, now: datetime) -> bool:
    return db.execute(
        text("UPDATE lending.lendingflow_leads SET event_emitted_at = :now "
             "WHERE id = :id AND event_emitted_at IS NULL RETURNING id"),
        {"now": now, "id": lead_id},
    ).first() is not None


def run_followups(followups: list[FollowUp]) -> None:
    """After commit: emit the event, then queue the pre-qual letter when the 4 core fields are present.
    Never raises; logs lead id only."""
    from src.lending.lendingflow_events import build_event, emit
    from src.lending.prequal import PrequalLead
    from src.lending.prequal_letters import queue_and_send_in_background

    for item in followups:
        lead = item.lead
        try:
            emit(build_event(lead))
        except Exception as exc:
            logger.error("[lendingflow] event emit failed lead=%s: %s", lead["id"], type(exc).__name__)
        prequal = PrequalLead(credit_band=lead["credit_band"], loan_amount=lead["loan_amount"],
                              property_state=lead["property_state"], loan_type=lead["loan_type"])
        if not all((prequal.credit_band, prequal.loan_amount, prequal.property_state, prequal.loan_type)):
            logger.info("[lendingflow] prequal_skipped_missing_fields lead=%s", lead["id"])
            continue
        try:
            queue_and_send_in_background(lead_source=LEAD_SOURCE, lead_ref=lead["vendor_lead_id"],
                                         contact_id=lead["ghl_contact_id"], lead=prequal)
        except Exception as exc:
            logger.error("[lendingflow] prequal hand-off failed lead=%s: %s", lead["id"], type(exc).__name__)


def deliver_and_follow_up(sink: Optional[LeadSink], *, lead_id: Optional[int] = None) -> int:
    """Background / sweep entry point: deliver (own session, committed), then follow up. Never raises."""
    from src.lending.db import lending_session

    try:
        with lending_session() as db:
            followups = deliver_pending(db, sink, lead_id=lead_id)
        run_followups(followups)
        return len(followups)
    except Exception as exc:
        logger.error("[lendingflow] background delivery crashed lead=%s: %s", lead_id, type(exc).__name__)
        return 0
