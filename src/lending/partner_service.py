"""Partner Door (P-1): create partners and manage their links, shared by ``/partners`` in Slack and the CLI.

Plain business logic: no Slack, no HTTP. Both callers get the same checks, and every write goes through the
functions in ``partners.py``. The raw link token only ever leaves here inside an :class:`IssuedLink`; callers show
it once and never log it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_partners import AGREEMENT_STATUSES, PARTNER_TYPES, RETRY_WINDOW_MINUTES
from config.settings import get_settings
from src.lending.partners import (
    LINK_PATH,
    create_partner,
    issue_new_link,
    partner_by_id,
    revoke_link,
    valid_email,
)
from src.services.phone_utils import normalize

_STATE = re.compile(r"^[A-Z]{2}$")


class PartnerError(ValueError):
    """The request cannot be applied. ``errors`` maps each :class:`PartnerIn` field (or ``partner_id``) to a message."""

    def __init__(self, errors: dict[str, str]) -> None:
        super().__init__("; ".join(errors.values()))
        self.errors = errors


class PartnerLinkNotConfigured(RuntimeError):
    """``LENDING_PARTNER_LINK_BASE_URL`` is unset or not https, so no full link can be shown."""


@dataclass(frozen=True)
class PartnerIn:
    name: str
    partner_type: str
    company: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    states: Optional[str] = None
    agreement_status: str = "pending"
    referral_terms: Optional[str] = None
    sms_consent: bool = False


@dataclass(frozen=True)
class IssuedLink:
    partner_id: int
    name: str
    token: str
    reused: bool = False  # a retried create re-issued the link of the partner it had just made


def link_base_url() -> Optional[str]:
    base = (get_settings().lending_partner_link_base_url or "").strip().rstrip("/")
    return base if base.startswith("https://") else None


def partner_link(token: str) -> str:
    base = link_base_url()
    if base is None:
        raise PartnerLinkNotConfigured("LENDING_PARTNER_LINK_BASE_URL is not set")
    return base + LINK_PATH.format(token=token)


def _blank(value: Optional[str]) -> Optional[str]:
    value = (value or "").strip()
    return value or None


def _states(raw: Optional[str]) -> tuple[Optional[str], bool]:
    codes = [code.strip().upper() for code in (raw or "").split(",") if code.strip()]
    if not all(_STATE.match(code) for code in codes):
        return None, False
    joined = ",".join(dict.fromkeys(codes))
    return joined or None, len(joined) <= 200


def clean(body: PartnerIn) -> PartnerIn:
    """The partner with blank fields dropped and values normalized. Raises :class:`PartnerError` with every problem."""
    errors: dict[str, str] = {}
    name = (body.name or "").strip()
    if not name or len(name) > 120:
        errors["name"] = "Name is required (120 characters at most)."
    if body.partner_type not in PARTNER_TYPES:
        errors["partner_type"] = f"Choose a type: {', '.join(PARTNER_TYPES)}."
    company = _blank(body.company)
    if company and len(company) > 160:
        errors["company"] = "Company is 160 characters at most."
    email = _blank(body.email)
    if email is not None:
        email = valid_email(email)
        if email is None or len(email) > 255:
            errors["email"] = "Enter a valid email address."
    phone = _blank(body.phone)
    if phone is not None:
        phone = normalize(phone)
        if phone is None:
            errors["phone"] = "Enter a valid US phone number."
    states, states_ok = _states(body.states)
    if not states_ok:
        errors["states"] = "Use two-letter codes separated by commas, e.g. FL,GA."
    if body.agreement_status not in AGREEMENT_STATUSES:
        errors["agreement_status"] = f"Choose an agreement status: {', '.join(AGREEMENT_STATUSES)}."
    terms = _blank(body.referral_terms)
    if terms and len(terms) > 2000:
        errors["referral_terms"] = "Referral terms are 2000 characters at most."
    if body.sms_consent and phone is None:
        errors.setdefault("phone", "Add the phone number the status texts go to.")
    if email is None and not (phone and body.sms_consent):
        errors.setdefault("email", "Add the partner's email so they get updates on their files "
                                   "(or add a phone and tick 'Partner agreed to status texts').")
    if errors:
        raise PartnerError(errors)
    return PartnerIn(name=name, partner_type=body.partner_type, company=company, email=email, phone=phone,
                     states=states, agreement_status=body.agreement_status, referral_terms=terms,
                     sms_consent=body.sms_consent)


def create(db, body: PartnerIn, *, created_by: Optional[str], now: Optional[datetime] = None) -> IssuedLink:
    """Create the partner and its link. The caller commits.

    The same person creating the same name again within ``RETRY_WINDOW_MINUTES`` is Slack redelivering the
    submission (the person usually never saw the first answer): that partner gets a fresh link instead of a twin.
    """
    body = clean(body)
    now = now or datetime.now(timezone.utc)
    if created_by:
        recent = db.execute(
            text("SELECT id, name FROM lending.partners WHERE created_by = :by AND lower(name) = lower(:name) "
                 "AND created_at >= :since ORDER BY id DESC LIMIT 1"),
            {"by": created_by, "name": body.name, "since": now - timedelta(minutes=RETRY_WINDOW_MINUTES)},
        ).first()
        if recent is not None:
            token = issue_new_link(db, int(recent.id), now)
            if token is not None:
                return IssuedLink(int(recent.id), recent.name, token, reused=True)
    partner_id, token = create_partner(
        db, name=body.name, partner_type=body.partner_type, company=body.company, phone=body.phone,
        email=body.email, states=body.states, agreement_status=body.agreement_status,
        referral_terms=body.referral_terms, sms_consent=body.sms_consent, created_by=created_by, now=now)
    return IssuedLink(partner_id, body.name, token)


def _existing(db, partner_id: int):
    partner = partner_by_id(db, partner_id)
    if partner is None:
        raise PartnerError({"partner_id": "That partner no longer exists."})
    return partner


def new_link(db, partner_id: int, now: Optional[datetime] = None) -> IssuedLink:
    """Replace the partner's link (also switches a revoked link back on). The caller commits."""
    partner = _existing(db, partner_id)
    token = issue_new_link(db, partner_id, now)
    if token is None:
        raise PartnerError({"partner_id": "That partner no longer exists."})
    return IssuedLink(partner.id, partner.name, token)


def revoke(db, partner_id: int, now: Optional[datetime] = None) -> str:
    """Switch the partner's link off and return the partner's name. The record and files stay. The caller commits."""
    partner = _existing(db, partner_id)
    if not revoke_link(db, partner_id, now):
        raise PartnerError({"partner_id": "This link is already switched off."})
    return partner.name


def _link_state(row: Any) -> str:
    if row["token_revoked_at"] is not None:
        return "revoked"
    return "live" if row["active"] else "inactive"


def list_partners(db, limit: int = 100) -> list[dict[str, Any]]:
    """Partners newest first, with the state of their link (live / revoked / inactive)."""
    rows = db.execute(
        text("SELECT id, name, partner_type, agreement_status, active, token_revoked_at, is_test, created_at "
             "FROM lending.partners ORDER BY created_at DESC, id DESC LIMIT :limit"),
        {"limit": limit},
    ).mappings().all()
    return [{**dict(row), "link_state": _link_state(row)} for row in rows]
