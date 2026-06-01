"""
First Leads delivery — inbound signup flow (Phase 3).

After a new subscriber is created from a Synthflow inbound call, push the top-3
teaser leads for their captured ZIP × vertical as a marketing SMS via Telnyx
`send_sms`. Owner contact is withheld; subscriber must convert to unlock.

Reuses `get_sample_leads` and `format_sms_body` from sample_leads_sms.py so
the lead selection and teaser format stay in one place.

SLA: this function must complete (SMS enqueued) within 60s of the
Synthflow inbound webhook being received. The measured boundary is the
`message_outcomes.sent_at` row written by `send_sms`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.sample_leads_sms import format_sms_body, get_sample_leads
from src.services.signed_links import encode_landing_token
from src.services.sms_compliance import can_send, send_sms

logger = logging.getLogger(__name__)

_DEFAULT_VERTICAL = "roofing"
_DEFAULT_ZIP = ""  # empty → county-wide fallback in get_sample_leads


@dataclass
class FirstLeadsResult:
    sent: bool
    lead_count: int
    fallback: bool  # True when ZIP/vertical was missing (Incomplete Capture)


def deliver_first_leads(
    *,
    subscriber_id: int,
    phone: str,
    zip_code: Optional[str],
    vertical: Optional[str],
    db: Session,
) -> FirstLeadsResult:
    """Select top-3 leads and push them as a marketing SMS via Telnyx.

    Uses the captured ZIP × vertical from the Synthflow inbound event. If either
    is missing the subscriber is an Incomplete Capture: we fall back to the
    county-wide top-scored leads on the default vertical and mark fallback=True.

    Returns FirstLeadsResult with sent/lead_count/fallback — never raises.
    """
    fallback = not zip_code or not vertical
    resolved_zip = zip_code or _DEFAULT_ZIP
    resolved_vertical = vertical or _DEFAULT_VERTICAL

    if fallback:
        logger.info(
            "[FirstLeads] sub=%d incomplete capture (zip=%r vertical=%r) — using fallback set",
            subscriber_id, zip_code, vertical,
        )

    try:
        leads = get_sample_leads(
            zip_code=resolved_zip,
            vertical=resolved_vertical,
            count=3,
        )
    except Exception as exc:
        logger.error("[FirstLeads] sub=%d lead query failed: %s", subscriber_id, exc)
        return FirstLeadsResult(sent=False, lead_count=0, fallback=fallback)

    body = format_sms_body(leads, zip_code=resolved_zip or "Hillsborough", vertical=resolved_vertical)

    # Append signed dashboard link so the marketing SMS is self-contained.
    token = encode_landing_token(subscriber_id, "missed_call", ttl_hours=24)
    settings = get_settings()
    if token:
        dashboard_url = f"{settings.app_base_url}/?signup_source=missed_call&token={token}"
    else:
        dashboard_url = f"{settings.app_base_url}/dashboard"

    body = f"{body}\n\nView your dashboard: {dashboard_url}"

    if not can_send(phone, db):
        logger.warning("[FirstLeads] sub=%d can_send=False — skipping marketing SMS", subscriber_id)
        return FirstLeadsResult(sent=False, lead_count=len(leads), fallback=fallback)

    try:
        send_sms(
            to=phone,
            body=body,
            db=db,
            message_type="marketing",
            subscriber_id=subscriber_id,
            task_type="first_leads",
        )
        logger.info(
            "[FirstLeads] sub=%d sent leads=%d fallback=%s zip=%r vertical=%r",
            subscriber_id, len(leads), fallback, resolved_zip, resolved_vertical,
        )
        return FirstLeadsResult(sent=True, lead_count=len(leads), fallback=fallback)
    except Exception as exc:
        logger.error("[FirstLeads] sub=%d send_sms failed: %s", subscriber_id, exc)
        return FirstLeadsResult(sent=False, lead_count=len(leads), fallback=fallback)
