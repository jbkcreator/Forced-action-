"""
Campaign conversion attribution — Phase B6.

Signed token encodes campaign_contact_id so we can attribute a subscriber
signup back to the exact (campaign, contractor) membership that drove it.

Token is HMAC-signed with ADMIN_JWT_SECRET (same secret as admin JWT).
It is embedded in the email CTA link and threaded through:
    email CTA → dashboard signup URL → Stripe checkout metadata → webhook

On checkout.session.completed the token is decoded and:
  - campaign_contacts.converted_at is stamped
  - dbpr_contacts.is_signed_up = TRUE (global suppression)
  - dbpr_contacts.subscriber_id + signed_up_at set
  - Subscriber.acquisition_source = 'dbpr_email'

Fallback: if token is absent/invalid, attempt email-match.
"""

import hashlib
import hmac
import logging
import time
from typing import Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)

_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 90  # 90 days (outlasts longest email sequence)
_SEP = "."


def _secret() -> bytes:
    s = get_settings()
    if not s.admin_jwt_secret:
        raise RuntimeError("ADMIN_JWT_SECRET not set — cannot sign attribution token")
    return s.admin_jwt_secret.get_secret_value().encode()


def encode_attribution_token(campaign_contact_id: int) -> str:
    """
    Return a signed token encoding campaign_contact_id + timestamp.
    Format: <campaign_contact_id>.<timestamp>.<hmac>
    """
    ts = str(int(time.time()))
    payload = f"{campaign_contact_id}{_SEP}{ts}"
    sig = hmac.new(_secret(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}{_SEP}{sig}"


def decode_attribution_token(token: str) -> Optional[int]:
    """
    Verify and decode a token. Returns campaign_contact_id or None if
    invalid/expired/tampered.
    """
    try:
        parts = token.split(_SEP)
        if len(parts) != 3:
            return None
        cid_str, ts_str, sig = parts
        payload = f"{cid_str}{_SEP}{ts_str}"
        expected = hmac.new(_secret(), payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            logger.warning("[Attribution] Token HMAC mismatch")
            return None
        age = int(time.time()) - int(ts_str)
        if age > _TOKEN_TTL_SECONDS:
            logger.warning("[Attribution] Token expired (age=%ds)", age)
            return None
        return int(cid_str)
    except Exception as exc:
        logger.warning("[Attribution] Token decode failed: %s", exc)
        return None


def record_conversion(
    db,
    campaign_contact_id: int,
    subscriber_id: int,
    signed_up_at,
) -> bool:
    """
    Stamp the campaign_contacts row and set global suppression on the contact.
    Called from the Stripe checkout webhook.
    Returns True if attribution was recorded, False if membership not found.
    """
    from datetime import datetime, timezone
    from src.core.models import CampaignContact, DBPRContact

    cc = db.get(CampaignContact, campaign_contact_id)
    if not cc:
        logger.warning("[Attribution] CampaignContact %d not found", campaign_contact_id)
        return False

    now = datetime.now(timezone.utc)
    cc.converted_at = signed_up_at or now
    db.add(cc)

    # Global suppression — prevent future campaign membership
    dc = db.get(DBPRContact, cc.dbpr_contact_id)
    if dc:
        dc.is_signed_up = True
        dc.subscriber_id = subscriber_id
        dc.signed_up_at = signed_up_at or now
        dc.updated_at = now
        db.add(dc)

    logger.info(
        "[Attribution] Conversion recorded: campaign_contact=%d subscriber=%d dbpr_contact=%d",
        campaign_contact_id, subscriber_id, cc.dbpr_contact_id,
    )
    return True


def try_email_fallback(db, email: str, subscriber_id: int, signed_up_at) -> bool:
    """
    Fallback: find a dbpr_contact by email and stamp is_signed_up.
    Used when the attribution token is absent (e.g. subscriber arrived
    through a forwarded link).
    """
    from datetime import datetime, timezone
    from src.core.models import DBPRContact

    if not email:
        return False

    dc = (
        db.query(DBPRContact)
        .filter(DBPRContact.email.ilike(email.strip()))
        .filter(DBPRContact.is_signed_up.is_(False))
        .first()
    )
    if not dc:
        return False

    now = datetime.now(timezone.utc)
    dc.is_signed_up = True
    dc.subscriber_id = subscriber_id
    dc.signed_up_at = signed_up_at or now
    dc.updated_at = now
    db.add(dc)
    logger.info("[Attribution] Email-fallback conversion: dbpr_contact=%d email=%s", dc.id, email)
    return True
