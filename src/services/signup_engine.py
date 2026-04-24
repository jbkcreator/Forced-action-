"""
Signup Engine — Item 21 (missed-call auto-signup skeleton).

Auto-creates a free Subscriber account from a phone number (missed call, DBPR
email capture, referral link). Sends welcome SMS with dashboard link.

NOTE: Subscriber.phone field is not yet in schema (2B-2). Until then, dedup by
phone is skipped — each missed call creates a new free account. The phone is
stored in the welcome SMS log only.
"""
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import Subscriber
from src.services.sms_compliance import can_send, send_sms

logger = logging.getLogger(__name__)

_MISSED_CALL_SMS = (
    "You called Forced Action — Hillsborough's distressed property platform. "
    "Free leads here: {url} — Reply STOP to opt out."
)


def create_free_account(
    phone: str,
    source: str,
    db: Session,
    name: Optional[str] = None,
    referral_code: Optional[str] = None,
    county_id: str = "hillsborough",
) -> Subscriber:
    """
    Create a free (tier='free') Subscriber from a phone number.

    source: 'missed_call' | 'referral' | 'dbpr' | 'manual'

    Idempotency note: no phone field on Subscriber yet — creates a new row each
    time. Will be deduplicated once Subscriber.phone is added in 2B-2.
    """
    now = datetime.now(timezone.utc)
    # Stripe customer ID placeholder until user completes checkout
    stripe_placeholder = f"free_{uuid.uuid4().hex[:12]}"

    sub = Subscriber(
        stripe_customer_id=stripe_placeholder,
        tier="free",
        vertical="roofing",          # default; updated on first login
        county_id=county_id,
        founding_member=False,
        status="active",
        event_feed_uuid=str(uuid.uuid4()),
        name=name,
        created_at=now,
        updated_at=now,
    )
    db.add(sub)
    db.flush()

    logger.info("Free account created: subscriber=%d source=%s phone=%s", sub.id, source, phone)

    if referral_code:
        try:
            from src.services.referral_engine import process_signup
            process_signup(sub.id, referral_code, db)
        except Exception as exc:
            logger.warning("Referral processing failed for subscriber %d: %s", sub.id, exc)

    return sub


def create_free_account_by_email(
	email: str,
	db: Session,
	vertical: str = "roofing",
	county_id: str = "hillsborough",
	name: Optional[str] = None,
	referral_code: Optional[str] = None,
) -> Subscriber:
	"""
	Create (or re-use) a free-tier Subscriber keyed by email.

	Creates a real Stripe customer so subsequent Payment Sheet charges can
	set `setup_future_usage=off_session` against it. Idempotent on email —
	a re-visit returns the existing subscriber row, not a duplicate.
	"""
	from sqlalchemy import select

	email = email.strip().lower()

	# Deduplicate by email — return existing free subscriber if found.
	existing = db.execute(
		select(Subscriber).where(Subscriber.email == email)
	).scalar_one_or_none()
	if existing:
		logger.info("free account re-used for email=%s → subscriber=%d", email, existing.id)
		return existing

	# Create a real Stripe customer so we can attach PaymentMethods later.
	stripe_customer_id = _create_stripe_customer(email, name)

	now = datetime.now(timezone.utc)
	sub = Subscriber(
		stripe_customer_id=stripe_customer_id,
		tier="free",
		vertical=vertical,
		county_id=county_id,
		email=email,
		name=name,
		founding_member=False,
		status="active",
		event_feed_uuid=str(uuid.uuid4()),
		referral_code=f"REF{uuid.uuid4().hex[:5].upper()}",
		created_at=now,
		updated_at=now,
	)
	db.add(sub)
	db.flush()

	logger.info("Free account created by email: subscriber=%d email=%s", sub.id, email)

	if referral_code:
		try:
			from src.services.referral_engine import process_signup
			process_signup(sub.id, referral_code, db)
		except Exception as exc:
			logger.warning("Referral processing failed for subscriber %d: %s", sub.id, exc)

	return sub


def _create_stripe_customer(email: str, name: Optional[str]) -> str:
	"""
	Create a Stripe customer, returning its ID. Falls back to a placeholder
	if Stripe is misconfigured so free signup never hard-fails over it — the
	placeholder blocks later Payment Sheet flows until a real customer is
	attached, but the subscriber row still gets created.
	"""
	try:
		import stripe
		key = settings.active_stripe_secret_key
		if key is None:
			return f"free_{uuid.uuid4().hex[:12]}"
		stripe.api_key = key.get_secret_value()
		customer = stripe.Customer.create(
			email=email,
			name=name,
			metadata={"fa_source": "free_signup_email"},
		)
		return customer.id
	except Exception as exc:
		logger.warning("Stripe customer create failed for %s: %s — using placeholder", email, exc)
		return f"free_{uuid.uuid4().hex[:12]}"


def handle_missed_call(from_number: str, db: Session) -> str:
    """
    Process an inbound missed/answered call from Twilio Voice.
    Creates a free account and sends a welcome SMS with the dashboard link.
    Returns TwiML response XML string.
    """
    sub = create_free_account(phone=from_number, source="missed_call", db=db)

    if sub.event_feed_uuid and can_send(from_number, db):
        feed_url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        sms_body = _MISSED_CALL_SMS.format(url=feed_url)
        send_sms(
            to=from_number,
            body=sms_body[:160],
            db=db,
            subscriber_id=sub.id,
            task_type="missed_call_welcome",
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Response>"
        "<Say voice=\"alice\">"
        "Thanks for calling Forced Action. "
        "We just texted you a link to your free property leads account. "
        "Have a great day!"
        "</Say>"
        "<Hangup/>"
        "</Response>"
    )
