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
    "Forced Action — Hillsborough property leads. Free signup: {url} — Reply STOP to opt out."
)

# Allow-list for Subscriber.signup_source — must stay in sync with the
# CHECK constraint defined by alembic migration fa017_signup_source.
ALLOWED_SIGNUP_SOURCES = frozenset({
	"direct",
	"landing_page",
	"dbpr_email",
	"cora_sms",
	"missed_call",
	"referral",
	"admin",
	"unknown",
})


def _coerce_signup_source(raw: Optional[str], default: str = "direct") -> str:
	"""Map an arbitrary input to an allowed value or fall back to 'unknown'.
	Empty / None falls back to `default` (caller-context aware)."""
	if not raw:
		return default
	candidate = str(raw).strip().lower()
	if candidate in ALLOWED_SIGNUP_SOURCES:
		return candidate
	logger.warning(
		"signup_source=%r not in allow-list — falling back to 'unknown'",
		raw,
	)
	return "unknown"


def _apply_signup_source(
	sub: Subscriber,
	new_source: Optional[str],
	utm_source: Optional[str] = None,
	utm_medium: Optional[str] = None,
	utm_campaign: Optional[str] = None,
	campaign_id: Optional[str] = None,
	attribution_token: Optional[str] = None,
) -> None:
	"""Persist source attribution onto a Subscriber row.

	Rules:
	- A freshly created Subscriber (signup_source unset OR 'direct'/'unknown')
	  accepts whatever the caller provided.
	- An existing Subscriber that already has a 'real' source (referral,
	  dbpr_email, cora_sms, missed_call, landing_page) is NOT overwritten
	  on a re-visit — first-touch attribution wins.
	- utm_*/campaign_id/attribution_token are always backfilled when missing
	  but never clobbered.
	"""
	coerced = _coerce_signup_source(new_source, default="direct")
	current = (sub.signup_source or "").strip().lower()
	upgradable = current in ("", "direct", "unknown")
	if coerced and upgradable:
		sub.signup_source = coerced

	# Free-text utm + campaign fields: backfill-only, never overwrite.
	if utm_source and not sub.utm_source:
		sub.utm_source = utm_source[:100]
	if utm_medium and not sub.utm_medium:
		sub.utm_medium = utm_medium[:100]
	if utm_campaign and not sub.utm_campaign:
		sub.utm_campaign = utm_campaign[:100]
	if campaign_id and not sub.campaign_id:
		sub.campaign_id = campaign_id[:50]
	if attribution_token and not sub.attribution_token:
		sub.attribution_token = attribution_token[:200]


def create_free_account(
    phone: str,
    source: str,
    db: Session,
    name: Optional[str] = None,
    referral_code: Optional[str] = None,
    county_id: str = "hillsborough",
    utm_source: Optional[str] = None,
    utm_medium: Optional[str] = None,
    utm_campaign: Optional[str] = None,
    campaign_id: Optional[str] = None,
    attribution_token: Optional[str] = None,
) -> Subscriber:
    """Create (or re-use) a free Subscriber keyed by phone number.

    Idempotent — if a Subscriber with this normalized E.164 phone already
    exists, returns it without creating a duplicate. New rows store the
    normalized phone so subsequent calls hit the dedup path.
    """
    normalized = _normalize_phone(phone)

    # Dedup: return existing subscriber for this phone number
    if normalized:
        existing = db.query(Subscriber).filter_by(phone=normalized).first()
        if existing:
            logger.info(
                "Free account deduped: subscriber=%d phone=%s source=%s",
                existing.id, normalized, source,
            )
            return existing

    now = datetime.now(timezone.utc)
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
        phone=normalized,
        signup_source=_coerce_signup_source(source, default="missed_call"),
        utm_source=(utm_source or None) and utm_source[:100],
        utm_medium=(utm_medium or None) and utm_medium[:100],
        utm_campaign=(utm_campaign or None) and utm_campaign[:100],
        campaign_id=(campaign_id or None) and campaign_id[:50],
        attribution_token=(attribution_token or None) and attribution_token[:200],
        created_at=now,
        updated_at=now,
    )
    db.add(sub)
    db.flush()

    logger.info(
        "Free account created: subscriber=%d source=%s phone=%s",
        sub.id, sub.signup_source, phone,
    )

    try:
        from src.services.business_events import log_business_event
        log_business_event(
            "SIGNUP_COMPLETED", subscriber_id=sub.id,
            payload={"channel": "phone", "signup_source": sub.signup_source}, db=db,
        )
        log_business_event(
            "SIGNUP_SOURCE_ATTRIBUTED", subscriber_id=sub.id,
            payload={
                "signup_source": sub.signup_source,
                "utm_source": sub.utm_source, "utm_medium": sub.utm_medium,
                "utm_campaign": sub.utm_campaign, "campaign_id": sub.campaign_id,
            }, db=db,
        )
    except Exception:
        pass  # never block signup on audit-log failure

    if referral_code:
        try:
            from src.services.referral_engine import process_signup
            process_signup(sub.id, referral_code, db)
        except Exception as exc:
            logger.warning("Referral processing failed for subscriber %d: %s", sub.id, exc)

    return sub


def _normalize_phone(raw: Optional[str]) -> Optional[str]:
	"""Best-effort E.164 normaliser. None/empty in → None out."""
	if not raw:
		return None
	s = str(raw).strip()
	if not s:
		return None
	if s.startswith("+"):
		digits = "".join(c for c in s[1:] if c.isdigit())
		return f"+{digits}" if 8 <= len(digits) <= 15 else None
	digits = "".join(c for c in s if c.isdigit())
	if len(digits) == 10:
		return f"+1{digits}"
	if len(digits) == 11 and digits.startswith("1"):
		return f"+{digits}"
	return None


def create_free_account_by_email(
	email: str,
	db: Session,
	vertical: str = "roofing",
	county_id: str = "hillsborough",
	name: Optional[str] = None,
	referral_code: Optional[str] = None,
	phone: Optional[str] = None,
	sms_consent: bool = False,
	signup_source: Optional[str] = None,
	utm_source: Optional[str] = None,
	utm_medium: Optional[str] = None,
	utm_campaign: Optional[str] = None,
	campaign_id: Optional[str] = None,
	attribution_token: Optional[str] = None,
	send_welcome: bool = True,
) -> Subscriber:
	"""
	Create (or re-use) a free-tier Subscriber keyed by email.

	If `phone` is supplied AND `sms_consent` is True, persists phone +
	an SmsOptIn row for marketing SMS. Always stores phone for transactional.

	signup_source / utm_*/campaign_id are validated + persisted via
	`_apply_signup_source` — re-visit doesn't clobber an already-attributed row.

	Idempotent on email.
	"""
	from sqlalchemy import select

	email = email.strip().lower()
	normalized_phone = _normalize_phone(phone)
	# Default email-flow source to 'landing_page' (user came via the FE form)
	# unless caller explicitly passes something else.
	resolved_source = _coerce_signup_source(signup_source, default="landing_page")
	# Referral_code presence implies referral source unless explicitly set.
	if referral_code and resolved_source in ("landing_page", "direct"):
		resolved_source = "referral"

	existing = db.execute(
		select(Subscriber).where(Subscriber.email == email)
	).scalar_one_or_none()
	if existing:
		_maybe_set_phone_and_opt_in(existing, normalized_phone, sms_consent, db)
		_apply_signup_source(
			existing, resolved_source,
			utm_source=utm_source, utm_medium=utm_medium,
			utm_campaign=utm_campaign, campaign_id=campaign_id,
			attribution_token=attribution_token,
		)
		db.flush()
		logger.info(
			"free account re-used for email=%s → subscriber=%d (source=%s)",
			email, existing.id, existing.signup_source,
		)
		return existing

	stripe_customer_id = _create_stripe_customer(email, name)

	now = datetime.now(timezone.utc)
	sub = Subscriber(
		stripe_customer_id=stripe_customer_id,
		tier="free",
		vertical=vertical,
		county_id=county_id,
		email=email,
		name=name,
		phone=normalized_phone,
		founding_member=False,
		status="active",
		event_feed_uuid=str(uuid.uuid4()),
		referral_code=f"REF{uuid.uuid4().hex[:5].upper()}",
		signup_source=resolved_source,
		utm_source=(utm_source or None) and utm_source[:100],
		utm_medium=(utm_medium or None) and utm_medium[:100],
		utm_campaign=(utm_campaign or None) and utm_campaign[:100],
		campaign_id=(campaign_id or None) and campaign_id[:50],
		attribution_token=(attribution_token or None) and attribution_token[:200],
		created_at=now,
		updated_at=now,
	)
	db.add(sub)
	db.flush()

	_maybe_set_phone_and_opt_in(sub, normalized_phone, sms_consent, db)

	logger.info(
		"Free account created by email: subscriber=%d email=%s phone=%s consent=%s source=%s",
		sub.id, email, normalized_phone, sms_consent, sub.signup_source,
	)

	try:
		from src.services.business_events import log_business_event
		log_business_event(
			"SIGNUP_COMPLETED", subscriber_id=sub.id,
			payload={"channel": "email", "signup_source": sub.signup_source}, db=db,
		)
		log_business_event(
			"SIGNUP_SOURCE_ATTRIBUTED", subscriber_id=sub.id,
			payload={
				"signup_source": sub.signup_source,
				"utm_source": sub.utm_source, "utm_medium": sub.utm_medium,
				"utm_campaign": sub.utm_campaign, "campaign_id": sub.campaign_id,
			}, db=db,
		)
	except Exception:
		pass

	if referral_code:
		try:
			from src.services.referral_engine import process_signup
			process_signup(sub.id, referral_code, db)
		except Exception as exc:
			logger.warning("Referral processing failed for subscriber %d: %s", sub.id, exc)

	if send_welcome:
		try:
			from src.services.email import send_welcome_email
			send_welcome_email(sub)
		except Exception as exc:
			logger.warning("Welcome email failed for new subscriber %d: %s", sub.id, exc)
	else:
		logger.info(
			"Welcome email deferred for subscriber=%d — caller will send post-payment",
			sub.id,
		)

	return sub


def _maybe_set_phone_and_opt_in(
	sub: Subscriber, phone: Optional[str], consent: bool, db: Session,
) -> None:
	"""Set Subscriber.phone if missing AND insert SmsOptIn(source='widget')
	when the user gave consent. Skip silently on conflicts so signup never
	fails over a phone reuse case."""
	if phone and not sub.phone:
		sub.phone = phone
		try:
			db.flush()
		except Exception as exc:
			logger.warning(
				"phone backfill conflict on subscriber=%d phone=%s: %s",
				sub.id, phone, exc,
			)
			return
	if phone and consent:
		try:
			from src.core.models import SmsOptIn
			from sqlalchemy import select
			existing_opt = db.execute(
				select(SmsOptIn).where(SmsOptIn.subscriber_id == sub.id)
			).scalar_one_or_none()
			if existing_opt is None:
				db.add(SmsOptIn(
					phone=phone,
					subscriber_id=sub.id,
					source="widget",
					opt_in_message="Free signup form — TCPA consent ticked",
					opted_in_at=datetime.now(timezone.utc),
				))
				db.flush()
		except Exception as exc:
			logger.warning("SmsOptIn insert failed for subscriber=%d: %s", sub.id, exc)


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
    """Process an inbound missed/answered call.
    Creates a free Subscriber with signup_source='missed_call', then sends a
    welcome SMS containing a SIGNED landing-page link so the subscriber lands
    on the landing page (attribution preserved) and is auto-redirected to
    their dashboard via /api/landing/resolve-token.

    Returns TwiML response XML string.
    """
    sub = create_free_account(phone=from_number, source="missed_call", db=db)

    if sub.event_feed_uuid and can_send(from_number, db):
        from src.services.signed_links import encode_landing_token
        token = encode_landing_token(sub.id, "missed_call", ttl_hours=24)

        if token:
            # Land on the public landing page with signed token. The frontend
            # POSTs to /api/landing/resolve-token, gets the feed_uuid back,
            # and navigates to /dashboard/<uuid> — attribution preserved.
            landing_url = (
                f"{settings.app_base_url}/?signup_source=missed_call&token={token}"
            )
            sub.attribution_token = token[:200]
        else:
            # Token signing disabled (no LANDING_TOKEN_SECRET) — fall back to
            # the direct dashboard URL. Attribution still recorded on the row.
            landing_url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"

        sms_body = _MISSED_CALL_SMS.format(url=landing_url)
        # Don't truncate to 160 — the signed token + URL is ~120 chars on its
        # own, and chopping off "Reply STOP to opt out" would be a TCPA
        # violation. Telnyx handles multi-segment SMS fine; this is a
        # one-per-subscriber welcome message, the cost is negligible.
        send_sms(
            to=from_number,
            body=sms_body,
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
