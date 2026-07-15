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
from config.vertical_display import DEFAULT_VERTICAL
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
	"affiliate",
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
    affiliate_ref: Optional[str] = None,
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

    if affiliate_ref:
        try:
            from src.services.affiliate_engine import attribute_signup
            attribute_signup(db, sub, affiliate_ref)
        except Exception as exc:
            logger.warning("Affiliate attribution failed for subscriber %d: %s", sub.id, exc)

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


def _record_annual_test_arm(sub: Subscriber, annual_test_arm: Optional[str], db: Session) -> None:
	"""Best-effort: persist the annual-at-signup A/B arm against this
	subscriber. Never blocks signup — a failure here just means that one
	subscriber is missing from the experiment's numbers. Wrapped in
	begin_nested() so a race-condition IntegrityError (e.g. two concurrent
	signups hitting ab_assignments' unique constraint) only rolls back this
	savepoint, not the caller's whole transaction."""
	if not annual_test_arm:
		return
	try:
		with db.begin_nested():
			from src.services.ab_engine import (
				ANNUAL_SIGNUP_TEST_NAME, ensure_annual_signup_test, record_pregenerated_arm,
			)
			ensure_annual_signup_test(db)
			record_pregenerated_arm(sub.id, ANNUAL_SIGNUP_TEST_NAME, annual_test_arm, db)
	except Exception:
		logger.warning("annual_at_signup_v1 arm recording failed for subscriber %d", sub.id, exc_info=True)


def create_free_account_by_email(
	email: str,
	db: Session,
	vertical: str = DEFAULT_VERTICAL,
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
	affiliate_ref: Optional[str] = None,
	send_welcome: bool = True,
	annual_test_arm: Optional[str] = None,
) -> Subscriber:
	"""
	Create (or re-use) a free-tier Subscriber keyed by email.

	If `phone` is supplied AND `sms_consent` is True, persists phone +
	an SmsOptIn row for marketing SMS. Always stores phone for transactional.

	signup_source / utm_*/campaign_id are validated + persisted via
	`_apply_signup_source` — re-visit doesn't clobber an already-attributed row.

	`annual_test_arm` ("variant"/"control") is the annual-at-signup A/B arm
	the frontend already bucketed this (still-anonymous) visitor into before
	a subscriber_id existed — recorded here via record_pregenerated_arm once
	the row exists. Best-effort; never blocks signup.

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
		_record_annual_test_arm(existing, annual_test_arm, db)
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
		onboarding_completed=False,
		created_at=now,
		updated_at=now,
	)
	db.add(sub)
	db.flush()

	_maybe_set_phone_and_opt_in(sub, normalized_phone, sms_consent, db)
	_record_annual_test_arm(sub, annual_test_arm, db)

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

	if affiliate_ref:
		try:
			from src.services.affiliate_engine import attribute_signup
			attribute_signup(db, sub, affiliate_ref)
		except Exception as exc:
			logger.warning("Affiliate attribution failed for subscriber %d: %s", sub.id, exc)

	if referral_code:
		try:
			from src.services.referral_engine import process_signup
			process_signup(sub.id, referral_code, db)
		except Exception as exc:
			logger.warning("Referral processing failed for subscriber %d: %s", sub.id, exc)

	if send_welcome:
		# Magic-link login — issue a fresh one-time link now and email it in
		# the welcome email. No password is ever generated or emailed.
		# When deferred (intent=upgrade/unlock) the link is issued by the
		# payment webhook instead.
		try:
			from src.services import subscriber_auth
			magic_url = subscriber_auth.magic_link_url(subscriber_auth.issue_magic_link(sub, db))
		except Exception as exc:
			magic_url = None
			logger.warning("Magic-link issuance failed for subscriber %d: %s", sub.id, exc)
		try:
			from src.services.email import send_welcome_email
			send_welcome_email(sub, magic_link_url=magic_url)
		except Exception as exc:
			logger.warning("Welcome email failed for new subscriber %d: %s", sub.id, exc)
	else:
		logger.info(
			"Welcome email deferred for subscriber=%d — caller will send post-payment",
			sub.id,
		)

	# Stage 12 — schedule the bankruptcy-alert invite (sent T+X min by the
	# invite sweep). Best-effort; never blocks signup.
	try:
		from src.services.bankruptcy_alert.invite import schedule_invite
		schedule_invite(db, sub.id)
	except Exception:
		logger.warning("Bankruptcy invite scheduling failed for subscriber %d", sub.id, exc_info=True)

	# New-lead <5-min outbound call — only fires if there's a phone to call.
	# No phone means no SLA clock and nothing for the fallback sweep to chase.
	try:
		if sub.phone:
			from src.agents.events.ingestion import publish_cora_event
			publish_cora_event({
				"event_type": "new_lead_signup",
				"subscriber_id": sub.id,
				"payload": {
					"vertical": sub.vertical,
					"county_id": sub.county_id,
					"signup_source": sub.signup_source,
				},
				"idempotency_key": f"new_lead_signup:{sub.id}",
			})
	except Exception:
		logger.warning("new_lead_signup event publish failed for subscriber %d", sub.id, exc_info=True)

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


def onboard_inbound_caller(
    phone: str,
    source: str,
    db: Session,
    *,
    zip_code: Optional[str] = None,
    vertical: Optional[str] = None,
    call_id: Optional[str] = None,
    name: Optional[str] = None,
) -> dict:
    """Provider-agnostic inbound signup core (Phases 1-3).

    Called by /webhooks/synthflow/inbound (primary) and by handle_missed_call
    (Telnyx fallback). Account-create, consent, and First Leads logic live here.

    Returns dict: subscriber_id, is_new, opt_in_created, first_leads_sent,
    lead_count, welcome_sent, capture_complete.

    Never raises — failures are logged and reflected in the return dict so
    the webhook always responds 200.
    """
    from src.services.first_leads import deliver_first_leads

    normalized = _normalize_phone(phone)

    # Detect new vs returning before create so is_new is reliable.
    is_new = True
    if normalized:
        existing = db.query(Subscriber).filter_by(phone=normalized).first()
        if existing:
            is_new = False

    sub = create_free_account(phone=phone, source=source, db=db, name=name)

    # Write captured vertical onto the subscriber; set capture_complete flag.
    capture_complete = bool(zip_code and vertical)
    if vertical:
        sub.vertical = vertical
    sub.capture_complete = capture_complete
    try:
        db.flush()
    except Exception as exc:
        logger.warning("[Onboard] flush after capture fields failed sub=%d: %s", sub.id, exc)

    # Phase 2: SmsOptIn — inbound call is express consent.
    opt_in_created = False
    try:
        from src.core.models import SmsOptIn
        from sqlalchemy import select as sa_select
        existing_opt = db.execute(
            sa_select(SmsOptIn).where(SmsOptIn.phone == normalized)
        ).scalar_one_or_none()
        if existing_opt is None and normalized:
            consent_msg = (
                f"Inbound call — caller dialed in to Forced Action DID. "
                f"call_id={call_id or 'n/a'}. "
                f"Express consent: caller initiated contact."
            )
            # Savepoint: a constraint failure rolls back only this insert,
            # not the whole transaction — session stays usable for First Leads.
            with db.begin_nested():
                db.add(SmsOptIn(
                    phone=normalized,
                    subscriber_id=sub.id,
                    source="synthflow_inbound",
                    opt_in_message=consent_msg,
                    opted_in_at=datetime.now(timezone.utc),
                ))
            opt_in_created = True
            logger.info("[Onboard] SmsOptIn created sub=%d call_id=%s", sub.id, call_id)
    except Exception as exc:
        logger.warning("[Onboard] SmsOptIn failed sub=%d: %s", sub.id, exc)

    # Phase 3: First Leads — only for newly created accounts.
    # Dedupe-resolved callers already received First Leads on their first call.
    first_leads_result = None
    if is_new:
        first_leads_result = deliver_first_leads(
            subscriber_id=sub.id,
            phone=phone,
            zip_code=zip_code,
            vertical=vertical,
            db=db,
        )

    # Welcome + signed link (transactional, independent of First Leads).
    welcome_sent = False
    try:
        if sub.event_feed_uuid and can_send(phone, db):
            from src.services.signed_links import encode_landing_token
            token = encode_landing_token(sub.id, "missed_call", ttl_hours=24)
            if token:
                landing_url = (
                    f"{settings.app_base_url}/?signup_source=missed_call&token={token}"
                )
                sub.attribution_token = token[:200]
            else:
                landing_url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"

            sms_body = _MISSED_CALL_SMS.format(url=landing_url)
            send_sms(
                to=phone,
                body=sms_body,
                db=db,
                message_type="transactional",
                subscriber_id=sub.id,
                task_type="missed_call_welcome",
            )
            welcome_sent = True
    except Exception as exc:
        logger.warning("[Onboard] welcome SMS failed sub=%d: %s", sub.id, exc)

    try:
        from src.services.subscriber_memory import append_memory_event

        append_memory_event(
            db,
            subscriber_id=sub.id,
            stream_source="SYNTHFLOW",
            event_type="voice_signup_captured",
            source_event_id=call_id or f"voice_signup:{sub.id}:{normalized}",
            source_event_name="synthflow.inbound_signup",
            occurred_at=datetime.now(timezone.utc),
            status="captured",
            summary="Subscriber captured from Synthflow inbound signup",
            channel="voice",
            actor={"type": "subscriber", "id": normalized or phone},
            call_id=call_id,
            raw={
                "zip_code": zip_code,
                "vertical": vertical,
                "capture_complete": capture_complete,
                "source": source,
            },
        )
    except Exception as exc:
        logger.warning("[Onboard] subscriber memory projection failed sub=%d: %s", sub.id, exc)

    return {
        "subscriber_id": sub.id,
        "is_new": is_new,
        "opt_in_created": opt_in_created,
        "first_leads_sent": first_leads_result.sent if first_leads_result else False,
        "lead_count": first_leads_result.lead_count if first_leads_result else 0,
        "welcome_sent": welcome_sent,
        "capture_complete": capture_complete,
    }


def handle_missed_call(from_number: str, db: Session) -> None:
    """Telnyx voice path — delegates to provider-agnostic onboard_inbound_caller.

    TwiML return removed: Telnyx uses Call Control REST, not a webhook response
    body. The Telnyx webhook handler ignores this return value.
    """
    onboard_inbound_caller(
        phone=from_number,
        source="missed_call",
        db=db,
    )
