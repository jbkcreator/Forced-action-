"""
Scenario test fixtures — seed + teardown + sandbox configuration.

All scenario tests require:
  - TWILIO_SANDBOX=true      → outbound captured in sandbox_outbox
  - REDIS_SANDBOX=true        → fakeredis in-memory
  - Claude mocked or real     → per-test choice

The autouse fixtures here enable the sandbox flags for the duration of the
test session and reset the clock / flags between tests so one scenario
cannot leak state into the next.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Iterator

import pytest

from config.settings import settings
from src.core import clock, redis_client
from src.core.database import db
from src.core.models import (
	AgentDecision,
	SandboxOutbox,
	SmsDeadLetter,
	SmsOptIn,
	SmsOptOut,
	Subscriber,
	UserSegment,
	WalletBalance,
	WalletTransaction,
	MessageOutcome,
)


# ──────────────────────────────────────────────────────────────────────────────
# Session-wide sandbox enablement
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def _enable_sandbox_for_session():
	"""Flip sandbox flags on for the entire scenarios test session."""
	prev_twilio_sandbox = settings.twilio_sandbox
	prev_redis_sandbox = settings.redis_sandbox
	prev_twilio_enabled = settings.twilio_enabled

	settings.twilio_sandbox = True
	settings.redis_sandbox = True
	settings.twilio_enabled = False     # dry-run path — no real Twilio calls

	redis_client.reset_client_cache()
	yield
	settings.twilio_sandbox = prev_twilio_sandbox
	settings.redis_sandbox = prev_redis_sandbox
	settings.twilio_enabled = prev_twilio_enabled
	redis_client.reset_client_cache()


# ──────────────────────────────────────────────────────────────────────────────
# Per-test state reset — clock + fakeredis contents
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _per_test_reset() -> Iterator[None]:
	"""Reset clock and flush fakeredis between scenarios."""
	clock.reset()
	# Flush fakeredis so the next test starts with a clean Redis state
	client = redis_client.get_redis()
	if client is not None:
		try:
			client.flushall()
		except Exception:
			pass
	yield
	clock.reset()


# ──────────────────────────────────────────────────────────────────────────────
# Subscriber seeding
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def seed_subscriber():
	"""
	Factory fixture — returns a callable that creates a test subscriber and
	records its id for teardown.
	"""
	created_ids: list[int] = []

	def _seed(
		*,
		email: str | None = None,
		name: str = "Scenario User",
		tier: str = "free",
		vertical: str = "roofing",
		county_id: str = "hillsborough",
		phone: str | None = None,
		has_saved_card: bool = False,
		status: str = "active",
		opt_in: bool = True,
	) -> Subscriber:
		suffix = uuid.uuid4().hex[:8]
		stripe_cust = f"cus_test_{suffix}"
		email_final = email or f"scenario_{suffix}@example.test"
		phone_final = phone or f"+155500{suffix[:5]}"

		with db.session_scope() as s:
			sub = Subscriber(
				stripe_customer_id=stripe_cust,
				tier=tier,
				vertical=vertical,
				county_id=county_id,
				email=email_final,
				name=name,
				status=status,
				has_saved_card=has_saved_card,
				event_feed_uuid=str(uuid.uuid4()),
				referral_code=f"REF{suffix[:5].upper()}",
			)
			s.add(sub)
			s.flush()
			sid = sub.id

			if opt_in:
				s.add(SmsOptIn(
					phone=phone_final,
					subscriber_id=sid,
					keyword_used="YES",
					source="double_opt_in",
					opt_in_message="scenario seed",
				))

			s.flush()

		created_ids.append(sid)

		# Re-query to return a detached but inspectable object
		with db.session_scope() as s:
			fresh = s.query(Subscriber).filter(Subscriber.id == sid).first()
			s.expunge_all()
			# Stash the phone for downstream scenario helpers
			fresh._test_phone = phone_final  # type: ignore[attr-defined]
			return fresh

	yield _seed

	# Teardown — cascading deletes. Order matters because of foreign keys.
	for sid in created_ids:
		with db.session_scope() as s:
			phone = None
			opt_in_rows = s.query(SmsOptIn).filter(SmsOptIn.subscriber_id == sid).all()
			if opt_in_rows:
				phone = opt_in_rows[0].phone

			# Child rows referencing subscriber
			s.query(SandboxOutbox).filter(SandboxOutbox.subscriber_id == sid).delete(synchronize_session=False)
			s.query(MessageOutcome).filter(MessageOutcome.subscriber_id == sid).delete(synchronize_session=False)
			s.query(AgentDecision).filter(AgentDecision.subscriber_id == sid).delete(synchronize_session=False)
			s.query(WalletTransaction).filter(WalletTransaction.subscriber_id == sid).delete(synchronize_session=False)
			s.query(WalletBalance).filter(WalletBalance.subscriber_id == sid).delete(synchronize_session=False)
			s.query(UserSegment).filter(UserSegment.subscriber_id == sid).delete(synchronize_session=False)
			s.query(SmsOptIn).filter(SmsOptIn.subscriber_id == sid).delete(synchronize_session=False)
			if phone:
				s.query(SmsOptOut).filter(SmsOptOut.phone == phone).delete(synchronize_session=False)
				s.query(SmsDeadLetter).filter(SmsDeadLetter.phone == phone).delete(synchronize_session=False)
			s.query(Subscriber).filter(Subscriber.id == sid).delete(synchronize_session=False)
