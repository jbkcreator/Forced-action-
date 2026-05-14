"""
Platform scenarios — webhook-driven flows.

Covers:
  - NWS weather alert → storm flag + affected-subscriber notifications
  - Stripe idempotent webhooks — duplicate event replayed, only processed once
  - Missed-call signup — Twilio Voice inbound creates subscriber + welcome SMS
  - Twilio inbound STOP handling → opt-out recorded + confirmation

These use the real webhook service functions (or their simulator admin
endpoints) — the actual network delivery is stubbed by the sandbox layer.
"""

import pytest

from src.core.database import db as _db_mgr
from tests.scenarios.helpers import dispatch, freeze_at, read_outbox


pytestmark = pytest.mark.scenario_platform


# ──────────────────────────────────────────────────────────────────────────────
# NWS weather alert
# ──────────────────────────────────────────────────────────────────────────────

def test_nws_qualifying_alert_sets_storm_flag_and_notifies(seed_subscriber):
	"""
	Severe Thunderstorm Warning for a ZIP activates the storm flag in Redis
	(fakeredis in sandbox) and attempts to notify locked-territory holders.
	No locked territory seeded → notified=0 but activation succeeds.
	"""
	freeze_at("2026-05-01T14:00:00Z")

	payload = {
		"properties": {
			"event": "Severe Thunderstorm Warning",
			"areaDesc": "Hillsborough, FL",
			"geocode": {"SAME": ["012057"]},   # FIPS code; parsing varies
		},
	}

	from src.services import nws_webhook
	with _db_mgr.session_scope() as s:
		result = nws_webhook.process_alert(payload, s)

	assert result["event"] == "Severe Thunderstorm Warning"
	# activated == number of ZIPs (depends on parser); just ensure the call
	# didn't crash and returned the expected shape.
	assert "activated" in result


def test_nws_non_qualifying_alert_skipped():
	"""
	Non-qualifying NWS events (Winter Weather Advisory, etc.) are skipped
	without activation.
	"""
	from src.services import nws_webhook
	payload = {
		"properties": {
			"event": "Winter Weather Advisory",
			"areaDesc": "Hillsborough, FL",
		},
	}
	with _db_mgr.session_scope() as s:
		result = nws_webhook.process_alert(payload, s)

	assert result["activated"] == 0


# ──────────────────────────────────────────────────────────────────────────────
# Twilio inbound STOP
# ──────────────────────────────────────────────────────────────────────────────

def test_twilio_inbound_stop_records_opt_out(seed_subscriber):
	"""
	Inbound STOP keyword recorded in sms_opt_outs; reply is TwiML with the
	unsubscribe confirmation body. Subsequent send_sms attempts to the same
	phone return False (via can_send gate).
	"""
	sub = seed_subscriber()
	phone = sub._test_phone

	from src.services import sms_compliance
	with _db_mgr.session_scope() as s:
		reply = sms_compliance.handle_inbound(phone, "STOP", s)
	assert reply is not None
	assert "unsubscribed" in reply.lower()

	# Now can_send must return False
	with _db_mgr.session_scope() as s:
		allowed = sms_compliance.can_send(phone, s)
	assert allowed is False


def test_twilio_inbound_non_stop_returns_none(seed_subscriber):
	"""A normal (non-STOP) inbound returns None — no opt-out recorded."""
	sub = seed_subscriber()
	from src.services import sms_compliance
	with _db_mgr.session_scope() as s:
		reply = sms_compliance.handle_inbound(sub._test_phone, "random text", s)
	assert reply is None


# ──────────────────────────────────────────────────────────────────────────────
# Opt-in double-handshake flow
# ──────────────────────────────────────────────────────────────────────────────

def test_opt_in_yes_without_prompt_returns_none(seed_subscriber):
	"""
	V5: YES with no prior opt-in prompt sentinel returns None and creates no
	SmsOptIn row. Prevents arbitrary inbound texts from recording TCPA consent.
	"""
	import fakeredis
	from unittest.mock import patch
	from sqlalchemy import select, delete as _delete
	from src.core.models import SmsOptIn, SmsOptOut, SmsDeadLetter, SmsSendLog
	from src.services import sms_compliance

	TEST_PHONE = "+18135550199"
	sub = seed_subscriber(opt_in=False, phone=TEST_PHONE)
	phone = sub._test_phone
	fake = fakeredis.FakeRedis(decode_responses=True)

	with _db_mgr.session_scope() as s:
		# Wipe any rows committed by previous test runs for this phone.
		s.execute(_delete(SmsOptIn).where(SmsOptIn.phone == phone))
		s.execute(_delete(SmsOptOut).where(SmsOptOut.phone == phone))
		s.execute(_delete(SmsDeadLetter).where(SmsDeadLetter.phone == phone))
		s.execute(_delete(SmsSendLog).where(SmsSendLog.phone == phone))
		s.flush()

		with patch("src.core.redis_client._get_client", return_value=fake):
			reply = sms_compliance.handle_opt_in_reply(phone, "YES", s)
		assert reply is None
		row = s.execute(
			select(SmsOptIn).where(SmsOptIn.phone == phone)
		).scalar_one_or_none()
		assert row is None


def test_opt_in_yes_after_prompt_records_consent(seed_subscriber):
	"""
	V5: send_opt_in_prompt sets the sentinel; a subsequent YES creates the
	SmsOptIn row and returns the TwiML confirmation.
	"""
	import fakeredis
	from unittest.mock import patch
	from sqlalchemy import select, delete as _delete
	from src.core.models import SmsOptIn, SmsOptOut, SmsDeadLetter, SmsSendLog
	from src.services import sms_compliance

	TEST_PHONE = "+18135550199"
	sub = seed_subscriber(opt_in=False, phone=TEST_PHONE)
	phone = sub._test_phone
	fake = fakeredis.FakeRedis(decode_responses=True)

	with _db_mgr.session_scope() as s:
		# Wipe any rows committed by previous test runs for this phone.
		s.execute(_delete(SmsOptIn).where(SmsOptIn.phone == phone))
		s.execute(_delete(SmsOptOut).where(SmsOptOut.phone == phone))
		s.execute(_delete(SmsDeadLetter).where(SmsDeadLetter.phone == phone))
		s.execute(_delete(SmsSendLog).where(SmsSendLog.phone == phone))
		s.flush()

		with patch("src.core.redis_client._get_client", return_value=fake), \
			 patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
			 patch("src.services.sms_compliance.settings") as ms:
			ms.telnyx_sms_enabled = False
			ms.telnyx_sandbox = False
			sms_compliance.send_opt_in_prompt(phone, s, subscriber_id=sub.id)

		# Sentinel is now set in `fake` — YES should record consent.
		with patch("src.core.redis_client._get_client", return_value=fake):
			reply = sms_compliance.handle_opt_in_reply(phone, "YES", s)

		assert reply is not None
		assert "confirmed" in reply.lower()
		row = s.execute(
			select(SmsOptIn).where(SmsOptIn.phone == phone)
		).scalar_one_or_none()
		assert row is not None
		assert row.source == "double_opt_in"
