"""
Tests for sandbox_outbox capture in sms_compliance.send_sms.

Verifies that when TWILIO_SANDBOX is on, a sandbox_outbox row is written
at every outcome path: opt-out suppression, dry-run, Twilio misconfig,
and live-send error.

Also tests the negative case — sandbox off → no capture.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.core.models import SandboxOutbox


@pytest.fixture
def sandbox_on(monkeypatch):
	from config.settings import settings
	monkeypatch.setattr(settings, "twilio_sandbox", True)
	monkeypatch.setattr(settings, "twilio_enabled", False)
	return settings


@pytest.fixture
def sandbox_off(monkeypatch):
	from config.settings import settings
	monkeypatch.setattr(settings, "twilio_sandbox", False)
	monkeypatch.setattr(settings, "twilio_enabled", False)
	return settings


def _fake_session(*, can_send=True):
	"""Minimal Session mock that supports the specific queries send_sms runs."""
	sess = MagicMock()

	# can_send path: session.execute(select(...).where(...)).first() returns None if nothing suppressed
	exec_result = MagicMock()
	exec_result.first.return_value = None if can_send else ("opt-out-row",)
	exec_result.scalar_one_or_none.return_value = None
	sess.execute.return_value = exec_result

	# For add_to_dead_letter path
	sess.add = MagicMock()
	sess.flush = MagicMock()
	return sess


# ──────────────────────────────────────────────────────────────────────────────
# Capture branches — sandbox ON
# ──────────────────────────────────────────────────────────────────────────────

def test_optout_suppression_captures_to_outbox(sandbox_on):
	from src.services import sms_compliance
	sess = _fake_session(can_send=False)

	result = sms_compliance.send_sms(
		to="+15555550001",
		body="test body",
		db=sess,
		subscriber_id=42,
		campaign="test_campaign",
		variant_id="a",
		decision_id="deadbeef",
	)
	assert result is False

	# Two rows were added: add_to_dead_letter + sandbox capture
	added_types = [type(c.args[0]).__name__ for c in sess.add.call_args_list]
	assert "SandboxOutbox" in added_types
	outbox_call = next(
		c for c in sess.add.call_args_list if type(c.args[0]).__name__ == "SandboxOutbox"
	)
	row = outbox_call.args[0]
	assert row.compliance_allowed is False
	assert row.compliance_reason == "opt_out"
	assert row.would_have_delivered is False
	assert row.campaign == "test_campaign"
	assert row.variant_id == "a"
	assert row.decision_id == "deadbeef"


def test_dry_run_captures_with_ok_reason(sandbox_on):
	from src.services import sms_compliance
	sess = _fake_session(can_send=True)

	result = sms_compliance.send_sms(
		to="+15555550002",
		body="dry run body",
		db=sess,
		subscriber_id=7,
		campaign="smoke",
	)
	assert result is True

	outbox_calls = [
		c for c in sess.add.call_args_list
		if type(c.args[0]).__name__ == "SandboxOutbox"
	]
	assert len(outbox_calls) == 1
	row = outbox_calls[0].args[0]
	assert row.compliance_allowed is True
	assert row.compliance_reason == "ok"
	assert row.would_have_delivered is True
	assert row.body == "dry run body"


# ──────────────────────────────────────────────────────────────────────────────
# Capture branches — sandbox OFF
# ──────────────────────────────────────────────────────────────────────────────

def test_sandbox_off_writes_no_outbox_row(sandbox_off):
	from src.services import sms_compliance
	sess = _fake_session(can_send=True)

	sms_compliance.send_sms(
		to="+15555550003",
		body="should not capture",
		db=sess,
		subscriber_id=1,
		campaign="offtest",
	)
	outbox_calls = [
		c for c in sess.add.call_args_list
		if type(c.args[0]).__name__ == "SandboxOutbox"
	]
	assert outbox_calls == []


def test_sandbox_off_optout_still_adds_dlq_only(sandbox_off):
	from src.services import sms_compliance
	sess = _fake_session(can_send=False)

	sms_compliance.send_sms(
		to="+15555550004",
		body="blocked",
		db=sess,
		subscriber_id=1,
		campaign="offtest",
	)
	# DLQ added, outbox NOT
	outbox_calls = [
		c for c in sess.add.call_args_list
		if type(c.args[0]).__name__ == "SandboxOutbox"
	]
	dlq_calls = [
		c for c in sess.add.call_args_list
		if type(c.args[0]).__name__ == "SmsDeadLetter"
	]
	assert outbox_calls == []
	assert len(dlq_calls) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Live-mode branches
# ──────────────────────────────────────────────────────────────────────────────

def test_live_mode_misconfig_captures_as_not_delivered(sandbox_on, monkeypatch):
	from config.settings import settings
	monkeypatch.setattr(settings, "twilio_enabled", True)
	monkeypatch.setattr(settings, "twilio_account_sid", None)  # not configured

	from src.services import sms_compliance
	sess = _fake_session(can_send=True)

	result = sms_compliance.send_sms(
		to="+15555550005",
		body="misconfig",
		db=sess,
		subscriber_id=9,
		campaign="misconfig_test",
	)
	assert result is False

	outbox_calls = [
		c for c in sess.add.call_args_list
		if type(c.args[0]).__name__ == "SandboxOutbox"
	]
	assert len(outbox_calls) == 1
	row = outbox_calls[0].args[0]
	assert row.compliance_allowed is True          # compliance passed
	assert row.would_have_delivered is False       # Twilio misconfigured


def test_live_mode_twilio_error_captures_as_not_delivered(sandbox_on, monkeypatch):
	from config.settings import settings
	from unittest.mock import MagicMock as MM

	secret = MM()
	secret.get_secret_value.return_value = "xxx"
	monkeypatch.setattr(settings, "twilio_enabled", True)
	monkeypatch.setattr(settings, "twilio_account_sid", "AC_x")
	monkeypatch.setattr(settings, "twilio_auth_token", secret)
	monkeypatch.setattr(settings, "twilio_from_number", "+15550000")

	# Twilio Client raises on send
	with patch("src.services.sms_compliance.Client") as mock_client_cls:
		mock_client_cls.return_value.messages.create.side_effect = RuntimeError("boom")

		from src.services import sms_compliance
		sess = _fake_session(can_send=True)

		result = sms_compliance.send_sms(
			to="+15555550006",
			body="live err",
			db=sess,
			subscriber_id=10,
			campaign="live_err",
		)
		assert result is False

	outbox_calls = [
		c for c in sess.add.call_args_list
		if type(c.args[0]).__name__ == "SandboxOutbox"
	]
	assert len(outbox_calls) == 1
	assert outbox_calls[0].args[0].would_have_delivered is False
