"""
V5 pass-condition tests — Opt-in sentinel state machine.

Each test maps to one named pass condition:

  PC1  send_opt_in_prompt creates opt_in_pending:{phone}
  PC2  YES with sentinel creates exactly one SmsOptIn row
  PC3  YES without sentinel creates zero SmsOptIn rows
  PC4  Sentinel consumed after first YES (second YES → no row)
  PC5  START / JOIN / SUBSCRIBE / UNSTOP follow the same rule
  PC6  STOP behavior not broken by V5
  PC7  opt-in prompt uses message_type="opt_in_prompt"
  PC8  SmsSendLog records the prompt send

Run:
    pytest tests/test_v5_pass_conditions.py -v
"""

import pytest
import fakeredis
from unittest.mock import patch
from sqlalchemy import delete, func, select

from src.core.models import SmsDeadLetter, SmsOptIn, SmsOptOut, SmsSendLog
from src.services import sms_compliance
from src.services.opt_in_sentinel import _key, _TTL_SECONDS

PHONE = "+18135550100"


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture(autouse=True)
def inject_redis(fake_redis):
    with patch("src.core.redis_client._get_client", return_value=fake_redis):
        yield


@pytest.fixture(autouse=True)
def clean_phone(fresh_db):
    """
    Delete any committed compliance rows for PHONE before each test.
    The scratch scripts we ran committed real opt-out / DLQ rows for this
    number; without cleanup those rows bleed into fresh_db's savepoint view.
    The deletes are rolled back along with the savepoint after each test.
    """
    fresh_db.execute(delete(SmsOptOut).where(SmsOptOut.phone == PHONE))
    fresh_db.execute(delete(SmsDeadLetter).where(SmsDeadLetter.phone == PHONE))
    fresh_db.execute(delete(SmsOptIn).where(SmsOptIn.phone == PHONE))
    fresh_db.flush()


def _dry_run_send(db, phone, body, **kwargs):
    """Call send_sms in dry-run mode (no Telnyx, quiet hours off)."""
    with patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
         patch("src.services.sms_compliance.settings") as ms:
        ms.telnyx_sms_enabled = False
        ms.telnyx_sandbox = False
        return sms_compliance.send_sms(phone, body, db, **kwargs)


def _send_prompt(db, phone=PHONE):
    """Fire send_opt_in_prompt in dry-run mode and return the result."""
    with patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
         patch("src.services.sms_compliance.settings") as ms:
        ms.telnyx_sms_enabled = False
        ms.telnyx_sandbox = False
        return sms_compliance.send_opt_in_prompt(phone, db)


# ── PC1: send_opt_in_prompt creates opt_in_pending:{phone} ───────────────────


class TestPC1SentinelCreated:
    def test_sentinel_key_exists_after_prompt(self, fresh_db, fake_redis):
        """PC1 — opt_in_pending:{phone} must exist immediately after send_opt_in_prompt."""
        _send_prompt(fresh_db)
        assert fake_redis.get(_key(PHONE)) == "1"

    def test_sentinel_has_ttl_of_15_minutes(self, fresh_db, fake_redis):
        """PC1 — sentinel TTL must be 15 minutes (900 s)."""
        _send_prompt(fresh_db)
        ttl = fake_redis.ttl(_key(PHONE))
        assert 0 < ttl <= _TTL_SECONDS

    def test_already_opted_in_does_not_set_sentinel(self, fresh_db, fake_redis):
        """PC1 inverse — no sentinel when number already has consent."""
        sms_compliance.record_opt_in(PHONE, "YES", "double_opt_in", fresh_db)
        _send_prompt(fresh_db)
        assert fake_redis.get(_key(PHONE)) is None

    def test_opted_out_phone_cleans_sentinel_on_failed_send(self, fresh_db, fake_redis):
        """PC1 — if send is blocked (opted-out), sentinel is cleaned up."""
        sms_compliance.record_opt_out(PHONE, "STOP", "inbound_sms", fresh_db)
        _send_prompt(fresh_db)
        assert fake_redis.get(_key(PHONE)) is None


# ── PC2: YES with sentinel → exactly one SmsOptIn row ────────────────────────


class TestPC2YesWithSentinel:
    def test_creates_exactly_one_opt_in_row(self, fresh_db, fake_redis):
        """PC2 — one SmsOptIn row after prompt → YES."""
        _send_prompt(fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        rows = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.phone == PHONE)
        ).scalars().all()
        assert len(rows) == 1

    def test_opt_in_row_source_is_double_opt_in(self, fresh_db, fake_redis):
        """PC2 — source must be 'double_opt_in'."""
        _send_prompt(fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        row = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.phone == PHONE)
        ).scalar_one()
        assert row.source == "double_opt_in"

    def test_twiml_confirmation_returned(self, fresh_db, fake_redis):
        """PC2 — caller gets TwiML confirmation reply."""
        _send_prompt(fresh_db)
        reply = sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        assert reply is not None
        assert "<Response>" in reply
        assert "confirmed" in reply.lower()


# ── PC3: YES without sentinel → zero SmsOptIn rows ───────────────────────────


class TestPC3YesWithoutSentinel:
    def test_no_opt_in_row_created(self, fresh_db):
        """PC3 — arbitrary YES (no prior prompt) must not record consent."""
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        row = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.phone == PHONE)
        ).scalar_one_or_none()
        assert row is None

    def test_returns_none(self, fresh_db):
        """PC3 — handle_opt_in_reply returns None (falls through to sms_commands)."""
        result = sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        assert result is None

    def test_no_new_dlq_row_created(self, fresh_db):
        """PC3 — no-sentinel YES is not an error; no DLQ row should be added."""
        before = fresh_db.execute(
            select(func.count(SmsDeadLetter.id)).where(SmsDeadLetter.phone == PHONE)
        ).scalar()
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        after = fresh_db.execute(
            select(func.count(SmsDeadLetter.id)).where(SmsDeadLetter.phone == PHONE)
        ).scalar()
        assert after == before


# ── PC4: Sentinel consumed after first YES ────────────────────────────────────


class TestPC4SentinelConsumed:
    def test_sentinel_gone_after_yes(self, fresh_db, fake_redis):
        """PC4 — key deleted from Redis after successful opt-in."""
        _send_prompt(fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        assert fake_redis.get(_key(PHONE)) is None

    def test_second_yes_returns_none(self, fresh_db, fake_redis):
        """PC4 — replay YES returns None (sentinel gone)."""
        _send_prompt(fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        result = sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        assert result is None

    def test_second_yes_creates_no_extra_row(self, fresh_db, fake_redis):
        """PC4 — total opt-in rows stays at exactly 1 after replay."""
        _send_prompt(fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, "YES", fresh_db)
        rows = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.phone == PHONE)
        ).scalars().all()
        assert len(rows) == 1


# ── PC5: START / JOIN / SUBSCRIBE / UNSTOP follow the same rule ──────────────


class TestPC5AltKeywords:
    @pytest.mark.parametrize("keyword", ["START", "JOIN", "SUBSCRIBE", "UNSTOP"])
    def test_keyword_with_sentinel_creates_row(self, keyword, fresh_db, fake_redis):
        """PC5 — each opt-in keyword works when sentinel is present."""
        _send_prompt(fresh_db)
        reply = sms_compliance.handle_opt_in_reply(PHONE, keyword, fresh_db)
        assert reply is not None
        row = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.phone == PHONE)
        ).scalar_one_or_none()
        assert row is not None

    @pytest.mark.parametrize("keyword", ["START", "JOIN", "SUBSCRIBE", "UNSTOP"])
    def test_keyword_without_sentinel_creates_no_row(self, keyword, fresh_db):
        """PC5 — each opt-in keyword blocked without sentinel."""
        result = sms_compliance.handle_opt_in_reply(PHONE, keyword, fresh_db)
        assert result is None
        row = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.phone == PHONE)
        ).scalar_one_or_none()
        assert row is None

    @pytest.mark.parametrize("keyword", ["START", "JOIN", "SUBSCRIBE", "UNSTOP"])
    def test_keyword_consumes_sentinel(self, keyword, fresh_db, fake_redis):
        """PC5 — sentinel consumed by alt keywords just like YES."""
        _send_prompt(fresh_db)
        sms_compliance.handle_opt_in_reply(PHONE, keyword, fresh_db)
        assert fake_redis.get(_key(PHONE)) is None


# ── PC6: STOP behavior not broken ────────────────────────────────────────────


class TestPC6StopUnbroken:
    def test_stop_records_opt_out(self, fresh_db):
        """PC6 — STOP still writes SmsOptOut row."""
        sms_compliance.handle_inbound(PHONE, "STOP", fresh_db)
        row = fresh_db.execute(
            select(SmsOptOut).where(SmsOptOut.phone == PHONE)
        ).scalar_one_or_none()
        assert row is not None

    def test_stop_returns_twiml(self, fresh_db):
        """PC6 — STOP still returns TwiML unsubscribe confirmation."""
        reply = sms_compliance.handle_inbound(PHONE, "STOP", fresh_db)
        assert reply is not None
        assert "unsubscribed" in reply.lower()

    def test_stop_blocks_subsequent_send(self, fresh_db):
        """PC6 — can_send returns False after STOP."""
        sms_compliance.handle_inbound(PHONE, "STOP", fresh_db)
        assert sms_compliance.can_send(PHONE, fresh_db) is False

    def test_stop_does_not_touch_sentinel(self, fresh_db, fake_redis):
        """PC6 — STOP leaves any existing opt_in_pending key untouched."""
        _send_prompt(fresh_db)
        assert fake_redis.get(_key(PHONE)) == "1"
        # Re-clean the opt-out that send_opt_in_prompt's dry-run send would
        # NOT have created (the send succeeded), but STOP is about to add one.
        sms_compliance.handle_inbound(PHONE, "STOP", fresh_db)
        # Sentinel must still be alive; STOP has no Redis side-effects.
        assert fake_redis.get(_key(PHONE)) == "1"

    @pytest.mark.parametrize("kw", ["UNSUBSCRIBE", "CANCEL", "QUIT", "END"])
    def test_all_stop_keywords_still_work(self, kw, fresh_db):
        """PC6 — all STOP variants still route through handle_inbound correctly."""
        reply = sms_compliance.handle_inbound(PHONE, kw, fresh_db)
        assert reply is not None
        assert sms_compliance.can_send(PHONE, fresh_db) is False


# ── PC7: opt-in prompt uses message_type="opt_in_prompt" ─────────────────────


class TestPC7MessageType:
    def test_prompt_bypasses_opt_in_gate(self, fresh_db, fake_redis):
        """PC7 — send_opt_in_prompt must succeed even with no SmsOptIn row
        (i.e. message_type='opt_in_prompt' bypasses the opt-in gate)."""
        result = _send_prompt(fresh_db)
        assert result is True

    def test_sms_send_log_records_opt_in_prompt_type(self, fresh_db, fake_redis):
        """PC7 — SmsSendLog.message_type == 'opt_in_prompt' for the prompt send."""
        _send_prompt(fresh_db)
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == PHONE)
        ).scalar_one_or_none()
        assert row is not None
        assert row.message_type == "opt_in_prompt"

    def test_send_sms_with_marketing_blocked_without_opt_in(self, fresh_db):
        """PC7 inverse — marketing type correctly blocked (opt-in gate still works)."""
        result = _dry_run_send(fresh_db, PHONE, "Buy now", message_type="marketing")
        assert result is False

    def test_send_sms_with_opt_in_prompt_passes_gate(self, fresh_db):
        """PC7 — opt_in_prompt type passes the opt-in gate directly."""
        result = _dry_run_send(fresh_db, PHONE, "Reply YES", message_type="opt_in_prompt")
        assert result is True


# ── PC8: SmsSendLog records the prompt send ───────────────────────────────────


class TestPC8AuditLog:
    def test_one_send_log_row_written(self, fresh_db, fake_redis):
        """PC8 — exactly one SmsSendLog row per send_opt_in_prompt call."""
        _send_prompt(fresh_db)
        rows = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == PHONE)
        ).scalars().all()
        assert len(rows) == 1

    def test_send_log_outcome_is_dry_run(self, fresh_db, fake_redis):
        """PC8 — dry-run outcome recorded correctly."""
        _send_prompt(fresh_db)
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == PHONE)
        ).scalar_one()
        assert row.outcome == "dry_run"

    def test_send_log_task_type_recorded(self, fresh_db, fake_redis):
        """PC8 — task_type='tcpa_opt_in_prompt' on the log row."""
        _send_prompt(fresh_db)
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == PHONE)
        ).scalar_one()
        assert row.task_type == "tcpa_opt_in_prompt"

    def test_send_log_body_preview_contains_prompt(self, fresh_db, fake_redis):
        """PC8 — body_preview captures the opt-in prompt text."""
        _send_prompt(fresh_db)
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == PHONE)
        ).scalar_one()
        assert row.body_preview is not None
        assert "YES" in row.body_preview

    def test_suppressed_prompt_still_writes_log(self, fresh_db, fake_redis):
        """PC8 — log row written even when send is suppressed (opted-out number)."""
        sms_compliance.record_opt_out(PHONE, "STOP", "inbound_sms", fresh_db)
        _send_prompt(fresh_db)
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == PHONE)
        ).scalar_one_or_none()
        assert row is not None
        assert row.outcome == "suppressed"
        assert row.suppress_reason == "opt_out"
