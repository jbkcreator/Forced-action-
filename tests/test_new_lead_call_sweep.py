"""
Tests for the new-lead call SLA-timeout sweep —
src/tasks/new_lead_call_sweep.py.

Covers the sweep's half of verify criterion 2 (fallback fires on failure
within SLA): a stalled new-lead signup (no matching agent_decisions row)
gets both a direct page (owner_alert.notify_owner) and a Slack post
(cora_slack.post_incident_alert), exactly once, with the phone-inbound
signup path correctly excluded from ever being flagged.
"""
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import text

from src.core.models import AgentDecision, Subscriber


def _mk_sub(fresh_db, *, created_at, signup_source="landing_page", phone=None):
    uid = uuid.uuid4().hex[:8]
    phone = phone or ("813" + str(uuid.uuid4().int)[:7])
    sub = Subscriber(
        stripe_customer_id=f"cus_nlcs_{uid}", tier="free", vertical="roofing",
        county_id="hillsborough", event_feed_uuid=f"nlcs-{uid}",
        email=f"nlcs_{uid}@example.com", phone=phone, status="active",
        signup_source=signup_source, created_at=created_at,
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


def _cleanup(fresh_db, sub):
    fresh_db.execute(text("DELETE FROM owner_alert_dispatch WHERE alert_key = :key"),
                      {"key": f"new_lead_call:{sub.id}"})
    fresh_db.execute(text("DELETE FROM agent_decisions WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.delete(sub)
    fresh_db.commit()


class TestNewLeadCallSweep:
    def test_stalled_signup_fires_both_fallback_channels(self, fresh_db):
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=6))
        fresh_db.commit()

        try:
            with patch("src.tasks.new_lead_call_sweep.notify_owner") as mock_notify, \
                 patch("src.tasks.new_lead_call_sweep.post_incident_alert") as mock_slack:
                count = sweep_stalled_new_lead_calls()

            assert count >= 1
            mock_notify.assert_called_once()
            _, kwargs = mock_notify.call_args
            assert kwargs["idempotency_key"] == f"new_lead_call:{sub.id}"
            mock_slack.assert_called_once()
        finally:
            _cleanup(fresh_db, sub)

    def test_signup_with_existing_decision_row_is_not_flagged(self, fresh_db):
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=6))
        fresh_db.add(AgentDecision(
            decision_id=str(uuid.uuid4()), graph_name="new_lead_voice_call",
            subscriber_id=sub.id, event_type="new_lead_signup", terminal_status="completed",
        ))
        fresh_db.commit()

        try:
            with patch("src.tasks.new_lead_call_sweep.notify_owner") as mock_notify, \
                 patch("src.tasks.new_lead_call_sweep.post_incident_alert") as mock_slack:
                sweep_stalled_new_lead_calls()

            mock_notify.assert_not_called()
            mock_slack.assert_not_called()
        finally:
            _cleanup(fresh_db, sub)

    def test_phone_inbound_signup_is_excluded(self, fresh_db):
        """A phone-inbound signup (missed_call/cora_sms/dbpr_email) never
        fires new_lead_signup at all — the sweep must not treat it as a
        stalled call, or every phone-inbound lead would falsely alert."""
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=6),
                      signup_source="missed_call")
        fresh_db.commit()

        try:
            with patch("src.tasks.new_lead_call_sweep.notify_owner") as mock_notify, \
                 patch("src.tasks.new_lead_call_sweep.post_incident_alert") as mock_slack:
                sweep_stalled_new_lead_calls()

            mock_notify.assert_not_called()
            mock_slack.assert_not_called()
        finally:
            _cleanup(fresh_db, sub)

    def test_too_recent_signup_is_not_flagged_yet(self, fresh_db):
        """A signup 1 minute old is still inside the SLA window — must not
        be flagged before the buffer_minutes threshold."""
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=1))
        fresh_db.commit()

        try:
            with patch("src.tasks.new_lead_call_sweep.notify_owner") as mock_notify, \
                 patch("src.tasks.new_lead_call_sweep.post_incident_alert") as mock_slack:
                sweep_stalled_new_lead_calls()

            mock_notify.assert_not_called()
            mock_slack.assert_not_called()
        finally:
            _cleanup(fresh_db, sub)

    def test_already_claimed_idempotency_key_skips_second_alert(self, fresh_db):
        """Once owner_alert_dispatch has claimed the key (real notify_owner
        call already happened on a prior sweep run), a later run must not
        re-fire either channel."""
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=6))
        fresh_db.execute(
            text(
                "INSERT INTO owner_alert_dispatch (alert_key, subject, body, status) "
                "VALUES (:key, 'test', 'test', 'sms_sent')"
            ),
            {"key": f"new_lead_call:{sub.id}"},
        )
        fresh_db.commit()

        try:
            with patch("src.tasks.new_lead_call_sweep.notify_owner") as mock_notify, \
                 patch("src.tasks.new_lead_call_sweep.post_incident_alert") as mock_slack:
                count = sweep_stalled_new_lead_calls()

            assert count == 0
            mock_notify.assert_not_called()
            mock_slack.assert_not_called()
        finally:
            _cleanup(fresh_db, sub)
