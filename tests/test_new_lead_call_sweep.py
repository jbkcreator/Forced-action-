"""
Tests for the new-lead call SLA-timeout sweep —
src/tasks/new_lead_call_sweep.py.

Covers the sweep's half of verify criterion 2 (fallback fires on failure
within SLA): a stalled new-lead signup (no matching agent_decisions row)
gets both a direct page (owner_alert.notify_owner) and a Slack post
(lifecycle_slack.post_incident_alert), exactly once, with the phone-inbound
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

    def test_successful_dispatch_row_is_not_flagged(self, fresh_db):
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=6))
        fresh_db.add(AgentDecision(
            decision_id=str(uuid.uuid4()), graph_name="new_lead_voice_call",
            subscriber_id=sub.id, event_type="new_lead_signup", terminal_status="completed",
            summary={"sent": True, "call_id": "call_abc"},
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

    def test_non_dispatched_decision_rows_still_flag(self, fresh_db):
        """PR #140 issue 3: the graph writes a decision row for compliance
        aborts, hierarchy blocks, Synthflow failures and exceptions too. Those
        are NOT successful dispatches — the fallback must still page the founder.
        Only terminal_status='completed' AND summary.sent='true' suppresses."""
        from src.tasks.new_lead_call_sweep import sweep_stalled_new_lead_calls

        non_dispatch_cases = [
            ("aborted", {"sent": False, "failure_reason": "compliance:dnc_check_required"}),
            ("aborted", {"sent": False, "failure_reason": "compliance:voice_consent_required"}),
            ("failed", {"sent": False, "failure_reason": "new_lead_call:initiate_failed"}),
            # completed row but nothing actually sent (defensive)
            ("completed", {"sent": False}),
        ]

        for terminal_status, summary in non_dispatch_cases:
            sub = _mk_sub(fresh_db, created_at=datetime.now(timezone.utc) - timedelta(minutes=6))
            fresh_db.add(AgentDecision(
                decision_id=str(uuid.uuid4()), graph_name="new_lead_voice_call",
                subscriber_id=sub.id, event_type="new_lead_signup",
                terminal_status=terminal_status, summary=summary,
            ))
            fresh_db.commit()

            try:
                with patch("src.tasks.new_lead_call_sweep.notify_owner") as mock_notify, \
                     patch("src.tasks.new_lead_call_sweep.post_incident_alert") as mock_slack:
                    sweep_stalled_new_lead_calls()

                assert mock_notify.call_count == 1, f"{terminal_status}/{summary} should still page"
                assert mock_slack.call_count == 1
            finally:
                _cleanup(fresh_db, sub)

    def test_phone_inbound_signup_is_excluded(self, fresh_db):
        """A phone-inbound signup (missed_call/lifecycle_sms/dbpr_email) never
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
