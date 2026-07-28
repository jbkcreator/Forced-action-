"""
Tests for the Auto Mode follow-up SMS (2nd/3rd touch) sweep —
src/tasks/auto_mode_followup_sms.py.

Covers the four sprint verify criteria:
  1. First-touch 2+ days ago, no reply -> second-touch fires.
  2. First-touch 5+ days ago, no reply -> third-touch fires (no second-touch
     double-send if it already fired).
  3. A lead that replied -> no follow-up fires. Verified two ways: a mocked
     replied_at, AND driving the real inbound-reply code path
     (lifecycle_suppression.record_generic_sms_reply) since a prior sprint item's
     mocked test missed a real nuance that only a live/real-path test caught.
  4. A lead suppressed by the compliance layer -> follow-up is suppressed
     (send_sms returns False) but the audit trail (MessageOutcome row) is
     still written, with zero changes to sms_compliance.py itself.

Uses fresh_db (real Postgres, rolled back per test) since the sweep's own
queries use Postgres-specific SQL (interval arithmetic) that SQLite can't run.
"""
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import text

from src.core.models import MessageOutcome, SmsOptIn, Subscriber


def _mk_sub(fresh_db, vertical="roofing"):
    uid = uuid.uuid4().hex[:8]
    phone = "813" + str(uuid.uuid4().int)[:7]
    sub = Subscriber(
        stripe_customer_id=f"cus_fsms_{uid}", tier="starter", vertical=vertical,
        county_id="hillsborough", event_feed_uuid=f"fsms-{uid}",
        email=f"fsms_{uid}@example.com", phone=phone, status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub, phone


def _seed_first_touch(fresh_db, sub, phone, *, sent_at):
    outcome = MessageOutcome(
        subscriber_id=sub.id, message_type="sms", template_id="auto_mode_first_text",
        channel="telnyx", sent_at=sent_at,
        context_snapshot={"phone": phone},
    )
    fresh_db.add(outcome)
    fresh_db.flush()
    fresh_db.execute(
        text(
            "INSERT INTO sms_send_logs (phone, subscriber_id, task_type, message_type, outcome, vendor, campaign, created_at) "
            "VALUES (:phone, :sid, 'auto_mode', 'marketing', 'sent', 'telnyx', 'auto_mode_first_text', :created_at)"
        ),
        {"phone": phone, "sid": sub.id, "created_at": sent_at},
    )
    fresh_db.flush()
    return outcome


def _cleanup(fresh_db, sub):
    fresh_db.execute(text("DELETE FROM sms_send_logs WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.execute(text("DELETE FROM message_outcomes WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.execute(text("DELETE FROM ab_assignments WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.execute(text("DELETE FROM sms_opt_ins WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.delete(sub)
    fresh_db.commit()


class TestAutoModeFollowupSms:
    def test_second_touch_fires_at_day_2(self, fresh_db):
        """Criterion 1."""
        from src.tasks.auto_mode_followup_sms import run

        sub, phone = _mk_sub(fresh_db)
        _seed_first_touch(fresh_db, sub, phone, sent_at=datetime.now(timezone.utc) - timedelta(days=3))
        fresh_db.commit()

        try:
            with patch("src.services.sms_compliance.send_sms", return_value=True) as mock_sms:
                stats = run(dry_run=False)
            assert stats["second_touch_sent"] >= 1

            row = fresh_db.execute(
                text("SELECT * FROM message_outcomes WHERE subscriber_id = :sid AND template_id = 'auto_mode_second_text'"),
                {"sid": sub.id},
            ).mappings().first()
            assert row is not None
            assert any(
                call.kwargs.get("campaign") == "auto_mode_second_text"
                for call in mock_sms.call_args_list
            )
        finally:
            _cleanup(fresh_db, sub)

    def test_third_touch_fires_without_double_sending_second(self, fresh_db):
        """Criterion 2."""
        from src.tasks.auto_mode_followup_sms import run

        sub, phone = _mk_sub(fresh_db)
        first_touch_sent_at = datetime.now(timezone.utc) - timedelta(days=6)
        first_touch = _seed_first_touch(fresh_db, sub, phone, sent_at=first_touch_sent_at)
        # Second-touch already fired on a prior sweep run.
        fresh_db.add(MessageOutcome(
            subscriber_id=sub.id, message_type="sms", template_id="auto_mode_second_text",
            channel="telnyx", sent_at=first_touch_sent_at + timedelta(days=2),
            context_snapshot={"first_touch_id": first_touch.id, "phone": phone},
        ))
        fresh_db.commit()

        try:
            with patch("src.services.sms_compliance.send_sms", return_value=True) as mock_sms:
                stats = run(dry_run=False)

            assert stats["third_touch_sent"] >= 1
            campaigns_sent = [call.kwargs.get("campaign") for call in mock_sms.call_args_list]
            assert "auto_mode_third_text" in campaigns_sent
            assert campaigns_sent.count("auto_mode_second_text") == 0  # not re-sent
        finally:
            _cleanup(fresh_db, sub)

    def test_replied_mocked_skips_all_touches(self, fresh_db):
        """Criterion 3 — mocked path."""
        from src.tasks.auto_mode_followup_sms import run

        sub, phone = _mk_sub(fresh_db)
        first_touch_sent_at = datetime.now(timezone.utc) - timedelta(days=6)
        _seed_first_touch(fresh_db, sub, phone, sent_at=first_touch_sent_at)
        fresh_db.add(MessageOutcome(
            subscriber_id=sub.id, message_type="sms", template_id="misc_reply_marker",
            channel="telnyx", sent_at=first_touch_sent_at + timedelta(hours=1),
            replied_at=first_touch_sent_at + timedelta(hours=2),
        ))
        fresh_db.commit()

        try:
            with patch("src.services.sms_compliance.send_sms") as mock_sms:
                run(dry_run=False)
            mock_sms.assert_not_called()
        finally:
            _cleanup(fresh_db, sub)

    def test_real_reply_webhook_path_skips_touches(self, fresh_db):
        """Criterion 3 — the real path. A mocked replied_at flag is an
        idealized assumption; the actual Telnyx inbound webhook attaches a
        reply to the newest unreplied sms MessageOutcome for the subscriber,
        not necessarily the row a sweep is evaluating. Drive the real
        lifecycle_suppression.record_generic_sms_reply() code path to prove the
        sweep's reply-check holds under real semantics."""
        from src.services.lifecycle_suppression import record_generic_sms_reply
        from src.tasks.auto_mode_followup_sms import run

        sub, phone = _mk_sub(fresh_db)
        first_touch_sent_at = datetime.now(timezone.utc) - timedelta(days=6)
        _seed_first_touch(fresh_db, sub, phone, sent_at=first_touch_sent_at)
        fresh_db.add(SmsOptIn(phone=phone, subscriber_id=sub.id, source="double_opt_in"))
        fresh_db.commit()

        try:
            # Drive the real inbound-reply path — same one the Telnyx webhook calls.
            result = record_generic_sms_reply(fresh_db, phone=phone, source_id="test-msg-id")
            fresh_db.commit()
            assert result is not None

            with patch("src.services.sms_compliance.send_sms") as mock_sms:
                run(dry_run=False)
            mock_sms.assert_not_called()
        finally:
            fresh_db.execute(text("DELETE FROM lifecycle_suppressions WHERE subscriber_id = :sid"), {"sid": sub.id})
            _cleanup(fresh_db, sub)

    def test_compliance_suppressed_still_logs_outcome(self, fresh_db):
        """Criterion 4 — send_sms returning False (quiet hours/DNC/etc,
        entirely inside sms_compliance.py, unchanged) must still leave an
        audit-trail MessageOutcome row, just undelivered."""
        from src.tasks.auto_mode_followup_sms import run

        sub, phone = _mk_sub(fresh_db)
        _seed_first_touch(fresh_db, sub, phone, sent_at=datetime.now(timezone.utc) - timedelta(days=3))
        fresh_db.commit()

        try:
            with patch("src.services.sms_compliance.send_sms", return_value=False) as mock_sms:
                run(dry_run=False)
            mock_sms.assert_called_once()

            row = fresh_db.execute(
                text("SELECT * FROM message_outcomes WHERE subscriber_id = :sid AND template_id = 'auto_mode_second_text'"),
                {"sid": sub.id},
            ).mappings().first()
            assert row is not None
            assert row["delivered_at"] is None
        finally:
            _cleanup(fresh_db, sub)

    def test_variant_arm_uses_later_thresholds(self, fresh_db):
        """A/B smoke test: control fires second-touch at day 2; a subscriber
        deterministically assigned to 'variant' should NOT fire yet at day 2
        (variant threshold is day 3)."""
        from src.services.ab_engine import ensure_followup_cadence_test
        from src.tasks.auto_mode_followup_sms import run

        # Seed enough subscribers and rely on assign_rollout_arm's deterministic
        # hash to find at least one of each arm within a small population —
        # simpler and just as valid as forcing a specific hash collision.
        subs = []
        try:
            with fresh_db.begin_nested():
                ensure_followup_cadence_test(fresh_db)
            for _ in range(12):
                sub, phone = _mk_sub(fresh_db)
                _seed_first_touch(fresh_db, sub, phone, sent_at=datetime.now(timezone.utc) - timedelta(days=2, hours=1))
                subs.append(sub)
            fresh_db.commit()

            from src.services.ab_engine import FOLLOWUP_CADENCE_TEST_NAME, assign_rollout_arm
            arms = {s.id: assign_rollout_arm(s.id, FOLLOWUP_CADENCE_TEST_NAME, fresh_db) for s in subs}
            fresh_db.commit()

            with patch("src.services.sms_compliance.send_sms", return_value=True) as mock_sms:
                run(dry_run=False)

            sent_to = set()
            for call in mock_sms.call_args_list:
                if call.kwargs.get("campaign") == "auto_mode_second_text":
                    sent_to.add(call.kwargs.get("subscriber_id"))

            control_ids = {sid for sid, arm in arms.items() if arm == "control"}
            variant_ids = {sid for sid, arm in arms.items() if arm == "variant"}
            # Every control-arm subscriber (threshold=2d, already past it) should fire.
            assert control_ids.issubset(sent_to) or not control_ids
            # No variant-arm subscriber (threshold=3d, not yet reached) should fire.
            assert not (variant_ids & sent_to)
        finally:
            for sub in subs:
                _cleanup(fresh_db, sub)
