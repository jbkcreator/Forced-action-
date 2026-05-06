"""
Auto Mode — Stage 5 — unit tests.

Run:
    pytest tests/test_auto_mode.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import MessageOutcome, Owner, Property, Subscriber, WalletBalance


# ============================================================================
# is_eligible
# ============================================================================


class TestAutoModeEligibility:
    def test_flag_on_makes_eligible(self):
        from src.services.auto_mode import is_eligible
        sub = MagicMock(auto_mode_enabled=True)
        db = MagicMock()
        db.get.return_value = sub
        assert is_eligible(1, db) is True

    def test_growth_wallet_makes_eligible(self):
        from src.services.auto_mode import is_eligible
        sub = MagicMock(auto_mode_enabled=False)
        db = MagicMock()
        db.get.return_value = sub
        wallet = WalletBalance(subscriber_id=1, wallet_tier="growth", credits_remaining=10)
        db.execute.return_value.scalar_one_or_none.return_value = wallet
        assert is_eligible(1, db) is True

    def test_power_wallet_makes_eligible(self):
        from src.services.auto_mode import is_eligible
        sub = MagicMock(auto_mode_enabled=False)
        db = MagicMock()
        db.get.return_value = sub
        wallet = WalletBalance(subscriber_id=1, wallet_tier="power", credits_remaining=10)
        db.execute.return_value.scalar_one_or_none.return_value = wallet
        assert is_eligible(1, db) is True

    def test_starter_wallet_not_eligible(self):
        from src.services.auto_mode import is_eligible
        sub = MagicMock(auto_mode_enabled=False)
        db = MagicMock()
        db.get.return_value = sub
        wallet = WalletBalance(subscriber_id=1, wallet_tier="starter_wallet", credits_remaining=10)
        db.execute.return_value.scalar_one_or_none.return_value = wallet
        assert is_eligible(1, db) is False

    def test_no_wallet_no_flag_not_eligible(self):
        from src.services.auto_mode import is_eligible
        sub = MagicMock(auto_mode_enabled=False)
        db = MagicMock()
        db.get.return_value = sub
        db.execute.return_value.scalar_one_or_none.return_value = None
        assert is_eligible(1, db) is False

    def test_missing_subscriber_not_eligible(self):
        from src.services.auto_mode import is_eligible
        db = MagicMock()
        db.get.return_value = None
        assert is_eligible(999, db) is False


# ============================================================================
# enqueue_action
# ============================================================================


class TestAutoModeEnqueueAction:
    def test_ineligible_returns_quickly(self):
        from src.services.auto_mode import enqueue_action
        with patch("src.services.auto_mode.is_eligible", return_value=False):
            db = MagicMock()
            result = enqueue_action(subscriber_id=1, property_id=42, db=db)
        assert result["eligible"] is False
        assert result["first_text_sent"] is False

    def test_no_phone_queues_skip_trace_and_returns(self):
        from src.services.auto_mode import enqueue_action
        prop = MagicMock(spec=Property)
        prop.address = "100 Main St"
        owner = MagicMock(spec=Owner)
        owner.phone_1 = None
        owner.owner_name = "Jane Doe"

        with patch("src.services.auto_mode.is_eligible", return_value=True):
            db = MagicMock()
            db.get.return_value = prop
            db.execute.return_value.scalar_one_or_none.return_value = owner
            result = enqueue_action(subscriber_id=1, property_id=42, db=db)
        assert result["eligible"] is True
        assert result["skip_trace_queued"] is True
        assert result["first_text_sent"] is False

    def test_with_phone_sends_first_text(self):
        from src.core.models import MessageOutcome
        from src.services.auto_mode import enqueue_action
        prop = MagicMock(spec=Property)
        prop.address = "100 Main St"
        owner = MagicMock(spec=Owner)
        owner.phone_1 = "+15555550100"
        owner.owner_name = "Jane Doe"

        with patch("src.services.auto_mode.is_eligible", return_value=True), \
             patch("src.services.sms_compliance.send_sms", return_value=True) as mock_send:
            db = MagicMock()
            db.get.return_value = prop
            db.execute.return_value.scalar_one_or_none.return_value = owner
            result = enqueue_action(subscriber_id=1, property_id=42, db=db)
        assert result["eligible"] is True
        assert result["first_text_sent"] is True
        # MessageOutcome row was added (id only set on real DB flush)
        added = [c[0][0] for c in db.add.call_args_list]
        assert any(isinstance(a, MessageOutcome) and a.template_id == "auto_mode_first_text" for a in added)
        # send_sms called with the owner's phone
        kwargs = mock_send.call_args.kwargs
        assert kwargs.get("to") == "+15555550100"
        assert kwargs.get("campaign") == "auto_mode_first_text"

    def test_quiet_hours_blocked_send_returns_false(self):
        """When sms_compliance.send_sms returns False (e.g. quiet hours), the
        outcome record is still created but first_text_sent is False."""
        from src.core.models import MessageOutcome
        from src.services.auto_mode import enqueue_action
        prop = MagicMock(spec=Property)
        prop.address = "100 Main St"
        owner = MagicMock(spec=Owner)
        owner.phone_1 = "+15555550100"
        owner.owner_name = "Jane Doe"

        with patch("src.services.auto_mode.is_eligible", return_value=True), \
             patch("src.services.sms_compliance.send_sms", return_value=False):
            db = MagicMock()
            db.get.return_value = prop
            db.execute.return_value.scalar_one_or_none.return_value = owner
            result = enqueue_action(subscriber_id=1, property_id=42, db=db)
        assert result["first_text_sent"] is False
        # MessageOutcome row still added — gives us audit on suppressed sends
        added = [c[0][0] for c in db.add.call_args_list]
        assert any(isinstance(a, MessageOutcome) for a in added)


# ============================================================================
# auto_mode_followup task
# ============================================================================


class TestAutoModeFollowup:
    def test_skips_replied_messages(self):
        from src.tasks.auto_mode_followup import run

        outcome = MagicMock(spec=MessageOutcome)
        outcome.id = 1
        outcome.template_id = "auto_mode_first_text"
        outcome.replied_at = datetime.now(timezone.utc)
        outcome.clicked_at = None
        outcome.subscriber_id = 1

        with patch("src.tasks.auto_mode_followup.get_db_context") as ctx_mgr:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [outcome]
            stats = run(dry_run=False)
        assert stats["skipped_replied"] == 1
        assert stats["vm_triggered"] == 0

    def test_skips_already_dispatched_vm(self):
        from src.tasks.auto_mode_followup import run

        outcome = MagicMock(spec=MessageOutcome)
        outcome.id = 1
        outcome.replied_at = None
        outcome.clicked_at = datetime.now(timezone.utc)  # already-dispatched marker
        outcome.subscriber_id = 1

        with patch("src.tasks.auto_mode_followup.get_db_context") as ctx_mgr:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [outcome]
            stats = run(dry_run=False)
        assert stats["skipped_already_done"] == 1

    def test_dry_run_does_not_trigger_vm(self):
        from src.tasks.auto_mode_followup import run

        outcome = MagicMock(spec=MessageOutcome)
        outcome.id = 1
        outcome.replied_at = None
        outcome.clicked_at = None
        outcome.subscriber_id = 1

        sub = MagicMock(spec=Subscriber)
        sub.id = 1
        sub.ghl_contact_id = "ghl_x"

        with patch("src.tasks.auto_mode_followup.get_db_context") as ctx_mgr, \
             patch("src.services.synthflow_service._apply_tags_to_contact") as mock_tag:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [outcome]
            db.get.return_value = sub
            stats = run(dry_run=True)
        assert stats["vm_triggered"] == 0
        mock_tag.assert_not_called()

    def test_triggers_vm_via_ghl_tag(self):
        from src.tasks.auto_mode_followup import run

        outcome = MagicMock(spec=MessageOutcome)
        outcome.id = 1
        outcome.replied_at = None
        outcome.clicked_at = None
        outcome.subscriber_id = 1

        sub = MagicMock(spec=Subscriber)
        sub.id = 1
        sub.ghl_contact_id = "ghl_42"

        with patch("src.tasks.auto_mode_followup.get_db_context") as ctx_mgr, \
             patch("src.services.synthflow_service._apply_tags_to_contact") as mock_tag:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [outcome]
            db.get.return_value = sub
            stats = run(dry_run=False)
        assert stats["vm_triggered"] == 1
        mock_tag.assert_called_once_with("ghl_42", ["auto_mode_vm"])
