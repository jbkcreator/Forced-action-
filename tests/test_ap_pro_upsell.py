"""
AutoPilot Pro upsell — Stage 5 — unit tests.

Run:
    pytest tests/test_ap_pro_upsell.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


class TestCloseRate:
    def test_zero_sent_returns_zero(self):
        from src.tasks.ap_pro_upsell import _close_rate
        db = MagicMock()
        # First execute() returns sent count = 0
        result = MagicMock()
        result.scalar.return_value = 0
        db.execute.return_value = result
        rate = _close_rate(1, db)
        assert rate == 0.0

    def test_close_rate_calculated_correctly(self):
        from src.tasks.ap_pro_upsell import _close_rate
        db = MagicMock()
        # Two execute calls — sent=20, deals=4 → 0.20
        results = [MagicMock(), MagicMock()]
        results[0].scalar.return_value = 20
        results[1].scalar.return_value = 4
        db.execute.side_effect = results
        rate = _close_rate(1, db)
        assert rate == 0.20


class TestRecentOfferGuard:
    def test_recent_offer_blocks(self):
        from src.tasks.ap_pro_upsell import _was_offered_recently
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = 99   # found
        assert _was_offered_recently(1, db) is True

    def test_no_recent_offer_passes(self):
        from src.tasks.ap_pro_upsell import _was_offered_recently
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        assert _was_offered_recently(1, db) is False


class TestSendOffer:
    def test_no_email_returns_false(self):
        from src.tasks.ap_pro_upsell import _send_offer
        sub = MagicMock(email=None)
        db = MagicMock()
        assert _send_offer(sub, 0.20, db) is False

    def test_email_failure_returns_false(self):
        from src.tasks.ap_pro_upsell import _send_offer
        sub = MagicMock(email="x@y.com", name="X", event_feed_uuid="u", ghl_contact_id=None, id=1)
        db = MagicMock()
        with patch("src.services.email.send_email", side_effect=Exception("smtp")):
            assert _send_offer(sub, 0.20, db) is False

    def test_email_success_records_outcome(self):
        from src.tasks.ap_pro_upsell import _send_offer
        sub = MagicMock(email="x@y.com", name="X", event_feed_uuid="u", ghl_contact_id=None, id=1)
        db = MagicMock()
        with patch("src.services.email.send_email") as mock_email:
            mock_email.return_value = None
            assert _send_offer(sub, 0.20, db) is True
        # MessageOutcome was added
        added_args = [c[0][0] for c in db.add.call_args_list]
        assert any(getattr(a, "template_id", None) == "ap_pro_upsell" for a in added_args)


class TestRunLoop:
    def test_run_filters_by_close_rate(self):
        """Subscribers below threshold should NOT be offered."""
        from src.tasks import ap_pro_upsell

        sub_low = MagicMock(id=1, tier="autopilot_lite", email="a@b.com",
                            name="A", event_feed_uuid="u1", ghl_contact_id=None)
        sub_high = MagicMock(id=2, tier="autopilot_lite", email="b@c.com",
                             name="B", event_feed_uuid="u2", ghl_contact_id=None)

        with patch.object(ap_pro_upsell, "get_db_context") as ctx_mgr, \
             patch.object(ap_pro_upsell, "_was_offered_recently", return_value=False), \
             patch.object(ap_pro_upsell, "_close_rate", side_effect=[0.05, 0.30]), \
             patch.object(ap_pro_upsell, "_send_offer", return_value=True) as mock_send:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [sub_low, sub_high]
            stats = ap_pro_upsell.run(dry_run=False)

        assert stats["checked"] == 2
        assert stats["qualified"] == 1   # only sub_high passed threshold
        assert stats["offered"] == 1
        # _send_offer called only for the high-rate subscriber
        assert mock_send.call_count == 1
        assert mock_send.call_args[0][0] is sub_high

    def test_dry_run_skips_send(self):
        from src.tasks import ap_pro_upsell
        sub = MagicMock(id=1, tier="autopilot_lite", email="a@b.com",
                        name="A", event_feed_uuid="u1", ghl_contact_id=None)
        with patch.object(ap_pro_upsell, "get_db_context") as ctx_mgr, \
             patch.object(ap_pro_upsell, "_was_offered_recently", return_value=False), \
             patch.object(ap_pro_upsell, "_close_rate", return_value=0.30), \
             patch.object(ap_pro_upsell, "_send_offer") as mock_send:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [sub]
            stats = ap_pro_upsell.run(dry_run=True)
        assert stats["qualified"] == 1
        assert stats["offered"] == 0
        mock_send.assert_not_called()

    def test_recent_offer_skips(self):
        from src.tasks import ap_pro_upsell
        sub = MagicMock(id=1, tier="autopilot_lite", email="a@b.com")
        with patch.object(ap_pro_upsell, "get_db_context") as ctx_mgr, \
             patch.object(ap_pro_upsell, "_was_offered_recently", return_value=True), \
             patch.object(ap_pro_upsell, "_close_rate", return_value=0.99), \
             patch.object(ap_pro_upsell, "_send_offer") as mock_send:
            db = MagicMock()
            ctx_mgr.return_value.__enter__.return_value = db
            db.execute.return_value.scalars.return_value.all.return_value = [sub]
            stats = ap_pro_upsell.run(dry_run=False)
        assert stats["skipped_recent"] == 1
        mock_send.assert_not_called()
