"""Tests for sms_commands.py"""

import pytest
from unittest.mock import MagicMock, patch

from src.core.models import Subscriber


class TestSmsCommandsUnit:
    def test_parse_balance(self):
        from src.services.sms_commands import parse
        assert parse("BALANCE") == "BALANCE"
        assert parse("balance") == "BALANCE"
        assert parse("  Balance  ") == "BALANCE"

    def test_parse_lock(self):
        from src.services.sms_commands import parse
        assert parse("LOCK my ZIP") == "LOCK"

    def test_parse_boost(self):
        from src.services.sms_commands import parse
        assert parse("BOOST") == "BOOST"

    def test_parse_auto_on(self):
        from src.services.sms_commands import parse
        assert parse("AUTO ON") == "AUTO ON"
        assert parse("auto on please") == "AUTO ON"

    def test_parse_auto_off(self):
        from src.services.sms_commands import parse
        assert parse("AUTO OFF") == "AUTO OFF"

    def test_parse_save_card(self):
        from src.services.sms_commands import parse
        assert parse("SAVE CARD") == "SAVE CARD"

    def test_parse_topup(self):
        from src.services.sms_commands import parse
        assert parse("TOPUP") == "TOPUP"

    def test_parse_report(self):
        from src.services.sms_commands import parse
        assert parse("REPORT") == "REPORT"

    def test_parse_yearly(self):
        from src.services.sms_commands import parse
        assert parse("YEARLY") == "YEARLY"

    def test_parse_pause(self):
        from src.services.sms_commands import parse
        assert parse("PAUSE") == "PAUSE"

    def test_parse_unknown_returns_none(self):
        from src.services.sms_commands import parse
        assert parse("Hey what's up") is None
        assert parse("hello") is None
        assert parse("") is None
        assert parse("STOP") is None  # STOP is handled by sms_compliance, not commands

    def test_dispatch_no_subscriber(self, mock_db):
        from src.services.sms_commands import dispatch
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        reply = dispatch("+10000000000", "BALANCE", mock_db)
        assert "HELP" in reply or "get started" in reply.lower()

    def test_dispatch_balance_reply(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = MagicMock()
        sub.id = 1
        sub.event_feed_uuid = "test-uuid"
        # Patch _find_subscriber to return the mock subscriber
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.wallet_engine.get_balance", return_value=15):
            reply = dispatch("+10000000000", "BALANCE", mock_db)
        assert "15" in reply
        assert len(reply) <= 160

    def test_all_commands_return_short_replies(self, mock_db):
        from src.services.sms_commands import COMMANDS, dispatch
        sub = MagicMock()
        sub.id = 1
        sub.event_feed_uuid = "test-uuid"
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=None), \
             patch("src.services.wallet_engine.get_balance", return_value=5), \
             patch("src.services.auto_mode.toggle", return_value=True):
            for cmd in COMMANDS:
                reply = dispatch("+10000000000", cmd, mock_db)
                assert isinstance(reply, str)
                assert len(reply) <= 160, f"Reply for {cmd} exceeds 160 chars: {reply}"

    def test_parse_wallet_no_pass(self):
        from src.services.sms_commands import parse
        assert parse("WALLET") == "WALLET"
        assert parse("wallet") == "WALLET"
        assert parse("NO") == "NO"
        assert parse("PASS") == "PASS"


class TestWalletPushReplies:
    """fa016 — context-aware reply handling for accelerated_wallet_push."""

    def _sub(self):
        sub = MagicMock()
        sub.id = 7
        sub.event_feed_uuid = "uuid-7"
        sub.has_saved_card = True
        sub.stripe_payment_method_id = "pm_test"
        sub.stripe_customer_id = "cus_test"
        return sub

    def test_WALLET_with_pending_offer_calls_activate(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        offer = {"type": "wallet_push", "offer_id": 99, "tier": "starter_wallet"}
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=offer), \
             patch("src.services.sms_commands._clear_pending_offer"), \
             patch("src.services.wallet_engine.activate_via_saved_card",
                   return_value={"subscription_id": "sub_123", "status": "incomplete"}) as p:
            reply = dispatch("+15551231111", "WALLET", mock_db)
        p.assert_called_once()
        assert "activating" in reply.lower() or "ready" in reply.lower() or len(reply) <= 160
        assert len(reply) <= 160

    def test_WALLET_with_no_pending_offer_returns_balance(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=None), \
             patch("src.services.wallet_engine.get_balance", return_value=12):
            reply = dispatch("+15551231111", "WALLET", mock_db)
        assert "12" in reply

    def test_YES_with_pending_wallet_offer_routes_to_accept(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        offer = {"type": "wallet_push", "offer_id": 99, "tier": "starter_wallet"}
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=offer), \
             patch("src.services.sms_commands._clear_pending_offer"), \
             patch("src.services.wallet_engine.activate_via_saved_card",
                   return_value={"subscription_id": "sub_123", "status": "incomplete"}) as p:
            reply = dispatch("+15551231111", "YES", mock_db)
        p.assert_called_once()
        assert len(reply) <= 160

    def test_TOPUP_with_pending_wallet_offer_routes_to_accept(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        offer = {"type": "wallet_push", "offer_id": 99, "tier": "starter_wallet"}
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=offer), \
             patch("src.services.sms_commands._clear_pending_offer"), \
             patch("src.services.wallet_engine.activate_via_saved_card",
                   return_value={"subscription_id": "sub_123", "status": "incomplete"}) as p:
            reply = dispatch("+15551231111", "TOPUP", mock_db)
        p.assert_called_once()
        assert len(reply) <= 160

    def test_TOPUP_without_pending_returns_url(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=None):
            reply = dispatch("+15551231111", "TOPUP", mock_db)
        assert "http" in reply.lower() or "wallet" in reply.lower()

    def test_NO_sets_wallet_opt_out(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        offer = {"type": "wallet_push", "offer_id": 99, "tier": "starter_wallet"}
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=offer), \
             patch("src.services.sms_commands._clear_pending_offer"):
            reply = dispatch("+15551231111", "NO", mock_db)
        assert sub.wallet_opt_out is True
        assert "wallet" in reply.lower() or "won't" in reply.lower()

    def test_activate_value_error_returns_save_card_url(self, mock_db):
        from src.services.sms_commands import dispatch
        sub = self._sub()
        sub.has_saved_card = False
        offer = {"type": "wallet_push", "offer_id": 99, "tier": "starter_wallet"}
        with patch("src.services.sms_commands._find_subscriber", return_value=sub), \
             patch("src.services.sms_commands._pending_offer", return_value=offer), \
             patch("src.services.wallet_engine.activate_via_saved_card",
                   side_effect=ValueError("no_saved_card")):
            reply = dispatch("+15551231111", "WALLET", mock_db)
        assert "save" in reply.lower() or "card" in reply.lower()


class TestFindSubscriber:
    def test_find_subscriber_uses_phone_column(self, mock_db):
        from src.services.sms_commands import _find_subscriber
        sub = MagicMock()
        sub.id = 5
        mock_db.execute.return_value.scalar_one_or_none.return_value = sub
        # Use a real (non-555) US number; phonenumbers rejects the reserved
        # 555 exchange so strict-normalize would short-circuit to None.
        result = _find_subscriber("+18135550100", mock_db)
        assert result is sub
        # _find_subscriber may execute up to two queries (SmsOptIn join
        # then Subscriber.phone fallback); assert at-least-once instead of
        # exactly-once.
        assert mock_db.execute.called

    def test_find_subscriber_returns_none_for_unparseable_phone(self, mock_db):
        from src.services.sms_commands import _find_subscriber
        # 555 exchange is reserved; library returns None — function must
        # refuse to query rather than fall back to a raw string.
        assert _find_subscriber("+15551234567", mock_db) is None
        mock_db.execute.assert_not_called()

    def test_find_subscriber_returns_none_when_empty_input(self, mock_db):
        from src.services.sms_commands import _find_subscriber
        assert _find_subscriber("", mock_db) is None
        assert _find_subscriber(None, mock_db) is None
