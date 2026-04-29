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
             patch("src.services.wallet_engine.get_balance", return_value=5), \
             patch("src.services.auto_mode.toggle", return_value=True):
            for cmd in COMMANDS:
                reply = dispatch("+10000000000", cmd, mock_db)
                assert isinstance(reply, str)
                assert len(reply) <= 160, f"Reply for {cmd} exceeds 160 chars: {reply}"
