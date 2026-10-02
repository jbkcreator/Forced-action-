"""Tests for WP-GL-10 ghl_messenger: Fake behaviour and factory logic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.lending.ghl_messenger import FakeGHLMessenger, MessageResult, get_messenger


class TestFakeGHLMessenger:
    def test_records_sent(self):
        m = FakeGHLMessenger()
        result = m.send_text(contact_phone="+18135550001", body="Hello")
        assert result.sent is True
        assert len(m.sent) == 1
        assert m.sent[0]["contact_phone"] == "+18135550001"

    def test_accumulates_multiple_sends(self):
        m = FakeGHLMessenger()
        m.send_text(contact_phone="+18135550001", body="A")
        m.send_text(contact_phone="+18135550002", body="B")
        assert len(m.sent) == 2

    def test_message_id_is_unique_per_send(self):
        m = FakeGHLMessenger()
        r1 = m.send_text(contact_phone="+18135550001", body="A")
        r2 = m.send_text(contact_phone="+18135550001", body="B")
        assert r1.message_id != r2.message_id


class TestGetMessenger:
    def _s(self, *, text_enabled: bool, mode: str) -> MagicMock:
        s = MagicMock()
        s.lending_text_enabled = text_enabled
        s.lending_ghl_messenger_mode = mode
        s.ghl_api_key = None
        s.ghl_location_id = None
        return s

    def test_returns_fake_when_text_not_enabled(self):
        with patch("src.lending.ghl_messenger.get_settings",
                   return_value=self._s(text_enabled=False, mode="live")):
            m = get_messenger()
        assert isinstance(m, FakeGHLMessenger)

    def test_returns_fake_when_mode_is_fake(self):
        with patch("src.lending.ghl_messenger.get_settings",
                   return_value=self._s(text_enabled=True, mode="fake")):
            m = get_messenger()
        assert isinstance(m, FakeGHLMessenger)

    def test_returns_fake_by_default(self):
        with patch("src.lending.ghl_messenger.get_settings",
                   return_value=self._s(text_enabled=False, mode="fake")):
            m = get_messenger()
        assert isinstance(m, FakeGHLMessenger)
