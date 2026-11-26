"""R1 0.6: the channel breaker's admit() refuses while opt-outs can't be verified, and a card refused that way is held
as ``suppression_unverified`` (back for approval once the feeds are fresh), not as a tripped breaker. No database.

Named test_channel_breaker_* so the lending conftest leaves the real admit() in place."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from packages.agent_core.relay import SendHeld
from src.lending import cora_agent
from src.lending import dependency_health as dh
from src.lending.dispatch import circuit_breaker as cb
from src.lending.ghl_sms import GhlChannelPaused



def test_admit_refuses_with_suppression_unverified_before_touching_the_breaker(monkeypatch):
    monkeypatch.setattr(dh, "outbound_allowed",
                        lambda channel, now=None: dh.Decision(channel, allowed=False, reason="GHL sync stale"))

    @contextmanager
    def no_breaker_read():
        raise AssertionError("breaker not read while paused")
        yield  # pragma: no cover

    monkeypatch.setattr(cb, "lending_session", no_breaker_read)
    with pytest.raises(cb.SuppressionUnverified) as paused:
        cb.admit(cb.CHANNEL_SMS)
    assert paused.value.held_reason == cb.HELD_SUPPRESSION_UNVERIFIED and "GHL sync stale" in str(paused.value)


def test_a_paused_card_is_held_as_suppression_unverified():
    def executor(payload):
        raise GhlChannelPaused(cb.SuppressionUnverified(cb.CHANNEL_SMS, "stale"))

    with pytest.raises(SendHeld) as held:
        cora_agent.breaker_aware(executor)({})
    assert held.value.held_reason == cb.HELD_SUPPRESSION_UNVERIFIED


def test_a_tripped_breaker_still_holds_as_breaker_open():
    def executor(payload):
        raise GhlChannelPaused(cb.ChannelPaused(cb.CHANNEL_SMS, "tripped"))

    with pytest.raises(SendHeld) as held:
        cora_agent.breaker_aware(executor)({})
    assert held.value.held_reason == cb.HELD_BREAKER_OPEN
