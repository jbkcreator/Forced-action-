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


def test_a_feed_going_stale_during_preparation_stops_the_final_send_and_holds_the_card(monkeypatch):
    """Review F4: the GHL upsert runs after admit(); a feed crossing its limit meanwhile stops the message send."""
    from tests.lending.test_channel_breaker_rules import GOOD, PHONE, _sms

    state = {"allowed": True}
    monkeypatch.setattr(dh, "outbound_allowed", lambda channel, now=None: dh.Decision(
        channel, allowed=state["allowed"], reason=None if state["allowed"] else "GHL sync stale"))
    monkeypatch.setattr(cb, "admit", lambda channel: None)
    sender = _sms(*GOOD)
    recorder = sender._http()

    def upsert_then_stale(method, url, **kwargs):
        state["allowed"] = False  # the feed crosses its freshness limit while the contact is upserted
        return type(recorder).__call__(recorder, method, url, **kwargs)

    sender._request = upsert_then_stale
    with pytest.raises(GhlChannelPaused) as paused:
        sender(PHONE, "hi", None)
    assert [url.rsplit("/", 1)[-1] for _, url in recorder.calls] == ["upsert"]  # the message send never went
    with pytest.raises(SendHeld) as held:
        cora_agent.breaker_aware(lambda payload: (_ for _ in ()).throw(paused.value))({})
    assert held.value.held_reason == cb.HELD_SUPPRESSION_UNVERIFIED


def test_the_recheck_refuses_on_stale_feeds_without_reading_the_breaker(monkeypatch):
    monkeypatch.setattr(dh, "outbound_allowed",
                        lambda channel, now=None: dh.Decision(channel, allowed=False, reason="mailbox stale"))

    @contextmanager
    def no_breaker_read():
        raise AssertionError("breaker not read while paused")
        yield  # pragma: no cover

    monkeypatch.setattr(cb, "lending_session", no_breaker_read)
    with pytest.raises(cb.SuppressionUnverified):
        cb.recheck(cb.CHANNEL_INSTANTLY)
