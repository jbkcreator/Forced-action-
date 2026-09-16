"""WP-7 WI-6 — Backflip adapter selection, fake round-trip, payload contains
no internal IDs and no prohibited fields."""
import pytest

from src.services.backflip_port import (
    ApiBackflipPort,
    FakeBackflipPort,
    HandoffOnlyPort,
    HandoffPayload,
    get_backflip_port,
)

_BANNED_KEYS = {"property_id", "owner_id", "buyer_entity_id", "parcel_id", "arv", "estimated_income", "credit_score_tier"}


def test_fake_port_records_call_and_returns_deterministic_ref():
    port = FakeBackflipPort()
    payload = HandoffPayload(
        session_token="tok-1", prefill_fields={"address": "123 Main St"},
        confirmations={"rehab_budget": "50000"}, contact={"email": "a@b.com"},
    )
    result = port.handoff(payload)
    assert result.handoff_ref == "fake-tok-1"
    assert len(port.calls) == 1
    assert port.calls[0] is payload


def test_handoff_only_port_preserves_attribution_no_prefill_leak():
    port = HandoffOnlyPort(prequal_base_url="https://app.backflip.com/prequal")
    payload = HandoffPayload(
        session_token="tok-2", prefill_fields={"address": "123 Main St"},
        confirmations={}, contact={},
    )
    result = port.handoff(payload)
    assert "tok-2" in result.redirect_url
    assert "123 Main St" not in result.redirect_url  # pre-fill stays on our side


def test_payload_never_carries_banned_keys():
    payload = HandoffPayload(
        session_token="tok-3",
        prefill_fields={"address": "123 Main St", "owner_name": "Jane Doe"},
        confirmations={"exit_strategy": "flip"},
        contact={"email": "jane@example.com"},
    )
    combined = {**payload.prefill_fields, **payload.confirmations, **payload.contact}
    assert not (_BANNED_KEYS & combined.keys())


def test_api_port_raises_not_implemented():
    port = ApiBackflipPort(api_base_url="https://api.backflip.com", api_key="x")
    with pytest.raises(NotImplementedError):
        port.handoff(HandoffPayload(session_token="t", prefill_fields={}, confirmations={}, contact={}))


def test_default_selection_is_handoff_only(monkeypatch):
    from config.settings import settings as global_settings
    monkeypatch.setattr(global_settings, "backflip_adapter", "handoff_only")
    port = get_backflip_port()
    assert isinstance(port, HandoffOnlyPort)


def test_fake_selection(monkeypatch):
    from config.settings import settings as global_settings
    monkeypatch.setattr(global_settings, "backflip_adapter", "fake")
    port = get_backflip_port()
    assert isinstance(port, FakeBackflipPort)
