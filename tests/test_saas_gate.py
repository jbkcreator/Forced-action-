"""
Unit tests for the SaaS tier gate (S3) — no DB required.

The gate's __call__ takes an already-resolved Subscriber (auth/ownership are
handled upstream by get_current_subscriber), so we exercise it by passing a
lightweight stand-in object and asserting block/allow behavior.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from src.middleware.saas_gate import SaasGate, require_active_subscriber


def _sub(status: str, sub_id: int = 1) -> SimpleNamespace:
    """Minimal Subscriber stand-in — the gate only reads .status and .id."""
    return SimpleNamespace(id=sub_id, status=status)


class TestBlockedStates:
    @pytest.mark.parametrize("status", ["churned", "cancelled"])
    def test_terminal_states_are_blocked_403(self, status):
        gate = SaasGate()
        with pytest.raises(HTTPException) as ei:
            gate(_sub(status))
        assert ei.value.status_code == 403

    def test_block_response_shape(self):
        gate = SaasGate()
        with pytest.raises(HTTPException) as ei:
            gate(_sub("churned"))
        assert ei.value.detail == {
            "error": "subscription_inactive",
            "message": "Subscription is not active",
        }


class TestAllowedStates:
    @pytest.mark.parametrize("status", ["active", "grace", "disputed", "paused"])
    def test_non_terminal_states_pass_through(self, status):
        gate = SaasGate()
        sub = _sub(status)
        # Returns the same subscriber it was given, no exception.
        assert gate(sub) is sub

    def test_unknown_status_is_allowed_by_default(self):
        # Default policy is a block-list, so an unrecognized status is NOT blocked.
        gate = SaasGate()
        sub = _sub("some_future_state")
        assert gate(sub) is sub


class TestConfigurablePolicy:
    def test_default_blocked_set(self):
        assert require_active_subscriber.blocked == frozenset({"churned", "cancelled"})

    def test_stricter_gate_can_block_extra_states(self):
        strict = SaasGate(blocked=frozenset({"churned", "cancelled", "paused"}))
        with pytest.raises(HTTPException) as ei:
            strict(_sub("paused"))
        assert ei.value.status_code == 403
        # And a state outside the custom set still passes.
        sub = _sub("active")
        assert strict(sub) is sub

    def test_module_singleton_is_a_saasgate(self):
        assert isinstance(require_active_subscriber, SaasGate)
