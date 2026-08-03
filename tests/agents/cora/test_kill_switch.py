from __future__ import annotations

from src.agents.cora.kill_switch import CORA_GLOBAL_FEATURE, cora_halted
from src.core.redis_client import get_redis


def test_not_halted_by_default():
    assert cora_halted() is False


def test_halted_when_feature_override_is_red():
    get_redis().set(f"kill_switch_override:{CORA_GLOBAL_FEATURE}", "red", ex=60)
    assert cora_halted() is True


def test_halted_when_global_override_is_red():
    get_redis().set("kill_switch_override:global", "red", ex=60)
    assert cora_halted() is True


def test_not_halted_when_override_is_green():
    get_redis().set(f"kill_switch_override:{CORA_GLOBAL_FEATURE}", "green", ex=60)
    assert cora_halted() is False
