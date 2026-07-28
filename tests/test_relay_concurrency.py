"""
Real-concurrency regression tests for RELAY-v2.2 PR #179 review finding #2
(daily ceiling not enforced across concurrent sweeps) and finding #1 (a
competing sweep can overwrite an in-progress send as 'skipped').

Finding #2 needs a REAL atomic counter under REAL concurrent access to mean
anything -- a single-threaded unit test asserting reserve_daily_slot()'s
return value (already covered in test_relay_guards.py) cannot demonstrate
that two truly concurrent callers can't both slip past the ceiling. Per
CLAUDE.md's tooling rule ("Cache/rate-limit: Redis (server). Use fakeredis
in tests/sandbox."), this uses the exact sandbox mechanism
src/core/redis_client.py already ships (settings.redis_sandbox=True ->
fakeredis) rather than a live Redis server, with real Python threads racing
against the SAME fakeredis client -- fakeredis's INCR/DECR are genuinely
atomic against concurrent access from multiple threads in one process, which
is what actually exercises the guarantee.

Finding #1 is proven for real in test_relay_e2e_receipts.py against real
Postgres (see test_concurrent_claim_never_corrupts_the_winners_row there).
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.core import redis_client
from src.services.relay import guards

_NOW = datetime(2026, 7, 27, 14, 0, tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)


@pytest.fixture
def fakeredis_sandbox(monkeypatch):
    """Force src.core.redis_client onto fakeredis for this test, regardless
    of whether a real Redis is reachable in this environment, then restore
    the real client cache afterward so other tests aren't affected."""
    monkeypatch.setattr(redis_client.settings, "redis_sandbox", True)
    redis_client.reset_client_cache()
    assert redis_client.redis_available(), "fakeredis sandbox failed to initialize"
    yield
    redis_client.reset_client_cache()


def _settings(ceiling: int) -> MagicMock:
    return MagicMock(relay_send_window_timezone="America/New_York", relay_daily_ceiling=ceiling)


def test_concurrent_reservations_never_exceed_the_ceiling(fakeredis_sandbox):
    """The core PR #179 finding #2 acceptance criterion: with ceiling=5 and
    20 threads racing to reserve a slot for the same channel/day, AT MOST 5
    may succeed -- proving the reservation is a real atomic cap, not a
    per-worker local count that concurrent callers can each independently
    believe has headroom."""
    ceiling = 5
    settings = _settings(ceiling)
    results = []
    lock = threading.Lock()

    def _attempt():
        ok = guards.reserve_daily_slot("email", _NOW, settings)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=_attempt) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == ceiling
    assert results.count(False) == 20 - ceiling


def test_release_gives_the_slot_back_for_reuse(fakeredis_sandbox):
    """A refunded reservation must free up real headroom -- not just
    decrement a number that never gets checked again."""
    ceiling = 1
    settings = _settings(ceiling)

    assert guards.reserve_daily_slot("email", _NOW, settings) is True
    assert guards.reserve_daily_slot("email", _NOW, settings) is False  # at ceiling

    guards.release_daily_slot("email", _NOW, settings)

    assert guards.reserve_daily_slot("email", _NOW, settings) is True  # freed up


def test_reservations_are_isolated_per_channel(fakeredis_sandbox):
    """Two channels must not share a ceiling counter."""
    settings = _settings(1)

    assert guards.reserve_daily_slot("email", _NOW, settings) is True
    assert guards.reserve_daily_slot("sms", _NOW, settings) is True  # separate counter
    assert guards.reserve_daily_slot("email", _NOW, settings) is False  # email's own cap hit
