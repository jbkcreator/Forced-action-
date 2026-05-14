"""
Unit tests for src/services/opt_in_sentinel.

All tests run against fakeredis — no real Redis or Postgres required.

Run:
    pytest tests/test_opt_in_sentinel.py -v
"""

import pytest
import fakeredis
from unittest.mock import patch

from src.services.opt_in_sentinel import consume_pending, mark_pending, _key, _TTL_SECONDS

PHONE = "+18135550100"


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture(autouse=True)
def inject_redis(fake_redis):
    """Route all redis_client calls through a fresh fakeredis instance."""
    with patch("src.core.redis_client._get_client", return_value=fake_redis):
        yield


# ── mark_pending ──────────────────────────────────────────────────────────────


class TestMarkPending:
    def test_sets_key(self, fake_redis):
        mark_pending(PHONE)
        assert fake_redis.get(_key(PHONE)) == "1"

    def test_sets_ttl(self, fake_redis):
        mark_pending(PHONE)
        ttl = fake_redis.ttl(_key(PHONE))
        assert 0 < ttl <= _TTL_SECONDS

    def test_empty_phone_noop(self, fake_redis):
        mark_pending("")
        assert fake_redis.keys("*") == []

    def test_none_phone_noop(self, fake_redis):
        mark_pending(None)
        assert fake_redis.keys("*") == []

    def test_overwrite_extends_ttl(self, fake_redis):
        """Calling mark_pending twice resets the TTL."""
        fake_redis.setex(_key(PHONE), 5, "1")  # low TTL
        mark_pending(PHONE)
        ttl = fake_redis.ttl(_key(PHONE))
        assert ttl > 5


# ── consume_pending ───────────────────────────────────────────────────────────


class TestConsumePending:
    def test_returns_true_when_key_exists(self, fake_redis):
        mark_pending(PHONE)
        assert consume_pending(PHONE) is True

    def test_deletes_key_after_consume(self, fake_redis):
        mark_pending(PHONE)
        consume_pending(PHONE)
        assert fake_redis.get(_key(PHONE)) is None

    def test_returns_false_without_mark(self):
        assert consume_pending(PHONE) is False

    def test_double_consume_second_is_false(self, fake_redis):
        mark_pending(PHONE)
        assert consume_pending(PHONE) is True
        assert consume_pending(PHONE) is False

    def test_empty_phone_returns_false(self):
        assert consume_pending("") is False

    def test_none_phone_returns_false(self):
        assert consume_pending(None) is False

    def test_different_phones_isolated(self, fake_redis):
        mark_pending(PHONE)
        assert consume_pending("+18135550199") is False  # different number
        assert consume_pending(PHONE) is True            # original still valid


# ── Redis-unavailable degradation ────────────────────────────────────────────


class TestNoRedis:
    def test_mark_pending_noop_when_unavailable(self):
        with patch("src.core.redis_client._get_client", return_value=None):
            mark_pending(PHONE)  # must not raise

    def test_consume_pending_returns_false_when_unavailable(self):
        with patch("src.core.redis_client._get_client", return_value=None):
            assert consume_pending(PHONE) is False


# ── GETDEL fallback path ──────────────────────────────────────────────────────


class TestGetdelFallback:
    """Exercise the except-branch fallback for Redis < 6.2."""

    def test_fallback_returns_true_when_key_exists(self, fake_redis):
        fake_redis.set(_key(PHONE), "1")

        def _broken_getdel(key):
            raise AttributeError("getdel not supported")

        fake_redis.getdel = _broken_getdel
        with patch("src.core.redis_client._get_client", return_value=fake_redis):
            assert consume_pending(PHONE) is True

    def test_fallback_deletes_key(self, fake_redis):
        fake_redis.set(_key(PHONE), "1")

        def _broken_getdel(key):
            raise AttributeError("getdel not supported")

        fake_redis.getdel = _broken_getdel
        with patch("src.core.redis_client._get_client", return_value=fake_redis):
            consume_pending(PHONE)
        assert fake_redis.get(_key(PHONE)) is None

    def test_fallback_returns_false_when_key_absent(self, fake_redis):
        def _broken_getdel(key):
            raise AttributeError("getdel not supported")

        fake_redis.getdel = _broken_getdel
        with patch("src.core.redis_client._get_client", return_value=fake_redis):
            assert consume_pending(PHONE) is False
