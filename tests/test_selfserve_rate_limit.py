"""WP-7 WI-4 — rate limiting. Unit-tested directly against `_rate_limited`
rather than by hammering the HTTP endpoint: a real IP-bucketed integration
test would share Redis keys with every other test using TestClient(app)
(fakeredis is in-process and TestClient always reports the same fake client
IP), so it would either need a fragile reset or contaminate unrelated tests.
A unique bucket name per test proves the same logic deterministically."""
import uuid

from src.api.selfserve_router import _rate_limited


def test_under_limit_is_not_rate_limited():
    bucket = f"test-{uuid.uuid4().hex}"
    for _ in range(5):
        assert _rate_limited(bucket, limit=5) is False


def test_exceeding_limit_is_rate_limited():
    bucket = f"test-{uuid.uuid4().hex}"
    for _ in range(5):
        _rate_limited(bucket, limit=5)
    assert _rate_limited(bucket, limit=5) is True


def test_different_buckets_are_independent():
    a, b = f"test-{uuid.uuid4().hex}", f"test-{uuid.uuid4().hex}"
    for _ in range(5):
        _rate_limited(a, limit=5)
    assert _rate_limited(a, limit=5) is True
    assert _rate_limited(b, limit=5) is False
