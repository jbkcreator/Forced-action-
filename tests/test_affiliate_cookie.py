"""Phase 2 — affiliate tracking-cookie middleware.

Captures ?aff= into a persistent (not session) cookie, last-touch wins.
No DB needed — middleware only touches the cookie.
"""
from fastapi.testclient import TestClient

from src.api.main import app
from src.services.affiliate_engine import (
    AFFILIATE_COOKIE_NAME,
    AFFILIATE_COOKIE_MAX_AGE,
    AFFILIATE_REF_MAX_LEN,
)

PATH = "/openapi.json"  # always-200 route; middleware runs on every request


def _set_cookie_header(resp):
    return " ".join(resp.headers.get_list("set-cookie")).lower()


def test_ref_sets_persistent_cookie():
    client = TestClient(app)
    resp = client.get(f"{PATH}?aff=joe123")
    header = _set_cookie_header(resp)
    assert AFFILIATE_COOKIE_NAME.lower() in header
    assert f"max-age={AFFILIATE_COOKIE_MAX_AGE}" in header  # persistent, not session
    assert "httponly" in header
    assert "samesite=lax" in header
    assert client.cookies.get(AFFILIATE_COOKIE_NAME) == "joe123"


def test_no_ref_does_not_set_cookie():
    client = TestClient(app)
    resp = client.get(PATH)
    assert AFFILIATE_COOKIE_NAME.lower() not in _set_cookie_header(resp)


def test_last_touch_overwrites():
    client = TestClient(app)
    client.get(f"{PATH}?aff=affiliate_a")
    client.get(f"{PATH}?aff=affiliate_b")
    assert client.cookies.get(AFFILIATE_COOKIE_NAME) == "affiliate_b"


def test_ref_value_is_length_capped():
    client = TestClient(app)
    client.get(f"{PATH}?aff={'x' * 100}")
    assert len(client.cookies.get(AFFILIATE_COOKIE_NAME)) <= AFFILIATE_REF_MAX_LEN
