"""
Unit tests for subscriber feed auth (fa061 + magic-link) — no DB required.

Covers: bcrypt hash/verify, JWT issue/verify (incl. tamper/expiry/wrong-type),
random password generation, reset-token hashing, magic-link token issuance.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from jose import jwt

from src.services import subscriber_auth as auth


class TestPasswordHashing:
    def test_hash_verify_roundtrip(self):
        h = auth.hash_password("Sup3r-secret")
        assert h != "Sup3r-secret"
        assert auth.verify_password("Sup3r-secret", h) is True

    def test_wrong_password_fails(self):
        h = auth.hash_password("correct-horse")
        assert auth.verify_password("wrong", h) is False

    def test_verify_handles_garbage_hash(self):
        assert auth.verify_password("x", "not-a-bcrypt-hash") is False


class TestRandomPassword:
    def test_format_and_charset(self):
        pw = auth.generate_random_password()
        # xxx-xxx-xxx
        parts = pw.split("-")
        assert len(parts) == 3 and all(len(p) == 3 for p in parts)
        stripped = pw.replace("-", "")
        assert all(c in auth._PW_ALPHABET for c in stripped)
        # no ambiguous chars
        assert not (set("0O1lI") & set(stripped))

    def test_unique_enough(self):
        pws = {auth.generate_random_password() for _ in range(200)}
        assert len(pws) > 190  # overwhelmingly unique


class TestResetToken:
    def test_raw_differs_from_hash_and_matches_sha256(self):
        import hashlib
        raw, hashed = auth.generate_reset_token()
        assert raw != hashed
        assert hashlib.sha256(raw.encode()).hexdigest() == hashed
        assert auth.hash_reset_token(raw) == hashed


class TestAccessToken:
    def _secret(self, monkeypatch):
        # Force a known secret so we can decode in the test.
        from config.settings import get_settings
        s = get_settings()
        monkeypatch.setattr(auth, "_subscriber_secret", lambda: "unit-test-secret")
        return "unit-test-secret"

    def test_issue_and_verify(self, monkeypatch):
        secret = self._secret(monkeypatch)
        tok = auth.create_access_token(42, "feed-uuid-abc")
        payload = auth.verify_access_token(tok)
        assert payload["sub"] == "42"
        assert payload["feed_uuid"] == "feed-uuid-abc"
        assert payload["type"] == "access"

    def test_tampered_token_401(self, monkeypatch):
        self._secret(monkeypatch)
        tok = auth.create_access_token(1, "u")
        with pytest.raises(Exception) as ei:
            auth.verify_access_token(tok + "x")
        assert getattr(ei.value, "status_code", None) == 401

    def test_wrong_type_rejected(self, monkeypatch):
        secret = self._secret(monkeypatch)
        bad = jwt.encode(
            {"sub": "1", "feed_uuid": "u", "type": "refresh",
             "exp": datetime.now(timezone.utc) + timedelta(hours=1)},
            secret, algorithm="HS256",
        )
        with pytest.raises(Exception) as ei:
            auth.verify_access_token(bad)
        assert getattr(ei.value, "status_code", None) == 401

    def test_expired_token_401(self, monkeypatch):
        secret = self._secret(monkeypatch)
        expired = jwt.encode(
            {"sub": "1", "feed_uuid": "u", "type": "access",
             "exp": datetime.now(timezone.utc) - timedelta(minutes=1)},
            secret, algorithm="HS256",
        )
        with pytest.raises(Exception) as ei:
            auth.verify_access_token(expired)
        assert getattr(ei.value, "status_code", None) == 401


class TestMagicLinkToken:
    def test_raw_differs_from_hash_and_matches_sha256(self):
        raw, hashed = auth.generate_magic_link_token()
        assert raw != hashed
        assert hashlib.sha256(raw.encode()).hexdigest() == hashed
        assert auth.hash_magic_link_token(raw) == hashed

    def test_tokens_are_unique(self):
        tokens = {auth.generate_magic_link_token()[0] for _ in range(50)}
        assert len(tokens) == 50


class TestIssueMagicLink:
    def test_sets_hash_expiry_and_clears_used_at(self):
        subscriber = SimpleNamespace(
            magic_link_hash=None, magic_link_expires_at=None, magic_link_used_at="stale"
        )
        db = MagicMock()
        before = datetime.now(timezone.utc)

        raw = auth.issue_magic_link(subscriber, db)

        assert subscriber.magic_link_hash == hashlib.sha256(raw.encode()).hexdigest()
        assert subscriber.magic_link_used_at is None
        assert subscriber.magic_link_expires_at > before
        assert subscriber.magic_link_expires_at <= before + timedelta(
            minutes=auth.MAGIC_LINK_EXPIRE_MINUTES + 1
        )
        db.flush.assert_called_once()

    def test_url_contains_raw_token(self, monkeypatch):
        from config.settings import get_settings
        monkeypatch.setattr(get_settings(), "app_base_url", "https://app.example.com")
        url = auth.magic_link_url("abc123")
        assert url == "https://app.example.com/auth/verify?token=abc123"


class TestLoginRequestValidation:
    def test_requires_exactly_one_identifier(self):
        from src.api.subscriber_router import LoginRequest
        # both → invalid
        with pytest.raises(Exception):
            LoginRequest(email="a@b.com", feed_uuid="u", password="x")
        # neither → invalid
        with pytest.raises(Exception):
            LoginRequest(password="x")
        # exactly one → valid
        assert LoginRequest(email="a@b.com", password="x").email == "a@b.com"
        assert LoginRequest(feed_uuid="u", password="x").feed_uuid == "u"
