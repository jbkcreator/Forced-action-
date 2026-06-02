"""
Unit tests for white-label auth service (Stage 12 / fa056).
Uses mock DB session — no real Postgres needed.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from src.services.white_label_auth import (
    create_access_token,
    create_refresh_token,
    generate_verification_token,
    hash_password,
    verify_access_token,
    verify_password,
)


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def test_hash_and_verify_password():
    plain = "correcthorsebattery"
    hashed = hash_password(plain)
    assert hashed != plain
    assert verify_password(plain, hashed)


def test_verify_wrong_password():
    hashed = hash_password("secret123")
    assert not verify_password("wrongpassword", hashed)


def test_hash_is_bcrypt():
    hashed = hash_password("testpassword")
    assert hashed.startswith("$2b$") or hashed.startswith("$2a$")


# ---------------------------------------------------------------------------
# JWT access tokens
# ---------------------------------------------------------------------------

def test_create_and_verify_access_token():
    with patch("src.services.white_label_auth._wl_secret", return_value="test-secret-key"):
        token = create_access_token(user_id=42, client_id=7)
        payload = verify_access_token(token)
        assert payload["sub"] == "42"
        assert payload["cid"] == 7
        assert payload["type"] == "access"


def test_verify_access_token_wrong_secret():
    with patch("src.services.white_label_auth._wl_secret", return_value="correct-secret"):
        token = create_access_token(1, 1)
    with patch("src.services.white_label_auth._wl_secret", return_value="wrong-secret"):
        with pytest.raises(HTTPException) as exc:
            verify_access_token(token)
        assert exc.value.status_code == 401


def test_verify_expired_token():
    from jose import jwt
    import time
    payload = {"sub": "1", "cid": 1, "type": "access", "exp": int(time.time()) - 10}
    with patch("src.services.white_label_auth._wl_secret", return_value="test-key"):
        token = jwt.encode(payload, "test-key", algorithm="HS256")
        with pytest.raises(HTTPException) as exc:
            verify_access_token(token)
        assert exc.value.status_code == 401


def test_verify_wrong_token_type():
    from jose import jwt
    import time
    payload = {"sub": "1", "cid": 1, "type": "refresh", "exp": int(time.time()) + 3600}
    with patch("src.services.white_label_auth._wl_secret", return_value="test-key"):
        token = jwt.encode(payload, "test-key", algorithm="HS256")
        with pytest.raises(HTTPException):
            verify_access_token(token)


# ---------------------------------------------------------------------------
# Refresh tokens
# ---------------------------------------------------------------------------

def test_create_refresh_token_format():
    raw, hashed = create_refresh_token()
    assert len(raw) > 20
    assert len(hashed) == 64  # SHA-256 hex digest
    assert hashlib.sha256(raw.encode()).hexdigest() == hashed


def test_refresh_tokens_are_unique():
    raw1, _ = create_refresh_token()
    raw2, _ = create_refresh_token()
    assert raw1 != raw2


# ---------------------------------------------------------------------------
# Verification tokens
# ---------------------------------------------------------------------------

def test_generate_verification_token():
    raw, hashed = generate_verification_token()
    assert len(raw) > 20
    assert hashlib.sha256(raw.encode()).hexdigest() == hashed


# ---------------------------------------------------------------------------
# get_current_wl_user dependency
# ---------------------------------------------------------------------------

def test_get_current_wl_user_no_credentials():
    from src.services.white_label_auth import get_current_wl_user
    db = MagicMock()
    with pytest.raises(HTTPException) as exc:
        get_current_wl_user(credentials=None, db=db)
    assert exc.value.status_code == 401


def test_get_current_wl_user_inactive():
    from src.services.white_label_auth import get_current_wl_user
    db = MagicMock()
    row = MagicMock()
    row.is_active = False
    row.client_status = "active"
    db.execute.return_value.fetchone.return_value = row

    with patch("src.services.white_label_auth._wl_secret", return_value="test-key"):
        token = create_access_token(user_id=1, client_id=1)
    creds = HTTPAuthorizationCredentials(scheme="bearer", credentials=token)

    with patch("src.services.white_label_auth._wl_secret", return_value="test-key"):
        with pytest.raises(HTTPException) as exc:
            get_current_wl_user(credentials=creds, db=db)
    assert exc.value.status_code == 403


def test_get_current_wl_user_suspended_client():
    from src.services.white_label_auth import get_current_wl_user
    db = MagicMock()
    row = MagicMock()
    row.is_active = True
    row.client_status = "suspended"
    db.execute.return_value.fetchone.return_value = row

    with patch("src.services.white_label_auth._wl_secret", return_value="test-key"):
        token = create_access_token(user_id=1, client_id=1)
    creds = HTTPAuthorizationCredentials(scheme="bearer", credentials=token)

    with patch("src.services.white_label_auth._wl_secret", return_value="test-key"):
        with pytest.raises(HTTPException) as exc:
            get_current_wl_user(credentials=creds, db=db)
    assert exc.value.status_code == 403
