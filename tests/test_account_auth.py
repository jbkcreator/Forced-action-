"""Tests for src/services/account_auth.py — resolves a bearer token to a CustomerAccount row."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from src.services.subscriber_auth import create_access_token


def _token(subscriber_id: int = 1) -> str:
    with patch("src.services.subscriber_auth._subscriber_secret", return_value="test-key"):
        return create_access_token(subscriber_id, "feed-uuid-not-used-here")


def test_get_current_account_no_credentials():
    from src.services.account_auth import get_current_account
    db = MagicMock()
    with pytest.raises(HTTPException) as exc:
        get_current_account(credentials=None, db=db)
    assert exc.value.status_code == 401


def test_get_current_account_no_matching_account():
    from src.services.account_auth import get_current_account
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    creds = HTTPAuthorizationCredentials(scheme="bearer", credentials=_token(1))

    with patch("src.services.subscriber_auth._subscriber_secret", return_value="test-key"):
        with pytest.raises(HTTPException) as exc:
            get_current_account(credentials=creds, db=db)
    assert exc.value.status_code == 404


def test_get_current_account_churned_is_blocked():
    from src.services.account_auth import get_current_account
    db = MagicMock()
    row = MagicMock()
    row.status = "churned"
    db.execute.return_value.fetchone.return_value = row
    creds = HTTPAuthorizationCredentials(scheme="bearer", credentials=_token(1))

    with patch("src.services.subscriber_auth._subscriber_secret", return_value="test-key"):
        with pytest.raises(HTTPException) as exc:
            get_current_account(credentials=creds, db=db)
    assert exc.value.status_code == 403


def test_get_current_account_active_passes_through():
    from src.services.account_auth import get_current_account
    db = MagicMock()
    row = MagicMock()
    row.status = "active"
    db.execute.return_value.fetchone.return_value = row
    creds = HTTPAuthorizationCredentials(scheme="bearer", credentials=_token(1))

    with patch("src.services.subscriber_auth._subscriber_secret", return_value="test-key"):
        result = get_current_account(credentials=creds, db=db)
    assert result is row
