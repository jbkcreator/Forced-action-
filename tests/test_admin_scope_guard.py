"""
Regression: the admin guard must require an explicit admin scope.

Demo tokens (scope="demo") are signed with the same secret as admin tokens,
so a signature-only check let a demo user authorize against /api/admin/*.
get_current_admin now requires scope == "admin".
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

import src.api.admin_router as admin


def _creds(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


@pytest.fixture(autouse=True)
def _fixed_secret():
    with patch("src.api.admin_router._jwt_secret", return_value="test-secret-123"):
        yield


def test_admin_token_is_accepted():
    token = admin.create_access_token({"sub": "admin", "scope": "admin"})
    claims = admin.get_current_admin(_creds(token))
    assert claims["scope"] == "admin"
    assert claims["sub"] == "admin"


def test_demo_token_is_rejected_by_admin_guard():
    # Same signer/secret as admin — this is exactly the escalation path.
    token = admin.create_access_token({"sub": "demo@x.com", "scope": "demo"})
    with pytest.raises(HTTPException) as exc:
        admin.get_current_admin(_creds(token))
    assert exc.value.status_code == 403


def test_scopeless_token_is_rejected_by_admin_guard():
    # Legacy tokens without a scope claim must not pass either.
    token = admin.create_access_token({"sub": "whoever"})
    with pytest.raises(HTTPException) as exc:
        admin.get_current_admin(_creds(token))
    assert exc.value.status_code == 403
