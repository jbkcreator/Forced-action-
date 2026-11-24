"""R1 0.2: staff API tokens (hash only, 90 days, revocable) and the role of a link's issuer."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from config.settings import get_settings
from src.lending.partners import hash_token
from src.lending.security.rbac import Role, role_of_issuer
from src.lending.security.staff_tokens import issue_token, list_tokens, revoke_token, token_for

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)


def test_issue_stores_hash_and_90_day_expiry(closing_db):
    raw = issue_token(closing_db, "David", Role.CALLER, "ops", NOW)
    row = closing_db.execute(text("SELECT token_hash, expires_at, role, name FROM lending.staff_api_tokens")).one()
    assert raw.startswith("ndl_") and len(raw) > 40
    assert row.token_hash == hash_token(raw) and raw not in row.token_hash
    assert row.expires_at == NOW + timedelta(days=90) and row.role == "caller" and row.name == "David"


def test_token_for_valid_expired_revoked_unknown(closing_db):
    raw = issue_token(closing_db, "Josh", Role.ADMIN, "ops", NOW)
    token = token_for(closing_db, raw, NOW + timedelta(days=89))
    assert token is not None and token.role is Role.ADMIN and token.name == "Josh"
    assert token_for(closing_db, raw, NOW + timedelta(days=90)) is None
    assert token_for(closing_db, "ndl_nope", NOW) is None and token_for(closing_db, "", NOW) is None
    assert revoke_token(closing_db, token.id, NOW) is True
    assert token_for(closing_db, raw, NOW) is None
    assert revoke_token(closing_db, token.id, NOW) is False  # already revoked


def test_list_never_shows_hash(closing_db):
    issue_token(closing_db, "Josh", Role.ADMIN, "ops", NOW)
    (row,) = list_tokens(closing_db)
    assert "token_hash" not in row and row["name"] == "Josh"


def test_role_of_issuer_slack_and_token(closing_db, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_cora_approver_user_ids", "U_JOSH", raising=False)
    admin = token_for(closing_db, issue_token(closing_db, "Josh", Role.ADMIN, "ops", NOW), NOW)
    caller = token_for(closing_db, issue_token(closing_db, "David", Role.CALLER, "ops", NOW), NOW)
    assert role_of_issuer(closing_db, "slack:U_JOSH", NOW) is Role.ADMIN
    assert role_of_issuer(closing_db, "slack:U_DAVID", NOW) is Role.CALLER
    assert role_of_issuer(closing_db, f"token:{admin.id}", NOW) is Role.ADMIN
    assert role_of_issuer(closing_db, f"token:{caller.id}", NOW) is Role.CALLER
    revoke_token(closing_db, admin.id, NOW)
    assert role_of_issuer(closing_db, f"token:{admin.id}", NOW) is Role.CALLER
    assert role_of_issuer(closing_db, "junk", NOW) is Role.CALLER
    assert role_of_issuer(closing_db, "token:abc", NOW) is Role.CALLER
