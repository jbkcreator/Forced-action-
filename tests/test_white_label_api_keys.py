"""
Unit tests for white-label API key service (Stage 12 / fa056).
Uses mock DB — no real Postgres needed.
"""

import hashlib
from unittest.mock import MagicMock, call, patch

import pytest
from fastapi import HTTPException

from src.services.white_label_api_key import (
    generate_api_key,
    revoke_api_key,
    validate_api_key,
)


# ---------------------------------------------------------------------------
# generate_api_key
# ---------------------------------------------------------------------------

def test_generated_key_format():
    db = MagicMock()
    row = MagicMock()
    row._mapping = {"id": 1, "client_id": 1, "key_prefix": "fa_wl_a1b2", "label": "Test", "is_active": True, "created_at": None}
    db.execute.return_value.fetchone.return_value = row

    raw, _ = generate_api_key(client_id=1, label="Test", created_by_id=1, db=db)
    assert raw.startswith("fa_wl_")
    assert len(raw) == 6 + 64  # "fa_wl_" + 64 hex chars


def test_generated_keys_are_unique():
    db = MagicMock()
    row = MagicMock()
    row._mapping = {"id": 1, "client_id": 1, "key_prefix": "fa_wl_a1b2", "label": "Test", "is_active": True, "created_at": None}
    db.execute.return_value.fetchone.return_value = row

    raw1, _ = generate_api_key(1, "Key 1", 1, db)
    raw2, _ = generate_api_key(1, "Key 2", 1, db)
    assert raw1 != raw2


def test_key_hash_stored_not_raw():
    db = MagicMock()
    row = MagicMock()
    row._mapping = {"id": 1, "client_id": 1, "key_prefix": "fa_wl_a1b2", "label": "Test", "is_active": True, "created_at": None}
    db.execute.return_value.fetchone.return_value = row

    raw, _ = generate_api_key(1, "Test", 1, db)
    expected_hash = hashlib.sha256(raw.encode()).hexdigest()

    # The INSERT SQL should have been called with the hash
    insert_call = str(db.execute.call_args_list[0])
    assert expected_hash in insert_call


# ---------------------------------------------------------------------------
# validate_api_key
# ---------------------------------------------------------------------------

def _make_valid_row():
    row = MagicMock()
    row.key_id = 1
    row.is_active = True
    row.requests_today = 0
    row.client_id = 1
    row.client_status = "active"
    row.api_enabled = True
    row.api_requests_per_day = 10000
    row.counties_enabled = ["hillsborough"]
    row.verticals_enabled = ["roofing"]
    row.company_name = "Test Corp"
    row.plan_tier = "standard"
    row._mapping = {
        "key_id": 1, "is_active": True, "requests_today": 0, "client_id": 1,
        "client_status": "active", "api_enabled": True, "api_requests_per_day": 10000,
        "counties_enabled": ["hillsborough"], "verticals_enabled": ["roofing"],
        "company_name": "Test Corp", "plan_tier": "standard",
    }
    return row


def test_validate_valid_key():
    raw = "fa_wl_" + "a" * 64
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = _make_valid_row()

    result = validate_api_key(raw, db)
    assert result["client_id"] == 1


def test_validate_invalid_format():
    db = MagicMock()
    with pytest.raises(HTTPException) as exc:
        validate_api_key("not_a_valid_key", db)
    assert exc.value.status_code == 401


def test_validate_not_found():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    with pytest.raises(HTTPException) as exc:
        validate_api_key("fa_wl_" + "b" * 64, db)
    assert exc.value.status_code == 401


def test_validate_revoked_key():
    db = MagicMock()
    row = _make_valid_row()
    row.is_active = False
    db.execute.return_value.fetchone.return_value = row
    with pytest.raises(HTTPException) as exc:
        validate_api_key("fa_wl_" + "c" * 64, db)
    assert exc.value.status_code == 401


def test_validate_suspended_client():
    db = MagicMock()
    row = _make_valid_row()
    row.client_status = "suspended"
    db.execute.return_value.fetchone.return_value = row
    with pytest.raises(HTTPException) as exc:
        validate_api_key("fa_wl_" + "d" * 64, db)
    assert exc.value.status_code == 403


def test_rate_limit_enforced():
    db = MagicMock()
    row = _make_valid_row()
    row.requests_today = 10000  # at the limit
    row.api_requests_per_day = 10000
    db.execute.return_value.fetchone.return_value = row
    with pytest.raises(HTTPException) as exc:
        validate_api_key("fa_wl_" + "e" * 64, db)
    assert exc.value.status_code == 429


# ---------------------------------------------------------------------------
# revoke_api_key
# ---------------------------------------------------------------------------

def test_revoke_existing_key():
    db = MagicMock()
    db.execute.return_value.rowcount = 1
    revoke_api_key(key_id=1, client_id=1, db=db)
    db.commit.assert_called_once()


def test_revoke_nonexistent_key():
    db = MagicMock()
    db.execute.return_value.rowcount = 0
    with pytest.raises(HTTPException) as exc:
        revoke_api_key(key_id=999, client_id=1, db=db)
    assert exc.value.status_code == 404
