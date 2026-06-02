"""
White-label API key generation, validation, and rate limiting (Stage 12 / fa056).

Key format:  fa_wl_{secrets.token_hex(32)}
Storage:     key_prefix (first 12 chars) + SHA-256 hash of full key
Lookup:      by prefix → compare hash
Rate limit:  requests_today counter on WhiteLabelApiKey row, reset nightly via
             src/tasks/reset_wl_api_counters.py (or cron SQL UPDATE).
"""

import hashlib
import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, Header, HTTPException
from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.core.database import get_db

logger = logging.getLogger(__name__)

_KEY_PREFIX_LEN = 12  # chars to store for display (e.g. "fa_wl_a1b2c3")


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------

def generate_api_key(client_id: int, label: str, created_by_id: Optional[int], db) -> tuple[str, dict]:
    """
    Create a new API key for a white-label client.
    Returns (raw_key_string, key_row_dict).
    Raw key is shown ONCE and never retrievable again.
    """
    raw_key = f"fa_wl_{secrets.token_hex(32)}"
    prefix = raw_key[:_KEY_PREFIX_LEN]
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    row = db.execute(
        sa_text("""
            INSERT INTO white_label_api_keys
                   (client_id, key_prefix, key_hash, label, is_active, created_by,
                    requests_today, total_requests, created_at)
            VALUES (:client_id, :prefix, :hash, :label, true, :created_by,
                    0, 0, now())
            RETURNING id, client_id, key_prefix, label, is_active, created_at
        """),
        {
            "client_id": client_id,
            "prefix": prefix,
            "hash": key_hash,
            "label": label,
            "created_by": created_by_id,
        },
    ).fetchone()
    db.commit()

    logger.info("[wl_api_key] generated key %s for client %d", prefix, client_id)
    return raw_key, dict(row._mapping)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_api_key(raw_key: str, db) -> dict:
    """
    Validate an incoming API key and return the associated client row.
    Raises HTTP 401 on invalid key; HTTP 403 on inactive client/key.
    Also increments usage counters (requests_today, total_requests).
    """
    if not raw_key or not raw_key.startswith("fa_wl_"):
        raise HTTPException(status_code=401, detail="Invalid API key format")

    prefix = raw_key[:_KEY_PREFIX_LEN]
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    row = db.execute(
        sa_text("""
            SELECT k.id AS key_id, k.is_active, k.requests_today,
                   c.id AS client_id, c.status AS client_status,
                   c.api_enabled, c.api_requests_per_day,
                   c.counties_enabled, c.verticals_enabled,
                   c.company_name, c.plan_tier, c.trial_ends_at
              FROM white_label_api_keys k
              JOIN white_label_clients c ON c.id = k.client_id
             WHERE k.key_prefix = :prefix AND k.key_hash = :hash
        """),
        {"prefix": prefix, "hash": key_hash},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if not row.is_active:
        raise HTTPException(status_code=401, detail="API key has been revoked")
    if row.client_status != "active":
        raise HTTPException(status_code=403, detail="Company account is not active")
    if not row.api_enabled:
        raise HTTPException(status_code=403, detail="API access is not enabled for this account")

    # Enforce daily request limit
    limit = row.api_requests_per_day
    if row.requests_today >= limit:
        raise HTTPException(
            status_code=429,
            detail=f"Daily API limit of {limit:,} requests reached. Resets at midnight UTC.",
        )

    # Increment counters (fire-and-forget; non-critical if this fails)
    try:
        db.execute(
            sa_text("""
                UPDATE white_label_api_keys
                   SET requests_today = requests_today + 1,
                       total_requests  = total_requests  + 1,
                       last_used_at    = now()
                 WHERE id = :key_id
            """),
            {"key_id": row.key_id},
        )
        db.commit()
    except Exception as exc:
        logger.warning("[wl_api_key] failed to update counters for key %s: %s", prefix, exc)

    return dict(row._mapping)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def get_api_key_client(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    db=Depends(get_db),
) -> dict:
    """
    FastAPI dependency for API-key–authenticated endpoints.
    Returns the client info dict or raises 401/403/429.
    """
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    return validate_api_key(x_api_key, db)


# ---------------------------------------------------------------------------
# Revocation
# ---------------------------------------------------------------------------

def revoke_api_key(key_id: int, client_id: int, db) -> None:
    """Mark a key as revoked. Idempotent."""
    result = db.execute(
        sa_text("""
            UPDATE white_label_api_keys
               SET is_active = false, revoked_at = now()
             WHERE id = :key_id AND client_id = :client_id
        """),
        {"key_id": key_id, "client_id": client_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="API key not found")
    logger.info("[wl_api_key] revoked key %d for client %d", key_id, client_id)
