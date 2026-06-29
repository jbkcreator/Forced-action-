"""Broker JWT authentication service — Layer 3C.

Issues HS256 bearer tokens for broker self-service API routes.
Falls back to ADMIN_JWT_SECRET when BROKER_JWT_SECRET is not set (dev).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.api.deps import get_db

logger = logging.getLogger(__name__)

_bearer = HTTPBearer()
_ALGORITHM = "HS256"
_ACCESS_EXPIRE_DAYS = 30
_REFRESH_EXPIRE_DAYS = 90
_TOKEN_TYPE = "broker_access"
_REFRESH_TOKEN_TYPE = "broker_refresh"


def _broker_secret() -> str:
    s = get_settings()
    secret = getattr(s, "broker_jwt_secret", None) or s.admin_jwt_secret
    if not secret:
        raise HTTPException(status_code=503, detail="Broker auth not configured")
    return secret.get_secret_value()


def create_broker_token(broker_id: str, email: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=_ACCESS_EXPIRE_DAYS)
    return jwt.encode(
        {
            "sub": str(broker_id),
            "email": email,
            "role": "broker",
            "type": _TOKEN_TYPE,
            "exp": exp,
        },
        _broker_secret(),
        algorithm=_ALGORITHM,
    )


def create_refresh_token(broker_id: str, email: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=_REFRESH_EXPIRE_DAYS)
    return jwt.encode(
        {
            "sub": str(broker_id),
            "email": email,
            "type": _REFRESH_TOKEN_TYPE,
            "exp": exp,
        },
        _broker_secret(),
        algorithm=_ALGORITHM,
    )


def verify_refresh_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, _broker_secret(), algorithms=[_ALGORITHM])
        if payload.get("type") != _REFRESH_TOKEN_TYPE:
            raise ValueError("not a refresh token")
        return payload
    except (JWTError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token") from exc


def _verify_broker_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, _broker_secret(), algorithms=[_ALGORITHM])
        if payload.get("type") != _TOKEN_TYPE:
            raise ValueError("not a broker token")
        return payload
    except (JWTError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc


def get_current_broker(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    db: Session = Depends(get_db),
) -> dict:
    """FastAPI dependency — resolves the authenticated broker from the JWT.

    Raises 401 for invalid/expired tokens, 403 for inactive brokers.
    """
    payload = _verify_broker_token(credentials.credentials)
    broker_id = payload.get("sub")
    row = db.execute(
        sa_text(
            "SELECT broker_id, email, name, is_active "
            "FROM brokers WHERE broker_id = CAST(:bid AS uuid)"
        ),
        {"bid": broker_id},
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="Broker account not found")
    if not row.is_active:
        raise HTTPException(status_code=403, detail="Broker account is inactive")
    return {
        "broker_id": str(row.broker_id),
        "email": row.email,
        "name": row.name,
    }
