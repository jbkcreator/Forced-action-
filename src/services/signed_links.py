"""
Signed landing links — short-lived HMAC tokens for missed-call / onboarding URLs.

A subscriber arrives at the landing page via a link like:
    https://app.forcedaction.io/?signup_source=missed_call&token=eyJ...

The frontend POSTs the token to /api/landing/resolve-token; this module
verifies the signature, decodes the payload, and returns the underlying
subscriber_id so the frontend can redirect them to /dashboard/{uuid}
without forcing a re-signup.

HS256 over `settings.landing_token_secret` (falls back to admin_jwt_secret
for local-dev convenience). Tokens carry sub_id + source + expiry only — no
PII, no scopes. Stored on Subscriber.attribution_token for audit.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt

from config.settings import settings

logger = logging.getLogger(__name__)

_ALG = "HS256"


def _secret() -> Optional[str]:
    """Return the signing secret. Prefer landing_token_secret, fall back
    to admin_jwt_secret so local dev works without extra env wiring."""
    s = settings.landing_token_secret
    if s is not None:
        return s.get_secret_value()
    s = settings.admin_jwt_secret
    if s is not None:
        return s.get_secret_value()
    return None


def encode_landing_token(
    subscriber_id: int,
    source: str,
    ttl_hours: int = 24,
) -> Optional[str]:
    """Sign a token. Returns None if no secret is configured (link feature
    silently disabled rather than crashing the caller)."""
    secret = _secret()
    if not secret:
        logger.warning(
            "encode_landing_token: no LANDING_TOKEN_SECRET / ADMIN_JWT_SECRET "
            "configured — signed link disabled",
        )
        return None
    now = datetime.now(timezone.utc)
    payload = {
        "sub_id": int(subscriber_id),
        "source": source,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=ttl_hours)).timestamp()),
    }
    try:
        return jwt.encode(payload, secret, algorithm=_ALG)
    except Exception as exc:
        logger.error("encode_landing_token failed for sub=%s: %s", subscriber_id, exc)
        return None


def decode_landing_token(token: str) -> Optional[dict]:
    """Verify + decode. Returns {sub_id, source, iat, exp} on success, None on
    bad signature / expired / malformed / no secret."""
    if not token:
        return None
    secret = _secret()
    if not secret:
        return None
    try:
        payload = jwt.decode(token, secret, algorithms=[_ALG])
    except JWTError as exc:
        logger.info("decode_landing_token rejected: %s", exc)
        return None
    if "sub_id" not in payload:
        return None
    return payload


_PROMPT_KIND = "referral_prompt"


def encode_prompt_attribution_token(funnel_id: int, ttl_days: int = 90) -> Optional[str]:
    """Sign a referral-prompt attribution token binding a share link to the
    exact referral_prompt_funnel row that generated it.

    Carries only the funnel row id (no PII). Returns None if no secret is
    configured, so the caller falls back to a plain, un-attributed share URL
    instead of crashing.
    """
    secret = _secret()
    if not secret:
        logger.warning(
            "encode_prompt_attribution_token: no signing secret — "
            "share link will be un-attributed",
        )
        return None
    now = datetime.now(timezone.utc)
    payload = {
        "pf": int(funnel_id),
        "kind": _PROMPT_KIND,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=ttl_days)).timestamp()),
    }
    try:
        return jwt.encode(payload, secret, algorithm=_ALG)
    except Exception as exc:
        logger.error("encode_prompt_attribution_token failed for funnel=%s: %s", funnel_id, exc)
        return None


def decode_prompt_attribution_token(token: str) -> Optional[int]:
    """Verify + decode a referral-prompt attribution token. Returns the funnel
    row id on success, None on bad signature / expired / wrong kind / no secret.
    """
    if not token:
        return None
    secret = _secret()
    if not secret:
        return None
    try:
        payload = jwt.decode(token, secret, algorithms=[_ALG])
    except JWTError as exc:
        logger.info("decode_prompt_attribution_token rejected: %s", exc)
        return None
    if payload.get("kind") != _PROMPT_KIND:
        return None
    pf = payload.get("pf")
    try:
        return int(pf) if pf is not None else None
    except (TypeError, ValueError):
        return None
