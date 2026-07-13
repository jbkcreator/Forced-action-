"""
Stateless one-click unsubscribe token — email side of cross-channel
suppression (see docs/adr/00XX-cross-channel-suppression-block-all.md).

Signed HS256 JWT (no DB row per token, same signing pattern as
subscriber_auth's access tokens). Long expiry since it's minted once, mailed,
and must keep working for as long as the recipient still has that email.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt

from config.settings import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_ALGORITHM = "HS256"
_DEFAULT_EXPIRY = timedelta(days=365)


def _secret() -> str:
    s = get_settings()
    secret = s.subscriber_jwt_secret or s.admin_jwt_secret
    if not secret:
        raise RuntimeError(
            "Unsubscribe tokens need SUBSCRIBER_JWT_SECRET or ADMIN_JWT_SECRET configured"
        )
    return secret.get_secret_value()


def mint_unsubscribe_token(email: str, expires_in: timedelta = _DEFAULT_EXPIRY) -> str:
    exp = datetime.now(timezone.utc) + expires_in
    return jwt.encode(
        {"email": email.strip().lower(), "type": "email_unsubscribe", "exp": exp},
        _secret(),
        algorithm=_ALGORITHM,
    )


def verify_unsubscribe_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, _secret(), algorithms=[_ALGORITHM])
    except JWTError:
        return None
    if payload.get("type") != "email_unsubscribe":
        return None
    return payload.get("email")
