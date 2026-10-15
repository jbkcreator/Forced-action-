"""Slack request signing for the lending Slack app's HTTP endpoints (interactivity and events).

Shared by every lending endpoint Slack calls, so they all verify requests the same way with
LENDING_SLACK_SIGNING_SECRET.
"""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional

SIGNATURE_TOLERANCE_SECONDS = 300


def verify_slack_signature(
    secret: str, timestamp: Optional[str], signature: Optional[str], body: bytes, *, now: Optional[float] = None,
) -> bool:
    """Slack's v0 request signature. False for a missing header, a stale timestamp or a bad digest."""
    if not secret or not timestamp or not signature:
        return False
    try:
        age = abs((now if now is not None else time.time()) - int(timestamp))
    except ValueError:
        return False
    if age > SIGNATURE_TOLERANCE_SECONDS:
        return False
    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
