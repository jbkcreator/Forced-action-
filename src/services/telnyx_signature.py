"""
Telnyx webhook signature verification (Ed25519).

Telnyx signs every webhook with Ed25519 instead of Twilio's HMAC-SHA1.
The signed message is `f"{timestamp}|{raw_body}"` (raw bytes — do NOT
re-serialize the JSON before verifying, that's the most common bug).

Two protections layered together:
  1. Cryptographic signature verification against the account's public key
     (rotated from Mission Control → Account Settings → Keys & Credentials)
  2. Timestamp tolerance window to defend against replay attacks
     (default 300 seconds, mirrors Telnyx's documented recommendation)

The verifier never raises — it returns False on any failure so the caller
(an HTTP handler) decides whether to return 403 or log-and-continue.
"""

from __future__ import annotations

import base64
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Telnyx webhook headers (lowercased in starlette/fastapi access)
SIGNATURE_HEADER = "telnyx-signature-ed25519"
TIMESTAMP_HEADER = "telnyx-timestamp"

# Replay-attack window (seconds either side of the server clock)
DEFAULT_TOLERANCE_SECONDS = 300


def verify(
    body: bytes,
    signature_b64: Optional[str],
    timestamp: Optional[str],
    public_key_b64: Optional[str],
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
) -> bool:
    """
    Return True if a Telnyx webhook signature is valid AND the timestamp
    is within the replay-tolerance window.

    Never raises — every failure mode maps to False so the caller can
    return 403 cleanly. Failure reasons are logged at WARNING level.
    """
    if not signature_b64 or not timestamp or not public_key_b64:
        logger.warning("[telnyx_sig] missing header or public key — rejecting")
        return False

    # Replay-attack defence
    try:
        sent_at = int(timestamp)
    except (TypeError, ValueError):
        logger.warning("[telnyx_sig] non-integer timestamp %r", timestamp)
        return False

    now = int(time.time())
    if abs(now - sent_at) > tolerance_seconds:
        logger.warning(
            "[telnyx_sig] timestamp outside %ds tolerance (delta=%ds)",
            tolerance_seconds, abs(now - sent_at),
        )
        return False

    # Cryptographic check
    try:
        from nacl.signing import VerifyKey         # type: ignore
        from nacl.exceptions import BadSignatureError  # type: ignore
    except ImportError:
        logger.error("[telnyx_sig] PyNaCl is not installed — cannot verify")
        return False

    try:
        sig_bytes = base64.b64decode(signature_b64)
        key_bytes = base64.b64decode(public_key_b64)
    except Exception as exc:
        logger.warning("[telnyx_sig] base64 decode failed: %s", exc)
        return False

    # Telnyx signed message is "{timestamp}|{raw_body}".
    # Critical: use the RAW body. Re-serializing the JSON would reorder keys
    # and break verification.
    if not isinstance(body, (bytes, bytearray)):
        try:
            body = body.encode("utf-8")  # type: ignore[union-attr]
        except Exception:
            logger.warning("[telnyx_sig] body is neither bytes nor str-encodable")
            return False

    signed_message = f"{timestamp}|".encode("utf-8") + bytes(body)

    try:
        VerifyKey(key_bytes).verify(signed_message, sig_bytes)
        return True
    except BadSignatureError:
        logger.warning("[telnyx_sig] bad signature for body length=%d", len(body))
        return False
    except Exception as exc:
        logger.warning("[telnyx_sig] verification raised: %s", exc)
        return False
