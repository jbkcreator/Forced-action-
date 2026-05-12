"""
Unit tests for the Telnyx Ed25519 webhook signature verifier.

Covers:
  - happy path: valid signature on the raw body verifies
  - replay-attack: same signature with a timestamp >tolerance returns False
  - re-serialization regression: JSON-roundtripping the body before signing
    breaks verification (the most common implementation bug)
  - missing headers / missing public key: returns False, never raises
  - bad base64 / wrong key length: returns False, never raises
"""

from __future__ import annotations

import base64
import json
import time

import pytest

pytest.importorskip("nacl")

from nacl.signing import SigningKey  # noqa: E402

from src.services.telnyx_signature import (  # noqa: E402
    DEFAULT_TOLERANCE_SECONDS,
    verify,
)


def _sign(body: bytes, timestamp: str, signing_key: SigningKey) -> str:
    """Sign `{timestamp}|{raw_body}` and return the base64 signature."""
    message = f"{timestamp}|".encode("utf-8") + body
    return base64.b64encode(signing_key.sign(message).signature).decode("ascii")


@pytest.fixture
def keypair():
    sk = SigningKey.generate()
    pub_b64 = base64.b64encode(bytes(sk.verify_key)).decode("ascii")
    return sk, pub_b64


def test_happy_path_valid_signature_passes(keypair):
    sk, pub_b64 = keypair
    body = b'{"data":{"event_type":"message.received"}}'
    ts = str(int(time.time()))
    sig = _sign(body, ts, sk)

    assert verify(body=body, signature_b64=sig, timestamp=ts, public_key_b64=pub_b64) is True


def test_replay_attack_old_timestamp_rejected(keypair):
    sk, pub_b64 = keypair
    body = b'{"data":{"event_type":"message.received"}}'
    ts = str(int(time.time()) - (DEFAULT_TOLERANCE_SECONDS + 60))
    sig = _sign(body, ts, sk)

    # Signature is mathematically valid but timestamp is outside the window
    assert verify(body=body, signature_b64=sig, timestamp=ts, public_key_b64=pub_b64) is False


def test_reserialized_body_fails_verification(keypair):
    """
    The verifier MUST use the raw bytes Telnyx sent. JSON-roundtripping
    re-orders keys / changes whitespace and breaks the signature.
    """
    sk, pub_b64 = keypair
    original = b'{"data": {"event_type": "message.received", "id": "msg_1"}}'
    ts = str(int(time.time()))
    sig = _sign(original, ts, sk)

    # Verify with re-serialized body (canonical-form, sorted keys)
    parsed = json.loads(original)
    reserialized = json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert reserialized != original

    assert verify(body=reserialized, signature_b64=sig, timestamp=ts, public_key_b64=pub_b64) is False


def test_missing_signature_header_returns_false(keypair):
    _, pub_b64 = keypair
    body = b'{}'
    ts = str(int(time.time()))
    assert verify(body=body, signature_b64=None, timestamp=ts, public_key_b64=pub_b64) is False


def test_missing_public_key_returns_false():
    body = b'{}'
    ts = str(int(time.time()))
    assert verify(body=body, signature_b64="x", timestamp=ts, public_key_b64=None) is False


def test_non_integer_timestamp_returns_false(keypair):
    _, pub_b64 = keypair
    body = b'{}'
    assert verify(body=body, signature_b64="x", timestamp="not-a-number",
                  public_key_b64=pub_b64) is False


def test_bad_base64_signature_returns_false(keypair):
    _, pub_b64 = keypair
    body = b'{}'
    ts = str(int(time.time()))
    # Invalid base64 — non-base64 character
    assert verify(body=body, signature_b64="!!!not-base64!!!", timestamp=ts,
                  public_key_b64=pub_b64) is False


def test_signature_from_different_key_returns_false(keypair):
    _, pub_b64 = keypair
    body = b'{"data":{"event_type":"message.received"}}'
    ts = str(int(time.time()))

    # Sign with a DIFFERENT key
    other_sk = SigningKey.generate()
    sig = _sign(body, ts, other_sk)

    assert verify(body=body, signature_b64=sig, timestamp=ts, public_key_b64=pub_b64) is False
