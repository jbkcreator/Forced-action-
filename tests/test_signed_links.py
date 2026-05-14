"""fa017 — signed_links HMAC token round-trip + tamper / expiry rejection."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _seed_secret(monkeypatch):
    """Provide a deterministic secret so tests don't depend on .env state."""
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "landing_token_secret", SecretStr("unit-test-secret-do-not-use-in-prod"))
    yield


class TestSignedLinksRoundTrip:
    def test_encode_decode_round_trip(self):
        from src.services.signed_links import encode_landing_token, decode_landing_token
        tok = encode_landing_token(4509, "missed_call")
        assert tok and isinstance(tok, str)
        payload = decode_landing_token(tok)
        assert payload is not None
        assert payload["sub_id"] == 4509
        assert payload["source"] == "missed_call"
        assert "exp" in payload

    def test_decode_returns_none_on_empty_token(self):
        from src.services.signed_links import decode_landing_token
        assert decode_landing_token("") is None
        assert decode_landing_token(None) is None  # type: ignore[arg-type]

    def test_decode_rejects_tampered_signature(self):
        from src.services.signed_links import encode_landing_token, decode_landing_token
        tok = encode_landing_token(123, "missed_call")
        assert tok is not None
        # Flip a character in the signature segment (last `.`-separated part)
        head, sig = tok.rsplit(".", 1)
        bad_char = "A" if sig[0] != "A" else "B"
        tampered = f"{head}.{bad_char}{sig[1:]}"
        assert decode_landing_token(tampered) is None

    def test_decode_rejects_expired_token(self):
        from src.services.signed_links import encode_landing_token, decode_landing_token

        past = datetime.now(timezone.utc) - timedelta(hours=48)
        with patch("src.services.signed_links.datetime") as fake_dt:
            # Make encode_landing_token think "now" was 48h ago so the
            # default 24h TTL is already expired.
            fake_dt.now.return_value = past
            fake_dt.timezone = timezone
            tok = encode_landing_token(7, "missed_call", ttl_hours=24)
        assert tok is not None
        # Decoding "now" should reject it.
        assert decode_landing_token(tok) is None
