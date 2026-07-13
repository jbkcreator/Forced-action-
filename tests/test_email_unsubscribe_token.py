from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from src.services.email_unsubscribe import mint_unsubscribe_token, verify_unsubscribe_token


def _settings():
    return SimpleNamespace(
        subscriber_jwt_secret=SimpleNamespace(get_secret_value=lambda: "test-secret"),
        admin_jwt_secret=None,
    )


def test_verify_returns_email_for_valid_token():
    with patch("src.services.email_unsubscribe.get_settings", return_value=_settings()):
        token = mint_unsubscribe_token("Contact@Example.com")
        assert verify_unsubscribe_token(token) == "contact@example.com"


def test_verify_returns_none_for_tampered_token():
    with patch("src.services.email_unsubscribe.get_settings", return_value=_settings()):
        token = mint_unsubscribe_token("contact@example.com")
    tampered = token[:-1] + ("a" if token[-1] != "a" else "b")
    with patch("src.services.email_unsubscribe.get_settings", return_value=_settings()):
        assert verify_unsubscribe_token(tampered) is None


def test_verify_returns_none_for_expired_token():
    with patch("src.services.email_unsubscribe.get_settings", return_value=_settings()):
        token = mint_unsubscribe_token("contact@example.com", expires_in=timedelta(seconds=-1))
        assert verify_unsubscribe_token(token) is None


def test_verify_returns_none_for_garbage_token():
    with patch("src.services.email_unsubscribe.get_settings", return_value=_settings()):
        assert verify_unsubscribe_token("not-a-real-token") is None
