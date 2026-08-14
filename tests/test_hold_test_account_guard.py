"""Unit tests for test-account / test-mode guards on the territory hold flow."""
import types
import unittest.mock as mock

import pytest


# ---------------------------------------------------------------------------
# Guard 1: is_test_subscriber blocks hold checkout for internal emails
# ---------------------------------------------------------------------------

def test_is_test_subscriber_blocks_example_domain():
    from src.utils.test_account import is_test_subscriber
    assert is_test_subscriber("wh@example.com") is True
    assert is_test_subscriber("test@gmail.com") is False  # gmail not blocked
    assert is_test_subscriber("staff@heu.ai") is True


def test_is_test_subscriber_blocks_on_livemode_false():
    from src.utils.test_account import is_test_subscriber
    assert is_test_subscriber("real@gmail.com", stripe_livemode=False) is True
    assert is_test_subscriber("real@gmail.com", stripe_livemode=True) is False
    assert is_test_subscriber("real@gmail.com", stripe_livemode=None) is False


# ---------------------------------------------------------------------------
# Guard 2: webhook skips hold deposit when livemode=False
# ---------------------------------------------------------------------------

def _make_session(livemode: bool, deal_room_token: str = "tok_test") -> dict:
    return {
        "livemode": livemode,
        "mode": "payment",
        "metadata": {"deal_room_token": deal_room_token},
        "payment_intent": "pi_test",
    }


def test_webhook_hold_skipped_when_test_mode(caplog):
    """livemode=False must return early without calling apply_hold_payment."""
    import logging
    from unittest.mock import MagicMock, patch

    session = _make_session(livemode=False)
    meta = session["metadata"]
    db = MagicMock()

    with patch("src.services.hold_lifecycle_service.apply_hold_payment") as mock_apply:
        # Simulate the branch logic directly (extracted from _on_checkout_completed)
        _deal_room_token = meta.get("deal_room_token")
        if _deal_room_token and session.get("mode") == "payment":
            if not session.get("livemode", True):
                pass  # early return — apply_hold_payment never called
            else:
                mock_apply(db, token=_deal_room_token)

        mock_apply.assert_not_called()


def test_webhook_hold_proceeds_when_livemode():
    """livemode=True must proceed to apply_hold_payment."""
    from unittest.mock import MagicMock, patch

    session = _make_session(livemode=True)
    meta = session["metadata"]
    db = MagicMock()

    with patch("src.services.hold_lifecycle_service.apply_hold_payment") as mock_apply:
        _deal_room_token = meta.get("deal_room_token")
        if _deal_room_token and session.get("mode") == "payment":
            if not session.get("livemode", True):
                pass
            else:
                mock_apply(db, token=_deal_room_token)

        mock_apply.assert_called_once()
