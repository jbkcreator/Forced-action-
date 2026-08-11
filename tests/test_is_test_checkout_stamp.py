"""Regression test: test-mode checkout stamps is_test on existing free subscriber.

PR #222 review finding: the existing-subscriber upgrade branch in
_on_checkout_completed never recomputed is_test, so a test-mode checkout
upgrading a pre-existing free account left is_test=False and polluted MRR.
"""

from __future__ import annotations

from src.utils.test_account import is_test_subscriber


def test_livemode_false_flags_test():
    assert is_test_subscriber("anyone@example.org", stripe_livemode=False) is True


def test_livemode_true_real_email_not_flagged():
    assert is_test_subscriber("customer@gmail.com", stripe_livemode=True) is False


def test_internal_email_flagged_regardless_of_livemode():
    assert is_test_subscriber("dev@heu.ai", stripe_livemode=True) is True


def test_internal_email_no_livemode_signal():
    assert is_test_subscriber("dev@heu.ai") is True


def test_real_email_no_livemode_signal_not_flagged():
    assert is_test_subscriber("buyer@realestate.com") is False


def test_none_email_livemode_false_still_flagged():
    assert is_test_subscriber(None, stripe_livemode=False) is True


def test_none_email_no_signal_not_flagged():
    assert is_test_subscriber(None) is False
