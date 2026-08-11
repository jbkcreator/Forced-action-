"""Detect internal/test subscriber accounts so they never pollute revenue reports.

Two signals:
  1. Stripe livemode=False  → account came through a test-mode Stripe key.
  2. Internal email domain   → staff testing with the LIVE key (livemode won't
     catch these, since Stripe considers real-money charges "live").

is_test is a REPORTING filter only — a flagged account still functions fully
(welcome email, lead delivery, dashboard all work). It is excluded from revenue
metrics/telemetry, Vera MRR reconciliation, and outbound win-back automation.
"""
from __future__ import annotations

from typing import Optional

_INTERNAL_EMAIL_DOMAINS = ("@heu.ai", "@example.com")


def is_test_subscriber(email: Optional[str], *, stripe_livemode: Optional[bool] = None) -> bool:
    """True if this account should be flagged is_test.

    stripe_livemode is False  → test-mode Stripe key (definitive test).
    email on an internal domain → staff account regardless of Stripe mode.
    Unknown livemode (None) alone never flags — avoids hiding real revenue.
    """
    if stripe_livemode is False:
        return True
    if email:
        e = email.lower()
        if any(domain in e for domain in _INTERNAL_EMAIL_DOMAINS):
            return True
    return False
