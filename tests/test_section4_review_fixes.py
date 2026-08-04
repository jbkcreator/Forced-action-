"""Regression tests for PR #183 review findings 2 & 3.

2 — send_welcome_email() must return the real send result so callers can gate
    stamp_welcome_email_sent() on it (never stamp a welcome that never sent).
3 — the deliverability monitor must only record the dedup page when the alert
    actually went out, else a failed alert silently suppresses retries.
"""
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import select

import src.services.email as email_mod
import src.tasks.email_deliverability_monitor as monitor
from src.core.models import ScraperAlertLog


def _fake_subscriber():
    return SimpleNamespace(
        id=1, email="welcome@example.com", name="Test", tier="starter",
        vertical="roofing", founding_member=False, event_feed_uuid="feed-x",
    )


def test_send_welcome_email_returns_false_when_send_fails(monkeypatch):
    """SMTP unconfigured / suppressed / send failure → send_email returns False
    → send_welcome_email must propagate False, not None."""
    monkeypatch.setattr(email_mod, "send_email", lambda **kwargs: False)
    assert email_mod.send_welcome_email(_fake_subscriber(), db=None) is False


def test_send_welcome_email_returns_true_on_success(monkeypatch):
    monkeypatch.setattr(email_mod, "send_email", lambda **kwargs: True)
    assert email_mod.send_welcome_email(_fake_subscriber(), db=None) is True


def test_send_welcome_email_returns_false_without_email(monkeypatch):
    sub = _fake_subscriber()
    sub.email = None
    assert email_mod.send_welcome_email(sub, db=None) is False


def _fake_trip(rule: str) -> monitor.Trip:
    return monitor.Trip(
        rule=rule, observed="10%", baseline="2%", threshold=">5%",
        context={"note": "test"}, tripped_at=datetime.now(timezone.utc),
    )


def test_failed_alert_is_not_recorded_as_paged(monkeypatch):
    """send_alert returns False → no ScraperAlertLog row, so the next run stays
    eligible instead of being deduped for the whole window."""
    rule = f"test_rule_{datetime.now(timezone.utc).timestamp()}"
    monkeypatch.setattr(monitor, "evaluate", lambda *a, **k: [_fake_trip(rule)])
    monkeypatch.setattr(monitor, "_is_soft_launch", lambda: False)
    monkeypatch.setattr(monitor, "send_alert", lambda *a, **k: False)

    monitor.run_and_page(county_id="hillsborough", dry_run=False)

    from src.core.database import get_db_context
    with get_db_context() as db:
        row = db.execute(
            select(ScraperAlertLog).where(ScraperAlertLog.alert_type == rule)
        ).scalar_one_or_none()
    assert row is None


def test_delivered_alert_is_recorded_as_paged(monkeypatch):
    rule = f"test_rule_ok_{datetime.now(timezone.utc).timestamp()}"
    monkeypatch.setattr(monitor, "evaluate", lambda *a, **k: [_fake_trip(rule)])
    monkeypatch.setattr(monitor, "_is_soft_launch", lambda: False)
    monkeypatch.setattr(monitor, "send_alert", lambda *a, **k: True)

    monitor.run_and_page(county_id="hillsborough", dry_run=False)

    from src.core.database import get_db_context
    with get_db_context() as db:
        row = db.execute(
            select(ScraperAlertLog).where(ScraperAlertLog.alert_type == rule)
        ).scalar_one_or_none()
    assert row is not None
