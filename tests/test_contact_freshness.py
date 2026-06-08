from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.services.contact_freshness import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_STALE,
    apply_contact_freshness,
    compute_contact_freshness,
)


NOW = datetime(2026, 6, 8, tzinfo=timezone.utc)


def _owner(**kwargs):
    data = {
        "phone_1": "+18135550000",
        "phone_2": None,
        "phone_3": None,
        "phone_metadata": {
            "phone_1": {
                "type": "mobile",
                "score": 90,
                "reachable": True,
            }
        },
    }
    data.update(kwargs)
    return SimpleNamespace(**data)


def _contact(days_old: int, **kwargs):
    data = {
        "enriched_at": NOW - timedelta(days=days_old),
        "confidence": 0.85,
        "verification_status": "valid",
    }
    data.update(kwargs)
    return SimpleNamespace(**data)


def test_recent_mobile_contact_is_high_confidence():
    freshness = compute_contact_freshness(_owner(), _contact(30), now=NOW)
    assert freshness.level == CONFIDENCE_HIGH
    assert freshness.refresh_status == "fresh"
    assert freshness.score >= 0.8


def test_four_month_old_contact_is_medium_confidence():
    freshness = compute_contact_freshness(_owner(), _contact(120), now=NOW)
    assert freshness.level == CONFIDENCE_MEDIUM
    assert freshness.refresh_status == "fresh"


def test_seven_month_old_contact_is_low_confidence_and_due():
    freshness = compute_contact_freshness(_owner(), _contact(210), now=NOW)
    assert freshness.level == CONFIDENCE_LOW
    assert freshness.refresh_status == "due"


def test_nine_month_old_contact_is_stale():
    freshness = compute_contact_freshness(_owner(), _contact(275), now=NOW)
    assert freshness.level == CONFIDENCE_STALE
    assert freshness.refresh_status == "due"
    assert freshness.next_refresh_at == NOW


def test_missing_phone_is_stale_even_with_recent_contact():
    owner = _owner(phone_1=None, phone_metadata={})
    freshness = compute_contact_freshness(owner, _contact(10), now=NOW)
    assert freshness.level == CONFIDENCE_STALE
    assert freshness.reason == "missing_phone"


def test_recent_sms_failure_forces_stale():
    failed_at = NOW - timedelta(days=3)
    freshness = compute_contact_freshness(_owner(), _contact(30), now=NOW, last_sms_failed_at=failed_at)
    assert freshness.level == CONFIDENCE_STALE
    assert freshness.reason == "recent_sms_failure"


def test_apply_contact_freshness_stamps_owner_fields():
    owner = _owner()
    freshness = compute_contact_freshness(owner, _contact(30), now=NOW)
    apply_contact_freshness(owner, freshness)

    assert owner.contact_info_confidence == CONFIDENCE_HIGH
    assert owner.contact_info_confidence_score == pytest.approx(freshness.score)
    assert owner.contact_refresh_status == "fresh"
