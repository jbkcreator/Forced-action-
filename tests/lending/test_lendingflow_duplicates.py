"""Runbook § 1, Principle 3 ("duplicates beat silence") for LendingFlow intake: a delivery that matches an
existing lead on only one key is raised to a person through Cora's inbox; an exact re-send stays quiet."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.lending import lendingflow

NOW = datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)
HASH = "a" * 64


class FakeDb:
    def __init__(self, existing) -> None:
        self._existing = existing

    def execute(self, statement, params=None):
        return SimpleNamespace(one_or_none=lambda: self._existing)


@pytest.fixture
def raised(monkeypatch):
    events = []
    monkeypatch.setattr(lendingflow, "add_to_inbox_safely", lambda db, **event: events.append(event) or 1)
    return events


def _existing(vendor: str = "LF-1", digest: str = HASH):
    return SimpleNamespace(id=7, lead_uuid="u-7", vendor_lead_id=vendor, dedupe_hash=digest, first_name="Maria")


def _parsed(vendor: str = "LF-1"):
    return SimpleNamespace(vendor_lead_id=vendor, certificate=None)


def test_an_exact_resend_stays_quiet(raised) -> None:
    result = lendingflow._record_duplicate(FakeDb(_existing()), _parsed(), HASH, NOW)
    assert result.created is False and result.lead_id == 7
    assert raised == []


def test_same_phone_and_email_with_a_new_lead_id_goes_to_a_person(raised) -> None:
    lendingflow._record_duplicate(FakeDb(_existing(vendor="LF-1")), _parsed(vendor="LF-2"), HASH, NOW)
    assert len(raised) == 1
    event = raised[0]
    assert event["event_type"] == "lendingflow_possible_duplicate" and event["target_channel"] == "dial_tasks"
    assert "same phone and email but a different LendingFlow lead id" in event["summary"]
    assert "matches Maria with" in event["summary"] and "lendingflow_lead" not in event["summary"]
    assert event["payload"] == {"lead_ref": "lendingflow_lead:7", "vendor_lead_id": "LF-2",
                                "matched_on": "phone_email_hash"}


def test_same_lead_id_with_new_contact_details_goes_to_a_person(raised) -> None:
    lendingflow._record_duplicate(FakeDb(_existing(digest="b" * 64)), _parsed(), HASH, NOW)
    assert len(raised) == 1 and raised[0]["payload"]["matched_on"] == "vendor_lead_id"
    assert "different phone or email" in raised[0]["summary"]


def test_the_alert_carries_no_phone_or_email(raised) -> None:
    lendingflow._record_duplicate(FakeDb(_existing(vendor="LF-1")), _parsed(vendor="LF-2"), HASH, NOW)
    text_out = raised[0]["summary"] + str(raised[0]["payload"]) + raised[0]["dedup_key"]
    assert "@" not in text_out and "+1" not in text_out and HASH not in text_out


def test_each_distinct_delivery_has_its_own_key(raised) -> None:
    lendingflow._record_duplicate(FakeDb(_existing(vendor="LF-1")), _parsed(vendor="LF-2"), HASH, NOW)
    lendingflow._record_duplicate(FakeDb(_existing(vendor="LF-1")), _parsed(vendor="LF-3"), HASH, NOW)
    assert raised[0]["dedup_key"] != raised[1]["dedup_key"]


def test_no_match_raises_nothing(raised) -> None:
    assert lendingflow._record_duplicate(FakeDb(None), _parsed(), HASH, NOW) is None
    assert raised == []
