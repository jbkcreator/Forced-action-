"""The reschedule link is passed in once per cycle, and a link dropped for length is recorded
on the sent row (skip_reason), not only logged. No DB: a recording fake stands in."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from config.lending_reminders import MAX_TEXT_CHARS
from src.lending import reminder_worker as worker

NOW = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)  # 10:00 ET, text window open


class FakeDb:
    def __init__(self):
        self.calls = []

    def execute(self, stmt, params=None):
        self.calls.append(params or {})


def _row():
    slot = NOW + timedelta(minutes=90)
    return {"id": 7, "kind": "ninety_min", "slot_start_utc": slot, "send_at": NOW, "first_name": "Jane",
            "property_address": None, "contact_phone": "+18135550199", "contact_email": None, "attempts": 1}


def _send(url):
    db, sent = FakeDb(), []

    def sender(phone, body, name, deadline):
        sent.append(body)
        return "m1"

    label = worker._send_text(db, _row(), now=NOW, sender=sender, enabled=True, number="+18135550100",
                              started=[False], reschedule_url=url)
    return label, sent[0], db.calls[-1]


def test_link_that_fits_is_sent_and_no_reason_recorded():
    label, body, record = _send("https://x.example/r")
    assert label == "sent" and "Reschedule: https://x.example/r" in body
    assert record["status"] == "sent" and record["reason"] is None


def test_link_dropped_for_length_is_recorded_on_the_sent_row():
    label, body, record = _send("https://x.example/" + "x" * MAX_TEXT_CHARS)
    assert label == "sent" and "Reschedule:" not in body
    assert record["status"] == "sent" and record["reason"] == worker.REASON_RESCHEDULE_LINK_OMITTED


def test_no_url_configured_records_no_reason():
    label, body, record = _send(None)
    assert label == "sent" and "Reschedule:" not in body and record["reason"] is None
