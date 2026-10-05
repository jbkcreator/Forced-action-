"""CDR poll pieces that need no database: the /last response shape and inbound-call consent."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.lending import call_pipeline, cdr_poll


def _cdr(cid=1, **over):
    base = {"id": cid, "direction": "out", "callStartTime": "2026-09-29T15:55:00Z", "callEndTime": "2026-09-29T15:56:00Z",
            "did": "8135550100", "customerNumber": "8135550142", "disposition": "ANSWER", "duration": 60,
            "agent": {"id": 7, "firstname": "Sam", "lastname": "C"}, "contact": {"id": 9}, "campaign": {"id": 55, "name": "Inbound"}}
    base.update(over)
    return base


@pytest.fixture(autouse=True)
def campaigns(monkeypatch):
    monkeypatch.setattr(call_pipeline, "get_settings", lambda: SimpleNamespace(lending_dialer_campaign_ids="55"))


def test_last_endpoint_answering_with_a_bare_array_is_read():
    assert cdr_poll.fetch_last(lambda *a, **k: [_cdr(1), _cdr(2)]) == [_cdr(1), _cdr(2)]
    assert cdr_poll.fetch_last(lambda *a, **k: {"items": [_cdr(3)]}) == [_cdr(3)]
    assert cdr_poll.fetch_last(lambda *a, **k: []) == []
    assert cdr_poll.fetch_last(lambda *a, **k: None) == []


def test_answered_inbound_call_grants_consent_once_and_never_regrants_a_later_revocation(monkeypatch):
    db = MagicMock()
    granted = []
    monkeypatch.setattr(cdr_poll, "record_consent", lambda *a, **k: granted.append((a[1:], k)))
    item = _cdr(7, direction="in")

    db.execute.return_value.scalar.return_value = False
    stats = cdr_poll.ingest(db, [item], only_changed=False)
    assert granted[0][0] == ("+18135550142", "inbound_call") and granted[0][1]["call_id"] == "7"
    assert stats.processed == 0  # no call row for a plain inbound call

    granted.clear()
    db.execute.return_value.scalar.return_value = True  # live grant, or revoked after this call
    cdr_poll.ingest(db, [item], only_changed=True)
    assert not granted


def test_unanswered_or_other_campaign_inbound_call_grants_nothing(monkeypatch):
    db = MagicMock()
    db.execute.return_value.scalar.return_value = False
    granted = []
    monkeypatch.setattr(cdr_poll, "record_consent", lambda *a, **k: granted.append(a))
    cdr_poll.ingest(db, [_cdr(8, direction="in", duration=0), _cdr(9, direction="in", campaign={"id": 999})], only_changed=False)
    assert not granted


def test_consent_failure_does_not_stop_ingestion(monkeypatch):
    db = MagicMock()
    db.execute.return_value.scalar.return_value = False
    monkeypatch.setattr(cdr_poll, "record_consent", MagicMock(side_effect=RuntimeError("db")))
    stats = cdr_poll.ingest(db, [_cdr(10, direction="in")], only_changed=False)
    assert stats.seen == 1 and stats.failed == 0
    db.rollback.assert_called()
