"""Where first-contact snapshots are taken: BatchDialer intake and the GHL call import. Pure: db and snapshot faked."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.lending import call_pipeline, first_contact, ghl_call_import
from src.lending.dispositions import parse_event

AT = datetime(2026, 11, 4, 14, 34, tzinfo=timezone.utc)


@pytest.fixture
def snapshots(monkeypatch):
    taken = []

    def fake(session, **kw):
        taken.append(kw)
        return True

    monkeypatch.setattr(call_pipeline, "snapshot_reached_call", fake)
    monkeypatch.setattr(ghl_call_import, "snapshot_reached_call", fake)
    return taken


def _event(duration, direction="out"):
    return parse_event({"id": 1223346777, "direction": direction, "customerNumber": "3057905210", "duration": duration,
                        "callStartTime": AT.isoformat(), "contact": {"id": 559662160}, "agent": {"id": 118924}})


def _recorded(phone="+13057905210"):
    return SimpleNamespace(phone=phone, caller_seat="118924")


def test_first_reached_call_is_snapshotted(monkeypatch, snapshots):
    monkeypatch.setattr(call_pipeline, "lookup_load_record",
                        lambda *a, **k: {"pool": "verified_maturity", "campaign_tag": "list_1"})
    call_pipeline._snapshot_first_contact(MagicMock(), _event(48), _recorded())
    assert snapshots == [{"phone": "+13057905210", "contacted_at": AT, "caller_seat": "118924",
                          "queue": "verified_maturity", "source_tag": "list_1", "call_ref": "1223346777"}]


@pytest.mark.parametrize("duration, direction", [(0, "out"), (None, "out"), (48, "in")])
def test_unanswered_or_inbound_call_is_not_snapshotted(monkeypatch, snapshots, duration, direction):
    monkeypatch.setattr(call_pipeline, "lookup_load_record", lambda *a, **k: None)
    call_pipeline._snapshot_first_contact(MagicMock(), _event(duration, direction), _recorded())
    assert snapshots == []


def test_snapshot_failure_never_raises(monkeypatch):
    monkeypatch.setattr(first_contact, "snapshot_first_contact", MagicMock(side_effect=RuntimeError("db")))
    assert first_contact.snapshot_reached_call(MagicMock(), phone="+13057905210", contacted_at=AT, caller_seat="s",
                                               queue="lendingflow", source_tag=None, call_ref="c1") is False


def test_snapshot_is_warm_only_for_lendingflow_and_website(monkeypatch):
    seen = []
    monkeypatch.setattr(first_contact, "snapshot_first_contact", lambda session, **kw: seen.append(kw["warm"]) or True)
    for queue in ("lendingflow", "website", "verified_maturity", None):
        first_contact.snapshot_reached_call(MagicMock(), phone="+13057905210", contacted_at=AT, caller_seat=None,
                                            queue=queue, source_tag=None, call_ref="c1")
    assert seen == [True, True, False, False]


def _ghl_row(outcome, talk, direction="outbound"):
    return {"call_id": "ghl-m1", "direction": direction, "phone": "+13057905210", "raw_outcome": f"ghl:{outcome}",
            "talk": talk, "started": AT, "caller_name": "Josh Kantor", "tag": "ghl"}


def test_first_connected_ghl_call_is_snapshotted(snapshots):
    db = MagicMock()
    db.execute.return_value.scalar.return_value = True  # a LendingFlow lead
    ghl_call_import._snapshot_if_reached(db, _ghl_row("completed", 120))
    assert snapshots[0]["queue"] == "lendingflow" and snapshots[0]["caller_seat"] == "Josh Kantor"


@pytest.mark.parametrize("outcome, talk, direction", [("no-answer", 0, "outbound"), ("voicemail", 30, "outbound"),
                                                      ("completed", 120, "inbound")])
def test_missed_ghl_call_is_not_snapshotted(snapshots, outcome, talk, direction):
    ghl_call_import._snapshot_if_reached(MagicMock(), _ghl_row(outcome, talk, direction))
    assert snapshots == []


def test_snapshot_runs_after_the_call_is_committed(monkeypatch):
    """A snapshot problem can never undo the call row or the attempt-cap writes: it runs after the final commit."""
    order = []
    db = MagicMock()
    db.commit.side_effect = lambda: order.append("commit")
    recorded = SimpleNamespace(row_id=1, call_id="1223346777", phone="+13057905210", caller_seat="118924",
                               call_ended=True, dnc_requested=False, opt_out_propagated=False)
    monkeypatch.setattr(call_pipeline, "record_dialer_event", lambda db, ev: recorded)
    monkeypatch.setattr(call_pipeline, "_check_consent", lambda *a, **k: None)
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", lambda *a, **k: order.append("attempt"))
    monkeypatch.setattr(call_pipeline, "_snapshot_first_contact", lambda *a, **k: order.append("snapshot"))
    call_pipeline.process_event(db, _event(48))
    assert order[-2:] == ["commit", "snapshot"] and order.index("attempt") < order.index("snapshot")


def test_ghl_lead_lookup_failure_never_stops_the_import(snapshots):
    db = MagicMock()
    db.execute.side_effect = RuntimeError("db")
    ghl_call_import._snapshot_if_reached(db, _ghl_row("completed", 120))
    assert snapshots == []
