"""R1 0.11 nightly opt-out sync: pull from GHL / Instantly, push to GHL / BatchDialer / Instantly. Fakes only."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from config.lending_compliance import OptOutChannel
from src.lending import compliance
from src.tasks import nightly_dnc_sync
from src.tasks.nightly_dnc_sync import pull_instantly, push_ghl, run_sync


class FakeDb:
    def __init__(self, known=()):
        self.known, self.statements = set(known), []

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        rows = [(e,) for e in (params or {}).get("emails", []) if e in self.known]

        class Result(list):
            rowcount = 0
        return Result(rows)


def _factory(db):
    @contextmanager
    def session():
        yield db
    return session


def test_pull_instantly_records_only_new_addresses_and_skips_domains(monkeypatch):
    recorded = []
    monkeypatch.setattr(compliance, "propagate_opt_out", lambda db, **kw: recorded.append(kw))
    db = FakeDb(known={"old@example.com"})
    entries = iter([
        {"id": "e1", "bl_value": "New@Example.com", "is_domain": False},
        {"id": "e2", "bl_value": "old@example.com", "is_domain": False},
        {"id": "e3", "bl_value": "example.org", "is_domain": True},
    ])
    assert pull_instantly(db, entries) == 1
    assert recorded == [{"email": "new@example.com", "source_ref": "instantly:e1", "channel": OptOutChannel.EMAIL}]


def test_push_ghl_repeats_batches_until_no_progress(monkeypatch):
    batches = [50, 50, 3, 0]
    monkeypatch.setattr(compliance, "sync_ghl_dnd", lambda db, ghl_dnd=None: batches.pop(0))
    assert push_ghl(_factory(FakeDb())) == 103


def test_synthetic_opt_out_from_instantly_reaches_ghl_and_the_dialer(monkeypatch):
    """Done-when: an opt-out made in Instantly is pulled in, then pushed to GHL and BatchDialer."""
    ledger: list[dict] = []
    ghl_set, dialer_removed = [], []

    monkeypatch.setattr(compliance, "propagate_opt_out",
                        lambda db, **kw: ledger.append({**kw, "ghl": False, "dialer": False}))

    def sync_ghl(db, ghl_dnd=None):
        pending = [e for e in ledger if not e["ghl"]]
        for event in pending:
            ghl_dnd(event["email"]) and event.update(ghl=True)
        return len(pending)

    def retry_dialer(db, remover):
        pending = [e for e in ledger if not e["dialer"]]
        for event in pending:
            remover(event["email"], reason="opt_out")
            event["dialer"] = True
        return len(pending)

    monkeypatch.setattr(compliance, "sync_ghl_dnd", sync_ghl)
    monkeypatch.setattr(compliance, "_retry_pending_dialer_removals", retry_dialer)
    monkeypatch.setattr(nightly_dnc_sync, "push_instantly", lambda db: 0)

    report = run_sync(
        _factory(FakeDb()),
        fetch_ghl_dnd_page=lambda page: [],
        fetch_instantly=lambda: iter([{"id": "e9", "bl_value": "stop@example.com"}]),
        ghl_dnd=lambda who: ghl_set.append(who) or True,
        dialer_remover=lambda who, reason: dialer_removed.append(who),
    )

    assert report.failures == []
    assert report.counts["instantly_pulled"] == 1
    assert ghl_set == ["stop@example.com"] and dialer_removed == ["stop@example.com"]


def test_a_failing_platform_does_not_stop_the_others(monkeypatch):
    def down():
        raise RuntimeError("instantly down")

    monkeypatch.setattr(compliance, "sync_ghl_dnd", lambda db, ghl_dnd=None: 0)
    monkeypatch.setattr(compliance, "_retry_pending_dialer_removals", lambda db, remover: 2)
    monkeypatch.setattr(nightly_dnc_sync, "push_instantly", lambda db: 4)

    report = run_sync(_factory(FakeDb()), fetch_ghl_dnd_page=lambda page: [], fetch_instantly=down)

    assert report.failures == ["instantly_pulled"]
    assert report.counts["dialer_pushed"] == 2 and report.counts["instantly_queued"] == 4


def test_main_alerts_ops_only_on_failure(monkeypatch):
    posted = []
    monkeypatch.setattr("src.lending.ops_alerts.post_ops_notice", lambda m: posted.append(m) or True)
    report = nightly_dnc_sync.SyncReport(counts={"ghl_pushed": 1})
    monkeypatch.setattr(nightly_dnc_sync, "run_sync", lambda factory: report)
    assert nightly_dnc_sync.main() == 0 and posted == []
    report.failures.append("ghl_pushed")
    assert nightly_dnc_sync.main() == 1 and len(posted) == 1
