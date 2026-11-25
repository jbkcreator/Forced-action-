"""R1 0.5 T8: the LendingFlow nginx edge-log reconcile slot (no database, no nginx).

The log is a temp file in the nginx ``lendingflow_body`` format (one JSON object per line: ``t``, ``status``, ``body``
as an escaped string). Which leads exist and the replay are injected fakes.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from src.lending.inbound import lendingflow_edge_log as edge
from src.tasks import lending_reconciliation as reconcile

SINCE = datetime(2026, 11, 24, 7, 0, tzinfo=timezone.utc)


def body(lead_id, phone="+18135550147", email="a@example.com"):
    return {"lead_id": lead_id, "phone": phone, "email": email, "first_name": "Ann"}


def line(t, payload, status="200"):
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    return json.dumps({"t": t, "status": status, "body": raw})


class _Book:
    """Fake store: leads that exist (by vendor id) and replays received."""

    def __init__(self, existing=(), outcome="created"):
        self.existing = set(existing)
        self.outcome = outcome
        self.replayed = []

    def exists(self, parsed_leads):
        return {p.vendor_lead_id for p in parsed_leads if p.vendor_lead_id in self.existing}

    def replay(self, raw):
        self.replayed.append(raw["lead_id"])
        if self.outcome == "created":
            self.existing.add(raw["lead_id"])
        return self.outcome


def slot_for(path, book):
    return edge.LendingFlowEdgeLogSlot(str(path), existing=book.exists, replay=book.replay)


def write(path, *lines):
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_heals_only_the_missing_lead(tmp_path):
    log = tmp_path / "lendingflow_body.log"
    write(log,
          line("2026-11-24T10:00:00+00:00", body("LF-1")),           # saved
          line("2026-11-24T11:00:00+00:00", body("LF-2"), "502"),    # lending-api was down
          line("2026-11-24T12:00:00+00:00", "not json {"))           # garbage
    book = _Book(existing={"LF-1"})
    slot = slot_for(log, book)

    report = reconcile.run_slot(slot, SINCE, dry_run=False)

    assert (report.found, report.healed, report.heal_failures) == (1, 1, 0)
    assert book.replayed == ["LF-2"]
    assert reconcile.run_slot(slot, SINCE, dry_run=False).found == 0  # rerun heals nothing


def test_reads_the_rotated_file_and_skips_lines_before_the_window(tmp_path):
    log = tmp_path / "lendingflow_body.log"
    write(tmp_path / "lendingflow_body.log.1",
          line("2026-11-23T20:00:00+00:00", body("LF-OLD")),         # before the window
          line("2026-11-24T08:00:00+00:00", body("LF-ROT")))
    write(log, line("2026-11-24T09:00:00+00:00", body("LF-NEW")))
    book = _Book()

    payloads = list(slot_for(log, book).find_missing(SINCE))

    assert sorted(p["lead_id"] for p in payloads) == ["LF-NEW", "LF-ROT"]


def test_same_body_twice_in_the_window_is_replayed_once(tmp_path):
    log = tmp_path / "lendingflow_body.log"
    write(log, line("2026-11-24T09:00:00+00:00", body("LF-9"), "500"),
          line("2026-11-24T09:00:05+00:00", body("LF-9"), "500"))

    assert len(list(slot_for(log, _Book()).find_missing(SINCE))) == 1


def test_unparseable_lead_is_skipped_not_replayed(tmp_path):
    log = tmp_path / "lendingflow_body.log"
    write(log, line("2026-11-24T09:00:00+00:00", {"lead_id": "LF-X"}))  # no phone: parse_lendingflow refuses

    assert list(slot_for(log, _Book()).find_missing(SINCE)) == []


def test_missing_log_is_a_warning_not_a_failure(tmp_path):
    report = reconcile.run_slot(slot_for(tmp_path / "absent.log", _Book()), SINCE, dry_run=False)

    assert report.warning and not report.failed


def test_unset_path_is_a_warning(tmp_path):
    report = reconcile.run_slot(edge.LendingFlowEdgeLogSlot("", existing=_Book().exists, replay=_Book().replay),
                                SINCE, dry_run=False)

    assert report.warning and not report.failed


@pytest.mark.parametrize("outcome", ["failed", "invalid"])
def test_replay_not_stored_counts_as_not_healed(tmp_path, outcome):
    log = tmp_path / "lendingflow_body.log"
    write(log, line("2026-11-24T09:00:00+00:00", body("LF-5"), "500"))

    report = reconcile.run_slot(slot_for(log, _Book(outcome=outcome)), SINCE, dry_run=False)

    assert (report.healed, report.heal_failures) == (0, 1)


def test_duplicate_replay_counts_as_healed(tmp_path):
    log = tmp_path / "lendingflow_body.log"
    write(log, line("2026-11-24T09:00:00+00:00", body("LF-6"), "500"))

    report = reconcile.run_slot(slot_for(log, _Book(outcome="duplicate")), SINCE, dry_run=False)

    assert (report.healed, report.heal_failures) == (1, 0)


def test_errors_never_carry_the_body(tmp_path, caplog):
    log = tmp_path / "lendingflow_body.log"
    write(log, line("2026-11-24T09:00:00+00:00", body("LF-7", phone="+18135550199"), "500"))

    reconcile.run_slot(slot_for(log, _Book(outcome="failed")), SINCE, dry_run=False)

    assert "5550199" not in caplog.text and "a@example.com" not in caplog.text


def test_default_slots_include_the_edge_log(monkeypatch):
    names = [s.name for s in reconcile.default_slots()]

    assert edge.LendingFlowEdgeLogSlot.name in names
