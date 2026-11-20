"""P2.4: borrowers' uploaded documents copied to Josh's restricted Drive folder (fake Drive; no network)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.closing import doc_copies
from src.lending.closing.doc_copies import (
    ALERT_AFTER,
    DriveCopyStep,
    copy_uploads_to_drive,
)

PDF = b"%PDF-1.4 test"
T0 = datetime(2026, 11, 17, 15, 0, tzinfo=timezone.utc)
FOLDER = "1JoshPrivateFolderId"


class FakeDrive:
    def __init__(self, fail: int = 0):
        self.calls, self.fail = [], fail

    def __call__(self, parent_id, subfolder, name, data, mime):
        if self.fail:
            self.fail -= 1
            raise ConnectionError(f"drive down for folder {parent_id}")
        self.calls.append((parent_id, subfolder, name, data, mime))
        return f"file-{len(self.calls)}"


@pytest.fixture
def uploads(closing_db, tmp_path):
    closing_db.execute(text(
        "INSERT INTO lending.doc_chases (lead_ref, first_name, phone, lender_key, accepted_at, accepted_by) "
        "VALUES ('web_lead:1', 'Mark', '+18135550101', 'central_lending', now(), 'slack:U1')"))
    chase_id = closing_db.execute(text("SELECT max(id) FROM lending.doc_chases")).scalar()
    ids = []
    for code, ext, mime in (("government_id", ".pdf", "application/pdf"), ("valuation", ".png", "image/png")):
        path = tmp_path / f"stored{ext}"
        path.write_bytes(PDF)
        ids.append(closing_db.execute(text(
            "INSERT INTO lending.doc_chase_uploads (chase_id, item_code, original_name, stored_path, content_type, "
            "size_bytes) VALUES (:c, :code, :orig, :path, :mime, :size) RETURNING id"),
            {"c": chase_id, "code": code, "orig": f"Mark Smith ID{ext}", "path": str(path), "mime": mime,
             "size": len(PDF)}).scalar())
    return chase_id, ids, tmp_path


def _state(db):
    return db.execute(text("SELECT drive_file_id, drive_attempts FROM lending.doc_chase_uploads ORDER BY id")).all()


def _copy(db, drive, now=T0):
    return copy_uploads_to_drive(db, folder_id=FOLDER, copier=drive, now=now)


def test_each_upload_is_copied_once_into_the_chase_folder_without_the_borrower_name(closing_db, uploads):
    chase_id, ids, _ = uploads
    drive = FakeDrive()
    assert _copy(closing_db, drive) == (2, None)
    assert [(c[0], c[1], c[2], c[4]) for c in drive.calls] == [
        (FOLDER, f"chase-{chase_id}", f"government_id-{ids[0]}.pdf", "application/pdf"),
        (FOLDER, f"chase-{chase_id}", f"valuation-{ids[1]}.png", "image/png")]
    assert all(c[3] == PDF for c in drive.calls) and not any("Mark" in c[2] for c in drive.calls)
    assert _state(closing_db) == [("file-1", 0), ("file-2", 0)]
    assert _copy(closing_db, drive) == (0, None) and len(drive.calls) == 2  # never copied twice


def test_nothing_is_copied_while_the_folder_is_not_set(closing_db, uploads):
    drive = FakeDrive()
    assert copy_uploads_to_drive(closing_db, folder_id="", copier=drive) == (0, None)
    assert drive.calls == [] and _state(closing_db) == [(None, 0), (None, 0)]


def test_a_failure_stops_the_batch_and_retries_after_a_growing_wait(closing_db, uploads):
    drive = FakeDrive(fail=2)
    assert _copy(closing_db, drive) == (0, "ConnectionError")  # first upload fails, the batch stops
    assert _state(closing_db) == [(None, 1), (None, 0)]
    assert _copy(closing_db, drive) == (0, "ConnectionError")  # the second upload (the first waits 1 minute)
    assert _state(closing_db) == [(None, 1), (None, 1)]
    assert _copy(closing_db, drive, now=T0 + timedelta(seconds=30)) == (0, None)  # both still waiting
    assert _copy(closing_db, drive, now=T0 + timedelta(minutes=1)) == (2, None)  # Drive is back
    assert _state(closing_db) == [("file-1", 1), ("file-2", 1)]


def test_it_never_gives_up(closing_db, uploads):
    drive, now = FakeDrive(fail=10_000), T0
    for _ in range(40):
        _copy(closing_db, drive, now=now)
        now += doc_copies.RETRY_MAX  # every wait has passed
    assert min(row[1] for row in _state(closing_db)) > ALERT_AFTER
    drive.fail = 0
    assert _copy(closing_db, drive, now=now) == (2, None)  # fixed: everything waiting goes


def test_thousands_of_failures_never_overflow_the_wait(closing_db, uploads):
    closing_db.execute(text("UPDATE lending.doc_chase_uploads SET drive_attempts = 5000"))
    assert _copy(closing_db, FakeDrive(fail=1)) == (0, "ConnectionError")
    retry_at = closing_db.execute(text("SELECT max(drive_retry_at) FROM lending.doc_chase_uploads")).scalar()
    assert retry_at - T0 == doc_copies.RETRY_MAX


def test_waits_grow_and_are_capped(closing_db, uploads):
    upload_id = uploads[1][0]
    closing_db.execute(text("DELETE FROM lending.doc_chase_uploads WHERE id = :i"), {"i": uploads[1][1]})
    drive = FakeDrive(fail=10_000)
    waits, now = [], T0
    for _ in range(12):
        _copy(closing_db, drive, now=now)
        retry_at = closing_db.execute(text("SELECT drive_retry_at FROM lending.doc_chase_uploads WHERE id = :i"),
                                      {"i": upload_id}).scalar()
        waits.append(retry_at - now)
        now = retry_at
    assert waits[:4] == [timedelta(minutes=m) for m in (1, 2, 4, 8)]
    assert max(waits) == doc_copies.RETRY_MAX


def test_a_missing_file_on_disk_counts_as_a_failed_copy(closing_db, uploads):
    (uploads[2] / "stored.pdf").unlink()
    assert _copy(closing_db, FakeDrive()) == (0, "FileNotFoundError")
    assert _state(closing_db) == [(None, 1), (None, 0)]


def test_the_time_budget_stops_a_slow_run(closing_db, uploads):
    ticks = iter([0.0, 0.0, doc_copies.TIME_BUDGET_SECONDS + 1])
    drive = FakeDrive()
    copied = copy_uploads_to_drive(closing_db, folder_id=FOLDER, copier=drive, now=T0, clock=lambda: next(ticks))
    assert copied == (1, None) and len(drive.calls) == 1


class _Begin:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


class _Session:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, *exc):
        return False


def test_the_step_pauses_after_a_failure_so_the_sweep_is_not_held(closing_db, uploads, monkeypatch):
    real_begin = closing_db.begin  # the step's begin() keeps the test's transaction; savepoints still work
    monkeypatch.setattr(closing_db, "begin", lambda nested=False: real_begin(nested=True) if nested else _Begin())
    clock = {"now": T0}
    drive = FakeDrive(fail=1)
    step = DriveCopyStep(lambda: _Session(closing_db), folder_id=FOLDER, copier=drive, clock=lambda: clock["now"])
    assert step() == 0  # fails: paused for PAUSE_FIRST
    clock["now"] = T0 + timedelta(minutes=2)
    assert step() == 0 and drive.calls == []  # paused: Drive not called
    clock["now"] = T0 + doc_copies.PAUSE_FIRST + timedelta(minutes=1)
    assert step() == 2  # resumed


def test_one_outage_sends_one_notice_not_one_per_document(closing_db, uploads, monkeypatch):
    real_begin = closing_db.begin
    monkeypatch.setattr(closing_db, "begin", lambda nested=False: real_begin(nested=True) if nested else _Begin())
    closing_db.execute(text("UPDATE lending.doc_chase_uploads SET drive_attempts = :n"), {"n": ALERT_AFTER})
    for i in range(20):  # 20 more documents waiting behind the same outage
        closing_db.execute(text(
            "INSERT INTO lending.doc_chase_uploads (chase_id, item_code, original_name, stored_path, content_type, "
            "size_bytes, drive_attempts) VALUES (:c, 'valuation', 'x.pdf', '/nowhere.pdf', 'application/pdf', 1, :n)"),
            {"c": uploads[0], "n": ALERT_AFTER + i % 3})
    clock, notices = {"now": T0}, []
    step = DriveCopyStep(lambda: _Session(closing_db), folder_id=FOLDER, copier=FakeDrive(fail=10_000),
                         notify=notices.append, clock=lambda: clock["now"])
    for _ in range(30):  # many failed runs inside the cooldown
        step()
        clock["now"] += doc_copies.PAUSE_MAX
        if clock["now"] - T0 >= doc_copies.ALERT_COOLDOWN:
            break
    assert len(notices) == 1 and "22 borrower document(s)" in notices[0]
    assert FOLDER not in notices[0] and "Mark" not in notices[0] and "+1813" not in notices[0]
    clock["now"] = T0 + doc_copies.ALERT_COOLDOWN + doc_copies.PAUSE_MAX
    step()
    assert len(notices) == 2  # still down after the cooldown: reminded once


class _Media:
    pass


def test_documents_over_5_mb_go_to_drive_as_a_resumable_upload():
    from src.lending import drive_upload

    seen = []

    class Files:
        def list(self, **kw):
            return type("R", (), {"execute": lambda self: {"files": []}})()

        def create(self, body, media_body, fields, supportsAllDrives):
            seen.append(media_body.resumable())
            return type("R", (), {"execute": lambda self: {"id": "f1"}})()

    service = type("S", (), {"files": lambda self: Files()})()
    drive_upload.upload_bytes(service, "folder", "small.csv", b"a,b\n")
    drive_upload.upload_bytes(service, "folder", "id.pdf", b"x" * (drive_upload.SIMPLE_UPLOAD_MAX_BYTES + 1),
                              mime="application/pdf")
    assert seen == [False, True]


def test_the_sweep_copies_only_when_the_folder_is_set(monkeypatch):
    """build_closing_steps runs the copy step after the chase step."""
    from types import SimpleNamespace

    import src.lending.cora_agent as cora

    calls = []

    class Step:
        def __init__(self, session_factory, *, folder_id, copier, notify):
            self.folder_id = folder_id

        def __call__(self):
            calls.append(self.folder_id)
            return 0

    class _S:
        def __init__(self, engine):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def begin(self):
            return self

    monkeypatch.setattr(cora, "run_chase_sweep", lambda db, gate, **kw: 3)
    monkeypatch.setattr(cora, "DriveCopyStep", Step)
    monkeypatch.setattr(cora, "live_copier", lambda: "copier")
    monkeypatch.setattr(cora, "Session", _S)
    config = SimpleNamespace(approval_channel_id="C_APPROVALS", slack_channel_id="C_CORA")
    for folder, expected in (("", []), ("JOSH_FOLDER", ["JOSH_FOLDER"])):
        calls.clear()
        settings = SimpleNamespace(lending_doc_upload_drive_folder_id=folder, lending_closing_blockers_enabled=False,
                                   lending_doc_chase_enabled=True)
        _, chase_sweep = cora.build_closing_steps(settings, engine=None, chat=None, gate=None, config=config)
        assert chase_sweep() == 3 and calls == expected
