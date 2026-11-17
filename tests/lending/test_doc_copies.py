"""P2.4: borrowers' uploaded documents copied to Josh's restricted Drive folder (fake Drive; no network)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.lending.closing.doc_copies import COPY_ATTEMPTS, copy_uploads_to_drive

PDF = b"%PDF-1.4 test"


class FakeDrive:
    def __init__(self, fail: int = 0):
        self.calls, self.fail = [], fail

    def __call__(self, parent_id, subfolder, name, data, mime):
        if self.fail:
            self.fail -= 1
            raise ConnectionError("drive down for folder FOLDER")
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


def _drive_ids(db):
    return db.execute(text("SELECT drive_file_id, drive_attempts FROM lending.doc_chase_uploads ORDER BY id")).all()


def test_each_upload_is_copied_once_into_the_chase_folder_without_the_borrower_name(closing_db, uploads):
    chase_id, ids, _ = uploads
    drive = FakeDrive()
    assert copy_uploads_to_drive(closing_db, folder_id="FOLDER", copier=drive) == 2
    assert [(c[0], c[1], c[2], c[4]) for c in drive.calls] == [
        ("FOLDER", f"chase-{chase_id}", f"government_id-{ids[0]}.pdf", "application/pdf"),
        ("FOLDER", f"chase-{chase_id}", f"valuation-{ids[1]}.png", "image/png")]
    assert all(c[3] == PDF for c in drive.calls) and not any("Mark" in c[2] for c in drive.calls)
    assert _drive_ids(closing_db) == [("file-1", 0), ("file-2", 0)]
    assert copy_uploads_to_drive(closing_db, folder_id="FOLDER", copier=drive) == 0      # never copied twice
    assert len(drive.calls) == 2


def test_nothing_is_copied_while_the_folder_is_not_set(closing_db, uploads):
    drive = FakeDrive()
    assert copy_uploads_to_drive(closing_db, folder_id="", copier=drive) == 0
    assert drive.calls == [] and _drive_ids(closing_db) == [(None, 0), (None, 0)]


def test_a_failed_copy_is_retried_on_the_next_sweep(closing_db, uploads):
    drive, notices = FakeDrive(fail=1), []
    assert copy_uploads_to_drive(closing_db, folder_id="FOLDER", copier=drive, notify=notices.append) == 1
    assert _drive_ids(closing_db) == [(None, 1), ("file-1", 0)]
    assert copy_uploads_to_drive(closing_db, folder_id="FOLDER", copier=drive, notify=notices.append) == 1
    assert _drive_ids(closing_db) == [("file-2", 1), ("file-1", 0)] and notices == []


def test_repeated_failures_stop_after_the_limit_with_one_notice(closing_db, uploads):
    drive, notices = FakeDrive(fail=10_000), []
    for _ in range(COPY_ATTEMPTS + 3):
        copy_uploads_to_drive(closing_db, folder_id="1JoshPrivateFolderId", copier=drive, notify=notices.append)
    assert _drive_ids(closing_db) == [(None, COPY_ATTEMPTS), (None, COPY_ATTEMPTS)]
    assert len(notices) == 2                                 # one per upload, at the limit only
    assert all("1JoshPrivateFolderId" not in n and "Mark" not in n and "+1813" not in n for n in notices)


def test_a_missing_file_on_disk_counts_as_a_failed_copy(closing_db, uploads):
    _, _, folder = uploads
    (folder / "stored.pdf").unlink()
    drive = FakeDrive()
    assert copy_uploads_to_drive(closing_db, folder_id="FOLDER", copier=drive) == 1
    assert _drive_ids(closing_db) == [(None, 1), ("file-1", 0)]


def test_the_sweep_copies_only_when_the_folder_is_set(monkeypatch):
    """build_closing_steps wires the copy after the chase step, in its own transaction."""
    from types import SimpleNamespace

    import src.lending.cora_agent as cora

    calls = []
    monkeypatch.setattr(cora, "run_chase_sweep", lambda db, gate, **kw: 3)
    monkeypatch.setattr(cora, "copy_uploads_to_drive", lambda db, **kw: calls.append(kw["folder_id"]))
    monkeypatch.setattr(cora, "live_copier", lambda: "copier")

    class _Session:
        def __init__(self, engine):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def begin(self):
            return self

    monkeypatch.setattr(cora, "Session", _Session)
    config = SimpleNamespace(approval_channel_id="C_APPROVALS", slack_channel_id="C_CORA")
    for folder, expected in (("", []), ("JOSH_FOLDER", ["JOSH_FOLDER"])):
        calls.clear()
        settings = SimpleNamespace(lending_doc_upload_drive_folder_id=folder, lending_closing_blockers_enabled=False,
                                   lending_doc_chase_enabled=True)
        _, chase_sweep = cora.build_closing_steps(settings, engine=None, chat=None, gate=None, config=config)
        assert chase_sweep() == 3 and calls == expected
