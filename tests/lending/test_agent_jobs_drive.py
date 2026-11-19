"""P1.1 Drive folder client: the googleapiclient service is stubbed; no network."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from src.lending.agent_jobs import drive
from src.lending.agent_jobs.drive import DriveFile, FakeJobFolders, GoogleDriveJobFolders

FOLDERS = dict(inbox="F_INBOX", processed="F_PROCESSED", done="F_DONE")


class _Request:
    def __init__(self, result) -> None:
        self._result = result

    def execute(self):
        return self._result


class StubFiles:
    """Records every files() call as (method, kwargs) and answers from canned results."""

    def __init__(self, listings: dict[str, list[dict]] | None = None, media: bytes = b"{}") -> None:
        self.calls: list[tuple[str, dict]] = []
        self._listings = listings or {}
        self._media = media

    def list(self, **kwargs):
        self.calls.append(("list", kwargs))
        folder = next((f for f in self._listings if f"'{f}' in parents" in kwargs["q"]), None)
        return _Request({"files": self._listings.get(folder, [])})

    def get_media(self, **kwargs):
        self.calls.append(("get_media", kwargs))
        return _Request(self._media)

    def update(self, **kwargs):
        self.calls.append(("update", kwargs))
        return _Request({"id": kwargs["fileId"]})

    def create(self, **kwargs):
        self.calls.append(("create", kwargs))
        return _Request({"id": "new-file"})


def _folders(files: StubFiles) -> GoogleDriveJobFolders:
    return GoogleDriveJobFolders(SimpleNamespace(files=lambda: files), **FOLDERS)


def test_list_filters_json_and_sorts_oldest_first() -> None:
    files = StubFiles({"F_INBOX": [
        {"id": "b", "name": "2_lf_b.json", "size": "20", "createdTime": "2026-11-12T15:00:00Z"},
        {"id": "x", "name": "notes.txt", "size": "5", "createdTime": "2026-11-12T13:00:00Z"},
        {"id": "a", "name": "1_lf_a.json", "size": "10", "createdTime": "2026-11-12T14:00:00.123Z"},
    ]})
    listed = _folders(files).list_inbox(limit=50)
    assert [f.file_id for f in listed] == ["a", "b"]
    assert listed[0] == DriveFile("a", "1_lf_a.json", 10, datetime(2026, 11, 12, 14, 0, 0, 123000, tzinfo=timezone.utc))
    kwargs = files.calls[0][1]
    assert kwargs["supportsAllDrives"] is True and kwargs["includeItemsFromAllDrives"] is True
    assert "trashed = false" in kwargs["q"]


def test_read_returns_bytes() -> None:
    files = StubFiles(media=b'{"job_id": 1}')
    assert _folders(files).read("a") == b'{"job_id": 1}'
    assert files.calls[0] == ("get_media", {"fileId": "a", "supportsAllDrives": True})


def test_move_uses_add_and_remove_parents() -> None:
    files = StubFiles()
    _folders(files).move_to_processed("a")
    method, kwargs = files.calls[0]
    assert method == "update"
    assert (kwargs["addParents"], kwargs["removeParents"], kwargs["supportsAllDrives"]) == ("F_PROCESSED", "F_INBOX", True)


def test_write_result_creates_new_file() -> None:
    files = StubFiles()
    _folders(files).write_result("job-1", {"status": "sent"})
    method, kwargs = files.calls[-1]
    assert method == "create"
    assert kwargs["body"] == {"name": "job-1.json", "parents": ["F_DONE"], "mimeType": "application/json"}
    assert json.loads(kwargs["media_body"].getbytes(0, 1000)) == {"status": "sent"}


def test_write_result_updates_existing_file() -> None:
    files = StubFiles({"F_DONE": [{"id": "old", "name": "job-1.json", "size": "2", "createdTime": "2026-11-12T14:00:00Z"}]})
    _folders(files).write_result("job-1", {"status": "rejected"})
    method, kwargs = files.calls[-1]
    assert (method, kwargs["fileId"]) == ("update", "old")


def test_missing_settings_returns_none(monkeypatch) -> None:
    monkeypatch.setattr(drive, "get_settings", lambda: SimpleNamespace(
        lending_drive_service_account_key_path="", lending_jobs_inbox_folder_id="F_INBOX",
        lending_jobs_processed_folder_id="F_PROCESSED", lending_jobs_done_folder_id="F_DONE"))
    assert drive.google_job_folders() is None


def test_fake_folders_round_trip() -> None:
    fake = FakeJobFolders()
    fake.add("1_lf_a.json", b"{}")
    [listed] = fake.list_inbox(10)
    assert fake.read(listed.file_id) == b"{}"
    fake.move_to_processed(listed.file_id)
    assert fake.list_inbox(10) == []
    fake.write_result("job-1", {"status": "sent"})
    assert fake.results["job-1"] == {"status": "sent"}
