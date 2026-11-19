"""Google Drive folders of the P1.1 job queue: "NDL Jobs/inbox" (agents' job files), "inbox/processed" (files Cora
has taken) and "NDL Jobs/done" (Cora's receipts, one ``{job_id}.json`` each, overwritten if written again).

Drive v3 through the lending service account (LENDING_DRIVE_SERVICE_ACCOUNT_KEY_PATH, the same pattern as the
disposition Sheet in src/lending/disposition_delivery.py). The folders must be in a Shared Drive: a service account
has no storage of its own, so it cannot create receipts in a personal My Drive. Every call passes
``supportsAllDrives``. A failed call raises :class:`DriveError` with the error class only.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Protocol

from config.settings import get_settings

logger = logging.getLogger(__name__)

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
_FIELDS = "files(id, name, size, createdTime)"


class DriveError(RuntimeError):
    """A Drive call failed; the message holds the operation and error class only."""


@dataclass(frozen=True)
class DriveFile:
    file_id: str
    name: str
    size: int
    created_time: datetime


class JobFolders(Protocol):
    def list_inbox(self, limit: int) -> list[DriveFile]: ...

    def read(self, file_id: str) -> bytes: ...

    def move_to_processed(self, file_id: str) -> None: ...

    def write_result(self, job_id: str, result: Mapping[str, Any]) -> None: ...


def _parse_time(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


class GoogleDriveJobFolders:
    def __init__(self, service: Any, *, inbox: str, processed: str, done: str) -> None:
        self._files = service.files
        self._inbox, self._processed, self._done = inbox, processed, done

    def _run(self, what: str, request: Any) -> Any:
        try:
            return request.execute()
        except Exception as exc:  # class only: Google errors can echo file names and ids
            logger.warning("[agent-jobs] Drive %s failed (%s)", what, type(exc).__name__)
            raise DriveError(f"Drive {what} failed ({type(exc).__name__})") from None

    def _list(self, query: str, limit: int) -> list[dict]:
        result = self._run("list", self._files().list(
            q=query, fields=_FIELDS, orderBy="createdTime", pageSize=min(max(limit, 1), 1000),
            supportsAllDrives=True, includeItemsFromAllDrives=True))
        return result.get("files") or []

    def list_inbox(self, limit: int) -> list[DriveFile]:
        query = f"'{self._inbox}' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
        files = [DriveFile(f["id"], f["name"], int(f.get("size") or 0), _parse_time(f["createdTime"]))
                 for f in self._list(query, limit * 2) if f.get("name", "").lower().endswith(".json")]
        return sorted(files, key=lambda f: f.created_time)[:limit]

    def read(self, file_id: str) -> bytes:
        return self._run("read", self._files().get_media(fileId=file_id, supportsAllDrives=True))

    def move_to_processed(self, file_id: str) -> None:
        self._run("move", self._files().update(fileId=file_id, addParents=self._processed,
                                               removeParents=self._inbox, supportsAllDrives=True, fields="id"))

    def write_result(self, job_id: str, result: Mapping[str, Any]) -> None:
        from googleapiclient.http import MediaInMemoryUpload

        name = f"{job_id}.json"
        media = MediaInMemoryUpload(json.dumps(result, indent=2, default=str).encode(), mimetype="application/json")
        existing = self._list(f"'{self._done}' in parents and name = '{name}' and trashed = false", 1)
        if existing:
            self._run("receipt update", self._files().update(fileId=existing[0]["id"], media_body=media,
                                                             supportsAllDrives=True, fields="id"))
        else:
            self._run("receipt create", self._files().create(
                body={"name": name, "parents": [self._done], "mimeType": "application/json"}, media_body=media,
                supportsAllDrives=True, fields="id"))


@dataclass
class FakeJobFolders:
    """In-memory folders for tests and the worker's ``--dry-run``."""

    inbox: dict[str, tuple[DriveFile, bytes]] = field(default_factory=dict)
    processed: list[str] = field(default_factory=list)
    results: dict[str, dict] = field(default_factory=dict)
    fail_writes: bool = False

    def add(self, name: str, raw: bytes, created_time: Optional[datetime] = None) -> str:
        count = len(self.inbox) + len(self.processed) + 1
        file_id = f"file-{count}"
        moment = created_time or datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=count)
        self.inbox[file_id] = (DriveFile(file_id, name, len(raw), moment), raw)
        return file_id

    def list_inbox(self, limit: int) -> list[DriveFile]:
        return sorted((entry[0] for entry in self.inbox.values()), key=lambda f: f.created_time)[:limit]

    def read(self, file_id: str) -> bytes:
        return self.inbox[file_id][1]

    def move_to_processed(self, file_id: str) -> None:
        self.inbox.pop(file_id, None)
        self.processed.append(file_id)

    def write_result(self, job_id: str, result: Mapping[str, Any]) -> None:
        if self.fail_writes:
            raise DriveError("Drive receipt create failed (HttpError)")
        self.results[job_id] = json.loads(json.dumps(result, default=str))


_warned_missing = False


def google_job_folders() -> Optional[GoogleDriveJobFolders]:
    """The live folders, or None (logged once) while a Drive setting is blank."""
    global _warned_missing
    s = get_settings()
    values = {"LENDING_DRIVE_SERVICE_ACCOUNT_KEY_PATH": s.lending_drive_service_account_key_path,
              "LENDING_JOBS_INBOX_FOLDER_ID": s.lending_jobs_inbox_folder_id,
              "LENDING_JOBS_PROCESSED_FOLDER_ID": s.lending_jobs_processed_folder_id,
              "LENDING_JOBS_DONE_FOLDER_ID": s.lending_jobs_done_folder_id}
    missing = [name for name, value in values.items() if not value]
    if missing:
        if not _warned_missing:
            logger.warning("[agent-jobs] Drive folders not configured; missing: %s", ", ".join(missing))
            _warned_missing = True
        return None
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    try:
        creds = service_account.Credentials.from_service_account_file(s.lending_drive_service_account_key_path,
                                                                      scopes=[DRIVE_SCOPE])
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:  # class only: a bad key file must not stop Cora
        logger.error("[agent-jobs] Drive service could not be built (%s); check the key file", type(exc).__name__)
        return None
    return GoogleDriveJobFolders(service, inbox=s.lending_jobs_inbox_folder_id,
                                 processed=s.lending_jobs_processed_folder_id, done=s.lending_jobs_done_folder_id)
