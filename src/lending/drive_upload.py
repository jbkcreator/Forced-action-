"""Upload a file into a dated subfolder of a shared Google Drive folder, through the lending service account.

A thin seam for P1.2 until the P1.1 Drive client lands: ``upload_csv(parent_id, subfolder, name, data)``. The subfolder
is found or created under the parent; a file of the same name is replaced in place, so a re-run is harmless. Needs the
full ``drive`` scope: the folders are Josh's, shared with the service account, which ``drive.file`` cannot see.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
FOLDER_MIME = "application/vnd.google-apps.folder"


def drive_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(
        get_settings().lending_sheets_service_account_key_path, scopes=[DRIVE_SCOPE])
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find(service: Any, parent_id: str, name: str, *, folder: bool) -> Optional[str]:
    mime = f"mimeType = '{FOLDER_MIME}'" if folder else f"mimeType != '{FOLDER_MIME}'"
    query = f"'{_quote(parent_id)}' in parents and name = '{_quote(name)}' and {mime} and trashed = false"
    found = service.files().list(q=query, fields="files(id)", pageSize=1, supportsAllDrives=True,
                                 includeItemsFromAllDrives=True).execute().get("files", [])
    return found[0]["id"] if found else None


def ensure_folder(service: Any, parent_id: str, name: str) -> str:
    existing = _find(service, parent_id, name, folder=True)
    if existing:
        return existing
    created = service.files().create(body={"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]},
                                     fields="id", supportsAllDrives=True).execute()
    return created["id"]


def upload_bytes(service: Any, folder_id: str, name: str, data: bytes, mime: str = "text/csv") -> str:
    from googleapiclient.http import MediaInMemoryUpload

    media = MediaInMemoryUpload(data, mimetype=mime, resumable=False)
    existing = _find(service, folder_id, name, folder=False)
    if existing:
        service.files().update(fileId=existing, media_body=media, supportsAllDrives=True).execute()
        return existing
    created = service.files().create(body={"name": name, "parents": [folder_id]}, media_body=media, fields="id",
                                     supportsAllDrives=True).execute()
    return created["id"]


class DriveUploader:
    """``Uploader`` for ledger_export.publish: one service, the dated folder resolved once per subfolder."""

    def __init__(self, service: Any = None):
        self._service = service
        self._folders: dict[tuple[str, str], str] = {}

    def __call__(self, parent_id: str, subfolder: str, name: str, data: bytes) -> None:
        service = self._service = self._service or drive_service()
        key = (parent_id, subfolder)
        if key not in self._folders:
            self._folders[key] = ensure_folder(service, parent_id, subfolder)
        upload_bytes(service, self._folders[key], name, data)

    def has_file(self, parent_id: str, subfolder: str, name: str) -> bool:
        service = self._service = self._service or drive_service()
        folder = _find(service, parent_id, subfolder, folder=True)
        return bool(folder and _find(service, folder, name, folder=False))


def latest_manifest(parent_id: str, name: str = "manifest.json", service: Any = None) -> Optional[dict]:
    """The newest dated subfolder's manifest (folders are named YYYY-MM-DD, so name order is date order), or None."""
    import json

    service = service or drive_service()
    folders = service.files().list(
        q=f"'{_quote(parent_id)}' in parents and mimeType = '{FOLDER_MIME}' and trashed = false",
        orderBy="name desc", fields="files(id,name)", pageSize=10, supportsAllDrives=True,
        includeItemsFromAllDrives=True).execute().get("files", [])
    for folder in folders:
        file_id = _find(service, folder["id"], name, folder=False)
        if file_id:
            data = service.files().get_media(fileId=file_id, supportsAllDrives=True).execute()
            return {"folder": folder["name"], **json.loads(data)}
    return None
