"""P2.4: copy borrowers' uploaded documents to Josh's restricted Drive folder, so he can open them.

Off until LENDING_DOC_UPLOAD_DRIVE_FOLDER_ID is set (a folder only Josh, and whoever he names, can open, shared with
the lending service account as Content manager; never one of the team-wide folders). Runs on Cora's minute sweep
after the chase step, in its own transaction: each upload not yet copied goes to ``chase-<id>/`` as
``<item>-<upload id><ext>`` (no borrower name in Drive), and its Drive file id is recorded, so a copy is never made
twice. A failed copy is retried on later sweeps; after COPY_ATTEMPTS failures it stops and posts one ops notice.
The file on the server stays (the source of truth and the server backup).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

COPY_ATTEMPTS = 5
BATCH = 20

# (parent folder id, subfolder name, file name, data, mime type) -> Drive file id
DriveCopier = Callable[[str, str, str, bytes, str], str]

_PENDING = text("""
    SELECT id, chase_id, item_code, stored_path, content_type
      FROM lending.doc_chase_uploads
     WHERE drive_file_id IS NULL AND drive_attempts < :attempts
     ORDER BY id
     LIMIT :batch
       FOR UPDATE SKIP LOCKED
""")
_COPIED = text("UPDATE lending.doc_chase_uploads SET drive_file_id = :file WHERE id = :id")
_FAILED = text("UPDATE lending.doc_chase_uploads SET drive_attempts = drive_attempts + 1 WHERE id = :id "
               "RETURNING drive_attempts")


def live_copier() -> DriveCopier:
    """Copies through the lending Drive service account (src/lending/drive_upload.py); folders resolved once."""
    from src.lending.drive_upload import drive_service, ensure_folder, upload_bytes

    service: Any = None
    folders: dict[tuple[str, str], str] = {}

    def copy(parent_id: str, subfolder: str, name: str, data: bytes, mime: str) -> str:
        nonlocal service
        service = service or drive_service()
        key = (parent_id, subfolder)
        if key not in folders:
            folders[key] = ensure_folder(service, parent_id, subfolder)
        return upload_bytes(service, folders[key], name, data, mime=mime)

    return copy


def copy_uploads_to_drive(db, *, folder_id: str, copier: DriveCopier,
                          notify: Optional[Callable[[str], Any]] = None) -> int:
    """Copy uploads not yet in Drive; returns how many were copied. Each upload in its own savepoint, so one failure
    never stops the others; the caller commits."""
    if not folder_id:
        return 0
    copied = 0
    for row in db.execute(_PENDING, {"attempts": COPY_ATTEMPTS, "batch": BATCH}).mappings().all():
        name = f"{row['item_code']}-{row['id']}{os.path.splitext(row['stored_path'])[1].lower()}"
        try:
            with open(row["stored_path"], "rb") as handle:
                data = handle.read()
            file_id = copier(folder_id, f"chase-{row['chase_id']}", name, data, row["content_type"])
            with db.begin_nested():
                db.execute(_COPIED, {"file": file_id, "id": row["id"]})
            copied += 1
        except Exception as exc:  # class only: Drive and file errors can carry ids and paths
            attempts = db.execute(_FAILED, {"id": row["id"]}).scalar()
            logger.error("[doc-copies] upload %s not copied to Drive (%s), attempt %s of %s", row["id"],
                         type(exc).__name__, attempts, COPY_ATTEMPTS)
            if attempts == COPY_ATTEMPTS and notify is not None:
                notify(f":warning: A borrower document (chase {row['chase_id']}, upload {row['id']}) could not be "
                       f"copied to Drive after {COPY_ATTEMPTS} tries ({type(exc).__name__}). It is safe on the "
                       "server; check LENDING_DOC_UPLOAD_DRIVE_FOLDER_ID and the folder's sharing.")
    if copied:
        logger.info("[doc-copies] %s document(s) copied to Drive", copied)
    return copied
