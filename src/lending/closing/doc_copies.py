"""P2.4: copy borrowers' uploaded documents to Josh's restricted Drive folder, so he can open them.

Off until LENDING_DOC_UPLOAD_DRIVE_FOLDER_ID is set (a Shared Drive or folder only Josh, and whoever he names, can
open, with the lending service account as Content manager; never one of the team-wide folders). Runs on Cora's
minute sweep after the chase step, in its own transaction: each upload not yet copied goes to ``chase-<id>/`` as
``<item>-<upload id><ext>`` (no borrower name in Drive), and its Drive file id is recorded, so a copy is never made
twice (``upload_bytes`` also replaces by name).

Never gives up: a failed copy is retried later with a growing wait per upload (RETRY_FIRST doubling to RETRY_MAX),
so once the cause is fixed every waiting document goes. A failure also stops the batch and pauses the whole step
(PAUSE_FIRST doubling to PAUSE_MAX), and each run has a time budget, so a hanging Drive never holds Cora's sweep for
long. One ops notice while uploads have failed ALERT_AFTER times or more, at most once per ALERT_COOLDOWN (one
cause, one notice, not one per file). The file on the
server stays (the source of truth and the server backup).
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

ALERT_AFTER = 5
BATCH = 5
TIME_BUDGET_SECONDS = 20.0
RETRY_FIRST = timedelta(minutes=1)
RETRY_MAX = timedelta(hours=6)
PAUSE_FIRST = timedelta(minutes=5)
PAUSE_MAX = timedelta(hours=1)
ALERT_COOLDOWN = timedelta(hours=6)

# (parent folder id, subfolder name, file name, data, mime type) -> Drive file id
DriveCopier = Callable[[str, str, str, bytes, str], str]

_PENDING = text("""
    SELECT id, chase_id, item_code, stored_path, content_type
      FROM lending.doc_chase_uploads
     WHERE drive_file_id IS NULL AND (drive_retry_at IS NULL OR drive_retry_at <= :now)
       AND quarantine_status IS DISTINCT FROM 'quarantined'
     ORDER BY drive_retry_at NULLS FIRST, id
     LIMIT :batch
       FOR UPDATE SKIP LOCKED
""")
_COPIED = text("UPDATE lending.doc_chase_uploads SET drive_file_id = :file, drive_retry_at = NULL WHERE id = :id")
# Next try after n failures: 1, 2, 4 ... minutes (2^(n-1)), at most RETRY_MAX. SET reads the old drive_attempts.
_FAILED = text("UPDATE lending.doc_chase_uploads SET drive_attempts = drive_attempts + 1, "
               "drive_retry_at = CAST(:now AS timestamptz) "
               "+ LEAST(power(2, LEAST(drive_attempts, 16)), :cap) * interval '1 minute' "
               "WHERE id = :id RETURNING drive_attempts")
_RETRY_CAP_MINUTES = int(RETRY_MAX / RETRY_FIRST)
_STUCK = text("SELECT count(*) FROM lending.doc_chase_uploads WHERE drive_file_id IS NULL "
              "AND drive_attempts >= :attempts AND quarantine_status IS DISTINCT FROM 'quarantined'")


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


def copy_uploads_to_drive(db, *, folder_id: str, copier: DriveCopier, now: Optional[datetime] = None,
                          clock: Callable[[], float] = time.monotonic) -> tuple[int, Optional[str]]:
    """Copy uploads that are due; returns (copied, error class of the failure or None). Stops at the first failure
    or when the time budget is used. Each copy is recorded in its own savepoint; the caller commits."""
    if not folder_id:
        return 0, None
    now = now or datetime.now(timezone.utc)
    started, copied, error = clock(), 0, None
    for row in db.execute(_PENDING, {"now": now, "batch": BATCH}).mappings().all():
        if clock() - started > TIME_BUDGET_SECONDS:
            break
        name = f"{row['item_code']}-{row['id']}{os.path.splitext(row['stored_path'])[1].lower()}"
        try:
            with open(row["stored_path"], "rb") as handle:
                data = handle.read()
            file_id = copier(folder_id, f"chase-{row['chase_id']}", name, data, row["content_type"])
            with db.begin_nested():
                db.execute(_COPIED, {"file": file_id, "id": row["id"]})
            copied += 1
        except Exception as exc:  # class only: Drive and file errors can carry ids and paths
            attempts = db.execute(_FAILED, {"id": row["id"], "now": now, "cap": _RETRY_CAP_MINUTES}).scalar() or 1
            logger.error("[doc-copies] upload %s not copied to Drive (%s), try %s; retrying later", row["id"],
                         type(exc).__name__, attempts)
            error = type(exc).__name__
            break
    if copied:
        logger.info("[doc-copies] %s document(s) copied to Drive", copied)
    return copied, error


def stuck_uploads(db) -> int:
    """Uploads not yet in Drive that have failed ALERT_AFTER times or more."""
    return int(db.execute(_STUCK, {"attempts": ALERT_AFTER}).scalar() or 0)


class DriveCopyStep:
    """The sweep step: runs ``copy_uploads_to_drive`` in its own session unless paused after a failure."""

    def __init__(self, session_factory: Callable[[], Any], *, folder_id: str, copier: DriveCopier,
                 notify: Optional[Callable[[str], Any]] = None, clock: Callable[[], datetime] = lambda: datetime.now(
                     timezone.utc)):
        self._session_factory, self._folder_id, self._copier, self._notify = session_factory, folder_id, copier, notify
        self._clock = clock
        self._pause = PAUSE_FIRST
        self._paused_until: Optional[datetime] = None
        self._alerted_at: Optional[datetime] = None

    def __call__(self) -> int:
        now = self._clock()
        if self._paused_until is not None and now < self._paused_until:
            return 0
        stuck = 0
        try:
            with self._session_factory() as db, db.begin():
                copied, error = copy_uploads_to_drive(db, folder_id=self._folder_id, copier=self._copier, now=now)
                if error is not None:
                    stuck = stuck_uploads(db)
        except Exception as exc:  # class only
            logger.error("[doc-copies] run skipped (%s)", type(exc).__name__)
            copied, error = 0, type(exc).__name__
        if error is not None:
            self._paused_until = now + self._pause
            self._pause = min(self._pause * 2, PAUSE_MAX)
            self._alert(stuck, error, now)
        else:
            self._paused_until, self._pause = None, PAUSE_FIRST
        return copied

    def _alert(self, stuck: int, error: str, now: datetime) -> None:
        """One notice per outage: only when uploads have failed ALERT_AFTER times, at most every ALERT_COOLDOWN."""
        if not stuck or self._notify is None:
            return
        if self._alerted_at is not None and now - self._alerted_at < ALERT_COOLDOWN:
            return
        self._alerted_at = now
        self._notify(f":warning: {stuck} borrower document(s) not yet copied to Drive after {ALERT_AFTER} or more "
                     f"tries ({error}). They are safe on the server and copying keeps retrying; check "
                     "LENDING_DOC_UPLOAD_DRIVE_FOLDER_ID, the folder's sharing and the Drive service account key.")
