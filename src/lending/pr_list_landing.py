"""Landing files for PropertyRadar calling-list pulls: every bought page is written to disk exactly as
PropertyRadar returned it, before anything is parsed or sent to the database.

The files are the source of truth for what was paid for. The database is loaded from them, and a
record that is in a file counts as already bought, so a failed database write is fixed by loading
the files again (``--load-files``), never by buying again.

Layout: ``<LANDING_ROOT>/<list_key>/pull_<pull_id>/page_<n>.json``, each a JSON list of the raw records.
"""
from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from config.lending_calling_lists import LANDING_ROOT

_PULL_DIR_PREFIX = "pull_"
_PAGE_GLOB = "page_*.json"


@dataclass(frozen=True)
class LandedPage:
    path: Path
    pull_id: int
    records: list[dict[str, Any]]


def _list_dir(list_key: str, root: Optional[Path] = None) -> Path:
    return (root or LANDING_ROOT) / list_key


def write_page(list_key: str, pull_id: int, page_no: int, records: list[dict[str, Any]],
               *, root: Optional[Path] = None) -> Path:
    """Write one bought page as-is. Atomic: a temp file renamed into place, so a page file is
    either complete or absent, never half written."""
    folder = _list_dir(list_key, root) / f"{_PULL_DIR_PREFIX}{pull_id}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"page_{page_no:04d}.json"
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(records, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return path


def iter_pages(list_key: str, *, root: Optional[Path] = None) -> Iterator[LandedPage]:
    """Every landed page for a list, oldest pull first, in page order."""
    base = _list_dir(list_key, root)
    if not base.exists():
        return
    pulls = sorted((p for p in base.iterdir() if p.is_dir() and p.name.startswith(_PULL_DIR_PREFIX)),
                   key=lambda p: int(p.name[len(_PULL_DIR_PREFIX):]))
    for pull_dir in pulls:
        pull_id = int(pull_dir.name[len(_PULL_DIR_PREFIX):])
        for path in sorted(pull_dir.glob(_PAGE_GLOB)):
            with path.open(encoding="utf-8") as handle:
                yield LandedPage(path=path, pull_id=pull_id, records=json.load(handle))


def landed_radar_ids(list_key: str, *, root: Optional[Path] = None) -> set[str]:
    return {str(r["RadarID"]) for page in iter_pages(list_key, root=root) for r in page.records if r.get("RadarID")}


class PullLocked(RuntimeError):
    """Another calling-list pull is running."""


def _holder_pid(path: Path) -> Optional[int]:
    try:
        content = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    parts = content.split()
    return int(parts[1]) if len(parts) == 2 and parts[0] == "pid" and parts[1].isdigit() else None


@dataclass(frozen=True)
class PullLock:
    path: Path
    fd: int


def acquire_pull_lock(*, root: Optional[Path] = None) -> PullLock:
    """One pull at a time on this machine: two pulls at once could buy the same records. An exclusive ``flock``
    on the lock file, held by the open descriptor until release: the kernel drops it when the holder exits, so a
    crashed run never leaves a lock behind and two processes can never both take one over. POSIX only
    (production)."""
    import fcntl

    folder = root or LANDING_ROOT
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / ".pull.lock"
    for _ in range(3):
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise PullLocked(f"another pull holds {path} (pid {_holder_pid(path)})") from None
        try:
            current = os.stat(path).st_ino == os.fstat(fd).st_ino
        except FileNotFoundError:
            current = False
        if current:
            os.ftruncate(fd, 0)
            os.write(fd, f"pid {os.getpid()}".encode())
            return PullLock(path=path, fd=fd)
        os.close(fd)  # the holder released and removed this file between our open and lock: lock the new one
    raise PullLocked(f"could not lock {path}; another pull keeps replacing it")


def release_pull_lock(lock: PullLock) -> None:
    """Removed while still locked, so a process waiting on the old file sees it is gone and locks the new one."""
    lock.path.unlink(missing_ok=True)
    os.close(lock.fd)
