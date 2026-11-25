"""The commit and newest migration file this process was started from, read once per process.

Alembic is retired (ADR 0024) and no table records applied migrations, so ``migration_head`` is the newest
``migrations/apply_*.py`` file the checkout ships, not proof it ran. A git failure gives ``unknown`` for that field
and never breaks the caller: the deploy gate and the watchdog depend on /health answering.
"""
from __future__ import annotations

import functools
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Sequence

from src.utils.logger import get_logger

logger = get_logger(__name__)

UNKNOWN = "unknown"
REPO_ROOT: Path = Path(__file__).resolve().parents[2]
_SHA = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class BuildInfo:
    commit_sha: str
    migration_head: str


def _git(args: Sequence[str]) -> str:
    # The systemd units set PATH to the venv's bin only, so a bare "git" is not found there.
    git = shutil.which("git") or "/usr/bin/git"
    return subprocess.run([git, "-C", str(REPO_ROOT), *args], capture_output=True, text=True, timeout=5,
                          check=True).stdout.strip()


def _commit_sha(git: Callable[[Sequence[str]], str]) -> str:
    sha = git(["rev-parse", "HEAD"])
    return sha if _SHA.fullmatch(sha) else UNKNOWN


def _migration_head(git: Callable[[Sequence[str]], str]) -> str:
    if git(["rev-parse", "--is-shallow-repository"]) == "true":
        return UNKNOWN  # a depth-1 clone makes its one commit look like it added every migration
    out = git(["log", "-1", "--diff-filter=A", "--format=", "--name-only", "--", "migrations/apply_*.py"])
    names = [line for line in out.splitlines() if line.strip()]
    return Path(max(names)).stem if names else UNKNOWN


def _safely(field: str, read: Callable[[Callable[[Sequence[str]], str]], str],
            git: Callable[[Sequence[str]], str]) -> str:
    try:
        return read(git)
    except Exception as exc:
        logger.warning("build info: could not read %s (%s)", field, type(exc).__name__)
        return UNKNOWN


def read_build_info(git: Optional[Callable[[Sequence[str]], str]] = None) -> BuildInfo:
    run = git or _git  # looked up at call time, so a test can replace _git
    return BuildInfo(_safely("commit sha", _commit_sha, run), _safely("migration head", _migration_head, run))


@functools.lru_cache(maxsize=1)
def build_info() -> BuildInfo:
    """Read once per process: after a ``git pull`` without a restart this still names the code that is running."""
    return read_build_info()
