"""Health of the lending-api process: /health (deploy gate, watchdog) and /api/lending/health (public, via nginx).

No database access on purpose: deploy.sh hard-gates on /health, so it must answer while the database is slow.
Both routes report the commit the process was started from (src/utils/build_info.py).
"""
from __future__ import annotations

from fastapi import APIRouter

from src.utils.build_info import build_info

router = APIRouter()
build_info()  # warms the cache when the worker starts; the handler still calls build_info() so tests can replace it


@router.get("/health")
@router.get("/api/lending/health")
def health() -> dict:
    info = build_info()
    return {"status": "ok", "commit_sha": info.commit_sha, "migration_head": info.migration_head}
