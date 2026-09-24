"""Idempotent migration — Pinellas liens playwright_code nodriver-v1 -> nodriver-v2.

Updates the DB-stored run_scrape for county_sources pinellas/liens from
migrations/assets/pinellas_liens_nodriver_run_scrape.py:

  - Retries the search click once when no CSV export button appears. The
    Pinellas portal renders an empty search identically to a search that never
    ran (no grid, no message, no button), so a single missed click used to read
    as "no records" — the 2026-09-23 run silently dropped all 2026-09-22 filings.
  - Detects the export by mtime change instead of falling back to the newest
    *.csv in the folder, which could pick up a stale or other-county file.
  - Raises on an unreadable export instead of returning an empty DataFrame.

    PYTHONPATH=. python migrations/apply_pinellas_liens_script_v2.py

Safe to re-run: only fires while the source is still on the known previous
version, so an operator's later manual fix is never overwritten.
"""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

_CODE_PATH = Path(__file__).resolve().parent / "assets" / "pinellas_liens_nodriver_run_scrape.py"

_PREVIOUS_VERSION = "nodriver-v1"
_NEW_VERSION = "nodriver-v2"


def run() -> None:
    from src.utils.action_sequence import validate_playwright_code

    code = _CODE_PATH.read_text(encoding="utf-8")
    validate_playwright_code(code)

    with get_db_context() as db:
        row = db.execute(
            text(
                "SELECT id, playwright_code_version FROM county_sources "
                "WHERE county_id = 'pinellas' AND signal_type = 'liens'"
            )
        ).fetchone()

        if row is None:
            logger.warning("[migration] no county_sources row for pinellas/liens — nothing to update")
            return

        source_id, current_version = row
        if current_version != _PREVIOUS_VERSION:
            logger.info(
                "[migration] pinellas/liens (source_id=%s) is on %r, not %r — skipping",
                source_id, current_version, _PREVIOUS_VERSION,
            )
            return

        db.execute(
            text(
                "UPDATE county_sources SET "
                "playwright_code = :code, "
                "playwright_code_version = :version, "
                "playwright_code_approved = true "
                "WHERE id = :source_id"
            ),
            {"code": code, "version": _NEW_VERSION, "source_id": source_id},
        )
        db.execute(
            text(
                "INSERT INTO playwright_code_history "
                "(source_id, county_id, code, prompt_version, reason, is_approved) "
                "VALUES (:source_id, 'pinellas', :code, :version, 'manual_paste', true)"
            ),
            {"source_id": source_id, "code": code, "version": _NEW_VERSION},
        )
        db.commit()
        logger.info(
            "[migration] pinellas/liens (source_id=%s) playwright_code %s -> %s (%d chars)",
            source_id, _PREVIOUS_VERSION, _NEW_VERSION, len(code),
        )

    try:
        from src.utils.county_config import invalidate_cache
        invalidate_cache("pinellas")
    except Exception as exc:
        logger.warning("[migration] cache invalidation failed (non-critical): %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
