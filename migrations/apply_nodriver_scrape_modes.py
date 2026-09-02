"""Idempotent migration — add nodriver_only / nodriver_then_ai scrape modes.

Widens the county_sources.scrape_mode CHECK constraint to accept two new
values that mirror playwright_only / playwright_then_ai but drive the scrape
through nodriver instead of Playwright. Needed for Cloudflare Turnstile-
protected portals (Pinellas Clerk) where Playwright's CDP automation
fingerprint re-triggers the challenge on every navigation even against an
already-warmed persistent browser profile — see
docs/PINELLAS_CLOUDFLARE_BYPASS.md and
docs/MULTI_COUNTY_SCRAPING_ARCHITECTURE.md Section 4.

The stored playwright_code contract is unchanged (still
`async def run_scrape(page, download_dir, start_date, end_date, url, county_id)`
returning a DataFrame) — execute_playwright_code() is driver-agnostic, it just
hands the code whichever page-like object the engine launched.

Also migrates the existing Pinellas liens source (county_id='pinellas',
signal_type='liens') from scrape_mode='playwright_then_ai' to
'nodriver_then_ai' and replaces its hardcoded-in-engine scrape logic with a
DB-stored, human-authored, approved playwright_code — restoring the
"fix a county via UPDATE, not a deploy" property this architecture is built
around.

    PYTHONPATH=. python migrations/apply_nodriver_scrape_modes.py

Safe to re-run: the CHECK constraint is dropped/recreated, and the source
row / history insert are idempotent (guarded by a WHERE that only fires when
scrape_mode is still the pre-migration value).
"""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

_CONSTRAINT_STATEMENTS = [
    # The live DB constraint is actually named "check_county_sources_scrape_mode"
    # (no ck_ prefix) — drifted from the "ck_county_sources_scrape_mode" name
    # models.py declares. Drop both possible names and recreate under the name
    # models.py expects, so create_all() and the live schema agree going forward.
    "ALTER TABLE county_sources DROP CONSTRAINT IF EXISTS check_county_sources_scrape_mode",
    "ALTER TABLE county_sources DROP CONSTRAINT IF EXISTS ck_county_sources_scrape_mode",
    (
        "ALTER TABLE county_sources ADD CONSTRAINT ck_county_sources_scrape_mode "
        "CHECK (scrape_mode IN ('ai_only','playwright_only','playwright_then_ai',"
        "'nodriver_only','nodriver_then_ai','static_download','api'))"
    ),
]

_CODE_PATH = Path(__file__).resolve().parent / "assets" / "pinellas_liens_nodriver_run_scrape.py"

_PROMPT_VERSION = "nodriver-v1"

_PRE_MIGRATION_MODE = "playwright_then_ai"


def _should_migrate_source(current_mode: str) -> bool:
    """True only when current_mode is still the known pre-migration value.

    Pure function — no I/O — so this guard is directly unit-testable
    without touching the live county_sources row it's actually applied
    against. Any other value — nodriver_then_ai (already migrated), or
    ai_only/playwright_only/etc. an operator deliberately set afterward
    (e.g. as a mitigation, or a manually approved fix) — must be left
    untouched. Without this guard, re-running this "safe to re-run"
    migration would silently stomp an operator's intentional mode change
    and replace their approved playwright_code with this migration's own
    version — found in PR review."""
    return current_mode == _PRE_MIGRATION_MODE


def run() -> None:
    code = _CODE_PATH.read_text(encoding="utf-8")

    with get_db_context() as db:
        for stmt in _CONSTRAINT_STATEMENTS:
            logger.info("[migration] %s", stmt)
            db.execute(text(stmt))

        row = db.execute(
            text(
                "SELECT id, scrape_mode FROM county_sources "
                "WHERE county_id = 'pinellas' AND signal_type = 'liens'"
            )
        ).fetchone()

        if row is None:
            logger.warning(
                "[migration] no county_sources row for pinellas/liens — "
                "constraint widened, but no source migrated"
            )
            db.commit()
            return

        source_id, current_mode = row
        if not _should_migrate_source(current_mode):
            logger.info(
                "[migration] pinellas/liens is on scrape_mode=%r (source_id=%s), not the "
                "pre-migration %r — skipping source update to preserve operator config",
                current_mode, source_id, _PRE_MIGRATION_MODE,
            )
        else:
            db.execute(
                text(
                    "UPDATE county_sources SET "
                    "scrape_mode = 'nodriver_then_ai', "
                    "playwright_code = :code, "
                    "playwright_code_version = :version, "
                    "playwright_code_approved = true "
                    "WHERE id = :source_id"
                ),
                {"code": code, "version": _PROMPT_VERSION, "source_id": source_id},
            )
            db.execute(
                text(
                    "INSERT INTO playwright_code_history "
                    "(source_id, county_id, code, prompt_version, reason, is_approved) "
                    "VALUES (:source_id, 'pinellas', :code, :version, 'manual_paste', true)"
                ),
                {"source_id": source_id, "code": code, "version": _PROMPT_VERSION},
            )
            logger.info(
                "[migration] pinellas/liens (source_id=%s) migrated %s -> nodriver_then_ai, "
                "playwright_code replaced (%d chars)",
                source_id, current_mode, len(code),
            )

        db.commit()

    try:
        from src.utils.county_config import invalidate_cache
        invalidate_cache("pinellas")
    except Exception as exc:
        logger.warning("[migration] cache invalidation failed (non-critical): %s", exc)

    logger.info("[migration] nodriver scrape modes applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
