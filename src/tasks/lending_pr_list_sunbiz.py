"""Sunbiz lookups for a calling list's company sellers (Wholesalers): who is behind the company, and where.

PropertyRadar gives a wholesaler only as the seller's name. A company seller is looked up on
search.sunbiz.org with the existing Sunbiz scraper (src/scrappers/sunbiz/sunbiz_engine.py): exact
normalized-name match only, never fuzzy, so the wrong company is never picked. A hit is saved to
``sunbiz_snapshots`` like every other Sunbiz lookup.

Idempotent and file-first: every name's outcome (found, not_found, failed) is appended to
``data/property_radar_lists/<list>/sunbiz/lookups.jsonl`` the moment it is known, before the database.
A run skips names already in that file or already in ``sunbiz_snapshots``, so re-running only looks up
what is missing; a stop part-way loses nothing. Only one run at a time (the calling-list lock).

Usage:
    python -m src.tasks.lending_pr_list_sunbiz --list wholesaler            # plan only
    python -m src.tasks.lending_pr_list_sunbiz --list wholesaler --live
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from sqlalchemy import text

from config.lending_calling_lists import CALLING_LISTS, LANDING_ROOT
from src.core.database import get_db_context
from src.lending import pr_list_landing as landing

logger = logging.getLogger(__name__)

# A seller name that is a registered entity, not a person.
_COMPANY_WORDS = re.compile(
    r"\b(LLC|L L C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|LLLP|LTD|PA|PLLC|HOLDINGS|PROPERTIES|INVESTMENTS|"
    r"GROUP|PARTNERS|FUND|HOMES|DEVELOPMENT|REALTY|CAPITAL|VENTURES|ENTERPRISES|ASSOCIATION|BANK)\b")
# Trusts and estates are not Sunbiz entities (the existing scraper skips them the same way).
_NOT_SUNBIZ = re.compile(r"\b(TRUST|TRUSTEE|TTEE|ESTATE OF|STATE OF)\b")
_SUNBIZ_ENTITY_PREFIX = re.compile(
    r"^(FLORIDA |FOREIGN )?(LIMITED LIABILITY COMPANY|PROFIT CORPORATION|NON PROFIT CORPORATION|"
    r"LIMITED PARTNERSHIP|GENERAL PARTNERSHIP)\s+")
STOP_AFTER_CONSECUTIVE_FAILURES = 4
FAILURE_BACKOFF_SECONDS = 60


def is_company(name: Optional[str]) -> bool:
    upper = (name or "").upper()
    return bool(_COMPANY_WORDS.search(upper)) and not _NOT_SUNBIZ.search(upper)


def lookups_path(list_key: str, root: Optional[Path] = None) -> Path:
    return (root or LANDING_ROOT) / list_key / "sunbiz" / "lookups.jsonl"


def done_names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as handle:
        return {json.loads(line)["normalized_name"] for line in handle if line.strip()}


def append_outcome(path: Path, outcome: dict) -> None:
    """One line per name, flushed and synced before anything else happens with it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(outcome, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def to_look_up(seller_names: Iterable[str], *, already_done: set[str], in_snapshots: set[str]) -> list[str]:
    """Distinct company seller names not yet looked up, in a stable order."""
    from src.scrappers.sunbiz.sunbiz_engine import _normalize

    seen, out = set(), []
    for name in sorted({n.strip() for n in seller_names if n and is_company(n)}):
        key = _normalize(name)
        if key in seen or key in already_done or key in in_snapshots:
            continue
        seen.add(key)
        out.append(name)
    return out


def _snapshot_names(db) -> set[str]:
    from src.scrappers.sunbiz.sunbiz_engine import _normalize

    return {_normalize(_SUNBIZ_ENTITY_PREFIX.sub("", (n or "").upper()))
            for (n,) in db.execute(text("SELECT raw_jsonb->>'name' FROM sunbiz_snapshots"))}


def _company_sellers(db, list_key: str) -> list[str]:
    return [r[0] for r in db.execute(text(
        "SELECT DISTINCT last_transfer_seller FROM lending.pr_list_records "
        "WHERE list_key = :k AND qualifies AND last_transfer_seller IS NOT NULL"), {"k": list_key})]


def _save_snapshot(db, html: Optional[str], snap) -> None:
    from src.core.models import SunbizSnapshot as SunbizSnapshotRow
    from src.scrappers.sunbiz.parser import PARSER_VERSION

    db.add(SunbizSnapshotRow(sunbiz_doc_number=snap.doc_number, raw_html=html, raw_jsonb=snap.to_jsonb(),
                             parser_version=snap.parser_version or PARSER_VERSION, status=snap.status))
    db.commit()


async def look_up(names: list[str], *, path: Path, db, headless: bool = True) -> dict[str, int]:
    """Search each name on Sunbiz (paced like the existing scraper), recording every outcome to disk first."""
    from playwright.async_api import async_playwright

    from src.scrappers.sunbiz.sunbiz_engine import (
        _DELAY_SECONDS, _clean_search_name, _normalize, _scrape_entity_detail)
    from src.utils.http_helpers import STEALTH_ARGS, STEALTH_UA, apply_stealth_to_page

    counts = {"found": 0, "not_found": 0, "failed": 0}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, args=STEALTH_ARGS)
        context = await browser.new_context(user_agent=STEALTH_UA, viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        try:
            await apply_stealth_to_page(page)
        except (ModuleNotFoundError, ImportError):
            pass
        consecutive_failures = 0
        try:
            for index, name in enumerate(names, 1):
                search = _clean_search_name(name)
                await asyncio.sleep(_DELAY_SECONDS)
                html = snap = None
                error = None
                for attempt in range(2):
                    try:
                        html, snap = await _scrape_entity_detail(page, search)
                        error = None
                        break
                    except Exception as exc:  # class only: page errors can echo the site
                        error = type(exc).__name__
                        await asyncio.sleep(3)
                found = snap is not None and snap.doc_number and snap.status in ("ok", "partial")
                status = "failed" if error else ("found" if found else "not_found")
                append_outcome(path, {"name": name, "normalized_name": _normalize(name), "status": status,
                                      "doc_number": snap.doc_number if found else None,
                                      "snapshot": snap.to_jsonb() if found else None, "error": error,
                                      "looked_up_at": datetime.now(timezone.utc)})
                if found:
                    _save_snapshot(db, html, snap)
                counts[status] += 1
                consecutive_failures = consecutive_failures + 1 if error else 0
                if consecutive_failures >= STOP_AFTER_CONSECUTIVE_FAILURES:
                    logger.warning("[list-sunbiz] %d failures in a row; backing off %ds", consecutive_failures,
                                   FAILURE_BACKOFF_SECONDS)
                    await asyncio.sleep(FAILURE_BACKOFF_SECONDS)
                    if consecutive_failures >= STOP_AFTER_CONSECUTIVE_FAILURES * 2:
                        logger.error("[list-sunbiz] Sunbiz keeps failing; stopping. Run again to resume.")
                        break
                if index % 25 == 0:
                    logger.info("[list-sunbiz] %d/%d looked up: %s", index, len(names), json.dumps(counts))
        finally:
            await browser.close()
    return counts


def _retry_failed_names(path: Path) -> None:
    """Failed lookups are retried on the next run: drop their lines so they count as not done."""
    if not path.exists():
        return
    with path.open(encoding="utf-8") as handle:
        lines = [line for line in handle if line.strip() and json.loads(line)["status"] != "failed"]
    tmp = path.with_suffix(".jsonl.tmp")
    tmp.write_text("".join(lines), encoding="utf-8")
    os.replace(tmp, path)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Sunbiz lookups for a calling list's company sellers.")
    parser.add_argument("--list", dest="list_key", choices=CALLING_LISTS, required=True)
    parser.add_argument("--live", action="store_true", help="run the lookups (default: plan only)")
    args = parser.parse_args(argv)
    path = lookups_path(args.list_key)
    if args.live:
        _retry_failed_names(path)
    with get_db_context() as db:
        sellers = _company_sellers(db, args.list_key)
        names = to_look_up(sellers, already_done=done_names(path), in_snapshots=_snapshot_names(db))
        logger.info("[list-sunbiz] %s: %d company sellers, %d to look up", args.list_key,
                    sum(1 for s in sellers if is_company(s)), len(names))
        if not args.live or not names:
            return 0
        try:
            lock = landing.acquire_pull_lock()
        except landing.PullLocked as exc:
            logger.error("[list-sunbiz] refused: %s", exc)
            return 3
        try:
            counts = asyncio.run(look_up(names, path=path, db=db))
        finally:
            landing.release_pull_lock(lock)
        logger.info("[list-sunbiz] done: %s (outcomes in %s)", json.dumps(counts), path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
