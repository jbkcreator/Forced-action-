"""Standalone court-docket DETAIL backfill — per-case, both tables.

Looks up each pending case by its DB case_number on courtrecords
(scrape_case — Playwright primary + browser-use fallback), and writes the docket
columns back. Covers BOTH:
  - legal_proceedings : Eviction / Probate / Divorce
  - legal_and_liens   : Judgment   (case# is a real court UCN from the backfill)

Per-case search => ~1 reCAPTCHA solve per case (2captcha). Full backlog
(~1,073 cases) ≈ $3–6 of 2captcha — acceptable per requirement.

Selection per (table, record_type):
  county_id='pinellas', clean Pinellas UCN, and **docket_status IS DISTINCT FROM
  'ok'** — so rows already extracted ('ok') are SKIPPED; NULL / failed
  (blocked/error/not_found/case_number_missing) are (re)attempted.

Idempotent + resumable: a successful row flips to 'ok' and is skipped next run.
One warmed session for the whole run; per-row commit; one bad row never aborts.

Usage:
  python scripts/backfill_docket_all.py --limit 2                 # smoke test (per record_type)
  python scripts/backfill_docket_all.py --table liens             # judgments only
  python scripts/backfill_docket_all.py --record-type Probate
  python scripts/backfill_docket_all.py                           # full backlog
  python scripts/backfill_docket_all.py --headful
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from src.core.database import Database
from src.scrappers.court_docket.pinellas.court_scraper import scrape_case, decompose_ucn
from src.scrappers.court_docket.pinellas.court_session import PinellasCourtSession
from src.scrappers.court_docket.pinellas.detail_enrichment import (
    _promote_mailing, _parse_money, _curate,
)
from src.utils.logger import get_logger

logger = get_logger("docket-backfill-all")

# (table, [record_types]) — both source tables, per-case search by case_number.
SCOPE = {
    "proceedings": ("legal_proceedings", ["Eviction", "Probate", "Divorce"]),
    "liens":       ("legal_and_liens",   ["Judgment"]),
}


def _select(table: str, record_type: str, limit: int | None) -> list[tuple[int, str]]:
    """Pending rows (clean UCN, docket_status != 'ok') for one table+record_type."""
    db = Database()
    sql = f"""
        SELECT id, case_number FROM {table}
        WHERE county_id = 'pinellas'
          AND record_type = :rt
          AND case_number IS NOT NULL
          AND docket_status IS DISTINCT FROM 'ok'
        ORDER BY id
    """
    with db.session_scope() as s:
        rows = s.execute(text(sql), {"rt": record_type}).fetchall()
    cands = [(r[0], r[1]) for r in rows if decompose_ucn(r[1])]
    return cands[:limit] if limit else cands


def _write(table: str, row_id: int, result: dict, record_type: str) -> None:
    """Write docket columns back to the source row (raw SQL — table-agnostic)."""
    db = Database()
    fields = {
        "id": row_id,
        "st": result.get("status") or "error",
        "dd": json.dumps(_curate(result)),
        "bal": _parse_money(result.get("balance_due")),
        "ma": _promote_mailing(result.get("parties", []), record_type),
        "pp": json.dumps(result.get("parties") or []),
        "ee": json.dumps(result.get("events") or []),
        "now": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    with db.session_scope() as s:
        s.execute(text(f"""
            UPDATE {table} SET
                docket_status          = :st,
                docket_detail           = cast(:dd as jsonb),
                balance_due             = :bal,
                mailing_address         = :ma,
                court_docket_parties    = cast(:pp as jsonb),
                court_docket_events     = cast(:ee as jsonb),
                court_docket_scraped_at = :now
            WHERE id = :id
        """), fields)


async def _run_group(session: PinellasCourtSession, table: str, record_type: str,
                     cands: list[tuple[int, str]]) -> dict:
    stats = {"table": table, "record_type": record_type, "candidates": len(cands),
             "ok": 0, "not_found": 0, "case_number_missing": 0, "blocked": 0,
             "error": 0, "failed": 0}
    for i, (row_id, case_number) in enumerate(cands, 1):
        try:
            result = await scrape_case(session, case_number)
            status = result.get("status") or "error"
            _write(table, row_id, result, record_type)
            stats[status] = stats.get(status, 0) + 1
            logger.info("[%s/%s] [%d/%d] id=%s %s -> %s (parties=%d events=%d)",
                        table, record_type, i, len(cands), row_id, case_number, status,
                        len(result.get("parties", [])), len(result.get("events", [])))
        except Exception as exc:
            stats["failed"] += 1
            logger.warning("[%s/%s] [%d/%d] id=%s %s FAILED: %s",
                           table, record_type, i, len(cands), row_id, case_number, str(exc)[:160])
    logger.info("[done] %s/%s: %s", table, record_type, stats)
    return stats


async def main(which: str, only_rt: str | None, limit: int | None, headless: bool) -> None:
    groups = []
    for key, (table, rts) in SCOPE.items():
        if which not in ("all", key):
            continue
        for rt in rts:
            if only_rt and rt != only_rt:
                continue
            cands = _select(table, rt, limit)
            if cands:
                groups.append((table, rt, cands))

    total = sum(len(c) for _, _, c in groups)
    logger.info("Backfill plan: %d groups, %d cases total", len(groups), total)
    for table, rt, cands in groups:
        logger.info("  %s / %s : %d", table, rt, len(cands))
    if not groups:
        logger.info("Nothing to do (all 'ok' or no clean-UCN rows).")
        return

    all_stats = []
    async with PinellasCourtSession(headless=headless, debug_dir=Path("scratch")) as session:
        for table, rt, cands in groups:
            all_stats.append(await _run_group(session, table, rt, cands))
    logger.info("ALL DONE: %s", all_stats)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Per-case court-docket detail backfill (both tables).")
    ap.add_argument("--table", choices=["all", "proceedings", "liens"], default="all")
    ap.add_argument("--record-type", dest="record_type",
                    choices=["Eviction", "Probate", "Divorce", "Judgment"], default=None)
    ap.add_argument("--limit", type=int, default=None, help="cap cases per record_type")
    ap.add_argument("--headful", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(a.table, a.record_type, a.limit, headless=not a.headful))
    sys.exit(0)
