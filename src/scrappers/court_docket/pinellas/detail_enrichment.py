"""Stage-2 court-docket detail enrichment for Pinellas legal_proceedings.

Reads case numbers off already-loaded rows, scrapes each case's detail page on
courtrecords.mypinellasclerk.gov (one warmed PinellasCourtSession per batch —
captcha solved ONCE, reused across all cases), and promotes the result onto the
source row's docket columns (fa069):

    docket_detail    — full scrape_case() payload (curated)
    balance_due      — Financial section Balance Due (parsed to Decimal)
    mailing_address  — promoted party address, per record_type
    docket_status    — ok | case_number_missing | not_found | blocked | error

Called in-process by each Pinellas filing engine (evictions/probate/divorce)
after load_scraped_data_to_db, so detail lands in the same daily run. Forward
-only by default (today's rows); the backlog is cleared by a separate
`--all` sweep (scripts/backfill_pinellas_docket_detail.py).

Design rationale: docs/adr/0012-court-docket-jsonb-on-row.md.

Selection rules:
  - county_id = 'pinellas', record_type matches, docket_status IS NULL
  - case_number parses as a clean Pinellas UCN (decompose_ucn — skips the
    historical 10-digit ORI rows the case-number backfill owns)
  - today_only=True restricts to date_added = today

Idempotency / retry:
  - docket_status IS NULL is the only eligibility gate, so completed rows are
    never re-scraped. Transient failures (blocked/error) are written back as
    that status; the 2-day filing search window re-surfaces them and the daily
    run retries (they're re-selected once a re-run clears them to NULL, or via
    the --all sweep). Per-row commit; one bad row never aborts the batch.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import text

from src.core.database import Database
from src.core.models import LegalProceeding
from src.scrappers.court_docket.pinellas.court_scraper import scrape_case, decompose_ucn
from src.scrappers.court_docket.pinellas.court_session import PinellasCourtSession
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Which party's address is promoted to mailing_address, per record_type.
# Priority order within each list (first non-empty match wins). The target is
# the property owner / actionable lead to skip-trace.
_MAILING_PARTY: dict[str, list[str]] = {
    "Eviction": ["defendant"],                       # defendant = property owner (this dataset)
    "Divorce":  ["petitioner"],                       # filing spouse
    "Probate":  ["personal representative", "petitioner"],  # PR/heir; skip attorney rows
    "Judgment": ["defendant"],                        # debtor = property owner pursued
}

# Fields kept from the scrape_case() result for docket_detail (drops local
# screenshot paths / debug noise — keeps the queryable/displayable payload).
_DETAIL_KEYS = (
    "status", "ucn", "extraction_path", "header", "parties",
    "events", "documents", "financial", "balance_due", "detail_url", "warnings",
)


def _parse_money(raw: Optional[str]) -> Optional[Decimal]:
    """'1,234.56' -> Decimal('1234.56'); None/garbage -> None."""
    if raw is None:
        return None
    cleaned = re.sub(r"[^0-9.]", "", str(raw))
    if not cleaned or cleaned == ".":
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _promote_mailing(parties: list[dict], record_type: str) -> Optional[str]:
    """Pick the lead party's mailing address for this record_type, or None."""
    wants = _MAILING_PARTY.get(record_type, [])
    for want in wants:  # priority order
        for p in parties or []:
            ptype = (p.get("party_type") or "").lower()
            addr = p.get("party_address")
            if want in ptype and addr:
                return addr
    return None


def _curate(result: dict) -> dict:
    return {k: result.get(k) for k in _DETAIL_KEYS}


def _select_candidates(county_id: str, record_type: str, today_only: bool,
                       limit: Optional[int]) -> list[tuple[int, str]]:
    """Return [(id, case_number)] for un-docketed rows with a clean UCN."""
    db = Database()
    sql = """
        SELECT id, case_number FROM legal_proceedings
        WHERE county_id = :cty
          AND record_type = :rt
          AND docket_status IS NULL
          AND case_number IS NOT NULL
    """
    if today_only:
        sql += " AND date_added = current_date"
    sql += " ORDER BY id"
    with db.session_scope() as s:
        rows = s.execute(text(sql), {"cty": county_id, "rt": record_type}).fetchall()
    # Clean-UCN filter (skips historical 10-digit ORI instrument rows).
    cands = [(r[0], r[1]) for r in rows if decompose_ucn(r[1])]
    if limit:
        cands = cands[:limit]
    return cands


async def _run(county_id: str, record_type: str, today_only: bool,
               limit: Optional[int], headless: bool) -> dict:
    cands = _select_candidates(county_id, record_type, today_only, limit)
    stats = {"record_type": record_type, "candidates": len(cands),
             "ok": 0, "not_found": 0, "case_number_missing": 0,
             "blocked": 0, "error": 0, "failed": 0}
    if not cands:
        logger.info("[docket-detail] %s/%s: no un-docketed rows", county_id, record_type)
        return stats

    logger.info("[docket-detail] %s/%s: %d cases to detail", county_id, record_type, len(cands))
    db = Database()
    async with PinellasCourtSession(headless=headless) as session:
        for i, (row_id, case_number) in enumerate(cands, 1):
            try:
                result = await scrape_case(session, case_number)
                status = result.get("status") or "error"
                mailing = _promote_mailing(result.get("parties", []), record_type)
                balance = _parse_money(result.get("balance_due"))
                with db.session_scope() as s:
                    lp = s.get(LegalProceeding, row_id)
                    if lp is None:
                        continue
                    lp.docket_status = status
                    lp.docket_detail = _curate(result)
                    # Promoted queryable projections (alongside the full docket_detail blob).
                    lp.court_docket_parties = result.get("parties") or []
                    lp.court_docket_events = result.get("events") or []
                    lp.court_docket_scraped_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    if balance is not None:
                        lp.balance_due = balance
                    if mailing:
                        lp.mailing_address = mailing
                stats[status] = stats.get(status, 0) + 1
                logger.info("[docket-detail] [%d/%d] id=%s %s -> %s (parties=%d events=%d)",
                            i, len(cands), row_id, case_number, status,
                            len(result.get("parties", [])), len(result.get("events", [])))
            except Exception as exc:
                stats["failed"] += 1
                logger.warning("[docket-detail] [%d/%d] id=%s %s FAILED: %s",
                               i, len(cands), row_id, case_number, str(exc)[:160])
    logger.info("[docket-detail] %s/%s DONE: %s", county_id, record_type, stats)
    return stats


def enrich_proceedings_detail(
    record_type: str,
    county_id: str = "pinellas",
    today_only: bool = True,
    limit: Optional[int] = None,
    headless: bool = True,
) -> dict:
    """Sync entry point — scrape + persist docket detail for one record_type.

    Safe to call inline from a filing engine: never raises (per-row failures are
    counted, not propagated), so a docket hiccup never fails the filing load.
    """
    import asyncio
    try:
        return asyncio.run(_run(county_id, record_type, today_only, limit, headless))
    except Exception as exc:  # session launch failure etc. — never break the load
        logger.error("[docket-detail] %s/%s batch aborted: %s", county_id, record_type, exc)
        return {"record_type": record_type, "candidates": 0, "aborted": str(exc)}
