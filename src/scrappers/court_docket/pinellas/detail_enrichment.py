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


_DATE_ONLY_RE = re.compile(r"^\s*\d{1,2}/\d{1,2}/\d{2,4}\s*$")


def _looks_like_address(s: Optional[str]) -> bool:
    """A real mailing address has letters (street/city/state). Reject DOB / number-
    only values — some criminal/family party_address cells hold a date of birth."""
    if not s:
        return False
    if _DATE_ONLY_RE.match(s):
        return False
    return bool(re.search(r"[A-Za-z]", s))


def _promote_mailing(parties: list[dict], record_type: str) -> Optional[str]:
    """Pick the lead party's mailing address for this record_type, or None.
    Skips party_address values that are a DOB / not a real address."""
    wants = _MAILING_PARTY.get(record_type, [])
    for want in wants:  # priority order
        for p in parties or []:
            ptype = (p.get("party_type") or "").lower()
            addr = p.get("party_address")
            if want in ptype and _looks_like_address(addr):
                return addr
    return None


def _curate(result: dict) -> dict:
    return {k: result.get(k) for k in _DETAIL_KEYS}


# ── Second-pass matching ────────────────────────────────────────────────────
# Pass 1 (loader) matches the filing's party NAME vs properties and inserts the
# row. Pass 2 (here) matches the scraped mailing_address vs properties to
# confirm/strengthen that match — agreement boosts match_confidence; a different
# property is logged as a conflict and the name match is KEPT (mailing addr,
# esp. divorce petitioner, may legitimately differ from the subject property).
# Pure-SQL cascade (no LLM/Anthropic cost). Audit recorded in docket_detail.
_LOADER_BY_TYPE: dict[str, str] = {
    "Eviction": "EvictionLoader",
    "Divorce": "DivorceLoader",
    "Probate": "ProbateLoader",
}
_ADDR_CONFIRM_CONFIDENCE = Decimal("0.98")


def _loader_for(record_type: str, session, county_id: str):
    from src.loaders import legal_proceedings as _lp
    cls = getattr(_lp, _LOADER_BY_TYPE.get(record_type, "EvictionLoader"))
    return cls(session, county_id=county_id)


def _second_pass_address_match(session, lp, mailing: str, record_type: str,
                               county_id: str) -> Optional[dict]:
    """Match scraped mailing_address vs properties as a second, independent
    signal. Agreement with the pass-1 property -> raise match_confidence; a
    different property -> keep pass-1, flag conflict. Never raises. Returns a
    small audit dict (stored in docket_detail) or None when not applicable."""
    if not _looks_like_address(mailing):
        return None
    try:
        from src.loaders._address_utils import split_address
        loader = _loader_for(record_type, session, county_id)
        _, city, zipc = split_address(str(mailing))
        prop, _method, score = loader.find_property_cascade(
            address=str(mailing), zip_code=zipc, city=city,
            addr_threshold=loader._thresholds.address_floor,
        )
    except Exception as exc:
        logger.warning("[docket-detail] pass-2 match error (%s): %s",
                       getattr(lp, "case_number", "?"), str(exc)[:120])
        return None

    if not prop:
        return {"result": "no_addr_match"}

    if prop.id == lp.property_id:
        before = lp.match_confidence
        if (before or Decimal(0)) < _ADDR_CONFIRM_CONFIDENCE:
            lp.match_confidence = _ADDR_CONFIRM_CONFIDENCE
        logger.info("[docket-detail] pass-2 CONFIRM id=%s pid=%s conf %s->%s (addr score=%s)",
                    lp.id, prop.id, before, lp.match_confidence, score)
        return {"result": "confirmed", "property_id": prop.id, "addr_score": score,
                "confidence_before": float(before) if before is not None else None,
                "confidence_after": float(lp.match_confidence)}

    logger.warning("[docket-detail] pass-2 CONFLICT id=%s name_pid=%s addr_pid=%s "
                   "(addr score=%s) — keeping name match",
                   lp.id, lp.property_id, prop.id, score)
    return {"result": "conflict", "name_property_id": lp.property_id,
            "addr_property_id": prop.id, "addr_score": score}


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
             "blocked": 0, "error": 0, "failed": 0,
             "addr_confirmed": 0, "addr_conflict": 0}
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
                    curated = _curate(result)
                    # Promoted queryable projections (alongside the full docket_detail blob).
                    lp.court_docket_parties = result.get("parties") or []
                    lp.court_docket_events = result.get("events") or []
                    lp.court_docket_scraped_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    if balance is not None:
                        lp.balance_due = balance
                    if mailing:
                        lp.mailing_address = mailing
                        sp = _second_pass_address_match(s, lp, mailing, record_type, county_id)
                        if sp:
                            curated["second_pass_match"] = sp
                            if sp.get("result") == "confirmed":
                                stats["addr_confirmed"] += 1
                            elif sp.get("result") == "conflict":
                                stats["addr_conflict"] += 1
                    lp.docket_detail = curated
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


def apply_detail_from_json(record_type: str, detail_json_path, county_id: str = "pinellas") -> dict:
    """Post-load step for the MERGED daily flow: take the detail JSON the merged
    scraper wrote (already-scraped, NO re-search, NO captcha) and UPDATE the rows
    the loader just inserted (matched, docket_status IS NULL) by case_number.

    Only matched rows exist in legal_proceedings, so unmatched grid cases are
    silently ignored. Never raises — a write hiccup never fails the daily run.
    """
    import json as _json
    from pathlib import Path as _Path

    stats = {"record_type": record_type, "updated": 0, "skipped_no_row": 0, "failed": 0,
             "addr_confirmed": 0, "addr_conflict": 0}
    try:
        cases = _json.loads(_Path(detail_json_path).read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("[docket-detail] could not read detail JSON %s: %s", detail_json_path, exc)
        return stats

    db = Database()
    for c in cases:
        case_number = c.get("case_number")
        if not case_number:
            continue
        try:
            mailing = _promote_mailing(c.get("parties", []), record_type)
            balance = _parse_money(c.get("balance_due"))
            with db.session_scope() as s:
                lp = (s.query(LegalProceeding)
                        .filter(LegalProceeding.county_id == county_id,
                                LegalProceeding.case_number == case_number,
                                LegalProceeding.docket_status.is_(None))
                        .first())
                if lp is None:
                    stats["skipped_no_row"] += 1
                    continue
                lp.docket_status = c.get("status") or "error"
                curated = _curate(c)
                lp.court_docket_parties = c.get("parties") or []
                lp.court_docket_events = c.get("events") or []
                lp.court_docket_scraped_at = datetime.now(timezone.utc).replace(tzinfo=None)
                if balance is not None:
                    lp.balance_due = balance
                if mailing:
                    lp.mailing_address = mailing
                    sp = _second_pass_address_match(s, lp, mailing, record_type, county_id)
                    if sp:
                        curated["second_pass_match"] = sp
                        if sp.get("result") == "confirmed":
                            stats["addr_confirmed"] += 1
                        elif sp.get("result") == "conflict":
                            stats["addr_conflict"] += 1
                lp.docket_detail = curated
            stats["updated"] += 1
        except Exception as exc:
            stats["failed"] += 1
            logger.warning("[docket-detail] apply %s failed: %s", case_number, str(exc)[:160])
    logger.info("[docket-detail] apply_detail_from_json %s/%s: %s", county_id, record_type, stats)
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
