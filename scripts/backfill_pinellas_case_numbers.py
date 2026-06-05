"""Backfill real court case numbers for Pinellas rows that lack one.

Source of truth: the Official Records Details popup CASENUMBER field
(officialrecords.mypinellasclerk.gov, CF Edge-profile path — no 2captcha). One
warmed Edge context is reused for the whole batch.

Scope (decided 2026-06-05):
  - legal_proceedings  Probate + Divorce: case_number currently holds the 10-digit
    ORI instrument. Overwrite case_number = dashed court UCN; stash the original
    instrument in meta_data['ori_instrument_number'].
  - legal_and_liens    Judgment: case_number is NULL; fill it. instrument_number
    column already holds the lookup key (unchanged).

Idempotent + resumable: every processed row gets meta_data['casenumber_lookup']
∈ {'found','none'}; reruns skip rows that already carry it. Per-row commit.

Usage:
    python scripts/backfill_pinellas_case_numbers.py --limit 3          # test
    python scripts/backfill_pinellas_case_numbers.py                     # full run
    python scripts/backfill_pinellas_case_numbers.py --headless          # unattended
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from src.core.database import Database
from src.utils.cf_persistent_browser import _launch_edge
from src.scrappers.court_docket.pinellas.court_scraper import decompose_ucn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")

SEARCH = "https://officialrecords.mypinellasclerk.gov/search/SearchTypeInstrumentNumber"


def to_dashed_base(raw: str) -> str | None:
    """Undashed/dashed CASENUMBER -> dashed base UCN (YY-NNNNNN-XX), suffix dropped."""
    u = decompose_ucn(raw)
    if not u:
        return None
    return f"{u['year']}-{u['number']}-{u['court_type']}"


def _candidates(session, limit: int | None):
    """Return list of dicts: {table, id, instrument, record_type}."""
    rows = []
    lp = session.execute(text("""
        select id, case_number, record_type from legal_proceedings
        where county_id='pinellas' and record_type in ('Probate','Divorce')
          and case_number ~ '^[0-9]{10}$'
          and (meta_data->>'casenumber_lookup') is null
        order by id
    """)).fetchall()
    for r in lp:
        rows.append({"table": "legal_proceedings", "id": r[0], "instrument": r[1], "record_type": r[2]})
    ll = session.execute(text("""
        select id, instrument_number, record_type from legal_and_liens
        where county_id='pinellas' and record_type='Judgment'
          and case_number is null and instrument_number is not null
          and (meta_data->>'casenumber_lookup') is null
        order by id
    """)).fetchall()
    for r in ll:
        rows.append({"table": "legal_and_liens", "id": r[0], "instrument": r[1], "record_type": r[2]})
    return rows[:limit] if limit else rows


async def _lookup_casenumber(ctx, instrument: str) -> tuple[str | None, str | None]:
    """On a reused Edge context: instrument -> (CASENUMBER raw, grantor). Opens
    the search page + Details popup, reads the fields, closes the popup."""
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    await page.goto(SEARCH, wait_until="domcontentloaded", timeout=30000)
    await page.locator("#InstrumentNumber, #btnButton").first.wait_for(state="visible", timeout=20000)
    if "Disclaimer" in page.url or await page.locator("#btnButton").count():
        try:
            if await page.locator("#btnButton").is_visible():
                await page.locator("#btnButton").click()
                await page.wait_for_selector("#InstrumentNumber", timeout=20000)
        except Exception:
            pass
    await page.fill("#InstrumentNumber", str(instrument), timeout=15000)
    await page.click("#btnSearch")
    # wait for the result row (replaces fixed sleep — more robust for a long batch)
    row_sel = f"tr:has-text('{instrument}')"
    try:
        await page.wait_for_selector(row_sel, timeout=20000)
    except Exception:
        return None, None  # no results
    popup = None
    try:
        async with ctx.expect_page(timeout=15000) as np:
            await page.locator(row_sel).first.click()
        popup = await np.value
        await popup.wait_for_load_state("domcontentloaded", timeout=20000)
        await popup.wait_for_timeout(1500)
        txt = await popup.locator("body").inner_text()
        cm = re.search(r"CASE\s*NUMBER[:\s]*([A-Za-z0-9\-]+)", txt, re.I)
        gm = re.search(r"Grantor[:\s]*(.+)", txt, re.I)
        return (cm.group(1).strip() if cm else None,
                gm.group(1).split("\n", 1)[0].strip() if gm else None)
    finally:
        if popup:
            try:
                await popup.close()
            except Exception:
                pass


def _apply(session, cand: dict, ucn: str | None, raw: str | None, grantor: str | None, now: str) -> str:
    """Write the result for one row. Returns the status applied:
    'found' (case_number set), 'none' (no CASENUMBER), or 'duplicate'
    (target UCN already exists on another row — many instruments -> one case;
    case_number left unchanged, flagged for a later dedup pass)."""
    tbl = cand["table"]
    status = "found" if ucn else "none"

    # Collision check: legal_proceedings.case_number is globally UNIQUE, and an
    # estate/case can have many recorded instruments. If the UCN already exists
    # on a different row, this row is a duplicate recording — flag, don't write.
    if ucn:
        existing = session.execute(
            text(f"select id from {tbl} where county_id='pinellas' and case_number=:ucn and id<>:id limit 1"),
            {"ucn": ucn, "id": cand["id"]},
        ).fetchone()
        if existing and tbl == "legal_proceedings":
            status = "duplicate"

    meta = {
        "casenumber_lookup": status,
        "casenumber_raw": raw,
        "casenumber_grantor": grantor,
        "casenumber_backfilled_at": now,
    }
    if tbl == "legal_proceedings":
        meta["ori_instrument_number"] = cand["instrument"]
    if status == "duplicate":
        meta["duplicate_case_number"] = ucn

    set_case = status == "found"
    if set_case:
        session.execute(text(f"""
            update {tbl}
            set case_number = :ucn,
                meta_data = coalesce(meta_data,'{{}}'::jsonb) || cast(:meta as jsonb)
            where id = :id
        """), {"ucn": ucn, "meta": _json(meta), "id": cand["id"]})
    else:
        session.execute(text(f"""
            update {tbl}
            set meta_data = coalesce(meta_data,'{{}}'::jsonb) || cast(:meta as jsonb)
            where id = :id
        """), {"meta": _json(meta), "id": cand["id"]})
    return status


def _json(d: dict) -> str:
    import json
    return json.dumps(d)


async def main(limit: int | None, headless: bool, profile: str, shard: str | None):
    db = Database()
    with db.session_scope() as s:
        cands = _candidates(s, None)  # full list first, then shard deterministically
    # Shard "i/n": keep candidates whose position mod n == i (disjoint across shards).
    if shard:
        i, n = (int(x) for x in shard.split("/"))
        cands = [c for idx, c in enumerate(cands) if idx % n == i]
        log.info("shard %d/%d", i, n)
    if limit:
        cands = cands[:limit]
    log.info("[%s] Backfill candidates: %d", profile, len(cands))
    if not cands:
        return

    import datetime as _dt
    found = blank = dup = failed = 0
    async with _launch_edge(profile_name=profile, headless=headless) as ctx:
        for i, c in enumerate(cands, 1):
            try:
                raw, grantor = await _lookup_casenumber(ctx, c["instrument"])
                ucn = to_dashed_base(raw) if raw else None
                now = _dt.datetime.now(_dt.timezone.utc).isoformat()
                with db.session_scope() as s:
                    status = _apply(s, c, ucn, raw, grantor, now)
                if status == "found":
                    found += 1
                elif status == "duplicate":
                    dup += 1
                else:
                    blank += 1
                log.info("[%d/%d] %s id=%s instr=%s -> %s%s",
                         i, len(cands), c["record_type"], c["id"], c["instrument"],
                         (ucn or "no CASENUMBER"),
                         f"  [{status}]" if status != "found" else "")
            except Exception as exc:
                failed += 1
                log.warning("[%d/%d] id=%s instr=%s FAILED: %s",
                            i, len(cands), c["id"], c["instrument"], str(exc)[:120])
            if i % 25 == 0:
                log.info("--- progress: %d/%d (found=%d dup=%d blank=%d failed=%d) ---",
                         i, len(cands), found, dup, blank, failed)
    log.info("DONE: found=%d duplicate=%d blank=%d failed=%d total=%d",
             found, dup, blank, failed, len(cands))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--profile", default="pinellas_clerk", help="CF Edge profile name")
    ap.add_argument("--shard", default=None, help="i/n — process every n-th candidate (disjoint streams)")
    a = ap.parse_args()
    asyncio.run(main(a.limit, a.headless, a.profile, a.shard))
