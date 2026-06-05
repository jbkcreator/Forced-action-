"""Scrape one Pinellas court case from courtrecords.mypinellasclerk.gov by case#.

Playwright-primary (stealth Chromium + 2captcha access via PinellasCourtSession),
with a browser-use/Sonnet fallback that fires ONLY on extraction/navigation drift
on an already-cleared page (see court_agent_fallback.py). Returns a structured
dict. NO database writes — persistence is the separate court_* enrichment stage.

Flow (per the Notion reference + 2026-06-04 detail screenshots):
  1. validate the dashed Pinellas UCN (type-LAST: YY-NNNNNN-XX[-suffix])
  2. (session) open Case search, activate the Case tab
  3. fill the Case Number field, Submit
  4. solve the reCAPTCHA (2captcha), wait for results
  5. click the matching blue case# link -> docket detail (caseId + encrypted
     caseIdEnc in the URL; NOT deep-linkable from the case number alone)
  6. DOM-scrape the four detail sections:
       Case Header | Parties | Events & Documents | Financial
  7. return structured JSON (parsed lists, not CSV paths — the portal has no
     per-tab CSV export, unlike HOVER)

Selectors flagged TO CONFIRM are best-effort and get pinned from the first live
probe dump (run this module's __main__ CLI). See
docs/court_docket_phase1/pinellas_courtrecords_selector_notes.md.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from playwright.async_api import Page, TimeoutError as PWTimeout

from .court_session import (
    PinellasCourtSession, PinellasCourtBlockedError, BASE_URL,
)

logger = logging.getLogger(__name__)

# Pinellas dashed UCN, type-LAST: 26-004970-CO, optional party suffix 25-07679-CF-B.
# (Contrast Hillsborough HOVER: type-MIDDLE, 26-CC-025949.)
UCN_RE = re.compile(r"^\s*(\d{2})-(\d+)-([A-Za-z]{2})(?:-([A-Za-z0-9]+))?\s*$")

# UCN 2-letter type -> internal case_type (Pinellas codes; refine from live data).
_TYPE_TO_CASE_TYPE = {
    "CO": "eviction",   # County civil / eviction (e.g. RESIDENTIAL EVICTION ...)
    "CC": "eviction",
    "DR": "divorce",
    "CP": "probate",
    "GA": "probate",
    "CA": "judgment",
    "CF": "judgment",
    "CT": "judgment",
    "MM": "criminal",   # misdemeanor (e.g. judgment-for-fines recordings)
    "MO": "criminal",
    "ES": "probate",    # Estate (Official Records CASENUMBER for PROBATE docs)
    "FD": "divorce",    # Family / Dissolution (CASENUMBER for divorce/DV docs)
    "GD": "probate",    # Guardianship
}

# Undashed CASENUMBER as shown in the Official Records Details popup, e.g.
# "26001007ES" / "26003282FD": 2-digit year + sequence + 2-letter type.
UNDASHED_RE = re.compile(r"^\s*(\d{2})(\d{3,})([A-Za-z]{2})([A-Za-z0-9]*)\s*$")


def decompose_ucn(case_number: str) -> Optional[dict]:
    """Split a Pinellas dashed UCN, or None if not a clean UCN.

    Returns {year, number, court_type, party_suffix, case_type}. Non-UCN values
    (e.g. the 10-digit ORI instrument numbers carried by Pinellas probate/divorce
    rows) return None — the caller marks those `case_number_missing`.
    """
    if not case_number:
        return None
    m = UCN_RE.match(case_number)
    if m:
        year, number, ctype, suffix = m.group(1), m.group(2), m.group(3).upper(), m.group(4)
    else:
        # Accept the undashed Details-popup CASENUMBER form (e.g. 26001007ES).
        m = UNDASHED_RE.match(case_number)
        if not m:
            return None
        year, number, ctype, suffix = m.group(1), m.group(2), m.group(3).upper(), (m.group(4) or None)
    return {
        "year": year,
        "number": number,
        "court_type": ctype,
        "party_suffix": suffix,
        "case_type": _TYPE_TO_CASE_TYPE.get(ctype),
    }


async def scrape_case(
    session: PinellasCourtSession,
    case_number: str,
    allow_fallback: bool = True,
    dump_dom: bool = False,
) -> dict:
    """Look up one case on courtrecords and return a structured result dict.

    status ∈ {ok, case_number_missing, not_found, blocked, error}
    extraction_path ∈ {playwright, browser_use}
    Never raises for per-case issues — failures are captured in the result.
    """
    safe = re.sub(r"[^A-Za-z0-9]+", "_", case_number or "unknown")
    result: dict = {
        "case_number": case_number,
        "county": "pinellas",
        "status": "error",
        "ucn": None,
        "extraction_path": "playwright",
        "header": {},
        "parties": [],
        "events": [],
        "documents": [],
        "financial": [],
        "balance_due": None,
        "detail_url": None,
        "warnings": [],
        "error": None,
        "screenshots": [],
    }

    ucn = decompose_ucn(case_number)
    if ucn is None:
        result["status"] = "case_number_missing"
        result["error"] = (
            "value is not a clean Pinellas UCN (YY-NNNNNN-XX). "
            "Note: Pinellas probate/divorce rows carry 10-digit ORI instrument "
            "numbers, not court-docket case numbers — these cannot be looked up here."
        )
        logger.info("[pinellas-court] %s -> case_number_missing", case_number)
        return result
    result["ucn"] = ucn

    page = session.page
    try:
        # 2. open search + activate Case tab
        await session.goto_search()

        # 3. fill Case Number + submit. Search with the BASE UCN (no party
        # suffix) — the portal's Case Number field matches the base case, and a
        # trailing party/count designator like "-L" yields no results (mirrors
        # HOVER's "search base UCN, party=ALL").
        search_term = f"{ucn['year']}-{ucn['number']}-{ucn['court_type']}"
        await _fill_case_number(page, search_term)
        await _submit_search(page)

        # 4. solve captcha (once per session; reused context skips it next time)
        await session.solve_captcha_if_present()
        try:
            await page.wait_for_load_state("networkidle", timeout=60000)
        except Exception:
            pass
        if await session.is_blocked():
            result["status"] = "blocked"
            result["error"] = "bot wall after search submit"
            result["screenshots"].append(str(await session.debug_capture(f"{safe}_blocked") or ""))
            return result

        # 5. land on the detail page. A single exact case-number match
        # auto-navigates straight to CaseDetails (the captcha callback
        # auto-submits); otherwise we click the matching blue case# in a grid.
        opened = await _ensure_on_detail(session, case_number)
        if not opened:
            result["status"] = "not_found"
            result["screenshots"].append(str(await session.debug_capture(f"{safe}_no_result") or ""))
            return result
        result["detail_url"] = page.url

        if dump_dom:
            await session.debug_capture(f"{safe}_detail")

        # 6. extract the four sections (Playwright-primary)
        try:
            result["header"] = await _scrape_header(page)
            result["parties"] = await _scrape_parties(page)
            result["events"], result["documents"] = await _scrape_events_and_documents(page)
            result["financial"], result["balance_due"] = await _scrape_financial(page)
        except Exception as exc:
            # Extraction/nav drift — hand the ALREADY-CLEARED page to the fallback.
            result["warnings"].append(f"playwright_extract: {type(exc).__name__}: {exc}")
            if allow_fallback:
                logger.warning("[pinellas-court] %s extraction drift — browser-use fallback", case_number)
                fb = await _run_browser_use_fallback(session, case_number)
                if fb is not None:
                    return fb
            result["screenshots"].append(str(await session.debug_capture(f"{safe}_extract_fail") or ""))

        # Consider it ok if we got at least a header or any parties/events.
        if result["header"] or result["parties"] or result["events"]:
            result["status"] = "ok"
        logger.info("[pinellas-court] %s -> %s (parties=%d events=%d docs=%d)",
                    case_number, result["status"], len(result["parties"]),
                    len(result["events"]), len(result["documents"]))
        return result

    except PinellasCourtBlockedError as exc:
        result["status"] = "blocked"
        result["error"] = str(exc)
        result["screenshots"].append(str(await session.debug_capture(f"{safe}_blocked") or ""))
        return result
    except Exception as exc:
        result["status"] = "error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["screenshots"].append(str(await session.debug_capture(f"{safe}_error") or ""))
        logger.exception("[pinellas-court] %s -> error", case_number)
        return result


# ── search-form helpers (front half — pinnable now via free probe) ──────────

async def _fill_case_number(page: Page, case_number: str) -> None:
    # CONFIRMED (2026-06-04 live): id="caseNumber" (lowercase), name="CaseNumber",
    # class "case-number". NB CSS ids are case-sensitive, so "#caseNumber" != the
    # capitalized form — keep the [i] attr selectors as fallback.
    candidates = [
        "#caseNumber", "input.case-number", "input[name='CaseNumber']",
        "input[id*='casenumber' i]", "input[placeholder*='Case Number' i]",
    ]
    for sel in candidates:
        try:
            el = page.locator(sel).first
            if await el.count() and await el.is_visible():
                await el.fill(case_number, timeout=8000)
                return
        except Exception:
            continue
    raise RuntimeError("Case Number input not found (selectors TO CONFIRM)")


async def _submit_search(page: Page) -> None:
    candidates = [
        "input[type='submit'][value='Submit']",
        "button:has-text('Submit')",
        "input[value='Search']",
        "button[type='submit']",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel).first
            if await btn.count() and await btn.is_visible():
                await btn.click(timeout=8000)
                return
        except Exception:
            continue
    raise RuntimeError("Search Submit button not found (selectors TO CONFIRM)")


async def _on_detail(page: Page) -> bool:
    return "CaseDetails" in page.url or bool(await page.locator("#caseHeader").count())


async def _ensure_on_detail(session: PinellasCourtSession, case_number: str) -> bool:
    """Make sure we end up on the case detail page, then wait for its AJAX
    sections to populate.

    Two paths:
      - single exact match -> portal already auto-navigated to CaseDetails;
      - multiple matches   -> a results grid; click the matching blue case#.
    The detail URL carries an encrypted caseIdEnc token, so the grid link must
    be clicked — it is not constructible from the case number.
    """
    page = session.page
    if not await _on_detail(page):
        # results grid path: click the matching case# link
        try:
            await page.wait_for_selector(
                "#caseHeader, a[href*='CaseDetails'], table, [class*='result'], [class*='grid']",
                timeout=90000,
            )
        except PWTimeout:
            return False
        if not await _on_detail(page):
            for sel in (f"a:has-text('{case_number}')",
                        f"tr:has-text('{case_number}') a[href*='CaseDetails']",
                        "a[href*='CaseDetails']"):
                try:
                    link = page.locator(sel).first
                    if await link.count() and await link.is_visible():
                        await link.click(timeout=8000)
                        await page.wait_for_load_state("domcontentloaded", timeout=30000)
                        break
                except Exception:
                    continue
        if not await _on_detail(page):
            return False

    # On the detail page — wait for progressive AJAX sections to fill in.
    try:
        await page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass
    # Parties tbody is the readiness signal; Events/Financial load alongside.
    try:
        await page.wait_for_selector("table[summary='case parties'] tbody tr", timeout=20000)
    except Exception:
        pass
    await session._pace()
    return True


# ── detail-page extraction (TO CONFIRM selectors — pinned from live dump) ────

_LABEL_TO_KEY = {
    "case type": "case_type",
    "date filed": "date_filed",
    "status": "status",
    "court": "court",
    "judicial officer": "judicial_officer",
    "ucn": "uniform_case_number",
}


async def _scrape_header(page: Page) -> dict:
    """Case Header (#headerCollapse): label/value column pairs in .header-row."""
    out: dict = {}
    rows = await page.locator("#headerCollapse .header-row, #caseHeader .header-row").all()
    for r in rows:
        try:
            cols = [c.strip() for c in await r.locator("> div").all_inner_texts()]
            if len(cols) < 2:
                continue
            label = cols[0].rstrip(":").strip().lower()
            key = _LABEL_TO_KEY.get(label)
            if key:
                out[key] = cols[1].strip() or None
        except Exception:
            continue
    # Style ("PLAINTIFF Vs. DEFENDANT") from the results/refine bar.
    try:
        bar = await page.locator(".search-bar-results").first.inner_text(timeout=3000)
        m = re.search(r"(.+?)\s*Vs\.?\s*(.+)", bar, re.I | re.S)
        if m:
            out["style_plaintiff"] = re.sub(r"\s+", " ", m.group(1)).strip().split(":")[-1].strip()
            out["style_defendant"] = re.sub(r"\s+", " ", m.group(2)).strip()
    except Exception:
        pass
    return out


def _clean(s: str) -> Optional[str]:
    s = re.sub(r"[ \t]+", " ", (s or "")).strip()
    s = re.sub(r"\n\s*", ", ", s)
    s = re.sub(r"(,\s*)+", ", ", s).strip(" ,")  # collapse/trim empty comma joins
    return s or None


async def _scrape_parties(page: Page) -> list[dict]:
    """Parties: table[summary='case parties'] — Name|Type|Party Address|Attorney|Lead Attorney Address."""
    parties: list[dict] = []
    rows = await page.locator("table[summary='case parties'] tbody tr").all()
    for r in rows:
        try:
            cells = await r.locator("td").all_inner_texts()
            if not any(c.strip() for c in cells):
                continue
            parties.append({
                "name": _clean(cells[0]) if len(cells) > 0 else None,
                "party_type": _clean(cells[1]) if len(cells) > 1 else None,
                "party_address": _clean(cells[2]) if len(cells) > 2 else None,
                "attorney": _clean(cells[3]) if len(cells) > 3 else None,
                "lead_attorney_address": _clean(cells[4]) if len(cells) > 4 else None,
            })
        except Exception:
            continue
    return parties


# glyphicon color token (on the Doc-column file icon) -> document status.
# Confirmed: gl-green = Public. Others TO CONFIRM on a VOR/sealed/confidential case.
_DOC_STATUS_BY_COLOR = {
    "gl-green": "public",
    "gl-blue": "view_on_request",
    "gl-orange": "confidential",
    "gl-red": "sealed",
    "gl-grey": "pending",
    "gl-gray": "pending",
}


async def _cell_text(row, sel: str) -> Optional[str]:
    try:
        loc = row.locator(sel).first
        if await loc.count():
            return _clean(await loc.inner_text())
    except Exception:
        pass
    return None


async def _scrape_events_and_documents(page: Page) -> tuple[list[dict], list[dict]]:
    """Events & Documents (table[summary='docket events']).

    The visible columns are Date|Event|Comments|eCertify|Docket#|Doc|Pages, but
    each row also carries hidden helper cells (hDocId, hPageCount, hCaseId) and a
    DocView link — we extract from those class-tagged cells, not by position.
    Returns (events, documents). A document is emitted for rows with a DocView
    link / status icon (Public/View-on-Request/Confidential/Sealed/Pending).
    """
    events: list[dict] = []
    documents: list[dict] = []
    seen: set = set()
    # Multiple docket-events tables exist (Events + Other Documents views, and
    # responsive duplicates); iterate all and dedupe by (date, title, docket#).
    tables = await page.locator("table[summary='docket events']").all()
    for table in tables:
        try:
            rows = await table.locator("tbody tr").all()
        except Exception:
            rows = []
        for r in rows:
            try:
                date = await _cell_text(r, "td.dDate")
                title = await _cell_text(r, "td.dDescription") or await _cell_text(r, "td.cdDocLink a")
                comments = await _cell_text(r, "td.ttd-col-sm")
                pages = await _cell_text(r, "td.hPageCount")
                doc_id = await _cell_text(r, "td.hDocId")
                if not (date or title):
                    continue
                # Docket#: the visible td-col-sm whose text is a bare number.
                docket_num = None
                for c in await r.locator("td.td-col-sm").all_inner_texts():
                    c = c.strip()
                    if c.isdigit():
                        docket_num = c
                        break
                # Document link + status icon (Doc column).
                doc_url = None
                try:
                    a = r.locator("td.cdDocLink a[href*='DocView']").first
                    if await a.count():
                        href = await a.get_attribute("href")
                        if href:
                            doc_url = href if href.startswith("http") else BASE_URL + href
                except Exception:
                    pass
                doc_status = None
                try:
                    icon = r.locator("td.cdDocLink span[class*='glyphicons-file']").first
                    if await icon.count():
                        cls = (await icon.get_attribute("class")) or ""
                        for tok, status in _DOC_STATUS_BY_COLOR.items():
                            if tok in cls:
                                doc_status = status
                                break
                        doc_status = doc_status or cls
                except Exception:
                    pass

                key = (date, title, docket_num)
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "date": date, "event": title, "comments": comments,
                    "docket_num": docket_num, "pages": pages, "doc_status": doc_status,
                })
                if doc_url or doc_status:
                    documents.append({
                        "title": title, "doc_date": date, "docket_num": docket_num,
                        "pages": pages, "doc_id": doc_id, "doc_url": doc_url,
                        "doc_status": doc_status,
                        "image_available": doc_status == "public" if doc_status else None,
                    })
            except Exception:
                continue
    return events, documents


async def _scrape_financial(page: Page) -> tuple[list[dict], Optional[str]]:
    """Financial: Date|Description|Amount|Actions + Balance Due.

    Group-label rows and the Balance Due footer use colspan — skipped. Amount is
    the 3rd column (the Actions column is ignored)."""
    lines: list[dict] = []
    balance_due = None
    table = page.locator("table:has(thead[aria-label='financialtableheader'])").first
    current_party = None
    try:
        rows = await table.locator("tbody tr").all()
    except Exception:
        rows = []
    for r in rows:
        try:
            tds = r.locator("td")
            n = await tds.count()
            first = tds.first
            colspan = await first.get_attribute("colspan")
            txt = _clean(await first.inner_text())
            if colspan and txt:
                if txt.lower().startswith("balance due"):
                    m = re.search(r"Balance Due[:\s]*([\d,.]+)", txt)
                    if m:
                        balance_due = m.group(1)
                else:
                    current_party = txt  # party-group label
                continue
            if n < 3:
                continue
            cells = [c.strip() for c in await tds.all_inner_texts()]
            if cells[0].lower() == "date":
                continue
            lines.append({
                "date": cells[0] or None,
                "description": cells[1] or None,
                "amount": cells[2] or None,
                "party": current_party,
            })
        except Exception:
            continue
    return lines, balance_due


# ── browser-use fallback (post-captcha, extraction-drift only) ──────────────

async def _run_browser_use_fallback(session: PinellasCourtSession, case_number: str) -> Optional[dict]:
    try:
        from .court_agent_fallback import extract_detail_with_agent
    except Exception as exc:
        logger.warning("[pinellas-court] fallback module unavailable: %s", exc)
        return None
    return await extract_detail_with_agent(session, case_number)


# ── CLI probe (probe-first: run one case live, dump DOM, pin selectors) ──────

def _main() -> None:
    import argparse, asyncio, json
    ap = argparse.ArgumentParser(description="Probe one Pinellas court case (dumps DOM for selector pinning).")
    ap.add_argument("--case", default="26-004970-CO", help="Pinellas dashed UCN, e.g. 26-004970-CO")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--use-proxy", action="store_true")
    ap.add_argument("--no-fallback", action="store_true", help="disable browser-use fallback")
    a = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scratch = Path("scratch"); scratch.mkdir(exist_ok=True)

    async def run():
        async with PinellasCourtSession(use_proxy=a.use_proxy, headless=not a.headful,
                                        debug_dir=scratch) as s:
            res = await scrape_case(s, a.case, allow_fallback=not a.no_fallback, dump_dom=True)
        out = scratch / f"pinellas_court_{re.sub(r'[^A-Za-z0-9]+','_',a.case)}.json"
        out.write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
        print(json.dumps(res, indent=2, default=str)[:4000])
        print(f"\nsaved {out} + DOM/screenshot under scratch/")

    asyncio.run(run())


if __name__ == "__main__":
    _main()
