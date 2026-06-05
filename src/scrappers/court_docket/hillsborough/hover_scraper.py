"""Scrape one Hillsborough court case from HOVER by Uniform Case Number.

Playwright + auto-waiting only (no Selenium, no browser-use, no random
sleeps beyond the session's small human-pacing). Returns a structured dict.
NO database writes — persistence is a separate, later step.

Flow (per the selector notes doc):
  1. decompose UCN -> form fields
  2. (session) navigate to Case Search via the Search link
  3. fill "Search by Case Number" tab
  4. submit
  5. open the matching result via its zoom icon
  6. scrape Summary from the DOM
  7. download Parties CSV
  8. download Events CSV
  9. capture other tab names + document/image metadata
 10. return structured JSON

Some selectors (zoom icon, Summary labels, CSV buttons) are best-effort with
fallbacks and are flagged in docs/court_docket_phase1/hillsborough_hover_selector_notes.md
for confirmation on the first live (past-PerimeterX) run.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from playwright.async_api import Page, TimeoutError as PWTimeout

from .hover_session import HoverSession, HoverBlockedError

logger = logging.getLogger(__name__)

# Hillsborough dashed UCN: YY-CT-NNNNNN, optional party suffix (e.g.
# 26-CC-025949, 26-WD-000969, judgment 25-CF-016327-A). The suffix is the
# party designator — we search with Party=ALL and ignore it for the form.
UCN_RE = re.compile(r"^\s*(\d{2})-([A-Z]{2})-(\d+)(?:-([A-Za-z0-9]+))?\s*$")

COUNTY_DESIGNATOR = "29"   # Hillsborough (fixed)
LOCATION = "HC"

# Summary labels we try to lift from the Summary tab (best-effort, see notes).
_SUMMARY_LABELS = {
    "case_number": "Case Number",
    "citation_number": "Citation Number",
    "case_category": "Case Category Description",
    "case_type": "Case Type Description",
    "case_sub_type": "Case Sub Type Description",
    "case_status": "Case Status",
    "case_filed_on": "Case Filed On",
    "judge": "Judge",
    "division": "Division",
    "balance_due": "Balance Due",
}


def decompose_ucn(case_number: str) -> Optional[dict]:
    """Split a Hillsborough dashed UCN into HOVER form fields, or None if the
    value is not a clean UCN (caller marks those `case_number_missing`)."""
    if not case_number:
        return None
    m = UCN_RE.match(case_number)
    if not m:
        return None
    year, court_type, number, party = m.group(1), m.group(2), m.group(3), m.group(4)
    return {
        "county": COUNTY_DESIGNATOR,
        "year": year,
        "court_type": court_type,
        "number": number.zfill(6),
        "party_suffix": party,  # informational; search uses Party=ALL
        "location": LOCATION,
    }


async def scrape_case(
    session: HoverSession,
    case_number: str,
    download_dir: Optional[Path] = None,
) -> dict:
    """Look up one case on HOVER and return a structured result dict.

    status ∈ {ok, case_number_missing, not_found, blocked, error}
    Never raises for per-case issues — failures are captured in the result.
    """
    download_dir = Path(download_dir) if download_dir else session.downloads_dir
    download_dir.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9]+", "_", case_number or "unknown")

    result: dict = {
        "case_number": case_number,
        "county": "hillsborough",
        "status": "error",
        "ucn": None,
        "summary": {},
        "parties_csv": None,
        "events_csv": None,
        "tabs": [],
        "documents": [],
        "events_image_available": None,
        "warnings": [],
        "error": None,
        "screenshots": [],
    }

    ucn = decompose_ucn(case_number)
    if ucn is None:
        result["status"] = "case_number_missing"
        result["error"] = "value is not a clean Hillsborough UCN (YY-CT-NNNNNN)"
        logger.info("[hover] %s -> case_number_missing", case_number)
        return result
    result["ucn"] = ucn

    page = session.page
    try:
        # 2. navigate to search (clicks the Search link, handles PX/cart)
        await session.goto_search()

        # 3. fill Search by Case Number tab
        await _fill_search_form(page, ucn)

        # 4. submit
        await _submit_search(page)
        await session.dismiss_cart_popup()
        if await session.is_blocked():
            result["status"] = "blocked"
            result["error"] = "PerimeterX block after search submit"
            result["screenshots"].append(str(await session.debug_capture(f"{safe}_blocked_after_submit") or ""))
            return result

        # 5. open the matching result via its zoom icon
        detail = await _open_result(session, case_number, ucn["number"])
        if detail is None:
            result["status"] = "not_found"
            result["screenshots"].append(str(await session.debug_capture(f"{safe}_no_result") or ""))
            return result

        # 6. Summary
        result["summary"] = await _scrape_summary(detail)

        # 9a. tab inventory (before downloading, so we know what's available)
        result["tabs"] = await _collect_tab_names(detail)

        # 7. Parties CSV
        try:
            result["parties_csv"] = await _download_tab_csv(detail, "Parties", download_dir / f"{safe}_parties.csv")
        except Exception as exc:
            result["warnings"].append(f"parties_csv: {exc}")

        # 8. Events CSV
        try:
            result["events_csv"] = await _download_tab_csv(detail, "Events", download_dir / f"{safe}_events.csv")
        except Exception as exc:
            result["warnings"].append(f"events_csv: {exc}")

        # 9b. document / image metadata from the Events tab
        try:
            docmeta = await _events_document_metadata(detail)
            result["events_image_available"] = docmeta.get("image_available_count")
            result["documents"] = docmeta.get("documents", [])
        except Exception as exc:
            result["warnings"].append(f"doc_metadata: {exc}")

        result["status"] = "ok"
        logger.info("[hover] %s -> ok (parties_csv=%s events_csv=%s tabs=%d)",
                    case_number, bool(result["parties_csv"]), bool(result["events_csv"]), len(result["tabs"]))
        return result

    except HoverBlockedError as exc:
        result["status"] = "blocked"
        result["error"] = str(exc)
        result["screenshots"].append(str(await session.debug_capture(f"{safe}_blocked") or ""))
        return result
    except Exception as exc:
        result["status"] = "error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["screenshots"].append(str(await session.debug_capture(f"{safe}_error") or ""))
        logger.exception("[hover] %s -> error", case_number)
        return result


# ── step helpers ────────────────────────────────────────────────────────

async def _fill_search_form(page: Page, ucn: dict) -> None:
    # County (29) and Location (HC) are prefilled; set defensively.
    for sel, val in (("#txtCountyDesignator", ucn["county"]),
                     ("#txtYear", ucn["year"]),
                     ("#txtNumber", ucn["number"]),
                     ("#txtLocation", ucn["location"])):
        try:
            await page.fill(sel, val, timeout=10000)
        except Exception:
            # county/location may be readonly; ignore if it already holds the value
            pass
    await page.select_option("#ddlCourtType", ucn["court_type"], timeout=10000)
    # Party Designator defaults to ALL — leave as-is.


async def _submit_search(page: Page) -> None:
    # CONFIRMED (2026-06-04 live): the form Search submit button is
    # #btnSubmitCaseSearch. Do NOT use button:has-text('Search') — that matches
    # the "Search by Case Number" TAB (#nav-CaseNumber-tab), not the submit.
    candidates = [
        "#btnSubmitCaseSearch",
        "#nav-CaseNumber button.btn-primary[aria-label='Search']",
        "button.btn-primary[aria-label='Search']",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if await loc.count() and await loc.is_visible():
                await loc.click(timeout=8000)
                return
        except Exception:
            continue
    raise RuntimeError("Search submit button (#btnSubmitCaseSearch) not found")


async def _open_result(session: HoverSession, case_number: str, number: str):
    """Find the matching result row and open its detail via the zoom icon.

    Returns the detail Page (a new tab if one opens, else the same page),
    or None if no result row is found. Zoom-icon selector is best-effort.
    """
    page = session.page
    # Wait for either a results table or a clear 'no results' state.
    try:
        await page.wait_for_selector("table tbody tr, .no-results, :text('no records')", timeout=20000)
    except PWTimeout:
        return None

    row = None
    for key in (case_number, number):
        cand = page.locator("tr", has_text=key).first
        try:
            if await cand.count():
                row = cand
                break
        except Exception:
            continue
    if row is None:
        return None

    # Zoom / view control inside the row (selector to confirm live).
    zoom_selectors = [
        "img[src*='magnif']", "img[src*='zoom']", "img[src*='view']",
        "a[onclick*='ase']", "a[href*='caseView']", "a[href*='View']",
        "[title*='View' i]", "[class*='zoom']", "button", "a img", "a",
    ]
    ctx = session.context
    pages_before = len(ctx.pages)
    clicked = False
    for sel in zoom_selectors:
        try:
            ctrl = row.locator(sel).first
            if await ctrl.count() and await ctrl.is_visible():
                await ctrl.click(timeout=6000)
                clicked = True
                break
        except Exception:
            continue
    if not clicked:
        await session.debug_capture("zoom_icon_not_found")
        return None

    await session.dismiss_cart_popup()
    # Detail may open in a new tab.
    if len(ctx.pages) > pages_before:
        detail = ctx.pages[-1]
        try:
            await detail.wait_for_load_state("domcontentloaded", timeout=20000)
        except Exception:
            pass
        return detail
    # Same-page navigation: wait for a tab strip to appear.
    try:
        await page.wait_for_selector(":text('Summary')", timeout=20000)
    except Exception:
        pass
    return page


async def _activate_tab(page: Page, name: str) -> bool:
    for sel in (f"[role='tab']:has-text('{name}')",
                f".nav-tabs >> text={name}",
                f"a.nav-link:has-text('{name}')",
                f"button:has-text('{name}')",
                f"a:has-text('{name}')"):
        try:
            loc = page.locator(sel).first
            if await loc.count() and await loc.is_visible():
                await loc.click(timeout=6000)
                await page.wait_for_timeout(600)  # let the pane render
                return True
        except Exception:
            continue
    return False


async def _scrape_summary(page: Page) -> dict:
    await _activate_tab(page, "Summary")
    # Pull the visible text of the main content and lift labeled values.
    try:
        text = await page.locator("body").inner_text(timeout=8000)
    except Exception:
        text = ""
    out: dict = {}
    for key, label in _SUMMARY_LABELS.items():
        out[key] = _value_after_label(text, label)
    # Uniform Case Number often appears as a header (undashed form).
    m = re.search(r"Uniform Case Number[:\s]*([0-9A-Z]+)", text, re.I)
    if m:
        out["uniform_case_number"] = m.group(1)
    return out


def _value_after_label(text: str, label: str) -> Optional[str]:
    """Return the value following 'Label:' up to end of line. Best-effort."""
    m = re.search(rf"{re.escape(label)}\s*:?\s*(.+)", text, re.I)
    if not m:
        return None
    val = m.group(1).split("\n", 1)[0].strip()
    return val or None


async def _collect_tab_names(page: Page) -> list[str]:
    names: list[str] = []
    for sel in ("[role='tab']", ".nav-tabs a", ".nav-link"):
        try:
            for el in await page.locator(sel).all():
                if await el.is_visible():
                    t = (await el.inner_text()).strip()
                    if t and t not in names:
                        names.append(t)
            if names:
                break
        except Exception:
            continue
    return names


async def _download_tab_csv(page: Page, tab_name: str, dest: Path) -> Optional[str]:
    if not await _activate_tab(page, tab_name):
        raise RuntimeError(f"tab '{tab_name}' not found")
    # The DataTables export button labelled 'CSV' within the active pane.
    btn = page.locator("button:has-text('CSV'), a:has-text('CSV')").first
    if not (await btn.count() and await btn.is_visible()):
        raise RuntimeError("CSV export button not visible")
    async with page.expect_download(timeout=20000) as dl_info:
        await btn.click(timeout=8000)
    download = await dl_info.value
    await download.save_as(str(dest))
    return str(dest)


async def _events_document_metadata(page: Page) -> dict:
    """From the Events tab, capture document/image availability metadata.

    The Events table has an 'Image' column whose icon indicates a viewable
    document. We count rows with an image icon and capture lightweight
    metadata (no document downloads in v1)."""
    await _activate_tab(page, "Events")
    docs: list[dict] = []
    image_count = 0
    try:
        rows = await page.locator("table tbody tr").all()
        for r in rows:
            try:
                imgs = await r.locator("img").count()
                txt = (await r.inner_text()).strip().replace("\n", " ")
                has_image = imgs > 0
                if has_image:
                    image_count += 1
                # keep a compact record for rows that look document-bearing
                if has_image and len(docs) < 100:
                    docs.append({"row_text": txt[:200], "image_available": True})
            except Exception:
                continue
    except Exception:
        pass
    return {"image_available_count": image_count, "documents": docs}
