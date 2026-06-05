"""
One-submit probe: capture REAL Style/Description strings for a Pinellas case type.

Step 2 of the probate/divorce-on-courtrecords plan. Drives the same flow the
eviction 2captcha scraper uses (case-type filter -> submit -> solve reCAPTCHA ->
results), but instead of exporting, it scrapes the on-page results grid and dumps
every row's Style/Description so we can pin the record-type-aware name parser
(probate `IN RE: ESTATE OF ...` vs divorce `PETITIONER Vs. RESPONDENT`).

Costs ONE 2captcha solve per run (one submit). Requires TWOCAPTCHA_API_KEY.

Usage (default = probate, last 30 days):
    python -m scripts.experiments.probe_pinellas_styles --record-type probate
    python -m scripts.experiments.probe_pinellas_styles --record-type divorce --days 30
    python -m scripts.experiments.probe_pinellas_styles --record-type probate --headful --no-proxy

Dumps scratch/pinellas_styles_<record_type>.json + _grid.png.
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright

from src.utils.http_helpers import (
    STEALTH_UA, STEALTH_ARGS, apply_stealth_to_page, get_playwright_proxy,
)
from src.scrappers.evictions.evictions_engine import _solve_recaptcha_2captcha
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

SCRATCH = Path("scratch")
FALLBACK_URL = "https://courtrecords.mypinellasclerk.gov"

# Case-type <option> keywords per record type (from the casetypes probe).
CASE_TYPE_KEYWORDS = {
    "probate":  ["estate", "guardianship"],
    "divorce":  ["dissolution"],
    "eviction": ["eviction"],
}


def _resolve_url(county_id: str) -> str:
    try:
        from src.utils.county_config import get_county_config
        src = (get_county_config(county_id).get("sources", {}).get("evictions")
               or get_county_config(county_id).get("sources", {}).get("court_records") or {})
        if src.get("url"):
            return src["url"]
    except Exception as e:
        logger.warning("[probe] county_config read failed (%s) — fallback", e)
    return FALLBACK_URL


# Select case-type options by keyword (mirrors evictions_engine in-browser filter).
_SELECT_JS = """
(keywords) => {
    const sel = document.querySelector('#caseTypesList')
             || document.querySelector('select[name="CaseType"]');
    if (!sel) return {ok: false, reason: 'caseTypesList not found'};
    for (const opt of sel.options) opt.selected = false;
    const matched = [];
    for (const opt of sel.options) {
        if (keywords.some(k => opt.text.toLowerCase().includes(k))) {
            opt.selected = true; matched.push(opt.text.trim());
        }
    }
    sel.dispatchEvent(new Event('change', {bubbles: true}));
    return {ok: matched.length > 0, matched};
}
"""

# Pull every results-grid row as an array of cell texts + the header labels.
_GRID_JS = """
() => {
    const tables = [...document.querySelectorAll('table')];
    // Pick the table with the most data rows (the results grid).
    let best = null, bestRows = 0;
    for (const t of tables) {
        const rows = t.querySelectorAll('tbody tr').length;
        if (rows > bestRows) { best = t; bestRows = rows; }
    }
    if (!best) return {headers: [], rows: []};
    const headers = [...best.querySelectorAll('thead th, thead td')]
        .map(h => (h.innerText || '').trim());
    const rows = [...best.querySelectorAll('tbody tr')].slice(0, 40).map(tr =>
        [...tr.querySelectorAll('td, th')].map(td => (td.innerText || '').trim())
    );
    return {headers, rows, total_rows: bestRows};
}
"""


async def probe(record_type: str, url: str, days: int, headless: bool, no_proxy: bool) -> dict:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    keywords = CASE_TYPE_KEYWORDS[record_type]
    end = datetime.now()
    start = end - timedelta(days=days)
    start_str, end_str = start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")
    proxy = None if no_proxy else get_playwright_proxy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=STEALTH_ARGS)
        ctx = await browser.new_context(user_agent=STEALTH_UA, proxy=proxy)
        page = await ctx.new_page()
        await apply_stealth_to_page(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            # Case tab
            for sel in ('a:has-text("Case")', '[role="tab"]:has-text("Case")'):
                t = page.locator(sel)
                if await t.count() > 0:
                    await t.first.click(); await page.wait_for_timeout(1200); break
            # dates
            await page.locator("#DateFrom").first.fill(start_str)
            await page.locator("#DateTo").first.fill(end_str)
            # case type filter
            res = await page.evaluate(_SELECT_JS, keywords)
            logger.info("[probe] case-type select: %s", res)
            if not res.get("ok"):
                logger.warning("[probe] no case types matched %s — submitting anyway", keywords)
            await page.wait_for_timeout(400)
            # submit
            await page.locator('button#caseSearch, button:has-text("Submit")').first.click()
            logger.info("[probe] submitted — waiting for reCAPTCHA / results")
            # captcha
            try:
                await page.wait_for_selector('iframe[src*="google.com/recaptcha"]', timeout=10000)
                logger.info("[probe] reCAPTCHA present — solving")
                await _solve_recaptcha_2captcha(page, page.url)
                try:
                    await page.wait_for_load_state("networkidle", timeout=60000)
                except Exception:
                    pass
            except Exception:
                logger.info("[probe] no reCAPTCHA within 10s")
            # results grid
            await page.wait_for_selector("table tbody tr", timeout=90000)
            await page.wait_for_timeout(1500)
            grid = await page.evaluate(_GRID_JS)
            grid.update(record_type=record_type, matched_case_types=res.get("matched"),
                        date_range=[start_str, end_str], url=url)
            out = SCRATCH / f"pinellas_styles_{record_type}.json"
            out.write_text(json.dumps(grid, indent=2), encoding="utf-8")
            await page.screenshot(path=str(SCRATCH / f"pinellas_styles_{record_type}_grid.png"),
                                  full_page=True)
            logger.info("[probe] %d rows captured (total grid rows=%s) -> %s",
                        len(grid.get("rows", [])), grid.get("total_rows"), out)
            return grid
        except Exception as e:
            ts = datetime.now().strftime("%H%M%S")
            try:
                await page.screenshot(path=str(SCRATCH / f"pinellas_styles_{record_type}_err_{ts}.png"),
                                      full_page=True)
                (SCRATCH / f"pinellas_styles_{record_type}_err_{ts}.html").write_text(
                    await page.content(), encoding="utf-8")
            except Exception:
                pass
            logger.error("[probe] failed: %s", e)
            raise
        finally:
            await browser.close()


def _print(grid: dict) -> None:
    print("\n" + "=" * 70)
    print(f"record_type={grid.get('record_type')}  matched={grid.get('matched_case_types')}  "
          f"range={grid.get('date_range')}  total_grid_rows={grid.get('total_rows')}")
    print(f"headers: {grid.get('headers')}")
    print("=" * 70)
    for r in grid.get("rows", [])[:25]:
        print(" | ".join(r))
    print()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--record-type", default="probate", choices=list(CASE_TYPE_KEYWORDS))
    ap.add_argument("--county-id", default="pinellas")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--no-proxy", action="store_true")
    args = ap.parse_args()
    url = _resolve_url(args.county_id)
    grid = asyncio.run(probe(args.record_type, url, args.days,
                             headless=not args.headful, no_proxy=args.no_proxy))
    _print(grid)


if __name__ == "__main__":
    main()
