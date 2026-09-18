"""
Pasco County permit scraper smoke test.

Confirms:
  1. Portal loads at aca-prod.accela.com/PASCO/
  2. Date-range search works
  3. Export button present / absent
  4. If export found → downloads CSV and prints column headers
  5. If no export → reads first page of table rows directly

Run:
    PYTHONPATH=. python scripts/smoke_pasco_permits.py
    PYTHONPATH=. python scripts/smoke_pasco_permits.py --headful   # see the browser
    PYTHONPATH=. python scripts/smoke_pasco_permits.py --days 3    # shorter range
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

PASCO_URL = "https://aca-prod.accela.com/PASCO/Cap/CapHome.aspx?module=Building"

# Same selectors as Hillsborough — Accela Citizen Access is consistent across agencies
_START_DATE_SEL = "input#ctl00_PlaceHolderMain_generalSearch_txtGSStartDate"
_END_DATE_SEL   = "input#ctl00_PlaceHolderMain_generalSearch_txtGSEndDate"
_START_FALLBACK = "input[id*='StartDate']"
_END_FALLBACK   = "input[id*='EndDate']"
_SEARCH_BTN_SEL = (
    "a#ctl00_PlaceHolderMain_btnNewSearch, "
    "input#ctl00_PlaceHolderMain_btnNewSearch, "
    "a[id*='btnNewSearch'], input[id*='btnNewSearch']"
)
_RESULTS_TABLE  = "#ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList"
_EXPORT_BTN     = (
    "#ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport, "
    "input[id*='btnExport'], a[id*='btnExport'], "
    "input[value*='Export'], a:has-text('Export')"
)


async def run(headful: bool, days: int) -> None:
    try:
        import pandas as pd
        from playwright.async_api import async_playwright
    except ImportError as e:
        print(f"Missing dependency: {e}")
        sys.exit(1)

    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    print(f"Pasco permit smoke test")
    print(f"URL:   {PASCO_URL}")
    print(f"Range: {start_str} to {end_str}  ({days} days)")
    print("-" * 50)

    browser_args = [
        "--no-sandbox", "--disable-setuid-sandbox",
        "--disable-dev-shm-usage", "--disable-gpu",
        "--disable-blink-features=AutomationControlled",
        "--window-size=1920,1080",
    ]

    with tempfile.TemporaryDirectory() as tmp:
        download_dir = Path(tmp)

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=not headful,
                args=browser_args,
            )
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            # ── 1. Load portal ────────────────────────────────────────────
            print("Step 1: Loading portal...")
            await page.goto(PASCO_URL, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            print(f"  Title: {await page.title()}")

            # ── 2. Expand search form ──────────────────────────────────────
            print("Step 2: Expanding search form...")
            try:
                await page.click(
                    "a#ctl00_PlaceHolderMain_generalSearchLink, "
                    "a[href*='GeneralSearch'], a:has-text('Search')",
                    timeout=8000,
                )
                await asyncio.sleep(1)
                print("  Search link clicked")
            except Exception:
                print("  Search form already expanded (or no link found)")

            # ── 3. Fill date range ─────────────────────────────────────────
            print("Step 3: Filling date range...")
            try:
                await page.wait_for_selector(_START_DATE_SEL, timeout=12000)
                start_sel, end_sel = _START_DATE_SEL, _END_DATE_SEL
                print("  Using primary date selectors")
            except Exception:
                print("  Falling back to generic date selectors")
                try:
                    await page.wait_for_selector(_START_FALLBACK, timeout=8000)
                    start_sel, end_sel = _START_FALLBACK, _END_FALLBACK
                except Exception:
                    # Dump all input IDs to diagnose the portal structure
                    inputs = await page.query_selector_all("input[type='text'], input:not([type])")
                    print("  All text inputs found on page:")
                    for inp in inputs:
                        iid = await inp.get_attribute("id") or ""
                        iname = await inp.get_attribute("name") or ""
                        iph = await inp.get_attribute("placeholder") or ""
                        if iid or iname:
                            print(f"    id={iid!r} name={iname!r} placeholder={iph!r}")
                    # Also dump page URL in case we got redirected
                    print(f"  Current URL: {page.url}")
                    print("  ERROR: Could not find date input fields")
                    await browser.close()
                    return

            # Type dates digit by digit (Accela date pickers are finicky)
            for sel, val in ((start_sel, start_str), (end_sel, end_str)):
                digits = re.sub(r"[^0-9]", "", val)
                await page.click(sel)
                await asyncio.sleep(0.2)
                await page.keyboard.press("Home")
                for ch in digits:
                    await page.keyboard.press(ch)
                    await asyncio.sleep(0.07)
                await page.keyboard.press("Tab")
                await asyncio.sleep(0.3)
            print(f"  Dates filled: {start_str} to {end_str}")

            # ── 4. Submit search ───────────────────────────────────────────
            print("Step 4: Submitting search...")
            await page.click(_SEARCH_BTN_SEL, timeout=10000)

            # Wait for loading overlay to clear
            try:
                await page.wait_for_selector(
                    "#divGlobalLoadingMask:not(.ACA_Hide)", timeout=6000
                )
            except Exception:
                pass
            try:
                await page.wait_for_selector(
                    "#divGlobalLoadingMask.ACA_Hide", timeout=30000
                )
            except Exception:
                await asyncio.sleep(4)

            # Wait for results table
            try:
                await page.wait_for_selector(_RESULTS_TABLE, timeout=20000)
                print("  Results table found")
            except Exception:
                print("  WARNING: Results table not found — search may have returned 0 results or timed out")
                # Still continue to check for export button

            await asyncio.sleep(1)

            # ── 5. Count results ───────────────────────────────────────────
            try:
                rows = await page.query_selector_all(f"{_RESULTS_TABLE} tbody tr")
                data_rows = [r for r in rows if await r.query_selector("td")]
                print(f"  Visible rows on page 1: {len(data_rows)}")
            except Exception:
                print("  Could not count rows")

            # ── 6. Check for export button ─────────────────────────────────
            print("Step 5: Checking for export button...")
            export_btn = await page.query_selector(_EXPORT_BTN)

            if export_btn:
                print("  EXPORT BUTTON FOUND - attempting download...")
                try:
                    async with page.expect_download(timeout=45000) as dl_info:
                        await export_btn.click()
                    download = await dl_info.value
                    dest = download_dir / (download.suggested_filename or "pasco_permits.csv")
                    await download.save_as(str(dest))
                    await asyncio.sleep(2)

                    size = dest.stat().st_size
                    print(f"  Downloaded: {dest.name}  ({size:,} bytes)")

                    # Load and inspect
                    df = None
                    for enc in ("utf-8", "latin1", "cp1252"):
                        try:
                            df = pd.read_csv(str(dest), encoding=enc)
                            if not df.empty:
                                break
                        except Exception:
                            continue

                    if df is not None and not df.empty:
                        print(f"\n{'='*50}")
                        print(f"CSV COLUMNS ({len(df.columns)}):")
                        for col in df.columns:
                            print(f"    {col!r}")
                        print(f"\nROW COUNT: {len(df)}")
                        print(f"SAMPLE ROW:")
                        print(df.iloc[0].to_string())
                        print(f"{'='*50}")
                        print("\nSCRAPE MODE RECOMMENDATION: selector (same as Hillsborough)")
                    else:
                        print("  WARNING: Downloaded file is empty or unreadable")
                        print("  SCRAPE MODE RECOMMENDATION: extract (AI table reader, same as Pinellas)")

                except Exception as e:
                    print(f"  Download failed: {e}")
                    print("  SCRAPE MODE RECOMMENDATION: extract (export button present but download failed)")

            else:
                print("  NO EXPORT BUTTON — reading table rows directly...")
                try:
                    rows = await page.query_selector_all(f"{_RESULTS_TABLE} tbody tr")
                    records = []
                    for row in rows:
                        cells = await row.query_selector_all("td")
                        if not cells:
                            continue
                        values = []
                        for cell in cells:
                            values.append(await cell.inner_text())
                        records.append(values)

                    # Try to get headers
                    headers = []
                    header_cells = await page.query_selector_all(
                        f"{_RESULTS_TABLE} thead th, {_RESULTS_TABLE} tr:first-child th"
                    )
                    for h in header_cells:
                        headers.append(await h.inner_text())

                    print(f"\n{'='*50}")
                    print(f"TABLE HEADERS ({len(headers)}):")
                    for h in headers:
                        print(f"    {h!r}")
                    print(f"\nVISIBLE ROWS: {len(records)}")
                    if records:
                        print(f"SAMPLE ROW: {records[0]}")
                    print(f"{'='*50}")
                    print("\nSCRAPE MODE RECOMMENDATION: extract (AI table reader, same as Pinellas)")
                except Exception as e:
                    print(f"  Table read failed: {e}")

            await browser.close()

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--headful", action="store_true", help="Show browser window")
    parser.add_argument("--days", type=int, default=7, help="Date range in days (default 7)")
    args = parser.parse_args()
    asyncio.run(run(headful=args.headful, days=args.days))
