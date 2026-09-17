"""WP-T2-8 Stage F — activate Pasco permit scraper in county_sources.

Updates the county_sources row for Pasco (source_name='Pasco Accela Building')
with the validated Playwright scraper code and sets is_active=TRUE so that
fresh or disaster-recovery deployments do not need a manual database mutation
to run the scheduled Pasco permit ingestion.

Idempotent: safe to re-run. Only updates the row; never inserts (the insert
is handled by apply_builder_pasco_county_config.py which must run first).

Usage:
    PYTHONPATH=. python migrations/apply_pasco_permit_scraper_activate.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_PLAYWRIGHT_CODE = '''async def run_scrape(page, download_dir, start_date, end_date, url, county_id):
    import asyncio, re
    from pathlib import Path
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(2)
        try:
            await page.click(
                "a#ctl00_PlaceHolderMain_generalSearchLink, "
                "a[href*=\\'GeneralSearch\\'], a:has-text(\\'Search\\')",
                timeout=8000,
            )
            await asyncio.sleep(1)
        except Exception:
            pass
        for primary, fallback, val in [
            ("input#ctl00_PlaceHolderMain_generalSearch_txtGSStartDate", "input[id*=\\'StartDate\\']", start_date),
            ("input#ctl00_PlaceHolderMain_generalSearch_txtGSEndDate", "input[id*=\\'EndDate\\']", end_date),
        ]:
            try:
                await page.wait_for_selector(primary, timeout=8000)
                sel = primary
            except Exception:
                sel = fallback
            digits = re.sub(r"[^0-9]", "", val)
            await page.click(sel)
            await asyncio.sleep(0.2)
            await page.keyboard.press("Home")
            for ch in digits:
                await page.keyboard.press(ch)
                await asyncio.sleep(0.07)
            await page.keyboard.press("Tab")
            await asyncio.sleep(0.3)
        await page.click(
            "a#ctl00_PlaceHolderMain_btnNewSearch, input#ctl00_PlaceHolderMain_btnNewSearch, "
            "a[id*=\\'btnNewSearch\\'], input[id*=\\'btnNewSearch\\']",
            timeout=10000,
        )
        try:
            await page.wait_for_selector("#divGlobalLoadingMask:not(.ACA_Hide)", timeout=6000)
        except Exception:
            pass
        try:
            await page.wait_for_selector("#divGlobalLoadingMask.ACA_Hide", timeout=30000)
        except Exception:
            await asyncio.sleep(4)
        export_sel = (
            "#ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport, "
            "input[id*=\\'btnExport\\'], a[id*=\\'btnExport\\'], "
            "input[value*=\\'Export\\'], a:has-text(\\'Export\\')"
        )
        export_btn = await page.query_selector(export_sel)
        if not export_btn:
            return pd.DataFrame()
        async with page.expect_download(timeout=45000) as dl_info:
            await export_btn.click()
        download = await dl_info.value
        dest = Path(download_dir) / (download.suggested_filename or "pasco_permits.csv")
        await download.save_as(str(dest))
        await asyncio.sleep(2)
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                df = pd.read_csv(str(dest), encoding=enc)
                df["county_id"] = county_id
                return df
            except Exception:
                continue
        return pd.DataFrame()
    except Exception as e:
        print(f"run_scrape error: {e}")
        return pd.DataFrame()
'''

DDL = """
UPDATE county_sources SET
    description              = 'Pasco County Accela building permit portal. '
                               'Validated 2026-09-17: exports ~2100 rows/week via date-range CSV. '
                               'Columns: Date, Record Number, Record Type, Project Name, '
                               'Description, Address, Status.',
    navigation_hint          = 'Fill start/end date, click Search, click Export. '
                               'CSV download: ~2100 rows/7-day window.',
    scrape_mode              = 'playwright_only',
    playwright_code          = :code,
    playwright_code_version  = '1',
    playwright_code_approved = TRUE,
    is_active                = TRUE,
    updated_at               = NOW()
WHERE county_id = 'pasco'
  AND source_name = 'Pasco Accela Building';
"""


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        result = conn.execute(text(DDL), {"code": _PLAYWRIGHT_CODE})
        logger.info(
            "apply_pasco_permit_scraper_activate: updated %d row(s) — Pasco is_active=TRUE",
            result.rowcount,
        )
        if result.rowcount == 0:
            logger.warning(
                "No row updated — run apply_builder_pasco_county_config.py first to seed the row."
            )


if __name__ == "__main__":
    main()
