"""
Eviction Filing Data Collection Pipeline — county-agnostic

Downloads civil filings from the county clerk portal, filters for eviction
case types, deduplicates against existing DB records, and saves results.

For Hillsborough: requests-based directory listing of daily CSV files.
For Pinellas (output_format=excel): direct Playwright + 2Captcha reCAPTCHA
solving navigates the clerk portal, searches by date range, filters to
eviction case types, and downloads the Excel export.

Usage:
    python -m src.scrappers.evictions.evictions_engine --county-id hillsborough --load-to-db
    python -m src.scrappers.evictions.evictions_engine --county-id pinellas --load-to-db --headful
"""

import asyncio
import re
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils.http_helpers import (
    requests_get_with_retry, STEALTH_UA, STEALTH_ARGS,
    apply_stealth_to_browser_use, apply_stealth_to_page,
    get_playwright_proxy, get_browser_use_proxy,
)

from config.constants import (
    RAW_EVICTIONS_DIR,
    EVICTION_CASE_PATTERNS,
    CIVIL_FILING_PATTERN,
    CIVIL_FILINGS_URL,
    HILLSCLERK_BASE_URL,
    DEFAULT_USER_AGENT,
    REQUEST_TIMEOUT_DEFAULT,
    REQUEST_TIMEOUT_LONG,
    OUTPUT_DATE_FORMAT,
    OUTPUT_SEPARATOR,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config as _get_county
from src.utils.logger import setup_logging, get_logger
from src.utils.db_deduplicator import filter_new_records

setup_logging()
logger = get_logger(__name__)



def _make_llm():
    from browser_use import ChatAnthropic
    from config.settings import get_settings
    settings = get_settings()
    return ChatAnthropic(
        model=BROWSER_MODEL,
        temperature=BROWSER_TEMPERATURE,
        api_key=settings.anthropic_api_key.get_secret_value(),
    )



def _get_eviction_source(county_id: str) -> dict:
    """Return the evictions source dict, falling back to court_records if absent."""
    cfg = _get_county(county_id)
    sources = cfg.get("sources", {})
    return sources.get("evictions") or sources.get("court_records") or {}


def _static_download(source: dict, dest_dir: Path, target_date: str = None, no_proxy: bool = False) -> Path:
    """
    Download a dated file directly from a URL pattern stored in source["url"].
    The URL must contain the literal placeholder {date} (replaced with YYYYMMDD).
    Walks back up to 7 days to find the latest available file when no date given;
    skips empty/weekend files (≤200 bytes).
    """
    url_pattern = source.get("url", "")
    if "{date}" not in url_pattern:
        raise ValueError(f"static_download source url must contain {{date}}: {url_pattern!r}")

    if target_date:
        dates_to_try = [datetime.strptime(target_date.replace("-", ""), "%Y%m%d")]
    else:
        today = datetime.now().date()
        dates_to_try = [
            datetime.combine(today - timedelta(days=i), datetime.min.time())
            for i in range(1, 8)
        ]

    dest_dir.mkdir(parents=True, exist_ok=True)
    for dt in dates_to_try:
        date_str = dt.strftime("%Y%m%d")
        download_url = url_pattern.replace("{date}", date_str)
        try:
            resp = requests_get_with_retry(
                download_url,
                headers={"User-Agent": DEFAULT_USER_AGENT},
                timeout=REQUEST_TIMEOUT_DEFAULT,
            )
            if resp.status_code != 200 or len(resp.content) <= 200:
                logger.debug("[evictions] %s — empty or missing (size=%d)", date_str, len(resp.content))
                continue
            out_path = dest_dir / f"CivilFiling_{date_str}.csv"
            out_path.write_bytes(resp.content)
            logger.info("[evictions] Downloaded %s (%.1f KB)", out_path.name, out_path.stat().st_size / 1024)
            return out_path
        except Exception as e:
            logger.warning("[evictions] Could not fetch %s: %s", download_url, e)

    raise FileNotFoundError("[evictions] No civil filing found in last 7 days")


async def _execute_playwright_code_on_page(page, playwright_code, url, start_str, end_str, county_id):
    """Run playwright_code on an already-open page; captures debug state on failure."""
    from src.utils.action_sequence import execute_playwright_code, PlaywrightCodeError
    try:
        return await execute_playwright_code(
            playwright_code, page, RAW_EVICTIONS_DIR,
            placeholders={"url": url, "start_date": start_str, "end_date": end_str},
            county_id=county_id,
        )
    except PlaywrightCodeError as e:
        logger.error("[evictions] Playwright scrape failed: %s", e)
        try:
            import datetime as _dt
            _ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            _dbg = RAW_EVICTIONS_DIR / "debug"
            _dbg.mkdir(parents=True, exist_ok=True)
            logger.error("[evictions][debug] URL: %s | Title: %s", page.url, await page.title())
            _shot = _dbg / f"evictions_debug_{county_id}_{_ts}.png"
            await page.screenshot(path=str(_shot), full_page=True)
            logger.error("[evictions][debug] Screenshot: %s", _shot)
            _html_path = _dbg / f"evictions_debug_{county_id}_{_ts}.html"
            _html_path.write_text(await page.content(), encoding="utf-8")
            logger.error("[evictions][debug] HTML: %s", _html_path)
            _keywords = re.compile(r"export|excel|download|csv|results", re.IGNORECASE)
            _found = []
            for _el in await page.locator("a, button").all():
                try:
                    _txt = (await _el.inner_text()).strip()
                    _href = await _el.get_attribute("href") or ""
                    if _keywords.search(_txt) or _keywords.search(_href):
                        _found.append(f"{_txt!r} href={_href!r}")
                except Exception:
                    pass
            logger.error("[evictions][debug] Export-related elements (%d): %s",
                         len(_found), _found or ["<none found>"])
        except Exception as _dbg_exc:
            logger.error("[evictions][debug] Debug capture failed: %s", _dbg_exc)
        raise


async def _scrape_with_playwright(
    source: dict, county_id: str, target_date: str | None,
    start_date: str | None, end_date: str | None, headful: bool = False,
    no_proxy: bool = False,
) -> Path:
    """Run the source's playwright_code and save the resulting DataFrame to disk."""
    playwright_code = source.get("playwright_code", "")
    if not playwright_code:
        raise ValueError(f"[evictions] playwright_code is empty for '{county_id}' source")

    if target_date:
        dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        start_str = end_str = dt.strftime("%Y%m%d")
    elif start_date:
        start_str = start_date.replace("-", "")
        end_str = (end_date or start_date).replace("-", "")
    else:
        yesterday = datetime.now() - timedelta(days=1)
        start_str = end_str = yesterday.strftime("%Y%m%d")

    url = source.get("url", "")
    cf_required = source.get("cf_bypass_required", False)
    profile_name = source.get("cf_bypass_profile_name", f"{county_id}_evictions_clerk")
    RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    if cf_required:
        from src.utils.cf_persistent_browser import launch_cf_bypass_context
        logger.info("[evictions] CF-bypass mode — using persistent Edge profile")
        async with launch_cf_bypass_context(
            profile_name=profile_name,
            county_id=county_id,
            portal_url=url,
            headless=False,
            accept_downloads=True,
        ) as ctx:
            page = await ctx.new_page()
            df = await _execute_playwright_code_on_page(page, playwright_code, url, start_str, end_str, county_id)
    else:
        from playwright.async_api import async_playwright
        _proxy = None if no_proxy else get_playwright_proxy()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=not headful,
                downloads_path=str(RAW_EVICTIONS_DIR),
                args=STEALTH_ARGS,
            )
            context = await browser.new_context(
                user_agent=STEALTH_UA,
                accept_downloads=True,
                proxy=_proxy,
            )
            page = await context.new_page()
            await apply_stealth_to_page(page)
            try:
                df = await _execute_playwright_code_on_page(page, playwright_code, url, start_str, end_str, county_id)
            finally:
                await browser.close()

    if df is None or df.empty:
        raise ValueError(f"[evictions] Playwright returned no data for '{county_id}'")

    out_path = RAW_EVICTIONS_DIR / f"evictions_playwright_{county_id}_{start_str}.xlsx"
    df.to_excel(out_path, index=False)
    logger.info("[evictions] Playwright data saved: %s (%d rows)", out_path.name, len(df))
    return out_path


# ---------------------------------------------------------------------------
# 2Captcha reCAPTCHA solving helpers (Pinellas-specific)
# ---------------------------------------------------------------------------

async def _solve_recaptcha_2captcha(page, page_url: str) -> bool:
    """
    Detect Google reCAPTCHA v2 on the current page, solve it via the 2captcha
    API, and inject the returned token so the form can be submitted.

    Returns True if a captcha was found and the token was injected successfully.
    Returns False if no captcha was found or if TWOCAPTCHA_API_KEY is unset.
    Raises on 2captcha API errors so the caller can decide whether to retry.
    """
    from config.settings import get_settings
    settings = get_settings()
    api_key = (
        settings.twocaptcha_api_key.get_secret_value()
        if settings.twocaptcha_api_key else None
    )
    if not api_key:
        logger.warning("[captcha] TWOCAPTCHA_API_KEY not set — cannot solve reCAPTCHA")
        return False

    # 1. Detect reCAPTCHA iframe
    captcha_frame = await page.query_selector('iframe[src*="google.com/recaptcha"]')
    if not captcha_frame:
        logger.debug("[captcha] No reCAPTCHA iframe on page")
        return False

    # 2. Extract sitekey from the page DOM
    sitekey = await page.evaluate("""
        () => {
            const selectors = [
                '[data-sitekey]',
                '.g-recaptcha[data-sitekey]',
                '[id*="recaptcha"][data-sitekey]',
            ];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el) {
                    const key = el.getAttribute('data-sitekey');
                    if (key) return key;
                }
            }
            return null;
        }
    """)

    if not sitekey:
        logger.error("[captcha] reCAPTCHA iframe found but sitekey not extractable from DOM")
        return False

    logger.info("[captcha] reCAPTCHA v2 detected — sitekey=%s...", sitekey[:12])

    # 3. Submit to 2captcha and wait for token (blocking SDK — run in thread)
    try:
        from twocaptcha import TwoCaptcha
        solver = TwoCaptcha(api_key)
        logger.info("[captcha] Submitting to 2captcha (this takes ~20-40 seconds)...")
        result = await asyncio.to_thread(solver.recaptcha, sitekey=sitekey, url=page_url)
        token = result["code"]
        logger.info("[captcha] 2captcha returned token (len=%d)", len(token))
    except Exception as e:
        logger.error("[captcha] 2captcha API call failed: %s", e)
        raise

    # 4. Inject token into g-recaptcha-response and trigger the submit callback
    await page.evaluate("""
        (token) => {
            // Set the hidden textarea value that the server reads
            const resp = document.getElementById('g-recaptcha-response');
            if (resp) {
                resp.innerHTML = token;
                resp.value = token;
            }

            // Try data-callback attribute on the widget element first
            const captchaEl = document.querySelector('[data-sitekey]')
                           || document.querySelector('.g-recaptcha');
            if (captchaEl) {
                const cb = captchaEl.getAttribute('data-callback');
                if (cb && typeof window[cb] === 'function') {
                    window[cb](token);
                    return;
                }
            }

            // Fall back to walking ___grecaptcha_cfg.clients for the callback fn
            if (window.___grecaptcha_cfg) {
                for (const clientKey in window.___grecaptcha_cfg.clients) {
                    const client = window.___grecaptcha_cfg.clients[clientKey];
                    if (!client) continue;
                    for (const k in client) {
                        if (client[k] && typeof client[k].callback === 'function') {
                            client[k].callback(token);
                            return;
                        }
                    }
                }
            }
        }
    """, token)

    logger.info("[captcha] Token injected — waiting for page response")
    await page.wait_for_timeout(1500)
    return True


async def _scrape_pinellas_with_2captcha(
    source: dict, county_id: str,
    target_date: str | None, start_date: str | None, end_date: str | None,
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """
    Direct Playwright scrape for Pinellas clerk portal (courtrecords.mypinellasclerk.gov)
    with integrated 2captcha reCAPTCHA v2 solving.

    Replaces _download_civil_filing_browser (browser-use AI agent) for the
    Pinellas output_format=excel path because the AI agent cannot handle the
    Google reCAPTCHA image challenge that fires on form Submit.

    Flow: navigate → fill date range → filter case types → submit → detect
    captcha → solve via 2captcha → inject token → re-submit if needed →
    wait for results → export Excel via direct URL navigation → save file.
    """
    from playwright.async_api import async_playwright

    # Build MM/DD/YYYY date strings for the Pinellas portal form
    if target_date:
        dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        start_str = end_str = dt.strftime("%m/%d/%Y")
    elif start_date:
        start_str = datetime.strptime(start_date.replace("-", ""), "%Y%m%d").strftime("%m/%d/%Y")
        end_str = datetime.strptime(
            (end_date or start_date).replace("-", ""), "%Y%m%d"
        ).strftime("%m/%d/%Y")
    else:
        end_dt = datetime.now()
        end_str = end_dt.strftime("%m/%d/%Y")
        start_str = (end_dt - timedelta(days=1)).strftime("%m/%d/%Y")

    url = source.get("url", "")
    RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    _proxy = None if no_proxy else get_playwright_proxy()

    logger.info("[evictions-captcha] Pinellas direct scrape: %s → %s  url=%s", start_str, end_str, url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headful,
            downloads_path=str(RAW_EVICTIONS_DIR),
            args=STEALTH_ARGS,
        )
        context = await browser.new_context(
            user_agent=STEALTH_UA,
            accept_downloads=True,
            proxy=_proxy,
        )
        page = await context.new_page()
        await apply_stealth_to_page(page)

        try:
            logger.info("[evictions-captcha] Navigating to portal")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Activate the Case search tab (shows date range + case type filters)
            try:
                case_tab = page.locator(
                    'a:has-text("Case"), '
                    'li:has-text("Case") > a, '
                    '[role="tab"]:has-text("Case"), '
                    '.tab:has-text("Case")'
                )
                if await case_tab.count() > 0:
                    await case_tab.first.click()
                    await page.wait_for_timeout(1000)
                    logger.info("[evictions-captcha] Activated Case search tab")
            except Exception as e:
                logger.warning("[evictions-captcha] Could not click Case tab (may already be active): %s", e)

            # Fill start date — known Pinellas IDs first, then generic fallbacks
            filled_start = False
            for sel in [
                '#DateFrom',
                'input[id="DateFrom"]',
                'input[name="DateFrom"]',
                'input[placeholder*="Start" i]',
                'input[placeholder*="From" i]',
                'input[placeholder*="Begin" i]',
                'input[aria-label*="start" i]',
                'input[aria-label*="from" i]',
                'input[id*="DateFrom" i]',
                'input[name*="DateFrom" i]',
            ]:
                el = page.locator(sel)
                if await el.count() > 0:
                    await el.first.fill(start_str)
                    filled_start = True
                    logger.info("[evictions-captcha] Filled start date (%s) via: %s", start_str, sel)
                    break

            filled_end = False
            for sel in [
                '#DateTo',
                'input[id="DateTo"]',
                'input[name="DateTo"]',
                'input[placeholder*="End" i]',
                'input[aria-label*="end" i]',
                'input[id*="DateTo" i]',
                'input[name*="DateTo" i]',
            ]:
                el = page.locator(sel)
                if await el.count() > 0:
                    await el.first.fill(end_str)
                    filled_end = True
                    logger.info("[evictions-captcha] Filled end date (%s) via: %s", end_str, sel)
                    break

            # Positional fallback: first two text/date inputs on the page
            if not filled_start or not filled_end:
                date_inputs = page.locator('input[type="date"], input[type="text"][class*="date" i]')
                n = await date_inputs.count()
                if n >= 2:
                    await date_inputs.nth(0).fill(start_str)
                    await date_inputs.nth(1).fill(end_str)
                    logger.info("[evictions-captcha] Filled date range via positional fallback")
                else:
                    logger.warning(
                        "[evictions-captcha] Could not locate date inputs "
                        "(found %d) — proceeding anyway", n
                    )

            await page.wait_for_timeout(500)

            # Filter Case Types dropdown to eviction-only before submitting.
            # The portal returns max 500 rows total — selecting only eviction
            # case types avoids wasting that cap on unrelated civil cases.
            try:
                eviction_values = await page.evaluate("""
                    () => {
                        const EVICTION_KEYWORDS = ['eviction', 'landlord', 'tenant', 'forcible'];
                        const candidates = [
                            ...document.querySelectorAll(
                                'select[id*="CaseType" i], select[name*="CaseType" i], '
                                + 'select[id*="casetype" i], select[id="CaseType"]'
                            )
                        ];
                        if (!candidates.length) return null;
                        const sel = candidates[0];
                        for (const opt of sel.options) opt.selected = false;
                        const matched = [];
                        for (const opt of sel.options) {
                            if (EVICTION_KEYWORDS.some(k => opt.text.toLowerCase().includes(k))) {
                                opt.selected = true;
                                matched.push(opt.text.trim());
                            }
                        }
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                        return matched.length ? matched : null;
                    }
                """)
                if eviction_values:
                    logger.info("[evictions-captcha] Case Types filtered to: %s", eviction_values)
                else:
                    logger.warning(
                        "[evictions-captcha] Could not filter Case Types dropdown "
                        "(element not found or no eviction options matched) — proceeding with all types"
                    )
            except Exception as ct_err:
                logger.warning("[evictions-captcha] Case Types filter failed: %s — proceeding with all types", ct_err)

            await page.wait_for_timeout(300)

            # Click Submit
            submit_btn = page.locator(
                'input[type="submit"][value="Submit"], '
                'button:has-text("Submit"), '
                'input[value="Search"], '
                'button[type="submit"]:has-text("Submit")'
            )
            if await submit_btn.count() == 0:
                raise ValueError(
                    "[evictions-captcha] Submit button not found — check debug screenshot"
                )
            logger.info("[evictions-captcha] Clicking Submit")
            await submit_btn.first.click()

            # Wait for reCAPTCHA overlay or results — whichever appears first
            captcha_found = False
            try:
                await page.wait_for_selector(
                    'iframe[src*="google.com/recaptcha"]', timeout=10000
                )
                captcha_found = True
                logger.info("[evictions-captcha] reCAPTCHA detected — solving via 2captcha")
            except Exception:
                logger.info("[evictions-captcha] No reCAPTCHA appeared within 10s")

            if captcha_found:
                solved = await _solve_recaptcha_2captcha(page, page.url)
                if not solved:
                    raise RuntimeError(
                        "[evictions-captcha] _solve_recaptcha_2captcha returned False — "
                        "check TWOCAPTCHA_API_KEY and sitekey extraction"
                    )
                logger.info("[evictions-captcha] Captcha solved — checking if form auto-submitted")

                # Some portals auto-submit via the reCAPTCHA callback; others just
                # validate the widget and require a second Submit click.  Wait
                # briefly for networkidle and, if the Submit button is still
                # present, click it again with the captcha token now embedded.
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass

                try:
                    resubmit = page.locator(
                        'input[type="submit"][value="Submit"], '
                        'button:has-text("Submit"), '
                        'input[value="Search"], '
                        'button[type="submit"]:has-text("Submit")'
                    )
                    if await resubmit.count() > 0:
                        logger.info(
                            "[evictions-captcha] Submit button still visible — "
                            "re-clicking to submit form with captcha token"
                        )
                        await resubmit.first.click()
                    else:
                        logger.info("[evictions-captcha] Submit button gone — captcha callback auto-submitted")
                except Exception as re_err:
                    logger.warning("[evictions-captcha] Re-submit check failed: %s", re_err)

                # Wait for the form-submission navigation to settle
                try:
                    await page.wait_for_load_state("networkidle", timeout=60000)
                except Exception as nw_exc:
                    logger.warning("[evictions-captcha] networkidle wait timed out: %s", nw_exc)

            # Wait for results table — extended timeout for slow portal responses
            await page.wait_for_selector(
                'table, .results, [class*="result"], [class*="grid"], [class*="case"]',
                timeout=90000,
            )
            logger.info("[evictions-captcha] Results page loaded")

            # Find Export to Excel link — prefer the direct ExportToExcel href.
            # Pinellas portal uses target="_blank" which opens a new tab, so we
            # extract the href and navigate the current page to it directly instead
            # of clicking, bypassing the new-tab behavior so expect_download fires.
            export_btn = page.locator(
                'a[href*="ExportToExcel"], '
                'a[href*="exporttoexcel" i], '
                'a:has-text("Excel"), '
                'button:has-text("Excel"), '
                '[title*="excel" i], '
                'a[href*=".xlsx"]'
            )
            if await export_btn.count() == 0:
                all_btns = await page.locator("a, button").all()
                labels = []
                for el in all_btns[:30]:
                    try:
                        labels.append((await el.inner_text()).strip())
                    except Exception:
                        pass
                logger.error("[evictions-captcha] Export button not found. Visible labels: %s", labels)
                raise ValueError(
                    "[evictions-captcha] Export/Excel button not found — "
                    "check debug screenshot and labels above"
                )

            # Get the href and navigate directly — avoids target="_blank" new-tab issue
            export_href = await export_btn.first.get_attribute("href")
            if export_href:
                base = "https://courtrecords.mypinellasclerk.gov"
                export_url = (base + export_href) if export_href.startswith("/") else export_href
                logger.info("[evictions-captcha] Navigating to export URL: %s", export_url)
                async with page.expect_download(timeout=90000) as dl_info:
                    try:
                        await page.goto(export_url, wait_until="commit", timeout=60000)
                    except Exception as _goto_err:
                        if "Download is starting" not in str(_goto_err):
                            raise
            else:
                # Fallback: catch new-tab popup and grab download from it
                logger.info("[evictions-captcha] No href found — using popup download fallback")
                async with context.expect_page() as popup_info:
                    await export_btn.first.click()
                popup = await popup_info.value
                async with popup.expect_download(timeout=90000) as dl_info:
                    pass
            download = await dl_info.value

            suggested = download.suggested_filename or (
                f"pinellas_evictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            save_path = RAW_EVICTIONS_DIR / suggested
            await download.save_as(str(save_path))
            logger.info(
                "[evictions-captcha] Saved: %s (%.1f KB)",
                save_path, save_path.stat().st_size / 1024,
            )
            return save_path

        except Exception as e:
            # Always capture a debug screenshot + HTML on failure
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dbg_dir = RAW_EVICTIONS_DIR / "debug"
            dbg_dir.mkdir(parents=True, exist_ok=True)
            shot = dbg_dir / f"captcha_debug_{county_id}_{ts}.png"
            html_f = dbg_dir / f"captcha_debug_{county_id}_{ts}.html"
            try:
                await page.screenshot(path=str(shot), full_page=True)
                html_f.write_text(await page.content(), encoding="utf-8")
                logger.error("[evictions-captcha] Debug screenshot: %s", shot)
                logger.error("[evictions-captcha] Debug HTML: %s", html_f)
            except Exception:
                pass
            logger.error("[evictions-captcha] Scrape failed: %s", e)
            raise
        finally:
            await browser.close()


async def _download_civil_filing_browser(
    county_id: str, source: dict, target_date: str | None, dest_dir: Path,
    headful: bool = False, no_proxy: bool = False,
) -> Path:
    """
    Browser-use agent download for counties whose civil portal requires a browser
    (output_format='excel', e.g. Pinellas courtrecords.mypinellasclerk.gov).
    Returns the path to the downloaded file.
    """
    from browser_use import Agent, Browser

    if target_date:
        target_dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        start_str = end_str = target_dt.strftime("%m/%d/%Y")
    elif source.get("start_date") and source.get("end_date"):
        start_str = datetime.strptime(source["start_date"], "%Y-%m-%d").strftime("%m/%d/%Y")
        end_str   = datetime.strptime(source["end_date"],   "%Y-%m-%d").strftime("%m/%d/%Y")
    else:
        end_dt = datetime.now()
        start_str = (end_dt - timedelta(days=1)).strftime("%m/%d/%Y")
        end_str = end_dt.strftime("%m/%d/%Y")

    url = source.get("url", "")
    nav_hint = source.get("navigation_hint") or ""
    task = (
        f"Go to {url}. "
        f"Search for civil court filings filed between {start_str} and {end_str}. "
        f"Export or download the full results as a file (Excel or CSV). "
        f"Wait for the download to complete. "
        f"Do not open new tabs or navigate away from the portal."
    )
    if nav_hint:
        task += f"\n\nPortal navigation hint: {nav_hint}"

    dest_dir.mkdir(parents=True, exist_ok=True)
    cf_required = source.get("cf_bypass_required", False)
    profile_name = source.get("cf_bypass_profile_name", f"{county_id}_evictions_clerk")
    cf_profile = None
    if cf_required:
        from src.utils.cf_session_manager import ensure_ready
        from src.utils.cf_persistent_browser import find_edge_binary
        logger.info("[evictions] CF-bypass mode — using persistent Edge profile")
        profile_dir = await ensure_ready(
            profile_name=profile_name,
            county_id=county_id,
            portal_url=url,
        )
        cf_profile = {"edge_path": find_edge_binary(), "profile_dir": str(profile_dir)}

    browser_kwargs = dict(
        headless=False if cf_profile else not headful,
        disable_security=True,
        downloads_path=str(dest_dir),
        ignore_default_args=["--enable-automation"],
        minimum_wait_page_load_time=1.5,
        wait_between_actions=1.0,
        args=STEALTH_ARGS,
    )
    if cf_profile:
        # CF bypass: use warmed Edge profile — proxy must be None to preserve
        # the fingerprint that earned the cf_clearance cookie.
        browser_kwargs.update(
            executable_path=cf_profile["edge_path"],
            user_data_dir=cf_profile["profile_dir"],
            proxy=None,
            enable_default_extensions=False,
        )
    else:
        browser_kwargs.update(
            user_agent=STEALTH_UA,
            enable_default_extensions=True,
            proxy=None if no_proxy else get_browser_use_proxy(),
        )

    browser = Browser(**browser_kwargs)
    await browser.start()
    if not cf_profile:
        await apply_stealth_to_browser_use(browser)
        logger.info("[evictions] Stealth fingerprint patches injected")

    start_time = time.time()
    agent = Agent(task=task, llm=_make_llm(), browser=browser, max_steps=60, use_judge=False)
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[evictions] Browser agent did not complete within step budget")
    except Exception as e:
        logger.error("[evictions] Browser agent failed: %s", e)
        raise

    await asyncio.sleep(5)
    candidates = [
        p for p in dest_dir.iterdir()
        if p.stat().st_mtime >= start_time and p.suffix.lower() in (".xlsx", ".xls", ".csv")
    ]
    if not candidates:
        raise FileNotFoundError(f"[evictions] No downloaded civil filing found in {dest_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def download_latest_civil_filing(
    target_date: str = None, county_id: str = "hillsborough", headful: bool = False,
    start_date: str = None, end_date: str = None, no_proxy: bool = False,
) -> Path:
    """
    Download the latest civil filing from the county clerk.
    Hillsborough: requests-based directory listing → CSV.
    Pinellas (output_format=excel): direct Playwright + 2captcha reCAPTCHA solving → Excel.
    Other counties with playwright_code: playwright_only / playwright_then_ai path.
    """
    source = _get_eviction_source(county_id)
    scrape_mode = source.get("scrape_mode", "")
    output_format = source.get("output_format", "csv")

    if start_date:
        source = dict(source, start_date=start_date, end_date=end_date or start_date)

    if scrape_mode == "static_download":
        logger.info("[evictions] Using static_download mode for '%s'", county_id)
        return _static_download(source, RAW_EVICTIONS_DIR, target_date, no_proxy=no_proxy)

    # output_format=excel check runs BEFORE scrape_mode so that the 2captcha path
    # always wins for Pinellas even when the DB config also has scrape_mode set.
    if output_format == "excel":
        # Merged single-session: ONE captcha search exports the filing Excel AND
        # clicks each case for docket detail (written to <dir>/eviction_*_detail.json).
        from src.scrappers.court_docket.pinellas.civil_filing import (
            scrape_pinellas_civil_with_detail, reconstruct_filing_list_from_detail,
        )
        logger.info("[evictions] County '%s' — Pinellas courtrecords merged scrape+detail", county_id)
        excel_path, results = asyncio.run(scrape_pinellas_civil_with_detail(
            "eviction", ["eviction"], source.get("url", ""),
            target_date=target_date, start_date=start_date, end_date=end_date,
            headful=headful, no_proxy=no_proxy, dest_dir=RAW_EVICTIONS_DIR,
        ))
        if excel_path is not None:
            return excel_path
        # Excel export step failed (site timeout/layout hiccup) but the
        # click-through docket-detail scrape may still have succeeded —
        # reconstruct the same raw filing-list shape from it instead of
        # losing the day's data. Live-verified 2026-07-08.
        fallback_df = reconstruct_filing_list_from_detail(results)
        if fallback_df.empty:
            return None
        RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)
        fallback_path = RAW_EVICTIONS_DIR / (
            f"eviction_reconstructed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        fallback_df.to_excel(fallback_path, index=False)
        logger.warning(
            "[evictions] Excel export unavailable — reconstructed %d case(s) "
            "from docket-detail scrape instead: %s",
            len(fallback_df), fallback_path.name,
        )
        return fallback_path

    if scrape_mode in ("playwright_only", "playwright_then_ai"):
        logger.info("[evictions] Using playwright mode for '%s'", county_id)
        try:
            return asyncio.run(
                _scrape_with_playwright(source, county_id, target_date, start_date, end_date, headful, no_proxy=no_proxy)
            )
        except Exception as pw_exc:
            if scrape_mode != "playwright_then_ai":
                raise
            logger.warning(
                "[evictions] Playwright failed for '%s' (%s) — falling back to browser-use AI agent",
                county_id, pw_exc,
            )
            return asyncio.run(
                _download_civil_filing_browser(
                    county_id, source, target_date, RAW_EVICTIONS_DIR,
                    headful=headful, no_proxy=no_proxy,
                )
            )

    # Hillsborough / CSV directory-listing path
    _county = _get_county(county_id)
    _civil_filings_url = _county["urls"].get("civil") or CIVIL_FILINGS_URL
    _clerk_base_url = _county["urls"].get("clerk_base") or HILLSCLERK_BASE_URL

    logger.info("[evictions] Fetching civil filings list from: %s", _civil_filings_url)
    RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    response = requests_get_with_retry(
        _civil_filings_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_DEFAULT,
    )
    soup = BeautifulSoup(response.content, "html.parser")
    file_links = [
        link["href"]
        for link in soup.find_all("a", href=True)
        if "CivilFiling_" in link["href"] and link["href"].endswith(".csv")
    ]
    if not file_links:
        raise ValueError("[evictions] No civil filing CSV files found on the page")

    date_pattern = re.compile(CIVIL_FILING_PATTERN)
    files_with_dates = []
    for href in file_links:
        m = date_pattern.search(href)
        if m:
            try:
                files_with_dates.append((datetime.strptime(m.group(1), "%Y%m%d"), href))
            except ValueError:
                continue
    if not files_with_dates:
        raise ValueError("[evictions] No valid dated civil files found")
    files_with_dates.sort(key=lambda x: x[0], reverse=True)

    if target_date:
        target_dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        matches = [(d, f) for d, f in files_with_dates if d.date() == target_dt.date()]
        if not matches:
            from src.utils.scraper_exceptions import ScraperNoDataError
            raise ScraperNoDataError(f"No civil filing found for date: {target_date}")
        latest_date, latest_file = matches[0]
    else:
        latest_date, latest_file = files_with_dates[0]

    logger.info("[evictions] Selected civil filing: %s (%s)", latest_file, latest_date.strftime("%Y-%m-%d"))
    download_url = (
        f"{_clerk_base_url}{latest_file}"
        if latest_file.startswith("/")
        else f"{_civil_filings_url.rstrip('/')}/{latest_file}"
    )
    output_path = RAW_EVICTIONS_DIR / Path(latest_file).name
    file_response = requests_get_with_retry(
        download_url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
        timeout=REQUEST_TIMEOUT_LONG,
    )
    output_path.write_bytes(file_response.content)
    logger.info("[evictions] Downloaded to: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
    return output_path


def _normalize_style_col_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Pinellas-style one-row-per-case court export to the multi-row
    format that Hillsborough uses and downstream loaders expect.

    Pinellas columns: Case Type, Case #, Filed, Style/Description, Status, Judicial Officer
    Canonical format: CaseTypeDescription, CaseNumber, FilingDate, Title, PartyType,
                      LastName/CompanyName, PartyAddress

    Style/Description format: "PLAINTIFF NAME\nVs.\nDEFENDANT NAME"
    Each case row is expanded into two rows (Plaintiff + Defendant).
    """
    rename = {
        "Case #":        "CaseNumber",
        "Filed":         "FilingDate",
        "Case Type":     "CaseTypeDescription",
        "Status":        "Title",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "Style/Description" not in df.columns:
        return df

    expanded_rows = []
    for _, row in df.iterrows():
        style = str(row.get("Style/Description", "") or "")
        # Split on "Vs." variants — newline-separated or just " Vs. "
        parts = re.split(r'\n[Vv][Ss]\.\n|[\s]+[Vv][Ss]\.[\s]+', style, maxsplit=1)
        plaintiff = parts[0].strip() if len(parts) >= 1 else ""
        defendant = parts[1].strip().rstrip(".").strip() if len(parts) >= 2 else ""
        # Strip common suffixes like "et al"
        for suffix in (" et al", " ET AL", " Et Al"):
            defendant = defendant.removesuffix(suffix).strip()

        for party_type, name in (("Plaintiff", plaintiff), ("Defendant", defendant)):
            if not name:
                continue
            new_row = row.to_dict()
            new_row["PartyType"] = party_type
            new_row["LastName/CompanyName"] = name
            new_row["FirstName"] = ""
            new_row["PartyAddress"] = None
            expanded_rows.append(new_row)

    if not expanded_rows:
        return df

    result = pd.DataFrame(expanded_rows)
    logger.info(
        "[evictions] Pinellas normalizer: %d cases → %d party rows",
        len(df), len(result),
    )
    return result


def process_civil_data(file_path: Path, county_id: str = "hillsborough") -> pd.DataFrame:
    """Load the civil filing (CSV or Excel) with multi-encoding support."""
    source = _get_eviction_source(county_id)
    output_format = source.get("output_format", "csv")

    if output_format == "excel" or file_path.suffix.lower() in (".xlsx", ".xls"):
        logger.info("[evictions] Reading Excel civil filing: %s", file_path)
        df = pd.read_excel(file_path)
    else:
        logger.info("[evictions] Loading civil filing from: %s", file_path)
        encodings = ["utf-8", "latin1", "cp1252"]
        df = None
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc)
                logger.info("[evictions] Loaded %d records (%s)", len(df), enc)
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        if df is None:
            raise ValueError(f"[evictions] Could not read civil filing with any standard encoding: {file_path}")

    # Apply Pinellas-style normalizer when Style/Description column parsing is needed
    if source.get("style_col") == "Style/Description":
        df = _normalize_style_col_format(df)

    return df


def filter_evictions(df: pd.DataFrame, county_id: str = "hillsborough") -> pd.DataFrame:
    """
    Filter civil filing data to include only eviction-related cases.
    Always filters on CaseTypeDescription (Hillsborough native name, or renamed from
    'Case Type' for Pinellas after _normalize_style_col_format runs in process_civil_data).
    """
    source = _get_eviction_source(county_id)
    filter_col = source.get("special_flags", {}).get("case_type_col", "CaseTypeDescription")
    if filter_col not in df.columns:
        logger.error("[evictions] Column '%s' not found — columns: %s", filter_col, list(df.columns))
        raise KeyError(f"Column '{filter_col}' not found in civil filing")

    mask = df[filter_col].str.contains("|".join(EVICTION_CASE_PATTERNS), case=False, na=False)
    evictions_df = df[mask].copy()
    logger.info("[evictions] Filtered %d eviction records from %d total (col: %s)",
                len(evictions_df), len(df), filter_col)
    return evictions_df


def save_processed_evictions(df: pd.DataFrame, county_id: str = "hillsborough", output_filename: str = "eviction_leads.csv") -> Path:
    """Save processed eviction data with dedup against DB."""
    RAW_EVICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    initial_count = len(df)
    df_new = filter_new_records(df, "evictions", record_type="Eviction", county_id=county_id)

    if df_new.empty:
        logger.info("[evictions] All evictions already in DB — nothing new")
        return None

    new_dir = RAW_EVICTIONS_DIR / "new"
    new_dir.mkdir(parents=True, exist_ok=True)
    final_file = new_dir / output_filename
    df_new.to_csv(final_file, index=False)
    logger.info("[evictions] Saved %d new evictions (filtered %d existing)", len(df_new), initial_count - len(df_new))
    return final_file


def run_eviction_pipeline(
    target_date: str = None, county_id: str = "hillsborough", headful: bool = False,
    start_date: str = None, end_date: str = None, no_proxy: bool = False,
):
    """Full pipeline: download → load → filter → dedup → save.

    Returns True when new eviction records were written (caller should load
    them), "no_data" when the run succeeded but genuinely found zero eviction
    cases (not a failure — exit code should still be 0, but there is no CSV to
    load), or False on an actual pipeline failure.
    """
    t0 = time.monotonic()
    try:
        logger.info(OUTPUT_SEPARATOR)
        logger.info("%s EVICTION DATA COLLECTION PIPELINE", county_id.upper())
        logger.info(OUTPUT_SEPARATOR)

        file_path = download_latest_civil_filing(
            target_date=target_date, county_id=county_id, headful=headful,
            start_date=start_date, end_date=end_date, no_proxy=no_proxy,
        )
        if file_path is None:
            # Pinellas merged scrape+detail found no exportable filing list this
            # run (Excel export button or post-export grid recheck timed out) —
            # a real data-collection gap, not a confirmed zero-case day, so this
            # stays a failure (retried by run.sh, alerted after 3 attempts) but
            # with a clear, specific reason instead of a NoneType crash trying
            # to read a file that was never produced.
            logger.error(
                "[evictions] Pinellas civil filing export unavailable this run "
                "(Excel export/grid did not load in time) — 0 cases collected"
            )
            try:
                from src.utils.scraper_db_helper import record_scraper_stats
                from config.scraper_outcomes import ScraperOutcome
                record_scraper_stats(
                    source_type="evictions", total_scraped=0, matched=0, unmatched=0, skipped=0,
                    error_type="export_unavailable", outcome=ScraperOutcome.TIMEOUT.value,
                    duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
                )
            except Exception as _se:
                logger.warning("[evictions] Could not record scraper stats: %s", _se)
            return False
        civil_df = process_civil_data(file_path, county_id=county_id)
        evictions_df = filter_evictions(civil_df, county_id=county_id)

        if len(evictions_df) == 0:
            logger.warning("[evictions] No eviction cases found")
            try:
                from src.utils.scraper_db_helper import record_scraper_stats
                from config.scraper_outcomes import ScraperOutcome
                record_scraper_stats(
                    source_type="evictions", total_scraped=0, matched=0, unmatched=0, skipped=0,
                    error_type="no_data", outcome=ScraperOutcome.NO_DATA.value,
                    duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
                )
            except Exception as _se:
                logger.warning("[evictions] Could not record scraper stats: %s", _se)
            return "no_data"

        today = datetime.now().strftime(OUTPUT_DATE_FORMAT)
        output_path = save_processed_evictions(evictions_df, county_id, f"eviction_leads_{today}.csv")

        logger.info(OUTPUT_SEPARATOR)
        logger.info("EVICTION PIPELINE COMPLETED — %d records, output: %s", len(evictions_df), output_path)
        logger.info(OUTPUT_SEPARATOR)

        return True

    except Exception as e:
        logger.error("[evictions] Pipeline failed: %s", e)
        logger.debug(traceback.format_exc())
        from src.utils.scraper_outcome_classifier import classify_exception
        from config.scraper_outcomes import ScraperOutcome
        classified = classify_exception(e)
        try:
            from src.utils.scraper_db_helper import record_scraper_stats
            # No hardcoded run_success=False here on purpose: a ScraperNoDataError
            # (e.g. download_latest_civil_filing's "no civil filing found for
            # date") legitimately reaches this branch, and forcing False would
            # misreport a genuine no-data day as a failure — the same bug class
            # this whole classification system exists to close.
            record_scraper_stats(
                source_type="evictions", total_scraped=0, matched=0, unmatched=0, skipped=0,
                outcome=classified, error_message=str(e)[:500],
                duration_seconds=round(time.monotonic() - t0, 2), county_id=county_id,
            )
        except Exception as _se:
            logger.warning("[evictions] Could not record scraper stats: %s", _se)
        # Match the DB row's classification: a NO_DATA-classified exception is
        # a clean no-data day per this pipeline's own tri-state contract, not a
        # pipeline failure — keep the return value and the stats row in agreement.
        return "no_data" if classified == ScraperOutcome.NO_DATA.value else False


if __name__ == "__main__":
    import sys
    import argparse
    from src.utils.scraper_db_helper import load_scraped_data_to_db, add_load_to_db_arg

    parser = argparse.ArgumentParser(description="Scrape county eviction filings")
    parser.add_argument("--date", type=str, default=None, help="Single target date YYYY-MM-DD")
    parser.add_argument("--start-date", dest="start_date", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", dest="end_date", type=str, default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--headful", action="store_true", default=False,
                        help="Run browser in headed (visible) mode for debugging")
    parser.add_argument("--no-proxy", dest="no_proxy", action="store_true", default=False,
                        help="Disable Oxylabs proxy for all requests")
    parser.add_argument("--skip-docket", dest="skip_docket", action="store_true", default=False,
                        help="Skip Stage-2 court-docket detail enrichment (Pinellas only)")
    add_load_to_db_arg(parser)
    args = parser.parse_args()

    result = run_eviction_pipeline(
        target_date=args.date, county_id=args.county_id, headful=args.headful,
        start_date=args.start_date, end_date=args.end_date, no_proxy=args.no_proxy,
    )
    success = result is True          # new records were written — proceed to load
    pipeline_ok = result is not False  # True or "no_data" both count as a clean run

    if success and args.load_to_db:
        try:
            new_dir = RAW_EVICTIONS_DIR / "new"
            csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            if csv_files:
                load_scraped_data_to_db(
                    "evictions", csv_files[0],
                    destination_dir=RAW_EVICTIONS_DIR, county_id=args.county_id,
                )
            else:
                logger.error("[evictions] No eviction CSV file found to load")
                sys.exit(1)
        except Exception as e:
            logger.error("[evictions] Failed to load data to database: %s", e)
            sys.exit(1)
    elif args.load_to_db and not pipeline_ok:
        logger.warning("[evictions] Skipping database load due to scraping failure")
    elif args.load_to_db:
        logger.info("[evictions] No new eviction cases today — nothing to load")

    # Stage 2 — apply docket detail scraped during the merged search (no re-search/captcha).
    if success and args.load_to_db and args.county_id == "pinellas" and not args.skip_docket:
        from src.scrappers.court_docket.pinellas.detail_enrichment import (
            apply_detail_from_json, rescue_unmatched_from_detail,
        )
        _dj = sorted(RAW_EVICTIONS_DIR.glob("eviction_*_detail.json"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
        if _dj:
            apply_detail_from_json("Eviction", _dj[0], county_id=args.county_id)
            # Rescue address-less evictions that failed pass-1: re-match them
            # using the defendant mailing address scraped into the docket detail.
            rescue_unmatched_from_detail("Eviction", _dj[0], county_id=args.county_id)
        else:
            logger.warning("[evictions] no docket detail JSON found — skipping detail apply")

    sys.exit(0 if pipeline_ok else 1)
