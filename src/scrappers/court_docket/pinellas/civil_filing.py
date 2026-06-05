"""
Shared Pinellas civil-filing scraper — courtrecords.mypinellasclerk.gov.

One deterministic Playwright + 2captcha path used by the eviction, probate and
divorce engines. Lifted out of evictions_engine._scrape_pinellas_with_2captcha
so probate/divorce stop routing through the browser-use AI agent (which cannot
solve the Google reCAPTCHA v2 that fires on Submit).

Public API:
    scrape_pinellas_civil(record_type, case_type_keywords, url, dest_dir, ...)
        -> Path to the downloaded .xlsx
    normalize_style_col(df, record_type) -> DataFrame of party rows
    solve_recaptcha_2captcha(page, page_url) -> bool   (re-exported for probes)

Case-type keyword sets per record_type live in
config.constants.PINELLAS_CASE_TYPE_KEYWORDS (probed from the live #caseTypesList
multiselect on 2026-06-05). The portal caps results at 500 rows total, so the
in-browser case-type filter is mandatory to stay under the cap on busy windows.

Future scope: this is the per-county integration seam — generalise to other
courtrecords-style portals once the Pinellas feed is proven.
"""

import asyncio
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.utils.http_helpers import (
    STEALTH_UA, STEALTH_ARGS, apply_stealth_to_page, get_playwright_proxy,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://courtrecords.mypinellasclerk.gov"


# ---------------------------------------------------------------------------
# 2captcha reCAPTCHA v2 solving
# ---------------------------------------------------------------------------

async def solve_recaptcha_2captcha(page, page_url: str) -> bool:
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

    captcha_frame = await page.query_selector('iframe[src*="google.com/recaptcha"]')
    if not captcha_frame:
        logger.debug("[captcha] No reCAPTCHA iframe on page")
        return False

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

    await page.evaluate("""
        (token) => {
            const resp = document.getElementById('g-recaptcha-response');
            if (resp) { resp.innerHTML = token; resp.value = token; }
            const captchaEl = document.querySelector('[data-sitekey]')
                           || document.querySelector('.g-recaptcha');
            if (captchaEl) {
                const cb = captchaEl.getAttribute('data-callback');
                if (cb && typeof window[cb] === 'function') { window[cb](token); return; }
            }
            if (window.___grecaptcha_cfg) {
                for (const clientKey in window.___grecaptcha_cfg.clients) {
                    const client = window.___grecaptcha_cfg.clients[clientKey];
                    if (!client) continue;
                    for (const k in client) {
                        if (client[k] && typeof client[k].callback === 'function') {
                            client[k].callback(token); return;
                        }
                    }
                }
            }
        }
    """, token)
    logger.info("[captcha] Token injected — waiting for page response")
    await page.wait_for_timeout(1500)
    return True


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def _date_window(target_date, start_date, end_date) -> tuple[str, str, str]:
    """Return (start_mmddyyyy, end_mmddyyyy, file_tag_yyyymmdd) for the portal form."""
    if target_date:
        dt = datetime.strptime(target_date.replace("-", ""), "%Y%m%d")
        return dt.strftime("%m/%d/%Y"), dt.strftime("%m/%d/%Y"), dt.strftime("%Y%m%d")
    if start_date:
        s = datetime.strptime(start_date.replace("-", ""), "%Y%m%d")
        e = datetime.strptime((end_date or start_date).replace("-", ""), "%Y%m%d")
        return s.strftime("%m/%d/%Y"), e.strftime("%m/%d/%Y"), s.strftime("%Y%m%d")
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=1)
    return (start_dt.strftime("%m/%d/%Y"), end_dt.strftime("%m/%d/%Y"),
            start_dt.strftime("%Y%m%d"))


async def scrape_pinellas_civil(
    record_type: str,
    case_type_keywords: list[str],
    url: str,
    dest_dir: Path,
    target_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    headful: bool = False,
    no_proxy: bool = False,
) -> Path:
    """
    Direct Playwright scrape for the Pinellas clerk portal Case search with
    integrated 2captcha reCAPTCHA v2 solving, filtered to `case_type_keywords`
    (matched against the #caseTypesList multiselect option text).

    Flow: navigate → activate Case tab → fill date range → filter case types →
    Submit → detect captcha → solve via 2captcha → re-submit if needed → wait
    for results → export Excel via direct URL navigation → save file.
    Returns the path to the downloaded .xlsx.
    """
    from playwright.async_api import async_playwright

    tag = f"{record_type}-captcha"
    start_str, end_str, file_tag = _date_window(target_date, start_date, end_date)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    _proxy = None if no_proxy else get_playwright_proxy()

    logger.info("[%s] Pinellas scrape: %s → %s  keywords=%s  url=%s",
                tag, start_str, end_str, case_type_keywords, url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headful, downloads_path=str(dest_dir), args=STEALTH_ARGS,
        )
        context = await browser.new_context(
            user_agent=STEALTH_UA, accept_downloads=True, proxy=_proxy,
        )
        page = await context.new_page()
        await apply_stealth_to_page(page)

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Activate the Case search tab
            try:
                case_tab = page.locator(
                    'a:has-text("Case"), li:has-text("Case") > a, '
                    '[role="tab"]:has-text("Case"), .tab:has-text("Case")'
                )
                if await case_tab.count() > 0:
                    await case_tab.first.click()
                    await page.wait_for_timeout(1000)
                    logger.info("[%s] Activated Case search tab", tag)
            except Exception as e:
                logger.warning("[%s] Could not click Case tab: %s", tag, e)

            # Fill dates (#DateFrom / #DateTo confirmed via probe)
            await page.locator("#DateFrom").first.fill(start_str)
            await page.locator("#DateTo").first.fill(end_str)
            logger.info("[%s] Filled date range %s → %s", tag, start_str, end_str)
            await page.wait_for_timeout(400)

            # Filter the #caseTypesList multiselect to the record_type's case types.
            # Portal caps results at 500 total — filtering here keeps us under it.
            matched = await page.evaluate(
                """
                (keywords) => {
                    const sel = document.querySelector('#caseTypesList')
                             || document.querySelector('select[name="CaseType"]')
                             || document.querySelector('select[id*="casetype" i]');
                    if (!sel) return null;
                    for (const opt of sel.options) opt.selected = false;
                    const m = [];
                    for (const opt of sel.options) {
                        if (keywords.some(k => opt.text.toLowerCase().includes(k))) {
                            opt.selected = true; m.push(opt.text.trim());
                        }
                    }
                    sel.dispatchEvent(new Event('change', {bubbles: true}));
                    return m;
                }
                """,
                [k.lower() for k in case_type_keywords],
            )
            if matched:
                logger.info("[%s] Case Types filtered to: %s", tag, matched)
            else:
                # Hard-fail: an unfiltered submit blows the 500-row cap and
                # silently truncates. Better to fail loud than ship partial data.
                raise RuntimeError(
                    f"[{tag}] Case-type filter matched ZERO options for "
                    f"{case_type_keywords} — refusing to submit unfiltered "
                    "(would hit the 500-row cap and silently truncate). "
                    "Re-probe #caseTypesList option labels."
                )
            await page.wait_for_timeout(300)

            # Submit
            submit_btn = page.locator(
                'button#caseSearch, input[type="submit"][value="Submit"], '
                'button:has-text("Submit"), input[value="Search"]'
            )
            if await submit_btn.count() == 0:
                raise ValueError(f"[{tag}] Submit button not found")
            logger.info("[%s] Clicking Submit", tag)
            await submit_btn.first.click()

            # reCAPTCHA?
            captcha_found = False
            try:
                await page.wait_for_selector(
                    'iframe[src*="google.com/recaptcha"]', timeout=10000
                )
                captcha_found = True
                logger.info("[%s] reCAPTCHA detected — solving via 2captcha", tag)
            except Exception:
                logger.info("[%s] No reCAPTCHA appeared within 10s", tag)

            if captcha_found:
                if not await solve_recaptcha_2captcha(page, page.url):
                    raise RuntimeError(
                        f"[{tag}] solve_recaptcha_2captcha returned False — "
                        "check TWOCAPTCHA_API_KEY and sitekey extraction"
                    )
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                try:
                    resubmit = page.locator(
                        'button#caseSearch, input[type="submit"][value="Submit"], '
                        'button:has-text("Submit"), input[value="Search"]'
                    )
                    if await resubmit.count() > 0:
                        logger.info("[%s] Re-clicking Submit with captcha token", tag)
                        await resubmit.first.click()
                    else:
                        logger.info("[%s] Submit gone — captcha callback auto-submitted", tag)
                except Exception as re_err:
                    logger.warning("[%s] Re-submit check failed: %s", tag, re_err)
                try:
                    await page.wait_for_load_state("networkidle", timeout=60000)
                except Exception as nw_exc:
                    logger.warning("[%s] networkidle wait timed out: %s", tag, nw_exc)

            # Results
            await page.wait_for_selector(
                'table, .results, [class*="result"], [class*="grid"], [class*="case"]',
                timeout=90000,
            )
            logger.info("[%s] Results page loaded", tag)

            # Export to Excel — read the href and navigate directly (the portal's
            # export uses target="_blank"; navigating avoids the new-tab trap).
            export_btn = page.locator(
                'a[href*="ExportToExcel"], a[href*="exporttoexcel" i], '
                'a:has-text("Excel"), button:has-text("Excel"), '
                '[title*="excel" i], a[href*=".xlsx"]'
            )
            if await export_btn.count() == 0:
                raise ValueError(f"[{tag}] Export/Excel button not found")
            export_href = await export_btn.first.get_attribute("href")
            if export_href:
                export_url = (BASE_URL + export_href) if export_href.startswith("/") else export_href
                logger.info("[%s] Navigating to export URL: %s", tag, export_url)
                async with page.expect_download(timeout=90000) as dl_info:
                    try:
                        await page.goto(export_url, wait_until="commit", timeout=60000)
                    except Exception as _goto_err:
                        if "Download is starting" not in str(_goto_err):
                            raise
            else:
                logger.info("[%s] No href — popup download fallback", tag)
                async with context.expect_page() as popup_info:
                    await export_btn.first.click()
                popup = await popup_info.value
                async with popup.expect_download(timeout=90000) as dl_info:
                    pass
            download = await dl_info.value

            suggested = download.suggested_filename or (
                f"pinellas_{record_type}_{file_tag}.xlsx"
            )
            save_path = dest_dir / f"pinellas_{record_type}_{file_tag}_{suggested}"
            await download.save_as(str(save_path))
            logger.info("[%s] Saved: %s (%.1f KB)", tag, save_path,
                        save_path.stat().st_size / 1024)
            return save_path

        except Exception as e:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dbg = dest_dir / "debug"
            dbg.mkdir(parents=True, exist_ok=True)
            try:
                await page.screenshot(path=str(dbg / f"{record_type}_debug_{ts}.png"),
                                      full_page=True)
                (dbg / f"{record_type}_debug_{ts}.html").write_text(
                    await page.content(), encoding="utf-8")
                logger.error("[%s] Debug artifacts in %s", tag, dbg)
            except Exception:
                pass
            logger.error("[%s] Scrape failed: %s", tag, e)
            raise
        finally:
            await browser.close()


# ---------------------------------------------------------------------------
# Style/Description normalisation — record-type aware
# ---------------------------------------------------------------------------

# Prefixes stripped from probate styles, longest-first. Probed 2026-06-05:
#   "IN RE: ANNEL L BOGUSKI"
#   "IN RE: THE ESTATE OF RICHARD EDWARD SILVERSTEIN"
#   "IN RE: THE MATTER OF FRANK MENDELBLATT"
#   "IN RE: MATTER OF NANCY WILSON INTER VIVOS TRUST AGREEMENT"
_PROBATE_PREFIX_RE = re.compile(
    r'^\s*IN\s+RE:?\s*(?:THE\s+)?(?:ESTATE\s+OF\s+|MATTER\s+OF\s+)?',
    re.IGNORECASE,
)
# Trailing trust/estate boilerplate stripped from the tail of probate names.
_PROBATE_TAIL_RE = re.compile(
    r'\s+(?:INTER\s+VIVOS\s+)?(?:REVOCABLE\s+)?(?:LIVING\s+)?TRUST(?:\s+AGREEMENT)?\s*$',
    re.IGNORECASE,
)

# Canonical column renames (Pinellas export → loader-expected names).
_RENAME = {
    "Case #": "CaseNumber", "Case#": "CaseNumber",
    "Filed": "FilingDate",
    "Case Type": "CaseTypeDescription",
    "Status": "Title",
}

# Party labels per record type for the two-party (Vs.-split) case types.
_VS_PARTY_LABELS = {
    "eviction": ("Plaintiff", "Defendant"),
    "divorce":  ("Petitioner", "Respondent"),
}


def _strip_probate_name(style: str) -> str:
    name = _PROBATE_PREFIX_RE.sub("", style or "").strip()
    name = _PROBATE_TAIL_RE.sub("", name).strip()
    return name


def normalize_style_col(df: pd.DataFrame, record_type: str) -> pd.DataFrame:
    """
    Expand the Pinellas one-row-per-case export into the multi-row party format
    downstream loaders expect (PartyType + LastName/CompanyName + ...).

    - eviction / divorce: split Style/Description on "Vs." into two party rows
      labelled Plaintiff/Defendant (eviction) or Petitioner/Respondent (divorce).
    - probate: no "Vs." — strip "IN RE: [THE] (ESTATE|MATTER) OF" + trailing
      trust boilerplate, emit ONE row PartyType='Decedent'.
    """
    df = df.rename(columns={k: v for k, v in _RENAME.items() if k in df.columns})
    if "Style/Description" not in df.columns:
        return df

    expanded: list[dict] = []

    if record_type == "probate":
        for _, row in df.iterrows():
            name = _strip_probate_name(str(row.get("Style/Description", "") or ""))
            if not name:
                continue
            nr = row.to_dict()
            nr["PartyType"] = "Decedent"
            nr["LastName/CompanyName"] = name
            nr["FirstName"] = ""
            nr["PartyAddress"] = None
            expanded.append(nr)
    else:
        first_label, second_label = _VS_PARTY_LABELS.get(
            record_type, ("Plaintiff", "Defendant")
        )
        for _, row in df.iterrows():
            style = str(row.get("Style/Description", "") or "")
            parts = re.split(r'\n[Vv][Ss]\.\n|[\s]+[Vv][Ss]\.[\s]+', style, maxsplit=1)
            first = parts[0].strip() if len(parts) >= 1 else ""
            second = parts[1].strip().rstrip(".").strip() if len(parts) >= 2 else ""
            for suffix in (" et al", " ET AL", " Et Al"):
                second = second.removesuffix(suffix).strip()
            for party_type, name in ((first_label, first), (second_label, second)):
                if not name:
                    continue
                nr = row.to_dict()
                nr["PartyType"] = party_type
                nr["LastName/CompanyName"] = name
                nr["FirstName"] = ""
                nr["PartyAddress"] = None
                expanded.append(nr)

    if not expanded:
        return df
    result = pd.DataFrame(expanded)
    logger.info("[%s] Style normalizer: %d cases -> %d party rows",
                record_type, len(df), len(result))
    return result
