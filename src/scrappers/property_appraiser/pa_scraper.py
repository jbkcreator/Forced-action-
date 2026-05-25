"""
HCPA Property Appraiser Scraper — search-based, three sources per property.

Flow per property:
  1. Navigate to HCPA Basic Search page
  2. Search by folio (all-digit parcel IDs) or PIN (alphanumeric), fall back to the other
  3. Click the first result → land on the parcel detail page
  4. Extract HCPA text + HTML
  5. Click the TRIM PDF button → trap download
  6. Click the Tax Collector link → navigate to county-taxes.net, extract text

This avoids constructing direct URLs (Tax Collector uses a signed redirect that
can't be reproduced without clicking the link on the HCPA page).

Designed to run inside a ThreadPoolExecutor (one Playwright context per thread).
Called by pa_engine.py.
"""

import logging
import re
import tempfile
import time
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import sync_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeout

from src.utils.http_helpers import STEALTH_UA, STEALTH_ARGS, get_stealth

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://gis.hcpafl.org/propertysearch/#/nav/Basic%20Search"

# Selectors (verified against live page HTML)
_SEL_FOLIO_RADIO   = "input[name='basicPinGroup'][value='folio']"
_SEL_PIN_RADIO     = "input[name='basicPinGroup'][value='pin']"
_SEL_FOLIO_INPUT   = "input[data-bind*=\"autocomplete: 'folio'\"]"
_SEL_PIN_INPUT     = "input.pin-sized"
_SEL_SEARCH_BTN    = "button.btn-primary[data-bind='click: search']"
_SEL_RESULTS_ROW   = "tbody[data-bind='foreach: results'] tr"
_SEL_RESULTS_LINK  = "tbody[data-bind='foreach: results'] tr td.link"

# Detail page selectors — these appear only after clicking a result
_SEL_TRIM_LINK     = "a:has-text('TRIM'), a[href*='trim'], a:has-text('Download TRIM')"
_SEL_TAX_LINK      = "a:has-text('Tax Collector'), a[href*='county-taxes'], button:has-text('Tax Collector')"
_SEL_DETAIL_READY  = ".property-info, table.property-details, .parcel-info, div.ng-scope"


def _is_alphanumeric(s: str) -> bool:
    return bool(re.search(r"[A-Za-z]", s or ""))


class HCPAScraper:
    """
    Sync Playwright scraper for Hillsborough County Property Appraiser.

    Uses the HCPA Basic Search UI rather than direct URLs so that Tax Collector
    and TRIM links are resolved naturally through the page without needing to
    construct signed or session-dependent URLs.

    One instance per thread in the ThreadPoolExecutor.
    """

    def __init__(self, config: dict, headful: bool = False, trim_output_dir: Optional[Path] = None):
        self._config = config
        self._headful = headful
        self._trim_dir = trim_output_dir or Path(tempfile.mkdtemp(prefix="hcpa_trim_"))
        self._trim_dir_owned = trim_output_dir is None  # only clean up dirs we created
        self._throttle_s = config.get("request_throttle_ms", 500) / 1000

        self._playwright = None
        self._browser = None
        self._context: Optional[BrowserContext] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=not self._headful,
            args=STEALTH_ARGS,
        )
        self._context = self._browser.new_context(
            user_agent=STEALTH_UA,
            accept_downloads=True,
        )
        try:
            get_stealth().apply_to_context(self._context)
        except Exception:
            pass

    def stop(self) -> None:
        try:
            if self._browser:
                self._browser.close()
            if self._playwright:
                self._playwright.stop()
        except Exception as e:
            logger.debug("Error closing browser: %s", e)
        # Individual PDF files are deleted in pa_engine._scrape_and_parse after parsing.
        # Only remove the directory itself (should already be empty by then).
        if self._trim_dir_owned:
            try:
                self._trim_dir.rmdir()
            except Exception:
                pass

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def scrape_property(self, parcel_id: str) -> dict:
        """
        Full scrape for one property: search → detail page → TRIM + Tax Collector.

        Returns dict with keys:
          parcel_id, hcpa_text, hcpa_html, trim_pdf_path, tax_text, errors
        """
        result = {
            "parcel_id":     parcel_id,
            "hcpa_text":     None,
            "hcpa_html":     None,
            "trim_pdf_path": None,
            "tax_text":      None,
            "errors":        [],
        }

        page: Page = self._context.new_page()
        try:
            # 1–3: Search → click result → land on detail page
            reached = self._search_and_open(page, parcel_id)
            if not reached:
                result["errors"].append(f"No search result found for {parcel_id}")
                return result

            # 4. Extract HCPA property data
            result["hcpa_text"] = self._extract_text(page)
            result["hcpa_html"] = page.content()

            # 5. Download TRIM PDF (click the link on this page)
            try:
                pdf_path = self._download_trim_from_page(page, parcel_id)
                result["trim_pdf_path"] = str(pdf_path) if pdf_path else None
            except Exception as e:
                logger.info("TRIM not available for %s: %s", parcel_id, e)
                result["errors"].append(f"TRIM: {e}")

            time.sleep(self._throttle_s)

            # 6. Click Tax Collector link and scrape that page
            try:
                result["tax_text"] = self._click_tax_and_scrape(page)
            except Exception as e:
                logger.warning("Tax Collector failed for %s: %s", parcel_id, e)
                result["errors"].append(f"Tax Collector: {e}")

        finally:
            page.close()

        return result

    # ------------------------------------------------------------------
    # Step 1–3: Search → results → detail page
    # ------------------------------------------------------------------

    def _search_and_open(self, page: Page, parcel_id: str) -> bool:
        """
        Navigate to the HCPA search page, search for parcel_id, and click the
        first result to reach the detail page.

        Tries folio first if parcel_id is all digits, PIN first if alphanumeric,
        then swaps if the first attempt returns no results.
        """
        page.goto(_SEARCH_URL, wait_until="networkidle", timeout=30000)
        # KO needs a moment to bind the form
        page.wait_for_timeout(1500)

        first_mode  = "pin"  if _is_alphanumeric(parcel_id) else "folio"
        second_mode = "folio" if first_mode == "pin" else "pin"

        for mode in (first_mode, second_mode):
            if self._do_search(page, parcel_id, mode):
                return True
            # No results — reload the search form and try the other mode
            page.goto(_SEARCH_URL, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(1500)

        return False

    def _do_search(self, page: Page, parcel_id: str, mode: str) -> bool:
        """
        Fill the search form for the given mode and submit.
        Returns True if a result row appeared and was clicked.
        """
        try:
            if mode == "folio":
                page.locator(_SEL_FOLIO_RADIO).first.click()
                page.wait_for_timeout(400)
                inp = page.locator(_SEL_FOLIO_INPUT).first
                inp.fill("")
                inp.type(parcel_id, delay=40)
            else:
                page.locator(_SEL_PIN_RADIO).first.click()
                page.wait_for_timeout(400)
                inp = page.locator(_SEL_PIN_INPUT).first
                inp.fill("")
                inp.type(parcel_id, delay=40)

            page.locator(_SEL_SEARCH_BTN).first.click()

            # Wait for at least one result row
            page.wait_for_selector(_SEL_RESULTS_ROW, timeout=8000)

        except PlaywrightTimeout:
            logger.debug("No results for %s mode=%s", parcel_id, mode)
            return False
        except Exception as e:
            logger.debug("Search error mode=%s: %s", mode, e)
            return False

        # Click first result td — KO SPA navigation (hash change, no browser nav event)
        try:
            first_cell = page.locator(_SEL_RESULTS_LINK).first
            first_cell.wait_for(state="visible", timeout=5000)
            url_before = page.url
            first_cell.click()
            # Wait for hash to change away from the search page
            page.wait_for_function(
                "(before) => window.location.href !== before",
                arg=url_before,
                timeout=15000,
            )
            # Wait for detail page content to render
            try:
                page.wait_for_selector(_SEL_DETAIL_READY, timeout=10000)
            except PlaywrightTimeout:
                pass
            page.wait_for_timeout(2000)
            logger.debug("Detail page loaded: %s", page.url)
            return True
        except Exception as e:
            logger.warning("Could not click result for %s: %s", parcel_id, e)
            return False

    # ------------------------------------------------------------------
    # Step 4: Extract HCPA page content
    # ------------------------------------------------------------------

    def _extract_text(self, page: Page) -> str:
        return page.evaluate("""() => {
            const noise = document.querySelectorAll('script,style,nav,footer,[aria-hidden="true"]');
            noise.forEach(el => el.remove());
            return document.body ? document.body.innerText : '';
        }""") or ""

    # ------------------------------------------------------------------
    # Step 5: TRIM PDF download
    # ------------------------------------------------------------------

    def _download_trim_from_page(self, page: Page, parcel_id: str) -> Optional[Path]:
        """
        Read the TRIM href from the detail page and download via requests.
        Playwright's PDF viewer opens the link inline (no download event),
        so we grab the URL and fetch it directly.
        """
        trim_locator = page.locator(_SEL_TRIM_LINK).first
        try:
            trim_locator.wait_for(state="visible", timeout=4000)
        except PlaywrightTimeout:
            logger.debug("TRIM link not visible on detail page for %s", parcel_id)
            return None

        href = trim_locator.get_attribute("href")
        if not href:
            logger.debug("TRIM link has no href for %s", parcel_id)
            return None

        safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", parcel_id)
        dest_path = self._trim_dir / f"trim_{safe_id}.pdf"

        try:
            resp = requests.get(href, timeout=20, headers={"User-Agent": STEALTH_UA})
            if resp.status_code == 200 and len(resp.content) > 1024:
                dest_path.write_bytes(resp.content)
                logger.debug("TRIM saved: %s (%d bytes)", dest_path, len(resp.content))
                return dest_path
            logger.info("TRIM empty/error for %s: HTTP %d", parcel_id, resp.status_code)
            return None
        except Exception as e:
            logger.info("TRIM download failed for %s: %s", parcel_id, e)
            return None

    # ------------------------------------------------------------------
    # Step 6: Tax Collector
    # ------------------------------------------------------------------

    def _click_tax_and_scrape(self, page: Page) -> str:
        """
        Click the Tax Collector link on the current detail page.
        The link redirects through county-taxes.com to a signed county-taxes.net URL.
        Returns the visible text of the final tax page.
        """
        tax_locator = page.locator(_SEL_TAX_LINK).first
        try:
            tax_locator.wait_for(state="visible", timeout=5000)
        except PlaywrightTimeout:
            logger.debug("Tax Collector link not found on detail page")
            return ""

        try:
            tax_locator.click()
            # Redirect chain: county-taxes.com → Cloudflare → county-taxes.net/{signed-token}
            # wait_for_url blocks until the final URL matches (skips CF challenge page)
            page.wait_for_url("*county-taxes.net*", timeout=35000)
            page.wait_for_timeout(2000)
            logger.debug("Tax Collector page: %s", page.url)
            return self._extract_text(page)
        except Exception as e:
            logger.warning("Tax Collector navigation failed: %s", e)
            return ""
