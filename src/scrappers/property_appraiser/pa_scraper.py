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
import html as html_lib
import re
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

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

_SEL_DETAIL_READY  = ".property-info, table.property-details, .parcel-info, div.ng-scope"

_PCPAO_BASE_URL = "https://www.pcpao.gov"
_PCPAO_DETAIL_READY = "#property_summary, #tblLastYearValue, #div-property-information"
_PCPAO_RESULT_LINKS = (
    "a[href*='/property-details'], "
    "a[href*='property-details?s='], "
    "table a[href*='property-details']"
)
_PCPAO_PARCEL_RADIO = "input#dogradio3[name='home_search_options'][value='parcel_number']"
_PCPAO_KEYWORD_INPUTS = "input#txtKeyWord, input#txtSearchProperty, .select2-search__field, input[placeholder*='14-31-15']"
_PCPAO_SEARCH_BUTTON = "#btnHomeQuickSearch, button:has-text('Search')"


def _is_alphanumeric(s: str) -> bool:
    return bool(re.search(r"[A-Za-z]", s or ""))


def hyphenate_pinellas_parcel(parcel_id: str) -> str:
    """
    Convert Pinellas compact parcel number to PCPAO public format.

    PCPAO parcel numbers use RR-TT-SS-BBBBB-BBB-LLLL. The local DB stores the
    same value compacted without hyphens after the one-time Pinellas backfill.
    """
    digits = re.sub(r"\D", "", parcel_id or "")
    if len(digits) != 18:
        return parcel_id
    return f"{digits[0:2]}-{digits[2:4]}-{digits[4:6]}-{digits[6:11]}-{digits[11:14]}-{digits[14:18]}"


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

class PCPAOScraper(HCPAScraper):
    """
    Sync Playwright scraper for Pinellas County Property Appraiser (PCPAO).

    The public detail URL needs PCPAO's internal `s` key, so this searches by
    the hyphenated public parcel number and opens the first property-details
    result instead of trying to synthesize the final detail URL.
    """

    def scrape_property(self, parcel_id: str) -> dict:
        result = {
            "parcel_id":     parcel_id,
            "hcpa_text":     None,
            "hcpa_html":     None,
            "trim_pdf_path": None,
            "errors":        [],
        }

        page: Page = self._context.new_page()
        try:
            reached = self._search_and_open_pcpao(page, parcel_id)
            if not reached:
                result["errors"].append(f"No PCPAO search result found for {parcel_id}")
                return result

            result["hcpa_text"] = self._extract_text(page)
            result["hcpa_html"] = page.content()

            try:
                pdf_path = self._download_trim_from_page(page, parcel_id)
                result["trim_pdf_path"] = str(pdf_path) if pdf_path else None
            except Exception as e:
                logger.info("PCPAO TRIM not available for %s: %s", parcel_id, e)
                result["errors"].append(f"TRIM: {e}")

            time.sleep(self._throttle_s)

        finally:
            page.close()

        return result

    def _search_and_open_pcpao(self, page: Page, parcel_id: str) -> bool:
        public_parcel = hyphenate_pinellas_parcel(parcel_id)

        try:
            detail_url = self._lookup_pcpao_detail_url(public_parcel)
            if detail_url:
                page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeout:
                    pass
                self._wait_for_pcpao_detail(page, parcel_id, public_parcel)
                page.wait_for_timeout(1000)
                logger.debug("PCPAO detail page loaded via search API: %s", page.url)
                return True

            page.goto(f"{_PCPAO_BASE_URL}/quick-search?qu=1", wait_until="networkidle", timeout=30000)
            page.wait_for_selector(_PCPAO_KEYWORD_INPUTS, timeout=15000)

            radio = page.locator(_PCPAO_PARCEL_RADIO).first
            if radio.count() and not radio.is_checked():
                radio.click(force=True)
                page.wait_for_timeout(700)

            keyword = self._pcpao_visible_keyword_input(page)
            keyword.fill("")
            keyword.type(public_parcel, delay=40)
            page.wait_for_timeout(500)
            self._dispatch_pcpao_search_events(page, public_parcel)

            # PCPAO starts searching automatically after the masked parcel input
            # is complete. Pressing Enter is harmless when auto-search already ran,
            # and helps if the JS event is delayed in headless Chromium.
            keyword.press("Enter")
            page.wait_for_timeout(500)

            button = page.locator(_PCPAO_SEARCH_BUTTON).first
            if button.count():
                try:
                    button.click(force=True, timeout=3000)
                except Exception:
                    pass

            if self._is_pcpao_detail_page(page):
                return True

            link = page.locator(f"a[href*='property-details'][href*='{public_parcel}'], {_PCPAO_RESULT_LINKS}").first
            link.wait_for(state="visible", timeout=20000)
            href = link.get_attribute("href")
            if href:
                page.goto(urljoin(_PCPAO_BASE_URL, href), wait_until="networkidle", timeout=30000)
            else:
                link.click()
            page.wait_for_selector(_PCPAO_DETAIL_READY, timeout=15000)
            page.wait_for_timeout(1000)
            logger.debug("PCPAO detail page loaded: %s", page.url)
            return True
        except PlaywrightTimeout:
            if self._pcpao_detail_content_present(page, public_parcel):
                logger.debug("PCPAO detail content accepted after timeout for %s", parcel_id)
                return True
            self._dump_pcpao_debug(page, parcel_id, public_parcel)
            logger.debug("No PCPAO result for %s (%s)", parcel_id, public_parcel)
            return False
        except Exception as e:
            if self._pcpao_detail_content_present(page, public_parcel):
                logger.debug("PCPAO detail content accepted after exception for %s: %s", parcel_id, e)
                return True
            self._dump_pcpao_debug(page, parcel_id, public_parcel)
            logger.warning("PCPAO search failed for %s: %s", parcel_id, e)
            return False

    def _wait_for_pcpao_detail(self, page: Page, parcel_id: str, public_parcel: str) -> None:
        try:
            page.wait_for_selector(_PCPAO_DETAIL_READY, timeout=15000)
            return
        except PlaywrightTimeout:
            if self._pcpao_detail_content_present(page, public_parcel):
                logger.debug("PCPAO detail content present despite selector timeout for %s", parcel_id)
                return
            raise

    def _pcpao_detail_content_present(self, page: Page, public_parcel: str) -> bool:
        try:
            html = page.content()
        except Exception:
            return False
        return "property_summary" in html or "Parcel Summary" in html or public_parcel in html

    def _lookup_pcpao_detail_url(self, public_parcel: str) -> Optional[str]:
        """
        Ask PCPAO's DataTables search endpoint for the real detail URL.

        The detail page needs PCPAO's internal `s` value. The quick-search API
        returns links with that value, so this is more reliable than replaying
        the custom radio/masked-input UI.
        """
        try:
            resp = requests.post(
                f"{_PCPAO_BASE_URL}/dal/quicksearch/searchProperty",
                data={
                    "draw": "1",
                    "start": "0",
                    "length": "10",
                    "input": public_parcel,
                    "searchsort": "parcel_number",
                    "url": _PCPAO_BASE_URL,
                },
                headers={"User-Agent": STEALTH_UA, "Referer": f"{_PCPAO_BASE_URL}/quick-search?qu=1"},
                timeout=20,
            )
            if resp.status_code != 200:
                logger.debug("PCPAO search API HTTP %s for %s", resp.status_code, public_parcel)
                return None
            payload = resp.json()
            if int(payload.get("recordsTotal") or 0) < 1:
                logger.debug("PCPAO search API returned no rows for %s", public_parcel)
                return None
            for row in payload.get("data") or []:
                for cell in row:
                    match = re.search(r'href=\\"([^\\"]*property-details[^\\"]*)\\"|href="([^"]*property-details[^"]*)"', str(cell))
                    if match:
                        href = html_lib.unescape(match.group(1) or match.group(2))
                        return urljoin(_PCPAO_BASE_URL, href.replace("\\/", "/"))
            return None
        except Exception as e:
            logger.debug("PCPAO search API failed for %s: %s", public_parcel, e)
            return None

    def _pcpao_visible_keyword_input(self, page: Page):
        inputs = page.locator(_PCPAO_KEYWORD_INPUTS)
        count = inputs.count()
        for idx in range(count):
            candidate = inputs.nth(idx)
            try:
                if candidate.is_visible() and candidate.is_editable():
                    candidate.click(force=True)
                    return candidate
            except Exception:
                continue
        candidate = inputs.first
        candidate.wait_for(state="visible", timeout=10000)
        candidate.click(force=True)
        return candidate

    def _dispatch_pcpao_search_events(self, page: Page, public_parcel: str) -> None:
        """Wake PCPAO's masked-input/search JavaScript after Playwright typing."""
        try:
            page.evaluate(
                """(value) => {
                    const el = document.querySelector('#txtKeyWord, #txtSearchProperty');
                    if (!el) return;
                    el.value = value;
                    for (const name of ['input', 'change', 'keyup', 'blur']) {
                        el.dispatchEvent(new Event(name, { bubbles: true }));
                    }
                    if (window.jQuery) {
                        window.jQuery(el).val(value).trigger('input').trigger('change').trigger('keyup');
                    }
                }""",
                public_parcel,
            )
        except Exception:
            pass

    def _dump_pcpao_debug(self, page: Page, parcel_id: str, public_parcel: str) -> None:
        try:
            debug_dir = Path("reports/audit/pcpao_debug")
            debug_dir.mkdir(parents=True, exist_ok=True)
            safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", parcel_id)
            html_path = debug_dir / f"{safe_id}.html"
            png_path = debug_dir / f"{safe_id}.png"
            html_path.write_text(page.content(), encoding="utf-8")
            page.screenshot(path=str(png_path), full_page=True)
            logger.warning(
                "PCPAO search failed for %s (%s); debug saved html=%s screenshot=%s url=%s",
                parcel_id,
                public_parcel,
                html_path,
                png_path,
                page.url,
            )
        except Exception as e:
            logger.debug("Could not save PCPAO debug dump for %s: %s", parcel_id, e)

    def _is_pcpao_detail_page(self, page: Page) -> bool:
        try:
            return page.locator(_PCPAO_DETAIL_READY).count() > 0
        except Exception:
            return False

    def _download_trim_from_page(self, page: Page, parcel_id: str) -> Optional[Path]:
        trim_locator = page.locator("a:has-text('TRIM Notice'), a[href*='trimNotice']").first
        try:
            trim_locator.wait_for(state="visible", timeout=4000)
        except PlaywrightTimeout:
            logger.debug("PCPAO TRIM link not visible for %s", parcel_id)
            return None

        href = trim_locator.get_attribute("href")
        if not href:
            return None

        safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", parcel_id)
        dest_path = self._trim_dir / f"trim_{safe_id}.pdf"
        url = urljoin(_PCPAO_BASE_URL, href)

        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": STEALTH_UA})
            if resp.status_code == 200 and len(resp.content) > 1024:
                dest_path.write_bytes(resp.content)
                logger.debug("PCPAO TRIM saved: %s (%d bytes)", dest_path, len(resp.content))
                return dest_path
            logger.info("PCPAO TRIM empty/error for %s: HTTP %d", parcel_id, resp.status_code)
            return None
        except Exception as e:
            logger.info("PCPAO TRIM download failed for %s: %s", parcel_id, e)
            return None

