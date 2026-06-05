"""
Court Docket PDF Downloader — Phase 1 of OCR v2 pipeline.

Hillsborough (ORI portal — plain HTTP, no browser needed):
  1. POST /Public/ORIUtilities/AppLogin/api/Session  (anonymous session)
  2. POST /oripublicaccess/api/CustomQuery/KeywordSearch  → document token
  3. GET  /Public/ORIUtilities/OverlayWatermark/api/Watermark/<token> → PDF
  Flow verified live 2026-06-03 (instrument 2026089313, 104KB PDF).

Pinellas (Cloudflare-protected AcclaimWeb portal — requires warmed Edge profile):
  1. Browser navigates to /search/SearchTypeInstrumentNumber
  2. Accepts disclaimer (POST /search/Disclaimer), fills form, POSTs search
  3. Clicks result row → AcclaimWeb Details popup → extract LoadImageInTab doc ID
  4. page.evaluate(fetch) downloads /Image/DocumentPdf/{doc_id} inside browser
  Flow verified live 2026-06-03 (instrument 2026148296, 73KB PDF).

Per ADR 0001: pdf_url stores the canonical instrument-search URL.
Local file deleted after successful extraction, kept on failure.

Usage (standalone):
    python -m src.scrappers.liens.docket_pdf_downloader \
        --county hillsborough --instrument 2026089313 --output-dir data/ocr_v2_pdfs
    python -m src.scrappers.liens.docket_pdf_downloader \
        --county pinellas --instrument 2026148296 --output-dir data/ocr_v2_pdfs
"""

import logging
import time
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

# ── County-specific ORI portal config ────────────────────────────────────────
COUNTY_PDF_CONFIG: dict = {
    "hillsborough": {
        "base": "https://publicaccess.hillsclerk.com",
        "session_path": "/Public/ORIUtilities/AppLogin/api/Session",
        "search_path": "/oripublicaccess/api/CustomQuery/KeywordSearch",
        "watermark_path": "/Public/ORIUtilities/OverlayWatermark/api/Watermark",
        "referer": "https://publicaccess.hillsclerk.com/oripublicaccess/",
        "instrument_query_id": 320,   # "ORI-Instrument #"
        "instrument_keyword_id": 1006,  # "Instrument #" field
        "parcel_regex": r"\b\d{2}-\d{2}-\d{2}-\d{4}-\d{4,5}-\d{4}\b",
    },
    "pinellas": {
        # Pinellas runs the same ORI portal product; query/keyword IDs need
        # confirmation on first run (use scripts/probe_ori_portal.py pattern).
        "base": "https://officialrecords.mypinellasclerk.gov",
        "session_path": "/Public/ORIUtilities/AppLogin/api/Session",
        "search_path": "/oripublicaccess/api/CustomQuery/KeywordSearch",
        "watermark_path": "/Public/ORIUtilities/OverlayWatermark/api/Watermark",
        "referer": "https://officialrecords.mypinellasclerk.gov/oripublicaccess/",
        "instrument_query_id": 320,
        "instrument_keyword_id": 1006,
        "parcel_regex": r"\b\d{2}/\d{2}/\d{2}/\d{5}/\d{3}/\d{4}\b",
    },
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Accept": "application/json, */*",
}

MAX_RETRIES = 3
RETRY_DELAY_SECS = 5
TIMEOUT_SECS = 60


class DownloadError(Exception):
    pass


# Module-level session cache per county — reuse cookies across many downloads
_sessions: dict = {}


def _get_session(county_id: str) -> requests.Session:
    """Get (or create) an authenticated portal session for a county."""
    if county_id in _sessions:
        return _sessions[county_id]

    cfg = COUNTY_PDF_CONFIG.get(county_id)
    if not cfg:
        raise ValueError(f"Unknown county_id: {county_id!r}")

    s = requests.Session()
    s.headers.update(_HEADERS)
    s.headers["Referer"] = cfg["referer"]
    s.headers["Origin"] = cfg["base"]

    # Anonymous session — sets the portal cookies
    resp = s.post(cfg["base"] + cfg["session_path"], json={}, timeout=TIMEOUT_SECS)
    resp.raise_for_status()
    logger.debug("Portal session established for %s", county_id)

    _sessions[county_id] = s
    return s


def reset_session(county_id: str) -> None:
    """Drop the cached session (e.g. after auth errors) so the next call re-creates it."""
    _sessions.pop(county_id, None)


_PINELLAS_CANONICAL_URL = (
    "https://officialrecords.mypinellasclerk.gov/search/SearchTypeInstrumentNumber"
    "?InstrumentNumber={instrument}"
)


def get_pdf_url(county_id: str, instrument_number: str) -> str:
    """Canonical permanent URL for this document: the portal search-by-instrument link."""
    if county_id == "pinellas":
        return _PINELLAS_CANONICAL_URL.format(instrument=instrument_number)
    cfg = COUNTY_PDF_CONFIG[county_id]
    return f"{cfg['referer']}?instrument={instrument_number}"


def _search_document_token(
    session: requests.Session, cfg: dict, instrument_number: str
) -> str:
    """Search the portal by instrument number; return the document token (Data[0].ID)."""
    payload = {
        "QueryID": cfg["instrument_query_id"],
        "Keywords": [{
            "ID": cfg["instrument_keyword_id"],
            "Value": str(instrument_number),
            "KeywordOperator": "=",
        }],
    }
    resp = session.post(
        cfg["base"] + cfg["search_path"], json=payload, timeout=TIMEOUT_SECS
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("Data") or []
    if not results:
        raise DownloadError(
            f"No portal results for instrument {instrument_number} "
            f"(response keys: {list(data.keys())})"
        )
    # Multiple rows are returned (one per party); they share the same document.
    doc_id = results[0].get("ID")
    if not doc_id:
        raise DownloadError(f"Result missing document ID for {instrument_number}")
    return doc_id


def download_pdf(
    county_id: str,
    instrument_number: str,
    output_dir: Path,
    session: Optional[requests.Session] = None,
) -> Tuple[str, Path]:
    """Download a single docket PDF by instrument number.

    Returns:
        (pdf_url, local_path) — pdf_url is the permanent portal search URL;
        local_path is where the PDF was saved.

    Raises:
        DownloadError on all retries exhausted, no results, or non-PDF content.
    """
    cfg = COUNTY_PDF_CONFIG.get(county_id)
    if not cfg:
        raise ValueError(f"Unknown county_id: {county_id!r}")

    pdf_url = get_pdf_url(county_id, instrument_number)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    local_path = output_dir / f"{county_id}_{instrument_number}.pdf"

    # Resume support — skip if already downloaded
    if local_path.exists() and local_path.stat().st_size > 1024:
        logger.debug("PDF already exists: %s", local_path)
        return pdf_url, local_path

    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            http = session or _get_session(county_id)

            # Step 1: search → document token
            doc_token = _search_document_token(http, cfg, instrument_number)

            # Step 2: watermark API → PDF bytes
            wm_url = (
                cfg["base"] + cfg["watermark_path"] + "/" + quote(doc_token, safe="")
            )
            resp = http.get(wm_url, timeout=TIMEOUT_SECS)
            resp.raise_for_status()

            if not resp.content.startswith(b"%PDF"):
                raise DownloadError(
                    f"Response is not a PDF (content-type="
                    f"{resp.headers.get('Content-Type')!r}, "
                    f"first bytes={resp.content[:20]!r})"
                )

            local_path.write_bytes(resp.content)
            logger.info(
                "Downloaded PDF %s/%s → %s (%d bytes)",
                county_id, instrument_number, local_path.name, len(resp.content),
            )
            return pdf_url, local_path

        except DownloadError as exc:
            # No-results and not-a-PDF are not transient — don't retry
            raise
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Download attempt %d/%d failed for %s/%s: %s",
                attempt, MAX_RETRIES, county_id, instrument_number, exc,
            )
            reset_session(county_id)  # session may have expired
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECS * attempt)

    raise DownloadError(
        f"All {MAX_RETRIES} download attempts failed for "
        f"{county_id}/{instrument_number}: {last_exc}"
    )


def cleanup_pdf(local_path: Path) -> None:
    """Delete local PDF after successful extraction (PDFs are transient per ADR)."""
    try:
        if local_path.exists():
            local_path.unlink()
            logger.debug("Deleted local PDF: %s", local_path)
    except Exception as exc:
        logger.warning("Could not delete PDF %s: %s", local_path, exc)


def get_county_parcel_regex(county_id: str) -> str:
    """Return the parcel ID regex pattern for a county."""
    cfg = COUNTY_PDF_CONFIG.get(county_id, {})
    return cfg.get("parcel_regex", r"\b\d{2}-\d{2}-\d{2}-\d{4}-\d{4,5}-\d{4}\b")


# ── Pinellas: Playwright-based downloader (CF-protected) ─────────────────────

_PINELLAS_SEARCH = "https://officialrecords.mypinellasclerk.gov/search/SearchTypeInstrumentNumber"
_PINELLAS_BASE = "https://officialrecords.mypinellasclerk.gov"


async def _download_pdf_pinellas_async(
    instrument_number: str, output_dir: Path
) -> Tuple[str, Path]:
    """Pinellas PDF download using warmed CF-bypass Edge profile.

    Flow (verified 2026-06-03):
      1. Browser POSTs instrument search (handles CF + disclaimer automatically)
      2. Clicks result row → AcclaimWeb Details popup
      3. Extracts internal doc_id from LoadImageInTab JS call
      4. page.evaluate(fetch) downloads /Image/DocumentPdf/{doc_id} inside browser

    Returns (pdf_url, local_path).
    Raises DownloadError on any failure.
    """
    import re as _re
    import base64 as _base64
    import asyncio as _asyncio
    from src.utils.cf_persistent_browser import _launch_edge

    pdf_url = get_pdf_url("pinellas", instrument_number)
    local_path = output_dir / f"pinellas_{instrument_number}.pdf"

    if local_path.exists() and local_path.stat().st_size > 1024:
        logger.debug("PDF already exists: %s", local_path)
        return pdf_url, local_path

    output_dir.mkdir(parents=True, exist_ok=True)

    async with _launch_edge(profile_name="pinellas_clerk", headless=False) as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # Step 1: navigate + accept disclaimer if shown.
        # Portal may redirect to /search/Disclaimer?st=... — wait for either the
        # search form or the disclaimer accept button, then click through.
        await page.goto(_PINELLAS_SEARCH, wait_until="domcontentloaded", timeout=30000)
        try:
            await page.locator("#InstrumentNumber, #btnButton").first.wait_for(
                state="visible", timeout=20000
            )
        except Exception as exc:
            raise DownloadError(f"Pinellas portal did not render (CF?): {exc}") from exc

        if "Disclaimer" in page.url or await page.locator("#btnButton").count():
            try:
                if await page.locator("#btnButton").is_visible():
                    await page.locator("#btnButton").click()
                    await page.wait_for_selector("#InstrumentNumber", timeout=20000)
            except Exception as exc:
                raise DownloadError(f"Pinellas disclaimer click failed: {exc}") from exc

        # Step 2: fill instrument number + search
        try:
            await page.fill("#InstrumentNumber", str(instrument_number), timeout=15000)
        except Exception as exc:
            raise DownloadError(f"Pinellas search form unavailable: {exc}") from exc
        await page.click("#btnSearch")
        await _asyncio.sleep(5)

        # Step 3: find result row
        row_locator = page.locator(f"tr:has-text('{instrument_number}')").first
        count = await page.locator(f"tr:has-text('{instrument_number}')").count()
        if count == 0:
            raise DownloadError(
                f"No search results for instrument {instrument_number} on Pinellas portal"
            )

        # Step 4: click row → Details popup opens
        try:
            async with ctx.expect_page(timeout=10000) as new_page_info:
                await row_locator.click()
            details_page = await new_page_info.value
        except Exception as exc:
            raise DownloadError(f"Details popup did not open: {exc}") from exc

        await details_page.wait_for_load_state("domcontentloaded", timeout=20000)
        await _asyncio.sleep(2)

        # Step 5: extract internal doc_id from LoadImageInTab JS call
        html = await details_page.content()
        doc_id_match = _re.search(r"LoadImageInTab\s*\([^)]*,\s*(\d+)", html)
        if not doc_id_match:
            raise DownloadError(
                f"Could not extract doc_id from Details page for instrument {instrument_number}"
            )
        doc_id = doc_id_match.group(1)
        logger.debug("Pinellas doc_id=%s for instrument %s", doc_id, instrument_number)

        # Step 6: download PDF via browser fetch() (stays inside CF session)
        doc_pdf_url = f"{_PINELLAS_BASE}/Image/DocumentPdf/{doc_id}"
        result = await details_page.evaluate(
            """
            async (url) => {
                const resp = await fetch(url);
                const buf = await resp.arrayBuffer();
                const arr = new Uint8Array(buf);
                let binary = '';
                for (let i = 0; i < arr.length; i++) binary += String.fromCharCode(arr[i]);
                return { status: resp.status, ct: resp.headers.get('content-type') || '', b64: btoa(binary) };
            }
            """,
            doc_pdf_url,
        )

        if result["status"] != 200:
            raise DownloadError(
                f"Pinellas DocumentPdf returned HTTP {result['status']} for doc_id={doc_id}"
            )

        pdf_bytes = _base64.b64decode(result["b64"])
        if not pdf_bytes.startswith(b"%PDF"):
            raise DownloadError(
                f"Response is not a PDF (first bytes={pdf_bytes[:20]!r}) for instrument {instrument_number}"
            )

        local_path.write_bytes(pdf_bytes)
        logger.info(
            "Downloaded PDF pinellas/%s → %s (%d bytes)",
            instrument_number, local_path.name, len(pdf_bytes),
        )
        return pdf_url, local_path


async def get_pinellas_case_number_async(instrument_number: str) -> Tuple[Optional[str], Optional[str]]:
    """Read the court CASENUMBER (and Grantor) from the Pinellas Official Records
    Details popup — without OCR.

    The Details popup exposes CASENUMBER as a structured field for every record
    type, so this works even when the document IMAGE is restricted/sealed
    (probate, divorce — FL §28.2221(5)(a)) or returns only an eCertify cover.
    Returns (casenumber_raw, grantor); either may be None. The raw value is
    undashed (e.g. "26001007ES") — feed it to the Pinellas court_scraper, whose
    decompose_ucn accepts the undashed form.
    """
    import re as _re
    import asyncio as _asyncio
    from src.utils.cf_persistent_browser import _launch_edge

    async with _launch_edge(profile_name="pinellas_clerk", headless=False) as ctx:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto(_PINELLAS_SEARCH, wait_until="domcontentloaded", timeout=30000)
        try:
            await page.locator("#InstrumentNumber, #btnButton").first.wait_for(state="visible", timeout=20000)
        except Exception as exc:
            raise DownloadError(f"Pinellas portal did not render (CF?): {exc}") from exc
        if "Disclaimer" in page.url or await page.locator("#btnButton").count():
            try:
                if await page.locator("#btnButton").is_visible():
                    await page.locator("#btnButton").click()
                    await page.wait_for_selector("#InstrumentNumber", timeout=20000)
            except Exception as exc:
                raise DownloadError(f"Pinellas disclaimer click failed: {exc}") from exc
        await page.fill("#InstrumentNumber", str(instrument_number), timeout=15000)
        await page.click("#btnSearch")
        await _asyncio.sleep(5)
        if await page.locator(f"tr:has-text('{instrument_number}')").count() == 0:
            raise DownloadError(f"No search results for instrument {instrument_number}")
        try:
            async with ctx.expect_page(timeout=10000) as new_page_info:
                await page.locator(f"tr:has-text('{instrument_number}')").first.click()
            details = await new_page_info.value
        except Exception as exc:
            raise DownloadError(f"Details popup did not open: {exc}") from exc
        await details.wait_for_load_state("domcontentloaded", timeout=20000)
        await _asyncio.sleep(2)
        txt = await details.locator("body").inner_text()
        cm = _re.search(r"CASE\s*NUMBER[:\s]*([A-Za-z0-9\-]+)", txt, _re.I)
        gm = _re.search(r"Grantor[:\s]*(.+)", txt, _re.I)
        case_number = cm.group(1).strip() if cm else None
        grantor = gm.group(1).split("\n", 1)[0].strip() if gm else None
        logger.info("Pinellas Details %s -> CASENUMBER=%s grantor=%s",
                    instrument_number, case_number, grantor)
        return case_number, grantor


def get_pinellas_case_number(instrument_number: str) -> Tuple[Optional[str], Optional[str]]:
    """Sync wrapper for get_pinellas_case_number_async."""
    import asyncio as _asyncio
    try:
        return _asyncio.run(get_pinellas_case_number_async(instrument_number))
    except DownloadError:
        raise
    except Exception as exc:
        raise DownloadError(f"Pinellas CASENUMBER lookup failed: {exc}") from exc


def download_pdf_pinellas(instrument_number: str, output_dir: Path) -> Tuple[str, Path]:
    """Sync wrapper for the async Pinellas downloader.

    Runs the async download in a new event loop (safe to call from sync context).
    Raises DownloadError on failure.
    """
    import asyncio as _asyncio
    try:
        return _asyncio.run(
            _download_pdf_pinellas_async(instrument_number, output_dir)
        )
    except DownloadError:
        raise
    except Exception as exc:
        raise DownloadError(f"Pinellas download failed: {exc}") from exc


# ── Unified download_pdf (routing by county) ─────────────────────────────────

_ORIG_DOWNLOAD_PDF = download_pdf  # keep reference to Hillsborough-only version


def download_pdf(  # type: ignore[no-redef]
    county_id: str,
    instrument_number: str,
    output_dir: Path,
    session: Optional[requests.Session] = None,
) -> Tuple[str, Path]:
    """Download a docket PDF for any supported county.

    Routes to the correct backend:
      - hillsborough: ORI portal (plain HTTP, fast)
      - pinellas: AcclaimWeb via Playwright CF-bypass (browser session required)
    """
    if county_id == "pinellas":
        return download_pdf_pinellas(instrument_number, Path(output_dir))
    return _ORIG_DOWNLOAD_PDF(county_id, instrument_number, output_dir, session)


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Download a single court docket PDF")
    parser.add_argument("--county", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--output-dir", default="data/ocr_v2_pdfs")
    args = parser.parse_args()

    try:
        url, path = download_pdf(args.county, args.instrument, Path(args.output_dir))
        print(f"OK: {url} -> {path}")
    except DownloadError as e:
        print(f"FAILED: {e}", file=sys.stderr)
        sys.exit(1)
