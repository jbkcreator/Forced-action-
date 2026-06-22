"""
Pinellas Code Violations Scraper — Multi-source

Sources:
  1. Pinellas County Accela  (aca-prod.accela.com/PINELLAS)
     Unincorporated Pinellas + Largo.  Date-range search via ASP.NET
     WebForms VIEWSTATE postback.  10 rows/page, server-side pagination.

  2. St. Petersburg Click2Gov  (stpe-egov.aspgov.com/Click2GovCE)
     City of St. Pete — largest municipality in Pinellas.  No date-range
     search; records are fetched via address sweep using property addresses
     already in the DB.  BIG-IP WAF requires Playwright to seed session
     cookies; subsequent requests use requests.Session.

  3. Clearwater Accela  (aca-prod.accela.com/CLEARWATER)
     City of Clearwater.  No date-range search; sweeps by street name using
     Clearwater property addresses already in the DB.  Record types:
     CDC{YYYY}-{NNNNN} (Community Development Code) and
     PNU{YYYY}-{NNNNN} (Public Nuisance).

  4. Tarpon Springs Click2Gov  (tarp-egov.aspgov.com/Click2GovCE)
     Same pattern as St. Pete Click2Gov.

Run cadence:
  --sources accela          daily      (date-range; fast)
  --sources stpete tarpon   weekly     (full address sweep; dedup skips seen)
  --sources clearwater      weekly     (street-name sweep; same dedup)

Entry point:
    scrape_pinellas_violations(county_id, date_range, sources, load_to_db)

CLI:
    python -m src.scrappers.violation.pinellas_violations_engine
    python -m src.scrappers.violation.pinellas_violations_engine --sources accela --start-date 2026-06-01 --end-date 2026-06-09
    python -m src.scrappers.violation.pinellas_violations_engine --sources stpete tarpon
    python -m src.scrappers.violation.pinellas_violations_engine --sources clearwater
"""

import logging
import re
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from lxml import html as lxhtml

from src.core.database import get_db_context
from src.loaders.violations import ViolationLoader
from src.utils.county_config import get_county
from src.utils.logger import setup_logging, get_logger
from src.utils.scraper_db_helper import record_scraper_stats
from sqlalchemy import text as sa_text

setup_logging()
logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ACCELA_PINELLAS_URL = (
    "https://aca-prod.accela.com/PINELLAS/Cap/CapHome.aspx"
    "?module=Enforcement&TabName=Enforcement"
)
_ACCELA_CLEARWATER_URL = (
    "https://aca-prod.accela.com/CLEARWATER/Cap/CapHome.aspx"
    "?module=CodeCompliance&TabName=CodeCompliance"
)
_STPETE_BASE = "https://stpe-egov.aspgov.com/Click2GovCE/"
_TARPON_BASE  = "https://tarp-egov.aspgov.com/Click2GovCE/"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_ACCELA_PREFIX   = "ctl00$PlaceHolderMain$generalSearchForm$"
_REQUEST_DELAY   = 1.5    # seconds between HTTP requests
_MAX_PAGES       = 200    # hard cap per Accela search
_SWEEP_LOOKBACK_YEARS = 3  # address-sweep sources only load records within this window


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"})
    return s


def _hidden(doc: lxhtml.HtmlElement, name: str) -> str:
    vals = doc.xpath(f'//input[@name="{name}"]/@value')
    return vals[0] if vals else ""


def _is_cloudflare_challenge(text: str) -> bool:
    return any(sig in text for sig in ("cf-browser-verification", "cf_chl_opt", "Just a moment"))


# ---------------------------------------------------------------------------
# Source 1 — Pinellas County Accela (date-range search)
# ---------------------------------------------------------------------------

def _accela_post(
    session: requests.Session,
    url: str,
    event_target: str,
    viewstate: str,
    viewstate_gen: str,
    start_str: str,
    end_str: str,
) -> Optional[lxhtml.HtmlElement]:
    """POST a search or paginate request to an Accela CapHome page."""
    payload = {
        "__EVENTTARGET":          event_target,
        "__EVENTARGUMENT":        "",
        "__VIEWSTATE":            viewstate,
        "__VIEWSTATEGENERATOR":   viewstate_gen,
        "__VIEWSTATEENCRYPTED":   "",
        _ACCELA_PREFIX + "txtGSPermitNumber": "",
        _ACCELA_PREFIX + "txtGSStreetName":   "",
        _ACCELA_PREFIX + "txtGSCity":         "",
        _ACCELA_PREFIX + "txtGSParcelNo":     "",
        _ACCELA_PREFIX + "ddlGSDirection":    "",
        _ACCELA_PREFIX + "ddlGSStreetSuffix": "",
        _ACCELA_PREFIX + "txtGSStartDate":    start_str,
        _ACCELA_PREFIX + "txtGSEndDate":      end_str,
    }
    resp = session.post(
        url, data=payload,
        headers={"Referer": url, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if not resp.ok:
        logger.error("[Accela] HTTP %d", resp.status_code)
        return None
    if _is_cloudflare_challenge(resp.text):
        logger.warning("[Accela] Cloudflare challenge — backing off 30 s")
        time.sleep(30)
        return None
    return lxhtml.fromstring(resp.text)


def _parse_accela_grid(doc: lxhtml.HtmlElement) -> list[dict]:
    """Pinellas Accela grid: date|type|number|status|...|address (7+ cols)."""
    rows = []
    for tr in doc.cssselect("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even"):
        tds = [" ".join(td.text_content().split()) for td in tr.cssselect("td")]
        if len(tds) < 7:
            continue
        rows.append({
            "Date":          tds[1],
            "Record Type":   tds[2],
            "Record Number": tds[3],
            "Status":        tds[4],
            "Address":       tds[6],
            "Description":   "",
            "Fine Amount":   "",
            "Is Lien":       "",
        })
    return rows


def _parse_clearwater_grid(doc: lxhtml.HtmlElement) -> list[dict]:
    """Clearwater Accela grid: date|number|type|desc|..|status|..|..|address (10 cols)."""
    rows = []
    for tr in doc.cssselect("tr.ACA_TabRow_Odd, tr.ACA_TabRow_Even"):
        tds = [" ".join(td.text_content().split()) for td in tr.cssselect("td")]
        if len(tds) < 10:
            continue
        rows.append({
            "Date":          tds[1],
            "Record Number": tds[2],
            "Record Type":   tds[3],
            "Description":   tds[4],
            "Status":        tds[6],
            "Address":       tds[9],
            "Fine Amount":   "",
            "Is Lien":       "",
        })
    return rows


def _next_page_event_target(doc: lxhtml.HtmlElement) -> Optional[str]:
    """
    Return the __EVENTTARGET string for the next page link, or None if
    we are on the last page.  Parses the __doPostBack href from the pager
    rather than computing the target index (which shifts with grid layout).
    """
    selected = doc.cssselect("span.SelectedPageButton")
    if not selected:
        return None
    try:
        current_page = int(selected[0].text_content().strip())
    except ValueError:
        return None

    for a in doc.cssselect("table.aca_pagination a"):
        label = a.text_content().strip()
        # Numeric next-page link
        try:
            if int(label) == current_page + 1:
                m = re.search(r"__doPostBack\('([^']+)'", a.get("href", ""))
                if m:
                    return m.group(1)
        except ValueError:
            pass
        # "Next" text link
        if "next" in label.lower():
            m = re.search(r"__doPostBack\('([^']+)'", a.get("href", ""))
            if m:
                return m.group(1)
    return None


def scrape_accela(
    accela_url: str,
    start_date: date,
    end_date: date,
    label: str = "Accela",
) -> pd.DataFrame:
    start_str = start_date.strftime("%m/%d/%Y")
    end_str   = end_date.strftime("%m/%d/%Y")
    logger.info("[%s] Date range: %s to %s", label, start_str, end_str)

    session = _new_session()
    try:
        resp = session.get(accela_url, timeout=30)
        if _is_cloudflare_challenge(resp.text):
            logger.warning("[%s] Cloudflare on initial GET", label)
            return pd.DataFrame()
        doc = lxhtml.fromstring(resp.text)
    except Exception as exc:
        logger.error("[%s] Initial GET failed: %s", label, exc)
        return pd.DataFrame()

    vs  = _hidden(doc, "__VIEWSTATE")
    vsg = _hidden(doc, "__VIEWSTATEGENERATOR")

    all_rows: list[dict] = []

    for page in range(1, _MAX_PAGES + 1):
        time.sleep(_REQUEST_DELAY)
        event = "ctl00$PlaceHolderMain$btnNewSearch" if page == 1 else _next_page_event_target(doc)
        if event is None:
            break

        doc = _accela_post(session, accela_url, event, vs, vsg, start_str, end_str)
        if doc is None:
            break

        vs  = _hidden(doc, "__VIEWSTATE")
        vsg = _hidden(doc, "__VIEWSTATEGENERATOR")

        page_rows = _parse_accela_grid(doc)
        if not page_rows:
            break
        all_rows.extend(page_rows)
        logger.info("[%s] Page %d: %d rows (total %d)", label, page, len(page_rows), len(all_rows))

        if _next_page_event_target(doc) is None:
            break

    logger.info("[%s] Extracted %d records", label, len(all_rows))
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Source 3 — Clearwater Accela (street-name sweep)
# ---------------------------------------------------------------------------
# Record types confirmed: CDC{YYYY}-{NNNNN} (Community Development Code)
#                         PNU{YYYY}-{NNNNN} (Public Nuisance)
# No date-range filter available; sweep all Clearwater streets from DB.
# ---------------------------------------------------------------------------

def _clearwater_street_names(limit: int = 400) -> list[str]:
    """Distinct street names from Pinellas properties with city=Clearwater."""
    try:
        with get_db_context() as session:
            rows = session.execute(sa_text("""
                SELECT DISTINCT
                    regexp_replace(
                        regexp_replace(upper(trim(address)), E'^[\\\\d/]+\\\\s+', ''),
                        E'\\\\s+(ST|AVE|BLVD|DR|RD|LN|WAY|CT|CIR|PL|PKWY|HWY|TER|TRL|LOOP)(\\\\s.*)?$', ''
                    ) AS street_name
                FROM properties
                WHERE county_id = 'pinellas'
                  AND upper(city) = 'CLEARWATER'
                  AND address IS NOT NULL
                  AND length(trim(address)) > 5
                ORDER BY 1
                LIMIT :lim
            """), {"lim": limit}).fetchall()
        streets = [r[0] for r in rows if r[0] and len(r[0].strip()) > 2 and not r[0].startswith("1/2")]
        logger.info("[Clearwater Accela] %d Clearwater street names loaded", len(streets))
        return streets
    except Exception as exc:
        logger.error("[Clearwater Accela] Failed to load street names: %s", exc)
        return []


def scrape_clearwater_accela() -> pd.DataFrame:
    """
    Sweep Clearwater CodeCompliance by street name.
    Returns CDC (Community Development Code) and PNU (Public Nuisance) records.
    """
    url     = _ACCELA_CLEARWATER_URL
    session = _new_session()

    try:
        doc = lxhtml.fromstring(session.get(url, timeout=30).text)
    except Exception as exc:
        logger.error("[Clearwater Accela] Initial GET failed: %s", exc)
        return pd.DataFrame()

    if _is_cloudflare_challenge(doc.text_content()):
        logger.warning("[Clearwater Accela] Cloudflare challenge on initial GET")
        return pd.DataFrame()

    vs  = _hidden(doc, "__VIEWSTATE")
    vsg = _hidden(doc, "__VIEWSTATEGENERATOR")

    streets  = _clearwater_street_names()
    if not streets:
        logger.warning("[Clearwater Accela] No Clearwater streets found in DB — skipping")
        return pd.DataFrame()

    all_rows: list[dict] = []
    seen:     set[str]   = set()

    for idx, street in enumerate(streets, 1):
        time.sleep(_REQUEST_DELAY)

        payload = {
            "__EVENTTARGET":          "ctl00$PlaceHolderMain$btnNewSearch",
            "__EVENTARGUMENT":        "",
            "__VIEWSTATE":            vs,
            "__VIEWSTATEGENERATOR":   vsg,
            "__VIEWSTATEENCRYPTED":   "",
            _ACCELA_PREFIX + "txtGSStreetName": street,
            _ACCELA_PREFIX + "txtGSPermitNumber": "",
        }
        try:
            resp = session.post(
                url, data=payload,
                headers={"Referer": url, "Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            doc = lxhtml.fromstring(resp.text)
            vs  = _hidden(doc, "__VIEWSTATE") or vs
            vsg = _hidden(doc, "__VIEWSTATEGENERATOR") or vsg
        except Exception as exc:
            logger.warning("[Clearwater Accela] street=%r failed: %s", street, exc)
            continue

        # Paginate within each street result set
        for page in range(1, _MAX_PAGES + 1):
            page_rows = _parse_clearwater_grid(doc)
            for row in page_rows:
                rec_num = row.get("Record Number", "")
                if rec_num and rec_num not in seen:
                    seen.add(rec_num)
                    all_rows.append(row)

            next_evt = _next_page_event_target(doc)
            if not next_evt or page >= _MAX_PAGES:
                break

            time.sleep(_REQUEST_DELAY)
            try:
                resp = session.post(
                    url,
                    data={
                        "__EVENTTARGET":        next_evt,
                        "__EVENTARGUMENT":      "",
                        "__VIEWSTATE":          vs,
                        "__VIEWSTATEGENERATOR": vsg,
                        "__VIEWSTATEENCRYPTED": "",
                    },
                    headers={"Referer": url, "Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30,
                )
                doc = lxhtml.fromstring(resp.text)
                vs  = _hidden(doc, "__VIEWSTATE") or vs
                vsg = _hidden(doc, "__VIEWSTATEGENERATOR") or vsg
            except Exception as exc:
                logger.warning("[Clearwater Accela] pagination failed street=%r page=%d: %s", street, page, exc)
                break

        if idx % 50 == 0:
            logger.info("[Clearwater Accela] Progress: %d/%d streets, %d records so far", idx, len(streets), len(all_rows))

    logger.info("[Clearwater Accela] Sweep complete: %d streets, %d unique records (unfiltered)", len(streets), len(all_rows))
    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    cutoff = (datetime.now().replace(tzinfo=None) - pd.DateOffset(years=_SWEEP_LOOKBACK_YEARS)).date()
    df["_parsed_date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce").dt.date
    df = df[df["_parsed_date"] >= cutoff].drop(columns=["_parsed_date"])
    logger.info("[Clearwater Accela] After %d-year filter: %d records", _SWEEP_LOOKBACK_YEARS, len(df))
    return df


# ---------------------------------------------------------------------------
# Sources 2 & 4 — Click2Gov (St. Pete + Tarpon Springs)
# ---------------------------------------------------------------------------

def _seed_waf_cookies(base_url: str, label: str) -> dict:
    """
    Launch a headless Playwright browser to load the Click2Gov casesearch page
    and harvest BIG-IP WAF + session cookies.  Returns {} if Playwright is not
    available or the page fails to load.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("[%s] Playwright not installed — WAF seeding skipped", label)
        return {}

    cookies: dict = {}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
            ctx     = browser.new_context(user_agent=_UA)
            page    = ctx.new_page()
            page.goto(base_url + "casesearch.html", timeout=30_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
            for c in ctx.cookies():
                cookies[c["name"]] = c["value"]
            browser.close()
        logger.info("[%s] Seeded %d cookies via Playwright", label, len(cookies))
    except Exception as exc:
        logger.warning("[%s] Playwright WAF seeding failed: %s", label, exc)

    return cookies


def _c2g_csrf(session: requests.Session, base_url: str, label: str) -> Optional[str]:
    try:
        resp = session.get(
            base_url + "casesearch.html", timeout=20,
            headers={"Referer": base_url},
        )
        m = re.search(r'name=["\']OWASP_CSRFTOKEN["\'][^>]*value=["\']([A-Z0-9\-]+)["\']', resp.text)
        if not m:
            m = re.search(r'OWASP_CSRFTOKEN["\s]+value[=\s"\']+([A-Z0-9\-]+)', resp.text)
        return m.group(1) if m else None
    except Exception as exc:
        logger.error("[%s] CSRF token fetch failed: %s", label, exc)
        return None


def _c2g_address_search(
    session: requests.Session,
    base_url: str,
    street_name: str,
    token: str,
    label: str,
) -> list[dict]:
    """
    POST addressownersearch.html with a street name.
    Returns list of {address, location_id, owner, parcel}.
    """
    locations: list[dict] = []
    paging_mode = "F"

    while True:
        try:
            resp = session.post(
                base_url + "addressownersearch.html",
                data={
                    "searchType":        "3",
                    "paging.mode":       paging_mode,
                    "addressNumber":     "",
                    "addressDirection":  "",
                    "addressName":       street_name,
                    "addressSuffix":     "",
                    "sbmtBtn":           "Search",
                    "OWASP_CSRFTOKEN":   token,
                },
                headers={"Referer": base_url + "casesearch.html"},
                timeout=20,
            )
        except Exception as exc:
            logger.warning("[%s] Address search error (%s): %s", label, street_name, exc)
            break

        if resp.status_code == 503:
            logger.warning("[%s] 503 on address search — WAF blocking; skipping remaining streets", label)
            return []  # Signal caller to abort sweep

        doc = lxhtml.fromstring(resp.text)
        for tr in doc.cssselect("table.jTable tbody tr"):
            tds = tr.cssselect("td")
            if len(tds) < 3:
                continue
            a_tags = tds[0].cssselect("a")
            if not a_tags:
                continue
            href = a_tags[0].get("href", "")
            m = re.search(r"locationId=(\d+)", href)
            if not m:
                continue
            locations.append({
                "address":     a_tags[0].text_content().strip(),
                "location_id": m.group(1),
                "owner":       tds[1].text_content().strip(),
                "parcel":      tds[2].text_content().strip(),
            })

        if doc.cssselect("li.next.page-item a.page-link"):
            paging_mode = "N"
        else:
            break

    return locations


def _c2g_location_cases(
    session: requests.Session,
    base_url: str,
    location: dict,
    token: str,
    label: str,
) -> list[dict]:
    """
    GET locationsearch.html for a location and return all its cases.
    """
    cases: list[dict] = []
    paging_mode = "F"
    address = location["address"]

    while True:
        try:
            resp = session.get(
                base_url + "locationsearch.html",
                params={"locationId": location["location_id"], "paging.mode": paging_mode, "OWASP_CSRFTOKEN": token},
                headers={"Referer": base_url + "casesearch.html"},
                timeout=20,
            )
        except Exception as exc:
            logger.warning("[%s] Location %s fetch failed: %s", label, location["location_id"], exc)
            break

        doc = lxhtml.fromstring(resp.text)
        for tr in doc.cssselect("table.jTable tbody tr"):
            tds = tr.cssselect("td")
            if len(tds) < 4:
                continue
            cases.append({
                "Record Number": tds[0].text_content().strip(),
                "Record Type":   tds[1].text_content().strip(),
                "Status":        tds[2].text_content().strip(),
                "Date":          tds[3].text_content().strip(),
                "Address":       address,
                "Description":   "",
                "Fine Amount":   "",
                "Is Lien":       "",
            })

        if doc.cssselect("li.next.page-item a.page-link"):
            paging_mode = "N"
        else:
            break

    return cases


def scrape_click2gov(
    base_url: str,
    street_names: list[str],
    label: str = "Click2Gov",
) -> pd.DataFrame:
    """
    Sweep Click2Gov by address using the provided street name list.
    Dedup is handled downstream by ViolationLoader (skip_duplicates=True).
    """
    logger.info("[%s] Starting address sweep — %d streets", label, len(street_names))

    session = _new_session()
    cookies = _seed_waf_cookies(base_url, label)
    if cookies:
        session.cookies.update(cookies)

    token = _c2g_csrf(session, base_url, label)
    if not token:
        logger.error("[%s] No CSRF token — aborting", label)
        return pd.DataFrame()

    all_rows: list[dict] = []
    seen_records: set[str] = set()

    for street in street_names:
        time.sleep(_REQUEST_DELAY)
        locations = _c2g_address_search(session, base_url, street, token, label)
        if locations == []:
            # Empty list returned as WAF-abort signal
            break

        for loc in locations:
            time.sleep(_REQUEST_DELAY)
            for case in _c2g_location_cases(session, base_url, loc, token, label):
                rn = case["Record Number"]
                if rn and rn not in seen_records:
                    seen_records.add(rn)
                    all_rows.append(case)

        logger.debug("[%s] Street '%s': %d locations, %d total cases",
                     label, street, len(locations), len(all_rows))

    logger.info("[%s] Extracted %d records (unfiltered)", label, len(all_rows))
    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    cutoff = (datetime.now().replace(tzinfo=None) - pd.DateOffset(years=_SWEEP_LOOKBACK_YEARS)).date()
    df["_parsed_date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce").dt.date
    df = df[df["_parsed_date"] >= cutoff].drop(columns=["_parsed_date"])
    logger.info("[%s] After %d-year filter: %d records", label, _SWEEP_LOOKBACK_YEARS, len(df))
    return df


# ---------------------------------------------------------------------------
# Address sweep — pull distinct street names from DB
# ---------------------------------------------------------------------------

def _pinellas_street_names(limit: int = 500) -> list[str]:
    """Extract distinct street names from existing Pinellas properties."""
    try:
        with get_db_context() as session:
            rows = session.execute(sa_text("""
                SELECT DISTINCT
                    regexp_replace(
                        regexp_replace(upper(trim(address)), E'^\\\\d+\\\\s+', ''),
                        E'\\\\s+(ST|AVE|BLVD|DR|RD|LN|WAY|CT|CIR|PL|PKWY|HWY|TER|TRL|LOOP)(\\\\s.*)?$', ''
                    ) AS street_name
                FROM properties
                WHERE county_id = 'pinellas'
                  AND address IS NOT NULL
                  AND length(trim(address)) > 5
                ORDER BY 1
                LIMIT :lim
            """), {"lim": limit}).fetchall()
        streets = [r[0] for r in rows if r[0] and len(r[0].strip()) > 2]
        logger.info("[Streets] %d distinct street names loaded for sweep", len(streets))
        return streets
    except Exception as exc:
        logger.error("[Streets] Failed to load street names: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def scrape_pinellas_violations(
    county_id: str = "pinellas",
    date_range: Optional[tuple[date, date]] = None,
    sources: Optional[list[str]] = None,
    load_to_db: bool = True,
) -> dict:
    """
    Scrape Pinellas code violations from configured sources and load to DB.

    Args:
        county_id:   Must be 'pinellas'.
        date_range:  (start, end) for date-range sources (Accela Pinellas).
                     Defaults to last 7 days.
        sources:     Subset of ['accela', 'stpete', 'clearwater', 'tarpon'].
                     Defaults to ['accela', 'stpete'].
        load_to_db:  Write matched records to DB.

    Returns:
        Per-source stats dict.
    """
    try:
        get_county(county_id)
    except KeyError:
        logger.error("[pinellas_violations] Unknown county_id: %s", county_id)
        return {"error": f"Unknown county_id: {county_id}"}

    if sources is None:
        sources = ["accela", "stpete"]

    if date_range is None:
        end_date   = date.today()
        start_date = end_date - timedelta(days=7)
    else:
        start_date, end_date = date_range

    stats: dict[str, dict] = {s: {"scraped": 0, "matched": 0, "unmatched": 0, "skipped": 0}
                               for s in ("accela", "stpete", "clearwater", "tarpon")}
    frames: list[tuple[str, pd.DataFrame]] = []

    if "accela" in sources:
        df = scrape_accela(_ACCELA_PINELLAS_URL, start_date, end_date, "Pinellas Accela")
        if not df.empty:
            stats["accela"]["scraped"] = len(df)
            frames.append(("accela", df))

    if "stpete" in sources:
        streets = _pinellas_street_names(limit=500)
        df = scrape_click2gov(_STPETE_BASE, streets, "St. Pete Click2Gov")
        if not df.empty:
            stats["stpete"]["scraped"] = len(df)
            frames.append(("stpete", df))

    if "clearwater" in sources:
        df = scrape_clearwater_accela()
        if not df.empty:
            stats["clearwater"]["scraped"] = len(df)
            frames.append(("clearwater", df))

    if "tarpon" in sources:
        streets = _pinellas_street_names(limit=200)
        df = scrape_click2gov(_TARPON_BASE, streets, "Tarpon Springs Click2Gov")
        if not df.empty:
            stats["tarpon"]["scraped"] = len(df)
            frames.append(("tarpon", df))

    if not frames:
        logger.info("[pinellas_violations] No records scraped from any source")
        return stats

    if not load_to_db:
        logger.info("[pinellas_violations] Skipping DB load (load_to_db=False)")
        return stats

    with get_db_context() as session:
        loader = ViolationLoader(session, county_id)
        for source_key, df in frames:
            try:
                matched, unmatched, skipped = loader.load_from_dataframe(df, skip_duplicates=True)
                stats[source_key].update(matched=matched, unmatched=unmatched, skipped=skipped)
                logger.info(
                    "[pinellas_violations] %s — matched=%d unmatched=%d skipped=%d",
                    source_key, matched, unmatched, skipped,
                )
            except Exception as exc:
                logger.error("[pinellas_violations] Load error (%s): %s", source_key, exc)
                logger.debug(traceback.format_exc())
        session.commit()

    total_scraped  = sum(s["scraped"]   for s in stats.values())
    total_matched  = sum(s["matched"]   for s in stats.values())
    total_unmatched= sum(s["unmatched"] for s in stats.values())
    total_skipped  = sum(s["skipped"]   for s in stats.values())

    try:
        record_scraper_stats(
            source_type="violations_pinellas",
            county_id=county_id,
            total_scraped=total_scraped,
            matched=total_matched,
            unmatched=total_unmatched,
            skipped=total_skipped,
        )
    except Exception as exc:
        logger.warning("[pinellas_violations] Stats recording failed (non-critical): %s", exc)

    logger.info("=" * 60)
    logger.info("PINELLAS VIOLATIONS COMPLETE")
    logger.info("  Total scraped : %d", total_scraped)
    logger.info("  Matched       : %d", total_matched)
    logger.info("  Unmatched     : %d", total_unmatched)
    logger.info("  Skipped (dup) : %d", total_skipped)
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Scrape Pinellas code violations (multi-source)"
    )
    parser.add_argument("--county-id", default="pinellas")
    parser.add_argument("--start-date", help="YYYY-MM-DD (default: 7 days ago)")
    parser.add_argument("--end-date",   help="YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--sources", nargs="+",
        default=["accela", "stpete"],
        choices=["accela", "stpete", "clearwater", "tarpon"],
        help="Sources to scrape (default: accela stpete)",
    )
    parser.add_argument("--no-db", action="store_true", help="Skip DB load")
    cli = parser.parse_args()

    dr = None
    if cli.start_date and cli.end_date:
        dr = (
            datetime.strptime(cli.start_date, "%Y-%m-%d").date(),
            datetime.strptime(cli.end_date,   "%Y-%m-%d").date(),
        )

    result = scrape_pinellas_violations(
        county_id=cli.county_id,
        date_range=dr,
        sources=cli.sources,
        load_to_db=not cli.no_db,
    )
    print(result)
    sys.exit(0)
