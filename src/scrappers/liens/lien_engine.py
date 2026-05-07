"""
Lien, Deed & Judgment Data Collection Pipeline — county-agnostic, browser-use only.

Navigates the county clerk's public access portal, performs a date-range Document Type
search with no type filter (returns all types), downloads the CSV export, then
normalises county-specific columns and categorises records in Python into
liens / deeds / judgments / probate and saves them to the appropriate directories.

County differences are driven entirely by config — no county-specific code here:
  - Column renames:   source["ori_column_map"]          (e.g. DirectName→Grantor)
  - BookPage split:   source["ori_book_page_col"]        (e.g. "BookPage" → Book + Page)
  - Doc type remap:   source["ori_doc_type_map"]         (verbose → canonical labels)
  - Filer detection:  county_cfg["city_filer_keywords"]  (code lien detection)
  - Filer labels:     county_cfg["code_lien_type_map"]   (TCL/CCL style labels)

These fields are stored in the CountySource.special_flags JSONB and County table
and populated via the admin UI.

Usage:
    python -m src.scrappers.liens.lien_engine --county-id hillsborough
    python -m src.scrappers.liens.lien_engine --county-id pinellas --start-date 2026-05-01 --end-date 2026-05-07
    python -m src.scrappers.liens.lien_engine --county-id hillsborough --load-to-db --headful
"""

import asyncio
import json
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from config.constants import (
    RAW_LIEN_DIR,
    PROCESSED_DATA_DIR,
    PROCESSED_LIENS_DIR,
    PROCESSED_DEEDS_DIR,
    PROCESSED_JUDGMENTS_DIR,
    DOWNLOAD_FILE_PATTERNS,
    TEMP_DOWNLOADS_DIR,
    BROWSER_DOWNLOAD_TEMP_PATTERN,
    HILLSCLERK_PUBLIC_ACCESS_URL,
    BROWSER_MODEL,
    BROWSER_TEMPERATURE,
)
from src.utils.county_config import get_county_config
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

for _d in (RAW_LIEN_DIR, PROCESSED_DATA_DIR, PROCESSED_LIENS_DIR,
           PROCESSED_DEEDS_DIR, PROCESSED_JUDGMENTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Built-in Hillsborough coded → canonical doc type normalisation.
# Hillsborough ORI exports use parenthesised codes like "(D) DEED", "(LN) LIEN".
# Pinellas ORI uses plain English names that are handled via ori_doc_type_map
# in county config. This built-in map is the fallback so Hillsborough works
# even with an empty ori_doc_type_map in config.
# ---------------------------------------------------------------------------
_HILLSBOROUGH_DOC_MAP: dict[str, str] = {
    "(D) DEED":                                          "DEED",
    "(TAXDEED) TAX DEED":                                "DEED",
    "(DPL) DEED PLAT":                                   "DEED",
    "(JUD) JUDGMENT":                                    "JUDGMENT",
    "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT":          "JUDGMENT",
    "(LN) LIEN":                                         "LIEN",
    "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA":     "TAX LIEN",
    "(LP) LIS PENDENS":                                  "LIS PENDENS",
    "LIS PENDENS":                                       "LIS PENDENS",
}

# HOA / IRS keywords are the same for every county.
_HOA_KEYWORDS = frozenset(["ASSOCIATION", "HOA", "CONDO", "COMMUNITY",
                            "VILLAGE", "TOWNHOME", "PROPERTY OWNERS"])
_IRS_KEYWORDS = frozenset(["UNITED STATES", "INTERNAL REVENUE",
                            "STATE OF FLORIDA", "DEPARTMENT OF REVENUE"])


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def _make_llm():
    from browser_use import ChatAnthropic
    from config.settings import get_settings
    settings = get_settings()
    return ChatAnthropic(
        model=BROWSER_MODEL,
        timeout=180,
        api_key=settings.anthropic_api_key.get_secret_value(),
        temperature=BROWSER_TEMPERATURE,
    )


def build_agent_task(source: dict, start_str: str, end_str: str) -> str:
    """
    Generate a browser-use agent task from county source metadata + date range.
    Uses Claude to produce step-by-step instructions tailored to the source's
    navigation_hint and description. Falls back to a template on LLM failure.
    """
    import anthropic
    from config.settings import get_settings

    clerk_url = source.get("url", HILLSCLERK_PUBLIC_ACCESS_URL)
    description = source.get("description", "")
    nav_hint = source.get("navigation_hint", "") or ""

    meta = {
        "clerk_url": clerk_url,
        "start_date": start_str,
        "end_date": end_str,
        "description": description,
        "navigation_hint": nav_hint,
    }

    system_prompt = (
        "You generate browser-automation task instructions for a browser-use Agent. "
        "The agent controls a Chromium browser. Write a concise, numbered task in plain English. "
        "The agent must trigger a file download (CSV export). "
        "Do NOT add any explanation outside the task text."
    )

    user_prompt = f"""Generate a browser-use agent task to download lien/deed/judgment records from a county clerk portal.

Source metadata:
{json.dumps(meta, indent=2)}

Requirements:
- Navigate to clerk_url.
- Select the "Document Type" search type from the left navigation panel.
- Leave the document type field EMPTY or unselected (to retrieve ALL document types).
- Set the filed-from date to start_date and filed-to date to end_date (MM/DD/YYYY format).
- Submit the search and wait for results to load.
- If an error appears saying results exceed 6000, note it and still attempt the export.
- Click the "Export to Spreadsheet" or "Export Results" button to download the CSV.
- Wait at least 15 seconds for the download to complete before finishing.
- Do not navigate away or open new tabs during the download.
- Keep the task under 300 words.
"""

    try:
        settings = get_settings()
        client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key.get_secret_value()
        )
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        task = response.content[0].text.strip()
        logger.info("[LLM] Generated agent task:\n%s", task)
        return task
    except Exception as e:
        logger.warning("[LLM] Task generation failed (%s) — using template fallback", e)
        return _template_task(source, start_str, end_str)


def _template_task(source: dict, start_str: str, end_str: str) -> str:
    clerk_url = source.get("url", HILLSCLERK_PUBLIC_ACCESS_URL)
    nav_hint = source.get("navigation_hint", "") or ""
    return (
        f"Go to {clerk_url}.\n"
        "Wait 5 seconds for the page to fully load.\n"
        f"{nav_hint}\n"
        "On the left panel, click 'Document Type' under Search Type.\n"
        "Leave the Document Type field blank — do NOT select a specific type.\n"
        f"Set the Filed From date to {start_str} and Filed To date to {end_str}.\n"
        "Click the Search button and wait for results to load.\n"
        "If a result-count pager appears, note the count.\n"
        "Click the 'Export to Spreadsheet' or 'Export Results' button.\n"
        "Wait 20 seconds for the CSV download to complete.\n"
        "Do not open new tabs or navigate away."
    )


# ---------------------------------------------------------------------------
# Browser-use agent (file download mode)
# ---------------------------------------------------------------------------

_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)


async def run_browser_agent(task: str, download_dir: Path, headful: bool = False,
                           no_proxy: bool = False) -> tuple:
    """
    Run a browser-use Agent that triggers a file download.
    Returns (history, start_time).
    """
    from browser_use import Agent, Browser
    from playwright_stealth import Stealth
    from src.utils.http_helpers import get_browser_use_proxy

    llm = _make_llm()

    browser = Browser(
        headless=not headful,
        disable_security=True,
        proxy=None if no_proxy else get_browser_use_proxy(),
        downloads_path=str(download_dir),
        user_agent=_STEALTH_UA,
        ignore_default_args=["--enable-automation"],
        enable_default_extensions=True,
        minimum_wait_page_load_time=1.5,
        wait_between_actions=1.0,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--window-size=1920,1080',
        ],
    )

    # Start browser before the agent so CDP is live for init script injection.
    # agent.run() calls start() again but it's idempotent (skips if already connected).
    await browser.start()

    # Inject playwright-stealth patches before any page loads.
    # Fixes: window.chrome missing, navigator.plugins=0, WebGL SwiftShader renderer —
    # all three are Cloudflare Turnstile detection signals.
    stealth = Stealth(
        chrome_runtime=True,
        navigator_webdriver=True,
        navigator_plugins=True,
        webgl_vendor=True,
        webgl_vendor_override="Google Inc. (Intel)",
        webgl_renderer_override=(
            "ANGLE (Intel, Intel(R) UHD Graphics 620 "
            "Direct3D11 vs_5_0 ps_5_0, D3D11)"
        ),
    )
    await browser._cdp_add_init_script(stealth.script_payload)
    logger.info("[Stealth] Injected fingerprint patches via CDP init script")

    agent = Agent(task=task, llm=llm, browser=browser, max_steps=60, use_judge=False)

    start_time = time.time()
    logger.info("[Agent] Starting browser-use agent (download mode)...")
    try:
        history = await agent.run()
        if not history.is_done():
            logger.warning("[Agent] Agent did not complete within step budget")
        return history, start_time
    except Exception as e:
        logger.error("[Agent] Run failed: %s", e)
        logger.debug(traceback.format_exc())
        return None, start_time


# ---------------------------------------------------------------------------
# Download file detection
# ---------------------------------------------------------------------------

def _locate_download(download_dir: Path, start_time: float) -> Optional[Path]:
    def recent_candidates(folder: Path):
        if not folder.exists():
            return []
        paths = []
        for pattern in DOWNLOAD_FILE_PATTERNS:
            for f in folder.glob(pattern):
                try:
                    if f.stat().st_mtime >= start_time:
                        paths.append(f)
                except FileNotFoundError:
                    continue
        return paths

    candidates = recent_candidates(download_dir)
    if TEMP_DOWNLOADS_DIR.exists():
        for temp_dir in TEMP_DOWNLOADS_DIR.glob(BROWSER_DOWNLOAD_TEMP_PATTERN):
            candidates.extend(recent_candidates(temp_dir))

    if not candidates:
        logger.warning("[Locate] No downloaded files found after agent run")
        return None

    most_recent = max(candidates, key=lambda p: p.stat().st_mtime)
    logger.info("[Locate] Found download: %s", most_recent)
    return most_recent


# ---------------------------------------------------------------------------
# Column normalisation — driven by source config (special_flags)
# ---------------------------------------------------------------------------

def _normalize_ori_columns(df: pd.DataFrame, source: dict) -> pd.DataFrame:
    """
    Apply county-specific column renames and structural fixes using values
    stored in source["ori_column_map"] and source["ori_book_page_col"].

    Hillsborough: ori_column_map={}, ori_book_page_col=None → no-op.
    Pinellas:     ori_column_map={"DirectName":"Grantor",...}, ori_book_page_col="BookPage".

    Both fields come from CountySource.special_flags and are merged into the
    source dict by get_county_config().
    """
    # 1. Column renames
    col_map: dict = source.get("ori_column_map") or {}
    if col_map:
        df = df.rename(columns=col_map)
        logger.info("[Normalize] Renamed columns: %s", list(col_map.keys()))

    # 2. Split combined BookPage column (e.g. "23544/1338" → Book="23544", Page="1338")
    book_page_col: Optional[str] = source.get("ori_book_page_col")
    if book_page_col and book_page_col in df.columns:
        split = df[book_page_col].astype(str).str.split("/", n=1, expand=True)
        df["Book"] = split[0].str.strip() if 0 in split.columns else ""
        df["Page"] = split[1].str.strip() if 1 in split.columns else ""
        df = df.drop(columns=[book_page_col])
        logger.info("[Normalize] Split %s → Book / Page", book_page_col)

    # 3. Ensure Filing Amt column exists (absent in Pinellas portal)
    if "Filing Amt" not in df.columns:
        df["Filing Amt"] = None

    return df


# ---------------------------------------------------------------------------
# Doc type normalisation — built-in Hillsborough codes + config override map
# ---------------------------------------------------------------------------

def _canonical_doc_type(raw: str, ori_doc_type_map: dict) -> str:
    """
    Normalise a single DocType value to a canonical label.

    Resolution order:
      1. County-specific ori_doc_type_map from config (handles Pinellas verbose types)
      2. Built-in Hillsborough coded map (handles "(D) DEED", "(LN) LIEN", etc.)
      3. Pass through as-is if not found in either map.
    """
    raw_stripped = raw.strip()
    raw_upper = raw_stripped.upper()

    # Config-level override (e.g. Pinellas "JUDGEMENT LIEN" → "JUDGMENT LIEN")
    if raw_stripped in ori_doc_type_map:
        return ori_doc_type_map[raw_stripped]
    if raw_upper in {k.upper(): v for k, v in ori_doc_type_map.items()}:
        return {k.upper(): v for k, v in ori_doc_type_map.items()}[raw_upper]

    # Built-in Hillsborough coded types
    if raw_stripped in _HILLSBOROUGH_DOC_MAP:
        return _HILLSBOROUGH_DOC_MAP[raw_stripped]
    if raw_upper in {k.upper(): v for k, v in _HILLSBOROUGH_DOC_MAP.items()}:
        return {k.upper(): v for k, v in _HILLSBOROUGH_DOC_MAP.items()}[raw_upper]

    return raw_stripped


def _normalize_doc_types(df: pd.DataFrame, source: dict) -> pd.DataFrame:
    """Apply canonical doc type normalisation to the DocType column."""
    ori_doc_type_map: dict = source.get("ori_doc_type_map") or {}
    if "DocType" in df.columns:
        df = df.copy()
        df["DocType"] = df["DocType"].fillna("").apply(
            lambda v: _canonical_doc_type(str(v), ori_doc_type_map)
        )
    return df


# ---------------------------------------------------------------------------
# Code lien label — county-aware, driven by county config
# ---------------------------------------------------------------------------

def _code_lien_label(grantor_upper: str, grantee_upper: str,
                     city_filer_keywords: list, code_lien_type_map: dict) -> Optional[str]:
    """
    Return the code lien document_type label if either party is a known
    government filer, or None if no filer match.

    Uses county_cfg["city_filer_keywords"] for detection and
    county_cfg["code_lien_type_map"] to build the type-coded label
    (e.g. "CODE LIENS (TCL)" for Hillsborough/Tampa).

    For counties without a code_lien_type_map, returns the generic "CODE LIEN".
    """
    combined = grantor_upper + " " + grantee_upper
    matched_keyword = next(
        (kw for kw in city_filer_keywords if kw.upper() in combined),
        None
    )
    if not matched_keyword:
        return None

    # Try to map to a specific type code (e.g. TCL, CCL for Hillsborough)
    for type_code, city_name in code_lien_type_map.items():
        if city_name and city_name.upper() in matched_keyword.upper():
            return f"CODE LIENS ({type_code})"
        if city_name is None and "COUNTY" in matched_keyword.upper():
            return f"CODE LIENS ({type_code})"

    return "CODE LIEN"


# ---------------------------------------------------------------------------
# Categorisation — county-aware
# ---------------------------------------------------------------------------

def categorize_and_split_data(combined_df: pd.DataFrame, county_cfg: dict) -> dict:
    """
    Route records to liens/deeds/judgments/probate using the canonical DocType
    values (already normalised by _normalize_doc_types).

    county_cfg supplies:
      - city_filer_keywords  → detect code liens by party name
      - code_lien_type_map   → build typed label (TCL/CCL) or fall back to "CODE LIEN"

    Saves each category to processed/<type>/new/.
    Returns {filename: row_count}.
    """
    city_filer_keywords: list = county_cfg.get("city_filer_keywords") or []
    code_lien_type_map: dict  = county_cfg.get("code_lien_type_map")  or {}

    logger.info("[Categorize] Routing %d records — county_filer_keywords: %s",
                len(combined_df), city_filer_keywords)

    def categorize_record(row) -> str:
        doc_type = str(row.get("DocType", "") or "").strip().upper()
        grantor  = str(row.get("Grantor",  "") or "").upper()
        grantee  = str(row.get("Grantee",  "") or "").upper()

        # Deeds
        if doc_type in ("DEED", "TAX DEED"):
            return "DEED"

        # Lis Pendens
        if "LIS PENDENS" in doc_type:
            return "LIS PENDENS"

        # Probate (Pinellas ORI export; no-op for Hillsborough)
        if doc_type in ("PROBATE", "PROBATE REAL PROPERTY"):
            return "PROBATE"

        # Divorce judgments (Pinellas ORI export; no-op for Hillsborough)
        if "DOMESTIC RELATIONS" in doc_type or "DISSOLUTION OF MARRIAGE" in doc_type:
            return "DIVORCE JUDGMENT"

        # Judgments and judgment liens
        if doc_type in ("JUDGMENT", "JUDGMENT LIEN"):
            label = _code_lien_label(grantor, grantee, city_filer_keywords, code_lien_type_map)
            return label if label else "JUDGMENT"

        # Tax lien (IRS / state)
        if doc_type == "TAX LIEN":
            return "TAX LIEN"

        # General lien — sub-categorise by party
        if doc_type in ("LIEN", "FINANCING STATEMENT", "CORPORATE LIEN"):
            if any(kw in grantor or kw in grantee for kw in _HOA_KEYWORDS):
                return "HOA LIENS (HL)"
            if any(kw in grantor or kw in grantee for kw in _IRS_KEYWORDS):
                return "TAX LIEN"
            label = _code_lien_label(grantor, grantee, city_filer_keywords, code_lien_type_map)
            if label:
                return label
            return "MECHANICS LIENS (ML)"

        return "SKIP"

    combined_df = combined_df.copy()
    combined_df["document_type"] = combined_df.apply(categorize_record, axis=1)

    lien_types     = {"HOA LIENS (HL)", "TAX LIEN", "MECHANICS LIENS (ML)",
                      "LIS PENDENS", "CODE LIEN"}
    deed_types     = {"DEED"}
    judgment_types = {"JUDGMENT", "JUDGMENT LIEN"}
    probate_types  = {"PROBATE"}
    divorce_types  = {"DIVORCE JUDGMENT"}

    # Also bucket any CODE LIENS (TCL) / CODE LIENS (CCL) style labels into liens
    def _is_lien(dt: str) -> bool:
        return dt in lien_types or dt.startswith("CODE LIENS (")

    skipped = (combined_df["document_type"] == "SKIP").sum()
    kept    = len(combined_df) - skipped
    logger.info("[Categorize] %d kept, %d discarded", kept, skipped)

    today_str = datetime.now().strftime("%Y%m%d")
    file_counts: dict = {}

    def _save(df_cat: pd.DataFrame, cat_dir: Path, filename: str):
        if df_cat.empty:
            return
        new_dir = cat_dir / "new"
        new_dir.mkdir(parents=True, exist_ok=True)
        out = new_dir / filename
        df_cat.to_csv(out, index=False)
        file_counts[out.name] = len(df_cat)
        logger.info("[Categorize] Saved %d records → %s", len(df_cat), out.name)
        for dt in df_cat["document_type"].unique():
            logger.info("  %s: %d", dt, (df_cat["document_type"] == dt).sum())

    _save(
        combined_df[combined_df["document_type"].apply(_is_lien)],
        PROCESSED_LIENS_DIR,
        f"all_liens_{today_str}.csv",
    )
    _save(
        combined_df[combined_df["document_type"].isin(deed_types)],
        PROCESSED_DEEDS_DIR,
        f"all_deeds_{today_str}.csv",
    )
    _save(
        combined_df[combined_df["document_type"].isin(judgment_types)],
        PROCESSED_JUDGMENTS_DIR,
        f"all_judgments_{today_str}.csv",
    )
    # Probate from Pinellas ORI — saved to processed/probate/new/
    probate_dir = PROCESSED_DATA_DIR / "probate"
    _save(
        combined_df[combined_df["document_type"].isin(probate_types)],
        probate_dir,
        f"all_probate_{today_str}.csv",
    )
    # Divorce judgments from Pinellas ORI — saved to processed/divorce/new/
    divorce_dir = PROCESSED_DATA_DIR / "divorce"
    _save(
        combined_df[combined_df["document_type"].isin(divorce_types)],
        divorce_dir,
        f"all_divorce_{today_str}.csv",
    )

    return file_counts


# ---------------------------------------------------------------------------
# Raw file loader
# ---------------------------------------------------------------------------

def process_lien_data(file_path: Path) -> pd.DataFrame:
    """Load the downloaded CSV/Excel file into a DataFrame."""
    logger.info("[Process] Loading: %s", file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Lien data file not found: {file_path}")

    if file_path.suffix.lower() in (".xls", ".xlsx"):
        df = pd.read_excel(file_path)
        logger.info("[Process] Loaded %d records from Excel", len(df))
        return df

    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            df = pd.read_csv(file_path, encoding=enc)
            logger.info("[Process] Loaded %d records (enc=%s)", len(df), enc)
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    raise ValueError(f"Could not read file with any known encoding: {file_path}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_lien_pipeline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    county_id: str = "hillsborough",
    headful: bool = False,
    load_to_db: bool = False,
    no_proxy: bool = False,
) -> bool:
    """County-agnostic lien/deed/judgment/probate scrape for a date range."""
    _t0 = time.monotonic()

    county_cfg = get_county_config(county_id)
    source = county_cfg["sources"].get("liens")
    if not source:
        logger.error("[%s] No liens source configured — add one via admin UI", county_id)
        return False

    if source.get("prr_only"):
        logger.info("[%s] Liens source is PRR-only — load CSV manually", county_id)
        return False

    _today    = datetime.now()
    _end_dt   = datetime.strptime(end_date,   "%Y-%m-%d") if end_date   else _today
    _start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else _end_dt
    start_str = _start_dt.strftime("%m/%d/%Y")
    end_str   = _end_dt.strftime("%m/%d/%Y")

    logger.info("=" * 70)
    logger.info("LIEN/DEED/JUDGMENT PIPELINE — %s", county_cfg["display_name"].upper())
    logger.info("Date range: %s → %s  headful=%s  load_to_db=%s",
                start_str, end_str, headful, load_to_db)
    logger.info("=" * 70)

    task = build_agent_task(source, start_str, end_str)
    history, start_time = await run_browser_agent(task, RAW_LIEN_DIR, headful=headful,
                                                  no_proxy=no_proxy)

    if history is None:
        logger.error("[Pipeline] Agent failed to run")
        _record_stats(0, False, _t0, county_id, error="Agent run failed")
        return False

    await asyncio.sleep(5)

    downloaded_file = _locate_download(RAW_LIEN_DIR, start_time)
    if not downloaded_file:
        logger.error("[Pipeline] No download detected after agent run")
        _record_stats(0, False, _t0, county_id, error="No download file found")
        return False

    try:
        df = process_lien_data(downloaded_file)
    except Exception as e:
        logger.error("[Pipeline] Failed to process downloaded file: %s", e)
        _record_stats(0, False, _t0, county_id, error=str(e))
        return False

    if df.empty:
        logger.info("[Pipeline] Downloaded file is empty — no records for this date range")
        _record_stats(0, True, _t0, county_id)
        return True

    # Normalise columns and doc types using county source config
    df = _normalize_ori_columns(df, source)
    df = _normalize_doc_types(df, source)

    file_counts = categorize_and_split_data(df, county_cfg)
    total = sum(file_counts.values())

    logger.info("=" * 70)
    logger.info("LIEN PIPELINE COMPLETE — %d records categorised", total)
    logger.info("=" * 70)

    if load_to_db:
        _load_to_database(county_id, _t0)

    _record_stats(total, True, _t0, county_id)
    return True


# ---------------------------------------------------------------------------
# ORI → legal_proceedings column bridge
# ---------------------------------------------------------------------------

# Fallback bridge used when no approved CountyColumnMapping exists for the source.
# Primary path is ColumnMapper (DB-driven, admin-editable via UI).
_ORI_TO_LEGAL_COLS_FALLBACK = {
    'Instrument':  'CaseNumber',
    'Grantor':     'LastName/CompanyName',
    'RecordDate':  'FilingDate',
    'Legal':       'PartyAddress',
}


def _load_ori_legal_proceedings(county_id: str, type_dir: Path, data_type: str) -> None:
    """
    Load ORI-sourced probate or divorce CSVs into legal_proceedings.

    Column bridge (ORI format → loader format) is resolved from CountyColumnMapping
    via ColumnMapper, falling back to _ORI_TO_LEGAL_COLS_FALLBACK if no mapping exists.

    ORI exports: Instrument, Grantor, Grantee, RecordDate, Legal
    Loaders expect: CaseNumber, LastName/CompanyName, FilingDate, PartyAddress
    """
    from src.loaders.legal_proceedings import ProbateLoader, DivorceLoader
    from src.loaders.column_mapper import ColumnMapper, SkipMapping
    from src.core.database import Database

    _loader_map = {'probate': ProbateLoader, 'divorce_filings': DivorceLoader}
    loader_class = _loader_map[data_type]

    new_dir = type_dir / "new"
    csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True) \
        if new_dir.exists() else []
    if not csv_files:
        logger.info("[DB] No new %s records to load", data_type)
        return

    csv_path = csv_files[0]
    logger.info("[DB] Loading ORI %s: %s", data_type, csv_path)

    try:
        df = pd.read_csv(csv_path)

        # Resolve column mapping from DB (approved CountyColumnMapping for this liens source)
        source_id = get_county_config(county_id)["sources"].get("liens", {}).get("source_id")
        if source_id:
            try:
                mapper = ColumnMapper()
                mapping = mapper.get_or_create(data_type, source_id, df)
                df = ColumnMapper.apply(df, mapping)
                logger.info("[DB] Applied ColumnMapper for %s source_id=%s", data_type, source_id)
            except SkipMapping:
                logger.warning("[DB] No ColumnMapper schema for %s — using built-in bridge", data_type)
                df = df.rename(columns=_ORI_TO_LEGAL_COLS_FALLBACK)
        else:
            logger.warning("[DB] No source_id for %s/%s — using built-in bridge", county_id, data_type)
            df = df.rename(columns=_ORI_TO_LEGAL_COLS_FALLBACK)

        # Ensure name-part columns exist so loader name-assembly doesn't raise
        for col in ('FirstName', 'MiddleName'):
            if col not in df.columns:
                df[col] = ''

        db = Database()
        with db.session_scope() as session:
            loader = loader_class(session, county_id)
            matched, unmatched, skipped = loader.load_from_dataframe(df, skip_duplicates=True)
            logger.info("[DB] ORI %s — matched=%d unmatched=%d skipped=%d",
                        data_type, matched, unmatched, skipped)

    except Exception as e:
        logger.error("[DB] Failed to load ORI %s: %s", data_type, e)


# ---------------------------------------------------------------------------
# DB loader
# ---------------------------------------------------------------------------

def _load_to_database(county_id: str, t0: float) -> None:
    from src.utils.scraper_db_helper import load_scraped_data_to_db

    load_targets = [
        ("liens",     PROCESSED_LIENS_DIR,     "liens"),
        ("deeds",     PROCESSED_DEEDS_DIR,     "deeds"),
        ("judgments", PROCESSED_JUDGMENTS_DIR, "judgments"),
    ]

    for label, type_dir, data_type in load_targets:
        new_dir = type_dir / "new"
        csv_files = sorted(new_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True) \
            if new_dir.exists() else []
        if csv_files:
            logger.info("[DB] Loading %s: %s", label, csv_files[0])
            try:
                load_scraped_data_to_db(data_type, csv_files[0], destination_dir=type_dir,
                                        county_id=county_id)
            except Exception as e:
                logger.error("[DB] Failed to load %s: %s", label, e)
        else:
            logger.info("[DB] No new %s records to load", label)

    # ORI-sourced probate and divorce → legal_proceedings via column bridge
    _load_ori_legal_proceedings(county_id, PROCESSED_DATA_DIR / "probate", "probate")
    _load_ori_legal_proceedings(county_id, PROCESSED_DATA_DIR / "divorce", "divorce_filings")


def _record_stats(total: int, success: bool, t0: float, county_id: str, error: str = None):
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        kwargs = dict(
            source_type="lien_ml",
            total_scraped=total,
            matched=0,
            unmatched=0,
            skipped=0,
            run_success=success,
            duration_seconds=round(time.monotonic() - t0, 2),
            county_id=county_id,
        )
        if error:
            kwargs["error_message"] = error[:500]
        record_scraper_stats(**kwargs)
    except Exception as e:
        logger.warning("[Stats] Could not record scraper stats: %s", e)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    from src.utils.scraper_db_helper import add_load_to_db_arg

    parser = argparse.ArgumentParser(
        description="Lien/deed/judgment scraper — county-agnostic, browser-use only"
    )
    parser.add_argument("--county-id", default="hillsborough",
                        help="County identifier (default: hillsborough)")
    parser.add_argument("--start-date", default=None,
                        help="Start date YYYY-MM-DD (default: today)")
    parser.add_argument("--end-date", default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--headful", action="store_true",
                        help="Run browser in visible mode")
    parser.add_argument("--no-proxy", action="store_true",
                        help="Skip Oxylabs proxy (useful for local testing)")
    add_load_to_db_arg(parser)

    args = parser.parse_args()
    success = asyncio.run(run_lien_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        county_id=args.county_id,
        headful=args.headful,
        load_to_db=args.load_to_db,
        no_proxy=args.no_proxy,
    ))

    import sys
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
