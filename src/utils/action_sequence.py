"""
Dynamic action sequence — two approaches for county portal scraping.

Approach 1 (legacy): JSON DSL with 9 ops, interpreter executes each step.
  Public API: generate_action_sequence, execute_action_sequence,
              persist_action_sequence, clear_action_sequence

Approach 2 (current): LLM generates a Python/Playwright async function.
  The code is AST-validated against a forbidden-name list before exec().
  Public API: generate_playwright_code, validate_playwright_code,
              execute_playwright_code, persist_playwright_code,
              clear_playwright_code

Both use the same self-heal pattern: cached in CountySource.special_flags,
cleared on failure, regenerated on next run.
"""

import ast
import asyncio
import builtins
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ActionSequenceError(Exception):
    """Raised when an action sequence step fails. Signals caller to clear cache."""


class PlaywrightCodeError(ActionSequenceError):
    """Raised when generated Playwright code fails validation or execution."""


# ---------------------------------------------------------------------------
# Approach 1 — JSON DSL (kept for backward compatibility)
# ---------------------------------------------------------------------------

ALLOWED_OPS = {
    "goto", "click", "fill", "wait_selector", "wait_load",
    "wait_ms", "js_eval", "download", "extract_rows",
}
DATA_OPS = {"download", "extract_rows"}

_SEQUENCE_SYSTEM_PROMPT = """\
You generate Playwright action sequences for a web scraping engine.
Given a portal URL, description, and CSS selectors with their roles, produce a JSON \
array of step objects that will:
  1. Navigate to the portal
  2. Fill in date range fields (start_date, end_date)
  3. Submit the search and wait for results
  4. Either download a file (if export_btn selector is provided) or paginate through \
all result pages and extract rows (if row / next_btn selectors are provided)

ALLOWED OPS and their required fields:
  {"op": "goto",          "url": "..."}
  {"op": "fill",          "selector": "...", "value": "..."}
  {"op": "click",         "selector": "..."}
  {"op": "wait_selector", "selector": "...", "timeout": 30000}
  {"op": "wait_load",     "state": "networkidle"}
  {"op": "wait_ms",       "ms": 2000}
  {"op": "js_eval",       "script": "..."}
  {"op": "download",      "selector": "..."}
  {"op": "extract_rows",  "selector": "...", "next_btn": "..."}

Use {url}, {start_date}, {end_date} as placeholders — they are substituted at runtime.
{start_date} and {end_date} are in MM/DD/YYYY format.

Rules:
- Start with a goto op using {url}.
- Use only ops from the list above. Unknown ops will be dropped.
- End with exactly one data op: "download" if export_btn selector is given, \
"extract_rows" if row/next_btn selectors are given.
- Return ONLY the JSON array. No explanation, no markdown fences.\
"""


def generate_action_sequence(source: dict, signal_type: str = "permits") -> list[dict]:
    """Generate a JSON action sequence via LLM (Approach 1)."""
    import anthropic
    from config.settings import get_settings

    url = source.get("url", "")
    description = source.get("description") or f"{signal_type} data portal"
    nav_hint = source.get("navigation_hint") or ""
    selectors = source.get("selectors") or {}

    if not selectors:
        raise ActionSequenceError("Cannot generate action sequence: source has no selectors config")

    user_prompt = (
        f"Generate a Playwright action sequence to scrape {signal_type} records.\n\n"
        f"Portal URL: {url}\nDescription: {description}\n"
        f"Navigation hint: {nav_hint or '(none)'}\n\n"
        f"CSS selectors (role → selector):\n{json.dumps(selectors, indent=2)}\n\n"
        f"Start date placeholder: {{start_date}}  (MM/DD/YYYY)\n"
        f"End date placeholder:   {{end_date}}    (MM/DD/YYYY)\n"
    )

    logger.info("[ActionSeq] Generating JSON action sequence for url=%s", url)

    try:
        settings = get_settings()
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key.get_secret_value())
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            temperature=0,
            system=_SEQUENCE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = response.content[0].text.strip()
    except Exception as exc:
        raise ActionSequenceError(f"LLM API call failed: {exc}") from exc

    text = raw
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    try:
        sequence = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ActionSequenceError(f"LLM returned invalid JSON: {exc}") from exc

    if not isinstance(sequence, list):
        raise ActionSequenceError(f"Expected JSON array, got {type(sequence).__name__}")

    cleaned = [s for s in sequence if isinstance(s, dict) and s.get("op") in ALLOWED_OPS]
    if not cleaned:
        raise ActionSequenceError("Sequence is empty after op validation")
    if not any(s.get("op") in DATA_OPS for s in cleaned):
        raise ActionSequenceError("Sequence has no data op (download or extract_rows)")

    return cleaned


async def execute_action_sequence(
    sequence: list[dict],
    page,
    download_dir: Path,
    placeholders: dict,
    county_id: Optional[str] = None,
) -> pd.DataFrame:
    """Execute a validated JSON action sequence (Approach 1)."""
    result: Optional[pd.DataFrame] = None

    for step in sequence:
        op = step.get("op")
        try:
            if op == "goto":
                url = step["url"].format(**placeholders)
                await page.goto(url, wait_until="networkidle", timeout=30_000)

            elif op == "fill":
                value = step["value"].format(**placeholders)
                sel = step["selector"]
                type_value = re.sub(r"/", "", value) if re.match(r"\d{2}/\d{2}/\d{4}$", value) else value
                await page.click(sel, click_count=3)
                await page.keyboard.type(type_value, delay=50)

            elif op == "click":
                await page.click(step["selector"])

            elif op == "wait_selector":
                await page.wait_for_selector(step["selector"], timeout=step.get("timeout", 30_000))

            elif op == "wait_load":
                await page.wait_for_load_state(step.get("state", "networkidle"))

            elif op == "wait_ms":
                await asyncio.sleep(step["ms"] / 1000)

            elif op == "js_eval":
                await page.evaluate(step["script"])

            elif op == "download":
                result = await _op_download(step, page, download_dir)

            elif op == "extract_rows":
                result = await _op_extract_rows(step, page, county_id)

        except ActionSequenceError:
            raise
        except Exception as exc:
            raise ActionSequenceError(f"Step op={op!r} failed: {exc}") from exc

    if result is None:
        raise ActionSequenceError("Sequence completed but produced no DataFrame")

    return result


async def _op_download(step: dict, page, download_dir: Path) -> pd.DataFrame:
    async with page.expect_download(timeout=60_000) as dl_info:
        await page.click(step["selector"])
    download = await dl_info.value
    fname = download.suggested_filename or f"seq_dl_{int(time.time())}.csv"
    dest = download_dir / fname
    await download.save_as(str(dest))
    logger.info("[ActionSeq/DL] Saved → %s", dest)
    try:
        df = pd.read_excel(dest) if dest.suffix.lower() in (".xls", ".xlsx") else _read_csv_any_encoding(dest)
    finally:
        try:
            dest.unlink()
        except Exception:
            pass
    return df


def _read_csv_any_encoding(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            return pd.read_csv(path, encoding=enc)
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    raise ActionSequenceError(f"Could not read CSV with any known encoding: {path}")


async def _op_extract_rows(step: dict, page, county_id: Optional[str]) -> pd.DataFrame:
    row_sel = step.get("selector", "tbody tr")
    next_sel = step.get("next_btn")
    table_sel = step.get("table_selector", "table")
    all_rows: list = []
    headers: list = []
    page_num = 0

    while True:
        page_num += 1
        if not headers:
            headers = await page.evaluate(
                """(sel) => {
                    const ths = document.querySelectorAll(sel + ' th');
                    if (ths.length) return Array.from(ths).map(e => e.textContent.trim()).filter(Boolean);
                    const tds = document.querySelectorAll(sel + ' thead td');
                    return Array.from(tds).map(e => e.textContent.trim()).filter(Boolean);
                }""",
                table_sel,
            )
        page_rows = await page.evaluate(
            """(rowSel) => Array.from(document.querySelectorAll(rowSel))
                .map(row => Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim()))
                .filter(cells => cells.length > 0)""",
            row_sel,
        )
        all_rows.extend(page_rows)
        logger.debug("[ActionSeq/EX] Page %d: %d rows", page_num, len(page_rows))

        if not next_sel:
            break
        next_handle = await page.query_selector(next_sel)
        if not next_handle:
            break
        if not await next_handle.is_visible():
            break
        href = (await next_handle.get_attribute("href")) or ""
        if "__doPostBack" not in href:
            break
        await page.evaluate("(s) => { const el = document.querySelector(s); if (el) el.click(); }", next_sel)
        try:
            await page.wait_for_selector("#divGlobalLoadingMask.ACA_Hide", timeout=15_000)
        except Exception:
            pass
        await page.wait_for_load_state("networkidle", timeout=30_000)

    logger.info("[ActionSeq/EX] Extracted %d rows across %d pages", len(all_rows), page_num)
    if not all_rows:
        return pd.DataFrame()

    n_cells = max(len(r) for r in all_rows)
    if headers:
        if len(headers) < n_cells:
            headers = ["_col0"] * (n_cells - len(headers)) + headers
        elif len(headers) > n_cells:
            headers = headers[:n_cells]
    df = pd.DataFrame(all_rows, columns=headers if headers else None)
    if county_id:
        df["county_id"] = county_id
    return df


def persist_action_sequence(county_id: str, source_id: int, sequence: list) -> None:
    _patch_source_flags(county_id, source_id, {"action_sequence": sequence})
    logger.info("[ActionSeq] Persisted %d-step sequence for source_id=%s", len(sequence), source_id)


def clear_action_sequence(county_id: str, source_id: int) -> None:
    _remove_source_flag(county_id, source_id, "action_sequence")
    logger.info("[ActionSeq] Cleared cached sequence for source_id=%s", source_id)


# ---------------------------------------------------------------------------
# Approach 2 — LLM-generated Python/Playwright code
# ---------------------------------------------------------------------------

# Bump this whenever the system prompt below is meaningfully changed. The version
# is stored alongside the cached code so we can answer "did this scraper start
# failing because of a prompt change or a portal change?" and target re-generation
# at a specific prompt cohort if needed.
_CODE_PROMPT_VERSION = "v1"

# Names that are blocked in generated code (AST walk).
_FORBIDDEN_NAMES = frozenset({
    "__import__", "open", "exec", "eval", "compile",
    "subprocess", "shutil", "requests", "socket", "urllib",
    "__builtins__", "globals", "locals", "vars", "dir",
    "getattr", "setattr", "delattr", "breakpoint", "input",
})

# Generated code runs with the real builtins module — the AST validator above
# (via _FORBIDDEN_NAMES) blocks dangerous names like __import__, open, exec, eval,
# getattr, etc. before code is exec'd, so a restricted builtin dict adds no security
# but does cause NameErrors on standard idioms (Exception, ValueError, str.format,
# exception chaining, etc.).

_CODE_SYSTEM_PROMPT = """\
You write Playwright Python async functions to scrape county government portals.

EXACT FUNCTION SIGNATURE (do not change):
  async def run_scrape(page, download_dir, start_date, end_date, url, county_id):

VARIABLES ALREADY IN SCOPE (do NOT import):
  page         Playwright Page object (blank, not yet navigated)
  download_dir pathlib.Path  directory for any file downloads
  start_date   str  "MM/DD/YYYY"
  end_date     str  "MM/DD/YYYY"
  url          str  portal URL
  county_id    str
  asyncio      asyncio module
  pd           pandas as pd
  re           re module
  json         json module
  Path         pathlib.Path

RETURN: pandas DataFrame with permit rows and a 'county_id' column.
  Zero results → return pd.DataFrame()
  File download → save to download_dir / 'result.xlsx' (or .csv), load, add county_id, return.

FORBIDDEN (code will be rejected):
  import, __import__, open(), exec(), eval(), compile()
  subprocess, os, sys, shutil, requests, socket, urllib

=== ACCELA .NET WEBFORMS — CRITICAL PATTERNS ===

1. DATE MASKED INPUT — standard fill() silently fails. Use this exact pattern:
   digits_start = re.sub(r'[^0-9]', '', start_date)  # "05012026"
   digits_end   = re.sub(r'[^0-9]', '', end_date)

   # Single click to focus, then Home to guarantee cursor at position 0,
   # then press() each digit one at a time (NOT keyboard.type — it batches events
   # and the .NET masked input drops the year portion).
   await page.click(start_sel)
   await asyncio.sleep(0.2)
   await page.keyboard.press("Home")
   await asyncio.sleep(0.1)
   for ch in digits_start:
       await page.keyboard.press(ch)
       await asyncio.sleep(0.08)
   await page.keyboard.press("Tab")   # triggers blur/change event to commit value
   await asyncio.sleep(0.4)

   await page.click(end_sel)
   await asyncio.sleep(0.2)
   await page.keyboard.press("Home")
   await asyncio.sleep(0.1)
   for ch in digits_end:
       await page.keyboard.press(ch)
       await asyncio.sleep(0.08)
   await page.keyboard.press("Tab")
   await asyncio.sleep(0.4)

2. WAITING AFTER SEARCH SUBMIT — results load via UpdatePanel (partial page refresh):
   await page.click(search_btn_sel)
   # Wait for the loading mask to appear then disappear
   try:
       await page.wait_for_selector("#divGlobalLoadingMask:not(.ACA_Hide)", timeout=8000)
   except Exception:
       pass
   try:
       await page.wait_for_selector("#divGlobalLoadingMask.ACA_Hide", timeout=30000)
   except Exception:
       await asyncio.sleep(3)

3. PAGINATION — UpdatePanel replaces the results table in place:
   # Capture unique marker from current page to detect DOM refresh
   try:
       marker = await page.text_content('tr.ACA_TabRow_Odd:first-child td:nth-child(2)')
   except Exception:
       marker = ""
   # JS click bypasses the ACA loading-mask overlay
   await page.evaluate("sel => { const el = document.querySelector(sel); if(el) el.click(); }", next_sel)
   # Wait for content to change (proves UpdatePanel fired and completed)
   try:
       await page.wait_for_function(
           "(m) => { const el = document.querySelector('tr.ACA_TabRow_Odd td:nth-child(2)');"
           " return el && el.textContent.trim() !== m; }",
           marker,
           timeout=25000,
       )
   except Exception:
       await asyncio.sleep(4)

4. DETECTING LAST PAGE — stop condition (check ALL of these):
   next_el = await page.query_selector(next_sel)
   if not next_el:
       break
   href = (await next_el.get_attribute("href")) or ""
   if "__doPostBack" not in href:   # last page: href is "#" or empty
       break
   cls = (await next_el.get_attribute("class")) or ""
   if "disabled" in cls.lower():
       break

5. EXTRACTING ROWS FROM RESULTS TABLE:
   rows = await page.evaluate(
       "(sel) => Array.from(document.querySelectorAll(sel))"
       ".map(tr => Array.from(tr.querySelectorAll('td')).map(td => td.textContent.trim()))"
       ".filter(cells => cells.some(c => c.length > 0))",
       row_sel,
   )

6. EXTRACTING HEADER ROW (critical — wrong header → DataFrame ends up with 0,1,2,...
   column names and downstream loaders break):
   Accela's grid header row uses <td> inside a row with class ACA_GridHeader,
   NOT <th>. Try in order: header td → ACA_GridHeader td → first non-data
   <tr>'s td cells. Pseudocode:

       headers = await page.evaluate(
           "(tableSel) => {"
           "  const t = document.querySelector(tableSel);"
           "  if (!t) return [];"
           "  // 1. Real <th> cells"
           "  let ths = Array.from(t.querySelectorAll('th'))"
           "    .map(e => e.textContent.trim()).filter(Boolean);"
           "  if (ths.length) return ths;"
           "  // 2. Accela ACA_GridHeader row of <td>"
           "  const hdr = t.querySelector('tr.ACA_GridHeader, tr.aca_grid_header');"
           "  if (hdr) return Array.from(hdr.querySelectorAll('td'))"
           "    .map(e => e.textContent.trim()).filter(Boolean);"
           "  return [];"
           "}",
           results_table_sel,
       )

   If headers come back empty, DO NOT fall through to `pd.DataFrame(rows)`
   (that gives integer column names). Instead either: (a) hard-code the column
   names from the navigation_hint / portal docs if the grid is fixed-shape, or
   (b) return pd.DataFrame() and let the engine surface the failure.

RULES:
  - Output ONLY the async def run_scrape(...): block — nothing else.
  - No nested classes. No top-level statements outside the function.
  - Wrap main logic in try/except; on unrecoverable error log and return pd.DataFrame().
  - Always set df['county_id'] = county_id before returning.
  - **Never** return a DataFrame with integer column names. If you can't recover
    real headers, return an empty DataFrame instead so the operator notices.
  - Keep under 160 lines total.\
"""


def generate_playwright_code(source: dict, signal_type: str = "permits") -> str:
    """
    Call Claude to generate a Python/Playwright async function for the given source.

    Returns the code string (the full async def run_scrape(...) function).
    Raises PlaywrightCodeError on API failure, syntax error, or validation failure.
    """
    import anthropic
    from config.settings import get_settings

    url = source.get("url", "")
    description = source.get("description") or f"{signal_type} data portal"
    nav_hint = source.get("navigation_hint") or ""
    selectors = source.get("selectors") or {}

    user_prompt = (
        f"Generate a run_scrape function to scrape {signal_type} records.\n\n"
        f"Portal URL: {url}\n"
        f"Description: {description}\n"
        f"Navigation hint: {nav_hint or '(none)'}\n\n"
        f"CSS selectors configured for this portal (role → selector):\n"
        f"{json.dumps(selectors, indent=2)}\n\n"
        f"The portal uses Accela Automation (ACA) — an ASP.NET WebForms application.\n"
        f"Results are displayed in a paginated grid table with class 'ACA_Grid_OverFlow'.\n"
        f"Row classes are 'ACA_TabRow_Odd' and 'ACA_TabRow_Even'.\n"
        f"Pagination uses ASP.NET UpdatePanel (partial page refresh via __doPostBack).\n"
    )

    logger.info("[CodeGen] Generating Playwright code for url=%s", url)

    try:
        settings = get_settings()
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key.get_secret_value())
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            temperature=0,
            system=_CODE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = response.content[0].text.strip()
        logger.debug("[CodeGen] LLM raw response:\n%s", raw)
    except Exception as exc:
        raise PlaywrightCodeError(f"LLM API call failed: {exc}") from exc

    code = _strip_fences(raw)
    validate_playwright_code(code)
    logger.info("[CodeGen] Generated and validated Playwright code (%d chars)", len(code))
    return code


def _strip_fences(text: str) -> str:
    """Remove markdown code fences if present."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # drop first line (```python or ```) and last line (```)
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()
    return text


def validate_playwright_code(code: str) -> None:
    """
    Validate LLM-generated code against the safety rules.

    Checks:
    - Valid Python syntax
    - No import statements
    - No forbidden builtins / dangerous names
    - Defines async def run_scrape(...)

    Raises PlaywrightCodeError on any violation.
    """
    if len(code) > 8000:
        raise PlaywrightCodeError("Generated code exceeds 8000-character safety limit")

    if re.search(r"\bimport\b", code):
        raise PlaywrightCodeError("Code must not contain import statements")

    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise PlaywrightCodeError(f"Syntax error in generated code: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in _FORBIDDEN_NAMES:
            raise PlaywrightCodeError(f"Forbidden name in generated code: {node.id!r}")
        if isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_NAMES:
            raise PlaywrightCodeError(f"Forbidden attribute in generated code: {node.attr!r}")

    func_names = [
        n.name for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
    ]
    if "run_scrape" not in func_names:
        raise PlaywrightCodeError("Generated code must define 'async def run_scrape(...)'")


async def execute_playwright_code(
    code: str,
    page,
    download_dir: Path,
    placeholders: dict,
    county_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Execute validated LLM-generated Playwright code.

    placeholders: {"url": ..., "start_date": ..., "end_date": ...}

    Returns DataFrame from the run_scrape function.
    Raises PlaywrightCodeError on execution failure.
    """
    exec_namespace = {
        "__builtins__": builtins,
        "asyncio": asyncio,
        "pd": pd,
        "re": re,
        "json": json,
        "Path": Path,
    }

    try:
        exec(compile(code, "<generated>", "exec"), exec_namespace)  # noqa: S102
    except Exception as exc:
        raise PlaywrightCodeError(f"Code compilation failed: {exc}") from exc

    run_scrape = exec_namespace.get("run_scrape")
    if not callable(run_scrape):
        raise PlaywrightCodeError("run_scrape is not callable after exec")

    try:
        result = await run_scrape(
            page=page,
            download_dir=download_dir,
            start_date=placeholders.get("start_date", ""),
            end_date=placeholders.get("end_date", ""),
            url=placeholders.get("url", ""),
            county_id=county_id or "",
        )
    except PlaywrightCodeError:
        raise
    except Exception as exc:
        raise PlaywrightCodeError(f"run_scrape raised an error: {exc}") from exc

    if result is None:
        return pd.DataFrame()
    if not isinstance(result, pd.DataFrame):
        raise PlaywrightCodeError(f"run_scrape returned {type(result).__name__}, expected DataFrame")

    return result


def persist_playwright_code(
    county_id: str,
    source_id: int,
    code: str,
    prompt_version: Optional[str] = None,
    is_approved: bool = False,
) -> None:
    """
    Store playwright_code in CountySource.special_flags and bust the config cache.
    Also appends a row to playwright_code_history for audit/rollback.

    Args:
        prompt_version: tag of the system prompt used to generate this code.
                        Defaults to the current module constant.
        is_approved: False on first generation by default — admin reviews via UI
                     and flips to True. Engine logs a warning when running
                     unapproved code (does not block execution).
    """
    pv = prompt_version or _CODE_PROMPT_VERSION
    _patch_source_flags(
        county_id,
        source_id,
        {
            "playwright_code":           code,
            "playwright_code_version":   pv,
            "playwright_code_approved":  is_approved,
        },
    )
    _record_code_history(
        county_id=county_id,
        source_id=source_id,
        code=code,
        prompt_version=pv,
        reason="generated",
        is_approved=is_approved,
    )
    logger.info(
        "[CodeGen] Persisted playwright_code source_id=%s (%d chars, version=%s, approved=%s)",
        source_id, len(code), pv, is_approved,
    )


def clear_playwright_code(county_id: str, source_id: int) -> None:
    """
    Remove cached playwright_code from CountySource.special_flags.
    Writes a history row so we can correlate failures to clears.
    """
    _remove_source_flag(county_id, source_id, "playwright_code")
    _remove_source_flag(county_id, source_id, "playwright_code_version")
    _remove_source_flag(county_id, source_id, "playwright_code_approved")
    _record_code_history(
        county_id=county_id,
        source_id=source_id,
        code=None,
        prompt_version=None,
        reason="cleared",
        is_approved=False,
    )
    logger.info("[CodeGen] Cleared playwright_code for source_id=%s — will regenerate next run", source_id)


def approve_playwright_code(county_id: str, source_id: int, approved_by: str = "admin") -> None:
    """
    Mark the cached playwright_code for this source as approved. Once approved,
    the engine no longer logs unapproved-code warnings on every run.

    Intended to be called from the admin UI after a human reviews the generated
    code. Idempotent.
    """
    _patch_source_flags(county_id, source_id, {"playwright_code_approved": True})
    _record_code_history(
        county_id=county_id,
        source_id=source_id,
        code=None,                # code already in special_flags — no need to duplicate
        prompt_version=None,
        reason=f"approved_by:{approved_by}",
        is_approved=True,
    )
    logger.info("[CodeGen] Approved playwright_code for source_id=%s by %s", source_id, approved_by)


def _record_code_history(
    county_id: str,
    source_id: int,
    code: Optional[str],
    prompt_version: Optional[str],
    reason: str,
    is_approved: bool,
) -> None:
    """Append a row to playwright_code_history. Non-critical — swallows errors."""
    try:
        from src.core.database import Database
        from src.core.models import PlaywrightCodeHistory

        db = Database()
        with db.session_scope() as session:
            session.add(PlaywrightCodeHistory(
                source_id=source_id,
                county_id=county_id,
                code=code,
                prompt_version=prompt_version,
                reason=reason,
                is_approved=is_approved,
            ))
    except Exception as exc:
        logger.warning("[CodeGen] history insert failed (non-critical): %s", exc)


# ---------------------------------------------------------------------------
# Shared DB helpers
# ---------------------------------------------------------------------------

def _patch_source_flags(county_id: str, source_id: int, updates: dict) -> None:
    try:
        from src.core.database import Database
        from src.core.models import CountySource
        from src.utils.county_config import invalidate_cache

        db = Database()
        with db.session_scope() as session:
            src = session.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
            if src is None:
                logger.warning("[ActionSeq] patch: source_id=%s county=%s not found", source_id, county_id)
                return
            flags = dict(src.special_flags or {})
            flags.update(updates)
            src.special_flags = flags

        invalidate_cache(county_id)
    except Exception as exc:
        logger.warning("[ActionSeq] patch_source_flags failed (non-critical): %s", exc)


def _remove_source_flag(county_id: str, source_id: int, key: str) -> None:
    try:
        from src.core.database import Database
        from src.core.models import CountySource
        from src.utils.county_config import invalidate_cache

        db = Database()
        with db.session_scope() as session:
            src = session.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
            if src is None:
                return
            flags = dict(src.special_flags or {})
            flags.pop(key, None)
            src.special_flags = flags

        invalidate_cache(county_id)
    except Exception as exc:
        logger.warning("[ActionSeq] remove_source_flag failed (non-critical): %s", exc)
