# Multi-County Scraping Architecture

> **Audience:** Engineers onboarding to this codebase who need to understand how
> scraping is driven entirely by database configuration, and how to add a new
> county without touching any application code.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Core Concept: DB-Driven Dispatch](#2-core-concept-db-driven-dispatch)
3. [Database Schema](#3-database-schema)
4. [Scrape Modes](#4-scrape-modes)
5. [playwright_code — The Scraper Function](#5-playwright_code--the-scraper-function)
6. [special_flags — Engine-Specific Config](#6-special_flags--engine-specific-config)
7. [Column Mapping](#7-column-mapping)
8. [How to Add a New County](#8-how-to-add-a-new-county)
9. [Engine Dispatch Reference](#9-engine-dispatch-reference)
10. [Risk Factors and Testing Checklist](#10-risk-factors-and-testing-checklist)
11. [Admin API Reference](#11-admin-api-reference)

---

## 1. Problem Statement

Scraping the same data type (violations, permits, PA records) across multiple counties
requires navigating different portals that share the same logical steps but differ in
DOM layout, pagination mechanics, date input behavior, and export format.

**Three approaches were evaluated:**

| Approach | Cost per run | Handles portal quirks | Code changes to add county |
|---|---|---|---|
| Hardcoded per-county classes | Zero | No — fragile | Yes |
| LLM agent every run | High — API call every run | Yes | No |
| **Stored Playwright code (current)** | **Zero** | **Yes** | **No** |

The chosen approach stores a tested Python/Playwright function directly in the database
per county source. Every subsequent run executes this function — no LLM cost, no
hardcoded branches. A new county is added by inserting DB rows only.

---

## 2. Core Concept: DB-Driven Dispatch

All scraper engines (`violation_engine`, `permit_engine`, `pa_engine`,
`master_engine`) follow the same pattern:

```
Engine starts
  |
  +- get_county_config(county_id)
  |     reads counties + county_sources from DB (5-min cache)
  |     returns source dict with scrape_mode, playwright_code, special_flags
  |
  +- dispatch on source["scrape_mode"]
  |     playwright_only      -> execute stored playwright_code; abort on failure
  |     playwright_then_ai   -> execute stored playwright_code; AI fallback on failure
  |     ai_only              -> browser-use Agent only
  |     static_download      -> direct HTTP GET/POST, no browser
  |     api                  -> reserved for REST API sources
  |
  +- normalize columns via ColumnMapper
        reads CountyColumnMapping row from DB
        renames raw portal columns -> canonical schema columns
```

**No engine contains county-specific `if` branches.** The county drives everything
through its `county_sources` row.

---

## 3. Database Schema

### `counties` table

One row per county. Managed via admin UI (`/api/admin/counties`).

| Column | Type | Purpose |
|---|---|---|
| `county_id` | `VARCHAR(50)` | Slug used as FK everywhere (`hillsborough`, `pinellas`) |
| `display_name` | `VARCHAR(100)` | Human label |
| `fips` | `VARCHAR(10)` | Federal FIPS code |
| `nws_zone` | `VARCHAR(20)` | NWS forecast zone(s), comma-separated for multi-zone |
| `parcel_id_format` | `VARCHAR(20)` | `folio` or `parcel` |
| `bankruptcy_division` | `VARCHAR(10)` | PACER division prefix |
| `city_filer_keywords` | `JSONB` | City tokens for address normalization |
| `code_lien_type_map` | `JSONB` | Lien code to type mappings |
| `address_city_tokens` | `JSONB` | City/CDP tokens stripped from address suffixes |
| `is_active` | `BOOLEAN` | Inactive counties are ignored by all engines |

### `county_sources` table

One row per `(county_id, signal_type)` pair. This is the primary config table for
scrapers.

| Column | Type | Purpose |
|---|---|---|
| `county_id` | `VARCHAR(50)` | FK to `counties.county_id` |
| `signal_type` | `VARCHAR(50)` | Matches the engine's lookup key: `violations`, `permits`, `property_appraiser`, `foreclosures`, `liens`, `court_records`, `master_data`, `tax_delinquency`, `deeds` |
| `url` | `TEXT` | Portal entry point |
| `description` | `TEXT` | Human description; also fed to LLM task generation |
| `navigation_hint` | `TEXT` | Extra step instructions for AI-mode agents |
| `scrape_mode` | `VARCHAR(32)` | Enum — see Section 4 |
| `playwright_code` | `TEXT` | Stored `async def run_scrape(...)` function |
| `playwright_code_version` | `VARCHAR(32)` | Prompt version tag for audit |
| `playwright_code_approved` | `BOOLEAN` | Must be `true` before production use |
| `special_flags` | `JSONB` | Engine-specific config — see Section 6 |
| `output_format` | `VARCHAR(20)` | `csv`, `excel`, `json` (informational) |
| `date_range_available` | `BOOLEAN` | Whether this portal supports date filtering |
| `is_active` | `BOOLEAN` | Inactive sources are skipped |

**DB constraint on `scrape_mode`:**

```sql
CHECK (scrape_mode IN (
  'ai_only', 'playwright_only', 'playwright_then_ai',
  'nodriver_only', 'nodriver_then_ai', 'static_download', 'api'
))
```

**Uniqueness constraint:** `(county_id, signal_type)` — one source row per county per
data type.

### `county_column_mappings` table

One row per column-mapping event (LLM-proposed or human-created).

| Column | Type | Purpose |
|---|---|---|
| `source_id` | `INTEGER` | FK to `county_sources.id` |
| `source_columns` | `JSONB` | List of raw column names the mapping was built against |
| `mapping` | `JSONB` | `{raw_col: canonical_col}` rename dict |
| `is_approved` | `BOOLEAN` | `false` = pending admin review; `true` = in use |
| `mapped_by` | `VARCHAR(10)` | `llm` or `human` |
| `reject_feedback` | `TEXT` | Admin rejection note fed back to LLM on retry |
| `post_processors` | `JSONB` | Ordered transforms (e.g., split `Book/Page` into two columns) |
| `value_maps` | `JSONB` | Per-column value normalization rules |
| `row_routing` | `JSONB` | Split rows into signal-type buckets by column value |

### `playwright_code_history` table

Append-only audit log of every code store, clear, or approval event.

| Column | Type | Purpose |
|---|---|---|
| `source_id` | `INTEGER` | FK to `county_sources.id` |
| `county_id` | `VARCHAR(50)` | Denormalized for query convenience |
| `code` | `TEXT` | Snapshot of the code at this event (NULL for clear events) |
| `prompt_version` | `VARCHAR(20)` | Prompt version if LLM-generated |
| `reason` | `VARCHAR(40)` | `generated`, `cleared`, `approved_by:<name>`, `manual_paste` |
| `is_approved` | `BOOLEAN` | Approval state at time of event |

---

## 4. Scrape Modes

The `scrape_mode` column is the primary dispatch signal. Every engine reads it from
the source config and routes accordingly.

### `playwright_only`

Execute the stored `playwright_code` function. If the function returns no data or
raises an error, **abort** — no AI fallback.

**Use when:** The scraper is a precision "sniper" task where an AI fallback would be
prohibitively expensive or unreliable. Example: property appraiser enrichment — one
Playwright call per property, running hundreds in parallel via `ThreadPoolExecutor`.

### `playwright_then_ai`

Execute the stored `playwright_code` first. If it returns no data or fails, fall
back to the browser-use AI agent.

**Use when:** Playwright is preferred for speed and consistency, but AI recovery is
acceptable as a safety net. Example: violations engine — Accela portal scraping where
a DOM change should not halt the entire daily run.

### `nodriver_only` / `nodriver_then_ai`

Identical semantics to `playwright_only` / `playwright_then_ai` — execute the stored
`playwright_code`, abort or fall back to AI on failure — but the engine launches
`nodriver` (the CDP-driven, undetected-chromedriver successor) instead of Playwright
to host the page. `execute_playwright_code()` (in `action_sequence.py`) is
driver-agnostic: it just calls the stored `run_scrape(page, ...)` function with
whatever page-like object it's handed, so the same function contract, storage,
validation, versioning, and approval workflow apply unchanged — only the object
passed as `page` differs (a nodriver `Tab`, not a Playwright `Page`), which means
`page`-level calls in the stored code must use nodriver's API (`select()`,
`.click()`, `.send_keys()`, `verify_cf()`, ...) — see the nodriver quick reference
in Section 5.

**Use when:** The portal is fronted by a bot-detection layer (e.g. Cloudflare
Turnstile) that keeps re-challenging Playwright's CDP fingerprint even against an
already-warmed persistent browser profile — nodriver's automation-marker patching
clears these challenges where Playwright cannot. Requires
`special_flags.cf_bypass_required = true` (see Section 6) so the engine warms/
validates a persistent profile via `cf_session_manager` before launching. Example:
Pinellas Clerk Official Records (`lien_engine.py`).

### `ai_only`

Skip Playwright entirely. Send a browser-use Agent task (LLM drives a real Chromium
browser) on every run.

**Use when:** No pre-tested Playwright code exists, or the portal structure is too
dynamic for stable code.

### `static_download`

Make a direct HTTP GET or POST request — no browser launched. Requires
`download_url` and optionally `download_params`, `download_method`, `download_headers`
in `special_flags`.

**Use when:** The portal exposes a stable download endpoint accessible without a
browser session.

### `api`

Reserved for future REST API sources. Not yet implemented in any engine.

---

## 5. `playwright_code` — The Scraper Function

### Function Contract

Every `playwright_code` value is a self-contained Python async function with this
exact signature:

```python
async def run_scrape(page, download_dir, start_date, end_date, url, county_id):
    ...
    return df  # pandas DataFrame
```

**Parameters available in scope (do NOT import):**

| Name | Type | Value |
|---|---|---|
| `page` | `playwright.Page` | Fresh browser page, not yet navigated |
| `download_dir` | `pathlib.Path` | Directory for file downloads |
| `start_date` | `str` | `"MM/DD/YYYY"` |
| `end_date` | `str` | `"MM/DD/YYYY"` |
| `url` | `str` | Portal URL from `county_sources.url` |
| `county_id` | `str` | County slug |
| `asyncio` | module | Available without import |
| `pd` | module | pandas — available without import |
| `re` | module | Available without import |
| `json` | module | Available without import |
| `Path` | class | `pathlib.Path` — available without import |

**Return value:**
- Rows found: `pd.DataFrame` with scraped data plus a `county_id` column
- Zero results: `pd.DataFrame()` (empty)
- Unrecoverable error: catch internally, log, return `pd.DataFrame()`

**Never return a DataFrame with integer column names.** If headers cannot be
extracted, return empty so the operator notices rather than silently loading garbage.

### Safety Validation

Before any function is stored or executed, `validate_playwright_code()` runs an AST
walk that rejects any use of:

```
__import__  open       exec       eval       compile
subprocess  shutil     requests   socket     urllib
__builtins__  globals  locals     vars       dir
getattr     setattr    delattr    breakpoint input
```

Additional rules enforced at validation time:
- No `import` statements anywhere in the function body
- Valid Python syntax (AST parse must succeed)
- Must define `async def run_scrape(...)`
- Must not exceed 8 000 characters

If any check fails, `PlaywrightCodeError` is raised and the code is never stored.

### How `playwright_code` Gets into the DB

**The only supported practice — human-authored code (ALL engines):**

```
1. Developer writes the function locally against the real portal.
2. Function passes a local smoke test (returns non-empty DataFrame).
3. Admin or developer inserts it directly:

   UPDATE county_sources
   SET playwright_code = '<function text>',
       playwright_code_approved = true
   WHERE county_id = '<county>' AND signal_type = '<type>';

4. No LLM involved. playwright_code_approved = true on insert.
```

**LLM code generation — DEFERRED (do not rely on it):**

A dormant auto-generation pathway still exists in `permit_engine._scrape_selector()`
(when `playwright_code IS NULL` it calls `generate_playwright_code()` → Claude →
AST validation → live smoke test → persist unapproved). **This pathway is deferred
from production use.** Repeated attempts produced inconsistent code and errors —
portal-specific details (masked inputs, UpdatePanel timing, session quirks) demand
context that only a human who has worked the portal can supply. The standing policy:

- All production `playwright_code` is written by a human, tested against the real
  portal, and inserted with `playwright_code_approved = true`.
- Do NOT leave a permit source with NULL `playwright_code` expecting generation to
  carry it — author the code, or set the source to an AI/browser-use mode instead.
- The generation utilities (`generate_playwright_code`, the rejection-feedback
  retry loop) remain in the codebase for potential future use but are not part of
  any operational workflow.

### Failure Cycle

When a portal updates its DOM and the stored code breaks:

```
Normal run:
  playwright_code present -> execute -> success -> no change

Portal DOM changes:
  playwright_code present -> execute -> PlaywrightCodeError
  -> clear_playwright_code() writes NULL to DB
  -> engine falls back (playwright_then_ai) or aborts (playwright_only)
  -> PlaywrightCodeHistory row records reason="cleared"

Subsequent runs:
  playwright_then_ai sources keep running via the browser-use AI agent until a
  human authors, tests, and re-inserts fresh playwright_code (approved=true).
  playwright_only sources stay down until the human fix lands.
```

LLM self-regeneration of cleared code is DEFERRED (see above) — the human is the
self-heal mechanism. Monitor `playwright_code_history` for `reason="cleared"`
events; each one is a to-do for a developer.

### nodriver API Quick Reference (`nodriver_only` / `nodriver_then_ai` sources)

`page` is a nodriver `Tab`, not a Playwright `Page` — method names and failure modes
differ. These gotchas are critical and must be followed exactly in any
`playwright_code` targeting a `nodriver_*` scrape mode:

**`select()` returns `None` on timeout — it does NOT raise:**
```python
el = await page.select("#someField", timeout=15)
if el is None:
    raise RuntimeError("PORTAL_LAYOUT_CHANGED - #someField not found")
```
Never call `.click()` / `.send_keys()` on a `select()` result without a `None` check
first — Playwright's `wait_for_selector` raises on timeout, nodriver's `select()`
just returns `None`, so the same guard pattern used elsewhere (`page.fill()`
throwing) does not apply here.

**Clearing an interactive Cloudflare Turnstile checkbox — text polling is not
enough:**
```python
challenge_markers = ("just a moment", "checking your browser", "verify you are human")
for attempt in range(5):
    title = await page.evaluate("document.title")
    body = await page.evaluate("document.body ? document.body.innerText.slice(0,500) : ''")
    text = (str(title) + " " + str(body)).lower()
    if not any(m in text for m in challenge_markers):
        break
    await page.verify_cf()   # real synthetic mouse click via CDP Input — requires
                              # opencv-python(-headless) installed for template matching
    await page.sleep(6)
```
A JS auto-challenge clearing (title stops saying "Just a moment...") is not the same
as an interactive checkbox being solved — always confirm by waiting for the actual
target element (e.g. via `select()`), not just the absence of challenge text.
`verify_cf()` silently no-ops if `opencv-python` isn't installed in the venv — it
raises nothing, so a missing dependency here looks identical to a portal that never
serves a checkbox at all unless you check for it.

**Distinguish a stale CF profile from a genuine code bug — raise, don't swallow:**
Unlike the general "catch internally, return `pd.DataFrame()`" guidance for
recoverable errors (Section 5, Return value), a `nodriver_*` source's `run_scrape`
should `raise RuntimeError("CF_CHALLENGE_NOT_CLEARED - ...")` when the Turnstile
checkbox never resolves, rather than returning an empty DataFrame. The engine's
nodriver launcher (`_scrape_with_nodriver` in `lien_engine.py`) specifically catches
this via the exception message and calls `cf_session_manager.mark_failed_during_scrape()`
to force a profile re-warm on the next run — without it, a stale profile silently
reports as "zero results today" indefinitely instead of self-healing. A plain
`pd.DataFrame()` return here would be indistinguishable from a legitimate no-data day.

**Downloads:** call `await page.set_download_path(str(download_dir))` once before
triggering any export click — nodriver has no `expect_download()` context manager
like Playwright; locate the new file afterward by diffing `download_dir.glob("*.csv")`
before/after the click.

### Accela .NET WebForms Patterns

Most active portals (Hillsborough, Pinellas) run on Accela Automation (ACA), an
ASP.NET WebForms application. These patterns are critical and must be followed
exactly in any `playwright_code` targeting an Accela portal:

**Date masked input — standard `fill()` silently fails:**
```python
digits = re.sub(r'[^0-9]', '', start_date)  # "06012026"
await page.click(start_sel)
await asyncio.sleep(0.2)
await page.keyboard.press("Home")
await asyncio.sleep(0.1)
for ch in digits:
    await page.keyboard.press(ch)
    await asyncio.sleep(0.08)
await page.keyboard.press("Tab")  # triggers blur/change to commit value
await asyncio.sleep(0.4)
```

**Waiting after search submit (UpdatePanel partial refresh):**
```python
await page.click(search_btn_sel)
try:
    await page.wait_for_selector("#divGlobalLoadingMask:not(.ACA_Hide)", timeout=8000)
except Exception:
    pass
try:
    await page.wait_for_selector("#divGlobalLoadingMask.ACA_Hide", timeout=30000)
except Exception:
    await asyncio.sleep(3)
```

**Pagination (UpdatePanel replaces table in place):**
```python
# Capture a marker from current page to detect DOM refresh
try:
    marker = await page.text_content('tr.ACA_TabRow_Odd:first-child td:nth-child(2)')
except Exception:
    marker = ""
# JS click bypasses ACA loading-mask overlay
await page.evaluate(
    "sel => { const el = document.querySelector(sel); if(el) el.click(); }",
    next_sel,
)
# Wait for content to change (proves UpdatePanel completed)
try:
    await page.wait_for_function(
        "(m) => { const el = document.querySelector('tr.ACA_TabRow_Odd td:nth-child(2)');"
        " return el && el.textContent.trim() !== m; }",
        marker, timeout=25000,
    )
except Exception:
    await asyncio.sleep(4)
```

**Detecting last page — check ALL conditions:**
```python
next_el = await page.query_selector(next_sel)
if not next_el:
    break
href = (await next_el.get_attribute("href")) or ""
if "__doPostBack" not in href:   # last page: href is "#" or empty
    break
cls = (await next_el.get_attribute("class")) or ""
if "disabled" in cls.lower():
    break
```

---

## 6. `special_flags` — Engine-Specific Config

`special_flags` is a `JSONB` column on `county_sources` for fine-grained config that
does not warrant a first-class DB column. All keys are spread into the source dict
by `county_config.py` before being handed to the engine.

**Load order (later values win on key collision):**

```
special_flags keys
  -> source_id, url, description, navigation_hint, output_format, date_range_available, frequency
     -> scrape_mode, playwright_code, playwright_code_version, playwright_code_approved
```

First-class columns always override any stale legacy key in `special_flags`.

### Known Keys by Engine

#### `permit_engine`

| Key | Type | Description |
|---|---|---|
| `permit_ai_strategy` | `str` | Sub-mode when top-level is `ai_only`: `download` (default), `extract`, `download_direct` |
| `download_url` | `str` | Full URL for `download_direct` mode |
| `download_method` | `str` | `GET` or `POST` (default `GET`) |
| `download_params` | `dict` | HTTP params; use `{start_date}` and `{end_date}` as placeholders |
| `download_date_format` | `str` | strftime format for param substitution (default `%Y-%m-%d`) |
| `download_headers` | `dict` | Extra HTTP headers for direct download |
| `selectors` | `dict` | CSS selector hints fed to LLM code generation prompt |

#### `pa_engine` (property appraiser)

| Key | Type | Description |
|---|---|---|
| `pa_scraper` | `str` | Which scraper class to use: `hcpa` (Hillsborough, default) or `pcpao` (Pinellas) |

#### `violation_engine`

| Key | Type | Description |
|---|---|---|
| `prr_only` | `bool` | If `true`, skip automated scraping — county requires a manual public-records request |

#### `lien_engine` (signal_type `liens`)

| Key | Type | Description |
|---|---|---|
| `cf_bypass_required` | `bool` | If `true`, engine warms/validates a persistent browser profile via `cf_session_manager.ensure_ready()` before scraping, and `nodriver_only`/`nodriver_then_ai` become usable for this source (see Section 4) |
| `cf_bypass_profile_name` | `str` | Profile identity in `cf_bypass_profiles` (default `f"{county_id}_clerk"`) |
| `prr_only` | `bool` | If `true`, skip automated scraping — county requires a manual public-records request |

#### `master_engine` (signal_type `master_data`)

| Key | Type | Description |
|---|---|---|
| `bulk_tables` | `list[str]` | Multi-table counties only (Pinellas): names of the bulk files to download; the engine locates each file by name after the download phase. Counties without this key follow the single-file path (download → detect format → convert). |
| `primary_table` | `str` | **PLANNED** — the parcel-shaped table handed to the loader when `bulk_tables` has more than one entry. See `docs/MASTER_SUPPLEMENTAL_TABLES_PLAN.md`. |
| `merge_tables` | `dict` | **PLANNED** — supplemental tables merged onto the primary by join key before loading (e.g., Pinellas `RP_SALES_HISTORY` filtered to `IMPORTANCE=1` for latest-sale data). See `docs/MASTER_SUPPLEMENTAL_TABLES_PLAN.md`. |

Notes on the master pipeline (weekly, fa077):
- The download phase is a browser-use AI agent whose task prompt is generated from
  the source row's `description` + `navigation_hint` + `bulk_tables` — when a
  county must fetch multiple files, those fields must name every file explicitly.
- The loader performs hash-based change detection: new parcels insert; existing
  parcels update via set-based SQL only when their source row actually changed;
  unchanged parcels get `last_seen_at` stamped. Downstream flags
  (`properties.needs_rescore`, `sync_status='pending_sync'`,
  `owners.skip_trace_stale`) fire only on real field drift. No deletes — parcels
  absent from the file simply stop receiving `last_seen_at`.
- Owner-name identity comparisons are punctuation/whitespace-insensitive, so a
  portal reformatting names cannot mass-flag stale traces or rescores.
- Rows with a parcel but no owner are quarantined to `unmatched_records`
  (`source_type='master_data'`, parcel id as `instrument_number`). Owner names
  carrying entity markers (LLC, TRUST, COUNTY, CHURCH...) are valid owners even
  when they look address-shaped.
- `property_type`: a county-provided label column (`TYPE`) wins; counties that
  publish only the bare DOR use code (`DOR_C`) get the statewide translation in
  `src/loaders/dor_use_codes.py` ("0100 Single Family" — Pinellas-compatible
  format, valid for every Florida county).

---

## 7. Column Mapping

Raw portal column names differ by county and portal version. The `ColumnMapper`
normalizes them to a canonical schema that loaders expect.

### Canonical Schemas (`SIGNAL_SCHEMAS` in `column_mapper.py`)

Every signal type has a defined canonical column list. Engines load data matching
these names. Examples:

**`violations`:**
`Record Number`, `Date`, `Record Type`, `Description`, `Status`, `Address`,
`Short Notes`, `Fine Amount`, `Is Lien`

**`permits`:**
`Record Number`, `Date`, `Record Type`, `Status`, `Address`, `Expiration Date`,
`Description`, `Action`, `Project Name`

**`master_data`** (canonical names the master loader consumes — map raw portal
columns to these):
`FOLIO`, `OWNER`, `SITE_ADDR`, `SITE_CITY`, `SITE_ZIP`, `TYPE`, `DOR_C`,
`LEGAL1`-`LEGAL4`, `HEAT_AR`, `ADDR_1`, `CITY`, `STATE`, `ZIP`, `ASD_VAL`,
`TAX_VAL`, `ACREAGE`, `YR_BLT` (aliases: `YEAR_BUILT`, `YR_BUILT`), `tBEDS`,
`tBATHS`, `SALE1_DATE` (aliases: `SALE_DATE`, `SALESDATE`), `SALE1_PRC`
(aliases: `SALE_PRC`, `SALESPRICE`)

Both counties have master_data mappings in the DB: Hillsborough (mostly identity
plus `TBEDS→tBEDS`, `TBATHS→tBATHS`, `S_DATE→SALE1_DATE`, `S_AMT→SALE1_PRC`,
`ACT→YR_BLT`) and Pinellas (full rename set, e.g. `PARCEL_NUMBER→FOLIO`,
`OWNER1→OWNER`, `TOTAL_LIVING_SQFT→HEAT_AR`, `CNTY_ASD_VALUE→ASD_VAL`).
Lesson learned the hard way: an unmapped canonical column fails SILENTLY (the
parser stores NULL) — Hillsborough ran for months with beds/baths/year_built/
sale data all NULL because of case/name mismatches. When adding a county, verify
every canonical column above is either mapped or confirmed absent at source.

### Resolution Order

```
1. Approved CountyColumnMapping with >=50% column overlap -> use directly
2. Pending (LLM-proposed, not yet approved) mapping with >=50% overlap ->
   apply optimistically while admin review is outstanding
3. No usable mapping -> call LLM -> save as pending (is_approved=false)
4. LLM fails -> raise NeedsMappingError -> admin must create mapping manually
```

The 50% overlap check prevents applying a mapping built against an old portal
column set when the portal has been updated.

### Mapping Row Features

**`mapping`** — basic rename dict:
```json
{
  "Filed Date": "Date",
  "Application Number": "Record Number",
  "Property Address": "Address"
}
```

**`post_processors`** — applied after rename. Currently supported op:
```json
[{"op": "split_on_separator", "from": "BookPage", "sep": "/", "into": ["Book", "Page"]}]
```

**`value_maps`** — per-column value normalization:
```json
{
  "DocType": {
    "JUDGEMENT": "JUDGMENT",
    "LIEN (IRS)": "TAX LIEN"
  }
}
```

**`row_routing`** — split rows into downstream signal buckets (used by liens ORI
export which contains deeds, judgments, probate, and liens in one file):
```json
{
  "column": "DocType",
  "default": "skip",
  "rules": [
    {"match_exact": ["DEED", "TAX DEED"], "bucket": "deeds"},
    {"match_contains": ["LIS PENDENS"], "bucket": "liens"},
    {"match_exact": ["JUDGMENT"], "bucket": "liens"}
  ]
}
```

### In-Engine Alias Fallback

For `violations` and `permits`, each engine defines a `COLUMN_ALIASES` dict as a
local fallback when no DB mapping exists yet. This handles common Accela header
variants without requiring a full ColumnMapper round-trip. Example:

```python
COLUMN_ALIASES = {
    "Date": ["Date", "Filed Date", "Opened Date", "Application Date"],
    "Record Number": ["Record Number", "Application Number", "Case Number"],
    "Address": ["Address", "Location", "Property Address"],
}
```

When a DB mapping exists it takes precedence; `COLUMN_ALIASES` applies to whatever
columns remain unmapped.

---

## 8. How to Add a New County

No application code needs to change. The following DB operations are all that is
required.

### Step 1 — Insert the county row

```sql
INSERT INTO counties (county_id, display_name, fips, nws_zone, parcel_id_format, is_active)
VALUES ('sarasota', 'Sarasota County', '12115', 'FLZ043', 'folio', true);
```

### Step 2 — Insert source rows

For each data type you want to scrape, insert one `county_sources` row.

**Violations on Accela — pre-tested playwright code (preferred):**

```sql
INSERT INTO county_sources (
  county_id, signal_type, source_name, url,
  description, navigation_hint,
  scrape_mode, playwright_code, playwright_code_approved,
  is_active
) VALUES (
  'sarasota', 'violations', 'Sarasota Accela Enforcement',
  'https://sarasota.portal.civicplus.com/CitizenAccess/',
  'Sarasota code enforcement violations via Accela ACA portal',
  'Click the Code Enforcement module tab before searching.',
  'playwright_then_ai',
  '<paste the tested run_scrape function here>',
  true,
  true
);
```

**Violations — no pre-tested code yet (AI fallback only):**

```sql
INSERT INTO county_sources (
  county_id, signal_type, source_name, url,
  description, navigation_hint,
  scrape_mode, is_active
) VALUES (
  'sarasota', 'violations', 'Sarasota Accela Enforcement',
  'https://sarasota.portal.civicplus.com/CitizenAccess/',
  'Sarasota code enforcement violations via Accela ACA portal.',
  'Click the Code Enforcement module tab. Search by filed date.',
  'ai_only',
  true
);
```

**Property appraiser — playwright_only with a county-specific scraper class:**

```sql
INSERT INTO county_sources (
  county_id, signal_type, source_name, url,
  scrape_mode, special_flags, is_active
) VALUES (
  'sarasota', 'property_appraiser', 'Sarasota County PA',
  'https://www.sc-pa.com/propertysearch/parcel/',
  'playwright_only',
  '{"pa_scraper": "pcpao"}',
  true
);
```

**Permits — static direct download:**

```sql
INSERT INTO county_sources (
  county_id, signal_type, source_name, url,
  scrape_mode, special_flags, is_active
) VALUES (
  'sarasota', 'permits', 'Sarasota Permits Direct Download',
  'https://permits.sarasotacountyfl.gov/',
  'static_download',
  '{
    "download_url": "https://permits.sarasotacountyfl.gov/export/permits.csv",
    "download_method": "GET",
    "download_params": {"startDate": "{start_date}", "endDate": "{end_date}"},
    "download_date_format": "%m/%d/%Y"
  }',
  true
);
```

### Step 3 — Develop and store `playwright_code`

For `playwright_only` or `playwright_then_ai` sources:

1. Develop the function locally against the real portal (headful browser, print
   statements allowed during development).
2. Run the validation check:
   ```python
   from src.utils.action_sequence import validate_playwright_code
   code = open("my_scraper.py").read()
   validate_playwright_code(code)  # raises PlaywrightCodeError if unsafe
   ```
3. Run one dry-run end-to-end test:
   ```bash
   python -m src.scrappers.violation.violation_engine \
     --county-id sarasota --start-date 2026-06-01 --end-date 2026-06-02 --headful
   ```
4. If results look correct, store with approval:
   ```sql
   UPDATE county_sources
   SET playwright_code = '<function body>',
       playwright_code_approved = true
   WHERE county_id = 'sarasota' AND signal_type = 'violations';
   ```

### Step 4 — Run first load and approve the column mapping

```bash
python -m src.scrappers.violation.violation_engine \
  --county-id sarasota --start-date 2026-06-01 --end-date 2026-06-07 --load-to-db
```

If no approved `CountyColumnMapping` exists, `ColumnMapper` will:
1. Call Claude to propose a mapping
2. Save it as `is_approved=false`
3. Apply it optimistically for this run

Review the pending mapping at `/api/admin/mappings/pending` and approve or reject
with feedback. Once approved, it is used on every subsequent run without any LLM call.

### Step 5 — Add the county to cron

Follow the hard stagger from `CLAUDE.md` (scrapers 04:00–06:30, CDS 07:00):

```bash
30 4 * * * cd /opt/fa && python -m src.scrappers.violation.violation_engine \
  --county-id sarasota --load-to-db >> logs/cron.log 2>&1
```

---

## 9. Engine Dispatch Reference

### `violation_engine.py`

```
scrape_mode
  playwright_only / playwright_then_ai
    -> _scrape_with_playwright(playwright_code, source, ...)
         calls execute_playwright_code() from action_sequence.py
         returns raw DataFrame
    -> _normalize_columns(rows, source)
         ColumnMapper lookup -> COLUMN_ALIASES fallback
    playwright_only:   no data returned -> abort
    playwright_then_ai: no data returned -> fall through to AI agent

  ai_only (or playwright_then_ai fallback)
    -> build_agent_task(source, start_str, end_str)
         LLM generates step-by-step browser-use task instructions
         from source.description + source.navigation_hint
    -> run_browser_agent(task, headful)
         browser-use Agent runs Chromium, returns history
    -> _parse_agent_result(history.final_result())
         accepts JSON array / markdown fences / pipe-delimited fallback
    -> _normalize_columns(rows, source)
```

**special_flags consumed:** `prr_only`

---

### `permit_engine.py`

The permit engine introduces an internal sub-mode layer via `_get_scrape_mode()`:

```
_get_scrape_mode(source):
  top-level playwright_only / playwright_then_ai -> returns "selector"
  ai_only / unset -> reads source["permit_ai_strategy"] -> "download" (default)

scrape_mode == "selector"
  playwright_code present (the supported path — human-authored, approved)
    -> _scrape_selector() -> execute_playwright_code() -> DataFrame
    playwright_code_approved=false -> warning logged, execution proceeds
  playwright_code absent
    -> DORMANT auto-generation path fires (generate_playwright_code -> AST
       validation -> live smoke test -> persist unapproved). DEFERRED from
       production use — do not configure sources to depend on it; author the
       code by hand (see Section 5).
  execution fails
    -> clear_playwright_code() [DB NULL]
    -> fall back to browser_use download mode
    -> a human must author and re-insert fresh code (no auto-regeneration)

scrape_mode == "download_direct"
  -> _scrape_download_direct(source, start_dt, end_dt, download_dir)
       reads download_url, download_params, download_method, download_headers

scrape_mode == "extract"
  -> browser-use Agent reads table rows directly, paginates, returns JSON

scrape_mode == "download" (default)
  -> browser-use Agent clicks export button, waits for file download
```

**special_flags consumed:** `permit_ai_strategy`, `download_url`, `download_params`,
`download_method`, `download_date_format`, `download_headers`, `selectors`

---

### `pa_engine.py`

PA engine uses `ThreadPoolExecutor` (sync Playwright per thread) rather than async,
because each property scrape is independent and benefits from true parallelism.

```
pa_config = county_config["sources"]["property_appraiser"]

ThreadPoolExecutor(max_workers=5) per batch of properties
  |
  +- _scrape_and_parse(prop, pa_config, county_id, ...)
       pa_variant = pa_config.get("pa_scraper", "hcpa")

       _SCRAPER_MAP = {
         "hcpa":  (HCPAScraper,  parse_hcpa_page),
         "pcpao": (PCPAOScraper, parse_pcpao_page),
       }

       scraper_cls, page_parser = _SCRAPER_MAP[pa_variant]
       with scraper_cls(config, headful=headful) as scraper:
           raw = scraper.scrape_property(parcel_id)
       df = to_canonical_dataframe(hcpa_data, trim_data, parcel_id)
```

PA engine does **not** use `playwright_code` or `execute_playwright_code()`. The
scraper classes (`HCPAScraper`, `PCPAOScraper`) are standalone sync Playwright wrappers
in `pa_scraper.py`. Adding a PA source for a new county means either:
- Pointing `pa_scraper` to an existing scraper class if the portal is the same type
- Writing a new scraper class with a `scrape_property(parcel_id)` method

**special_flags consumed:** `pa_scraper`

---

### `lien_engine.py` (signal_type `liens`)

```
cf_required = source["cf_bypass_required"]  (bool, from special_flags)

if cf_required:
  cf_session_manager.ensure_ready(profile_name, county_id, portal_url)
    -> warms/validates the persistent browser profile (nodriver-driven warm/
       validate — see docs/PINELLAS_CLOUDFLARE_BYPASS.md), returns profile_dir
  find_edge_binary() -> cf_profile = {edge_path, profile_dir}
  (raises CFBypassFailedError -> abort with cf_bypass_failed error, if the
   profile can't be made ready)

scrape_mode
  nodriver_only / nodriver_then_ai   (requires cf_required=true + playwright_code)
    -> _scrape_with_nodriver(playwright_code, source, ..., cf_profile)
         launches nodriver against the warmed profile (no Turnstile-solving
         logic here — that lives in the stored playwright_code, see Section 5's
         nodriver quick reference)
         -> execute_playwright_code(code, page=<nodriver Tab>, ...) [driver-agnostic]
    on PlaywrightCodeError containing "CF_CHALLENGE_NOT_CLEARED":
         cf_session_manager.mark_failed_during_scrape() — profile re-warms next run;
         playwright_code is NOT cleared (the code isn't the problem)
    on any other PlaywrightCodeError:
         clear_playwright_code() [DB NULL] — same documented failure cycle as
         Section 5; a playwright_code_history row records reason="cleared"
    nodriver_only:     no data / error -> abort
    nodriver_then_ai:  no data / error -> fall through to AI agent

  playwright_only / playwright_then_ai  (requires playwright_code)
    -> _scrape_with_playwright(playwright_code, source, ..., cf_profile)
         calls execute_playwright_code() from action_sequence.py
    playwright_only:     no data / error -> abort
    playwright_then_ai:  no data / error -> fall through to AI agent

  ai_only (or *_then_ai fallback)
    -> build_agent_task(source, start_str, end_str)
         LLM generates step-by-step browser-use task instructions
    -> run_browser_agent(task, headful, cf_profile)
         browser-use Agent runs Chromium (bundled, NOT the warmed CF profile —
         AI fallback for a CF-protected source has no special bypass and may
         itself get challenged; it is a last resort, not a guaranteed recovery)
    -> process_lien_data(downloaded_file) -> raw DataFrame

ColumnMapper.get_or_create("liens", source_id, df) -> mapping_row
ColumnMapper.apply_transformations(df, mapping_row)
  -> row_routing splits the ORI export into liens / deeds / judgments buckets
  -> _sub_categorise_liens() further splits the liens bucket into
     HOA / TAX / MECHANICS / CODE LIEN labels via county_cfg filer keywords
```

**special_flags consumed:** `cf_bypass_required`, `cf_bypass_profile_name`, `prr_only`

---

### `master_engine.py` (signal_type `master_data`)

Weekly bulk parcel refresh (Sunday 01:00/02:00 UTC per county). Unlike the daily
signal engines it is download-only on the scrape side (no playwright_code) and
does its heavy lifting in the loader:

```
Phase 1 — Download (skippable via --skip-download)
  build_agent_task(source) -> LLM generates browser-use task from
  description + navigation_hint (+ bulk_tables for multi-table counties)
  -> browser-use Agent downloads bulk file(s) to data/reference/<county>/

Phase 2 — Discover + normalize
  single-file county (Hillsborough): detect format by magic bytes
    (CSV / XLS / XLSX / DBF, plain or zipped) -> convert to CSV
  multi-table county (Pinellas): locate each bulk_tables entry
  [PLANNED: merge_tables supplements joined onto primary_table here —
   see docs/MASTER_SUPPLEMENTAL_TABLES_PLAN.md]

Phase 3 — Load (MasterPropertyLoader.load_from_csv)
  encoding detection (utf-8 -> cp1252 fallback)
  -> ColumnMapper -> per-row parse + md5 hash of canonical fields
  -> partition: NEW (insert) / CHANGED (stage -> set-based UPDATE)
              / UNCHANGED (last_seen_at stamp only)
  -> downstream flags on real drift only: needs_rescore, pending_sync
     (already-synced rows), skip_trace_stale (owner change w/ trace data)
  -> no-owner rows quarantined to unmatched_records
  --dry-run: full classification + real counts, rolled back
```

CLI: `--county-id`, `--skip-download`, `--load-to-db`, `--dry-run`,
`--chunk-size`, `--headful`. Phase timings stream to
`logs/timing/master_loader.jsonl` (TimeTracker).

**special_flags consumed:** `bulk_tables` (+ planned `primary_table`,
`merge_tables`)

---

### `county_config.py` — Config Shape

```python
county_config = get_county_config("hillsborough")
# Returns:
{
  "county_id": "hillsborough",
  "display_name": "Hillsborough County",
  "nws_zone": "FLZ151,FLZ251",
  "nws_zones": ["FLZ151", "FLZ251"],
  "sources": {
    "violations": {
      # special_flags spread first (lower priority)
      "prr_only": False,
      # first-class columns (higher priority, override special_flags)
      "source_id": 12,
      "url": "https://aca-prod.accela.com/HILLSBOROUGH/",
      "scrape_mode": "playwright_then_ai",
      "playwright_code": "async def run_scrape(...): ...",
      "playwright_code_approved": True,
    },
    "permits":             { ... },
    "property_appraiser":  { ... },
  }
}
```

Cache TTL: 5 minutes. Call `invalidate_cache(county_id)` after admin edits to force
an immediate reload without a service restart.

---

## 10. Risk Factors and Testing Checklist

### Playwright Code Risks

| Risk | Mitigation |
|---|---|
| Code uses forbidden builtins (`requests`, `subprocess`, etc.) | `validate_playwright_code()` AST walk blocks these at storage time |
| Accela date input silently ignored | Use digit-by-digit `keyboard.press()` — never `page.fill()` on masked inputs |
| UpdatePanel navigation race condition | Wait for `#divGlobalLoadingMask.ACA_Hide` and page content change marker |
| Infinite pagination loop | Include a `max_page` guard (e.g., `if page_num > 200: break`) |
| DataFrame with integer column names loads silently | Return `pd.DataFrame()` if headers cannot be extracted; the operator will see zero records |
| Portal DOM change breaks selector | Engine catches `PlaywrightCodeError`, clears code from DB, falls back or aborts |
| `playwright_code_approved=false` in production | Engine logs WARNING but does not block; ensure approval before enabling cron |

**Required tests before storing `playwright_code`:**

- [ ] Date range populates correctly — inspect input value after digit-by-digit entry
- [ ] Search submits and loading mask appears then disappears
- [ ] At least one page of results is returned for a known date range
- [ ] Pagination advances past page 1 (if portal has multi-page results)
- [ ] Returned DataFrame has named (non-integer) columns
- [ ] `county_id` column is present in the returned DataFrame
- [ ] An empty date range returns `pd.DataFrame()`, not an error

### Column Mapping Risks

| Risk | Mitigation |
|---|---|
| LLM maps a column to a wrong canonical name | Admin reviews pending mapping before approving |
| Portal updates add or rename columns; overlap drops below 50% | New LLM call triggered automatically; prior rejection feedback is injected |
| Different counties have different column names for same signal | Each `county_source` row has its own independent `CountyColumnMapping` |
| Stale approved mapping after portal column rename | 50% overlap check causes fallback to pending or re-generation |

**Required tests for column mapping:**

- [ ] Dry-run produces a pending mapping with the correct `source_columns` list
- [ ] All canonical columns required by the loader are covered by the mapping
- [ ] After approval, a second run uses the approved mapping without an LLM call
- [ ] Rejecting with feedback triggers a new LLM attempt that references the rejection note

### Performance Risks

| Risk | Mitigation |
|---|---|
| PA engine with large property set stalls event loop | `ThreadPoolExecutor` is isolated from the async loop — no blocking |
| Playwright browser leak on exception | `browser.close()` is always in a `finally` block |
| All counties cron at the same time | Stagger: scrapers 04:00–06:30 UTC, CDS 07:00, skip-trace 07:30, GHL sync 08:00 |
| Wide date range creates a huge download | Default to 1-day windows in cron; use wider ranges for backfill runs only |

### Operational Risks

| Risk | Mitigation |
|---|---|
| `county_sources` row missing for a signal type | Engines log WARNING when `sources.get("violations")` is None; monitor scraper stats |
| Config cache stale after admin edit | Admin PATCH endpoint must call `invalidate_cache(county_id)` |
| New `special_flags` key absent on existing rows | Engines must use `source.get("key", default)` not `source["key"]` |
| Source left dependent on deferred LLM code generation | LLM generation is DEFERRED platform-wide (inconsistent output; portals demand human context). All engines require human-authored, pre-approved code; the dormant permit-engine pathway must not be relied upon |
| Canonical column silently unmapped for master_data | Parser stores NULL with no error — after a county's first master load, verify field coverage (`count(col)` per canonical column) instead of trusting a clean run |

---

## 11. Admin API Reference

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/admin/counties` | List all counties |
| `POST` | `/api/admin/counties` | Create new county |
| `GET` | `/api/admin/counties/{id}/sources` | List sources for a county |
| `POST` | `/api/admin/counties/{id}/sources` | Create new source |
| `PATCH` | `/api/admin/counties/{id}/sources/{src_id}` | Update source config (scrape_mode, special_flags, playwright_code, etc.) |
| `GET` | `/api/admin/mappings/pending` | List unapproved column mappings awaiting review |
| `POST` | `/api/admin/mappings/{id}/approve` | Approve a pending mapping |
| `POST` | `/api/admin/mappings/{id}/reject` | Reject with feedback (triggers LLM retry on next run) |
| `POST` | `/api/admin/mappings/manual` | Upload a hand-crafted mapping CSV |
| `GET` | `/api/admin/sources/{src_id}/playwright-history` | View playwright_code audit history for a source |

---

*For questions about a specific scraper's behavior, refer to the engine file itself —
`src/scrappers/<type>/<type>_engine.py`. The module docstring describes the supported
modes and CLI flags for that engine.*
