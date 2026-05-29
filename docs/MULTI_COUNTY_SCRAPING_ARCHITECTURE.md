# Multi-County Scraping Architecture

**Goal:** Add any new county by configuring the Admin UI only. Zero new Python files.
Zero DB migrations. Every engine reads county config at runtime and routes to the
correct scraping strategy automatically.

**Companion docs:**
- `COUNTY_EXPANSION_BUILD_PLAN.md` — overall multi-county task tracker
- `PINELLAS_EXPANSION_RESEARCH.md` — Pinellas-specific source analysis

---

## Current State

| Component | Status | Notes |
|---|---|---|
| DB-backed county config (`County`, `CountySource`) | ✅ Done | Admin UI CRUD at `/admin` |
| Parcel ID normalizer — folio + STRAP | ✅ Done | `src/utils/parcel_id.py` |
| ColumnMapper middleware — LLM auto-map + admin approval | ✅ Done | `src/loaders/column_mapper.py` |
| Master engine — county-agnostic, browser-use + dynamic LLM prompt | ✅ Done | `src/scrappers/master/master_engine.py` |
| Master loader — ColumnMapper wired, county-scoped | ✅ Done | `src/loaders/master.py` |
| All signal scrapers — `--county-id` flag, `get_county_config()` | ✅ Done | Every engine under `src/scrappers/` |
| BaseLoader — all matching strategies scoped to `county_id` | ✅ Done | `src/loaders/base.py` |
| CDS engine — `--county-id` flag, county-scoped property load | ✅ Done | `src/services/cds_engine.py` |
| API — county filter on all lead feed endpoints | ✅ Done | `src/api/main.py` |
| Three-mode scrape routing (`playwright_only`, `playwright_then_ai`, `ai_only`) | ✅ Done | Per-engine via `source["scrape_mode"]` |
| Playwright code generation, validation, caching, self-healing | ✅ Done | `src/utils/action_sequence.py` |
| Admin UI — county + source CRUD, scrape mode config | ✅ Done | `CountyManagementDashboard.jsx` |

---

## Three Scrape Modes

Each `CountySource` row has a `scrape_mode` column that picks one of three strategies:

```
scrape_mode: "playwright_only"      — run cached Playwright code; error on failure (no fallback)
scrape_mode: "playwright_then_ai"   — run cached Playwright code; fall back to browser-use on failure
scrape_mode: "ai_only"              — run browser-use Agent directly (LLM-driven, no cached code)
```

Engines read the mode from county config and dispatch accordingly:

```python
source = get_county_config(county_id)["sources"]["permits"]
mode = source.get("scrape_mode")   # "playwright_only" | "playwright_then_ai" | "ai_only"
```

### Mode 1 — `playwright_only`

Executes pre-cached async Python/Playwright code stored in `CountySource.special_flags["playwright_code"]`.

- No fallback. If the code raises, the engine errors and exits.
- Use this when the portal is stable and the code has been reviewed and approved.
- Warning logged on each run if `playwright_code_approved == False`.

### Mode 2 — `playwright_then_ai`

Attempts the cached Playwright code first. On failure, clears the code from DB
and falls back to browser-use Agent for that run.

- **Self-healing**: the next run will regenerate Playwright code from scratch.
- Use this when rolling out new code — stays operational if the code breaks.
- Best default when Playwright code exists but hasn't been battle-tested yet.

### Mode 3 — `ai_only`

Skips cached code entirely and drives the portal via browser-use Agent (LLM + Chromium).

- LLM receives the source URL, description, and navigation hint from county config.
- LLM generates plain-English browser task instructions (not code).
- browser-use Agent executes with a max of 80–120 steps.
- Falls back to a template task if LLM task-generation fails.
- ~4 minutes per run, ~$0.08–0.20 in LLM costs.
- Use for CAPTCHA-heavy or JS-heavy portals where Playwright is not viable.

#### AI sub-modes (`permit_ai_strategy`)

Permit engine has additional sub-mode routing for AI path behavior:

| `permit_ai_strategy` | What happens |
|---|---|
| `download_direct` | Direct HTTP GET/POST to `source.download_url` — no browser required |
| `download` (default) | browser-use Agent clicks export, waits for file download |
| `extract` | browser-use Agent reads the table as JSON; no file download |

---

## Playwright Code: Generation, Caching, and Approval

**Location:** `src/utils/action_sequence.py`

When `scrape_mode` is `playwright_only` or `playwright_then_ai` and no code is cached yet
(first run or after a self-heal clear), the engine auto-generates code via Claude.

### Generation Flow

1. Claude receives a system prompt describing:
   - Exact required function signature: `async def run_scrape(page, download_dir, start_date, end_date, url, county_id)`
   - Available scope: `asyncio`, `pd`, `re`, `json`, `Path` (pre-imported in exec namespace)
   - Security blocklist: `__import__`, `open`, `exec`, `eval`, etc.
   - Portal-specific patterns (e.g. Accela `.NET` WebForms, masked date inputs, UpdatePanel pagination)
2. LLM returns an async function (max 160 lines, markdown stripped).
3. Code is validated:
   - Syntax check via `ast.parse()`
   - AST walk to block forbidden names
   - Must define `async def run_scrape(...)`
4. Code is smoke-tested against a live browser session.
5. On success → persisted to `CountySource.special_flags["playwright_code"]` with `playwright_code_approved: false`.
6. History row written to `playwright_code_history` table.

### Execution Flow

```python
# Engine reads cached code from source config
code = source["playwright_code"]

# Execution namespace — only permitted names available
exec_namespace = {"__builtins__": builtins, "asyncio": asyncio, "pd": pd, ...}
exec(compile(code, "<generated>", "exec"), exec_namespace)

# Run with runtime placeholders substituted
await exec_namespace["run_scrape"](page, download_dir, start_date, end_date, url, county_id)
```

### Self-Healing (playwright_then_ai only)

On code execution failure:
1. `clear_playwright_code(county_id, source_id)` — removes code from DB
2. History row logged with `reason="cleared"`
3. Engine falls through to browser-use Agent for this run
4. Next run regenerates code from scratch

### Admin Approval

After a human reviews the generated code:
```
Admin UI → County → Source → "Approve Playwright Code"
```
This calls `approve_playwright_code(county_id, source_id, approved_by="admin")`, which
flips `playwright_code_approved` to `True`. Suppresses the per-run warning.

---

## Config Shape

All per-source settings live in `CountySource.special_flags` JSONB, spread into the
source dict by `county_config._load_from_db()`.

### playwright_only / playwright_then_ai

```json
{
  "scrape_mode": "playwright_only",
  "playwright_code": "async def run_scrape(page, download_dir, start_date, end_date, url, county_id):\n    ...",
  "playwright_code_version": "v1",
  "playwright_code_approved": false
}
```

`playwright_code` is written by the engine (not admin). Admin only sets `scrape_mode` and
approves the generated code via the UI.

### ai_only (browser-use with navigation hint)

```json
{
  "scrape_mode": "ai_only",
  "navigation_hint": "Go to Building module → search by date range → Export to Spreadsheet"
}
```

`navigation_hint` is a first-class column on `CountySource` — no `special_flags` entry
needed. The engine composes the LLM task from `url + description + navigation_hint`.

### ai_only with direct download (permit sub-mode)

```json
{
  "scrape_mode": "ai_only",
  "permit_ai_strategy": "download_direct",
  "download_url": "https://portal.example.com/api/permits/export",
  "download_params": { "startDate": "{start_date}", "endDate": "{end_date}", "format": "csv" },
  "download_date_format": "%Y-%m-%d"
}
```

`{start_date}` and `{end_date}` are substituted at runtime. No browser launched.

### Special flags reference

| Key | Type | Purpose |
|---|---|---|
| `scrape_mode` | str | `playwright_only` \| `playwright_then_ai` \| `ai_only` |
| `playwright_code` | str | Cached async function body (engine-managed, not admin-set) |
| `playwright_code_version` | str | Prompt version used to generate the code |
| `playwright_code_approved` | bool | Admin has reviewed; suppresses per-run warning |
| `permit_ai_strategy` | str | Permit engine sub-mode: `download_direct` \| `extract` \| `download` |
| `prr_only` | bool | Portal requires manual PRR upload — engine logs skip and exits |
| `cf_bypass_required` | bool | Portal is behind Cloudflare; use persistent Edge profile |
| `bulk_tables` | list | Master engine: list of bulk CSV filenames to download |
| `download_url` | str | Direct HTTP endpoint for `download_direct` sub-mode |
| `download_params` | dict | Query params or POST body for `download_direct` |
| `download_date_format` | str | strftime format for date placeholders in `download_params` |
| `ori_column_map` | dict | Lien engine: `{"DirectName": "Grantor", ...}` |
| `ori_book_page_col` | str | Lien engine: column to split into Book + Page |
| `ori_doc_type_map` | dict | Lien engine: doc type normalization map |
| `navigation_hint` | str | Passed to LLM for browser-use task generation |

---

## County Config System

**Location:** `src/utils/county_config.py`

5-minute in-memory cache. Primary interface:

```python
cfg = get_county_config(county_id)  # dict
source = cfg["sources"]["permits"]   # per-signal source dict
```

Full config shape:

```python
{
    "county_id": "hillsborough",
    "name": "Hillsborough County",
    "fips": "12057",
    "nws_zone": "FLC057",
    "parcel_id_format": "folio",        # "folio" | "strap"
    "bankruptcy_division": "8:",
    "city_filer_keywords": [...],
    "code_lien_type_map": {...},
    "sources": {
        "foreclosures": {
            "source_id": 1,
            "url": "https://...",
            "description": "...",
            "navigation_hint": "...",
            "output_format": "csv",
            "scrape_mode": "playwright_then_ai",
            "playwright_code": "async def run_scrape(...): ...",
            "playwright_code_approved": False,
            # + any other special_flags keys spread in
        },
        "permits": { ... },
        "violations": { ... },
        "liens": { ... },
        # foreclosures | permits | violations | liens | court_records |
        # tax_delinquency | master_data | fire | flood | storm | ...
    }
}
```

Cache invalidation: `invalidate_cache(county_id)` — clears one county or all.

---

## Engine Pattern

Every scraper engine follows the same structure:

```
src/scrappers/
  bankruptcy/      bankruptcy_engine.py
  dbpr/            dbpr_engine.py
  deliquencies/    tax_delinquent_engine.py
  divorce/         divorce_engine.py
  evictions/       evictions_engine.py
  fire/            fire_engine.py
  flood/           flood_engine.py
  foreclosures/    foreclosure_engine.py
  insurance/       insurance_engine.py
  liens/           lien_engine.py
  master/          master_engine.py
  permit/          permit_engine.py
  probate/         probate_engine.py
  roofing_permits/ roofing_permit_engine.py
  storm/           storm_engine.py
  sunbiz/          sunbiz_engine.py
  violation/       violation_engine.py
```

Each engine:
1. Parses `--county-id` CLI arg
2. Calls `get_county_config(county_id)` → reads source for its signal type
3. Dispatches to the correct mode via `source["scrape_mode"]`
4. Saves output CSV to `data/raw/{signal}/{county_id}/new/`
5. Optionally loads to DB via its `*Loader` class (triggered by `--load-to-db`)
6. Optionally rescores affected properties via `cds_engine`

---

## Routing Pattern (Permit Engine as Reference)

```python
def _get_scrape_mode(source: dict) -> str:
    top_level = source.get("scrape_mode")
    if top_level in ("playwright_only", "playwright_then_ai"):
        return "selector"   # use cached Playwright code
    # ai_only path — route by permit sub-mode
    return source.get("permit_ai_strategy") or "download"


async def run_permit_pipeline(county_id, start_date, end_date, headful):
    source = get_county_config(county_id)["sources"]["permits"]
    mode = _get_scrape_mode(source)

    if mode == "download_direct":
        df = _scrape_download_direct(source, start_date, end_date)

    elif mode == "selector":
        df = await _scrape_selector(source, start_date, end_date, download_dir, county_id)
        # _scrape_selector: exec()s cached playwright_code; on failure (playwright_then_ai):
        #   clear_playwright_code() → fall through to browser-use

    elif mode == "extract":
        df = await _scrape_browser_use_extract(source, start_date, end_date, headful)

    else:  # "download" (default ai_only path)
        df = await _scrape_browser_use_download(source, start_date, end_date, headful)
```

---

## Adding a New County

### Step 1 — Admin UI: Add County Row

```
county_id:           manatee
display_name:        Manatee County
fips:                12081
nws_zone:            FLZ039
parcel_id_format:    folio
bankruptcy_division: 8
city_filer_keywords: MANATEE COUNTY, CITY OF BRADENTON
```

### Step 2 — Admin UI: Add Sources

For each signal, set `scrape_mode` based on the portal type:

**Known SaaS platform (Accela, RealAuction, county-taxes.com):**
```
signal_type:  permits
url:          https://aca-prod.accela.com/MANATEE
scrape_mode:  playwright_then_ai
```
Leave `playwright_code` empty. Engine generates code on first run, falls back to
browser-use if generation fails. Promote to `playwright_only` after code is approved.

**Unknown portal:**
```
signal_type:     court_records
url:             https://[county clerk portal]
scrape_mode:     ai_only
navigation_hint: Navigate to civil case search → filter by file date range → export CSV
```
Engine uses browser-use Agent. After a successful run, optionally promote to
`playwright_then_ai` if the portal is stable enough to cache code.

**Direct download (portal exposes a CSV URL):**
```
signal_type:          permits
scrape_mode:          ai_only
permit_ai_strategy:   download_direct
download_url:         https://portal.example.com/permits/export
download_params:      {"startDate": "{start_date}", "endDate": "{end_date}"}
```

**PRR-only (no public portal):**
```
signal_type:  violations
prr_only:     true
```
Engine logs a clean skip. Load the PRR CSV manually via Admin UI data upload.

### Step 3 — Run

```bash
# 1. Master data first (properties table must exist before signals)
python -m src.scrappers.master.master_engine --county-id manatee --load-to-db

# 2. Approve master column mapping in Admin UI → Column Mappings tab

# 3. Signal scrapers (can run concurrently after master load)
python -m src.scrappers.permit.permit_engine             --county-id manatee --load-to-db
python -m src.scrappers.foreclosures.foreclosure_engine  --county-id manatee --load-to-db
python -m src.scrappers.liens.lien_engine                --county-id manatee --load-to-db
python -m src.scrappers.evictions.evictions_engine       --county-id manatee --load-to-db
python -m src.scrappers.deliquencies.tax_delinquent_engine --county-id manatee --load-to-db
python -m src.scrappers.violation.violation_engine       --county-id manatee --load-to-db

# 4. Approve column mappings for each signal (Admin UI → Column Mappings tab)
#    Required only on first run — approved mapping reused on every subsequent run

# 5. Score
python -m src.services.cds_engine --county-id manatee --rescore-all

# 6. Verify
# SELECT tier, COUNT(*) FROM distress_scores WHERE county_id='manatee' GROUP BY tier
```

### Step 4 — Promote to playwright_only (Optional)

After `playwright_then_ai` has run successfully several times and the generated code
has been reviewed:

1. Admin UI → County → Source → "Approve Playwright Code"
2. Change `scrape_mode` from `playwright_then_ai` to `playwright_only`

Every subsequent run uses the cached code directly (~15–30 seconds, $0 LLM cost)
instead of the browser-use fallback (~4 minutes, ~$0.10).

---

## Decision Reference — Which `scrape_mode` for a New Source

```
Does the portal expose a direct CSV/ZIP download URL (no browser needed)?
    YES → scrape_mode: ai_only  +  permit_ai_strategy: download_direct
    NO  ↓

Does the county use a platform already in use?
    (Accela, RealAuction, county-taxes.com, same clerk ORI vendor)
    YES → scrape_mode: playwright_then_ai
          (engine will generate code from the known platform pattern)
    NO  ↓

Does the portal have CAPTCHA or heavy JS that resists Playwright?
    YES → scrape_mode: ai_only (permanent — browser-use handles it)
    NO  → scrape_mode: playwright_then_ai
          (engine generates code on first run; browser-use fallback until approved)
```

---

## Cost / Speed Reference

| Mode | Time per run | LLM cost |
|---|---|---|
| `playwright_only` / `playwright_then_ai` (code cached) | ~15–30 seconds | $0 |
| `ai_only` with `download_direct` | ~3–5 seconds | ~$0.01 (task gen only) |
| `ai_only` browser-use Agent | ~4 minutes | ~$0.08–0.20 |
| Code generation (one-time, first run) | ~30–60 seconds | ~$0.02–0.05 |

---

## What Never Changes When Adding a County

- **ColumnMapper** — LLM maps columns after every download, admin approves once.
  Works identically for all three scrape modes and every signal type.
- **BaseLoader** — 3-strategy address matching, owner matching, parcel ID matching,
  all scoped to `county_id`. No changes needed per county.
- **CDS engine** — `--county-id` flag already exists on every signal.
- **DB schema** — no Alembic migrations for new counties. Only `County` and
  `CountySource` rows are added via Admin UI.
- **API** — county filter already present on all lead feed endpoints.

---

## CAPTCHA Handling

Portals that serve strong CAPTCHA (reCAPTCHA v3, hCaptcha) cannot use Playwright
reliably. Options in order of preference:

1. **`ai_only` with stealth patches** — Playwright-stealth + custom user-agent defeats
   most bot-detection short of reCAPTCHA v3. Already applied in `evictions_engine.py`
   and `liens/lien_engine.py`.

2. **`ai_only` permanently** — accept the cost (~$0.10/run) for portals that require
   full LLM vision. No Playwright code generated or cached.

3. **`prr_only`** — set `prr_only: true` in `special_flags`. Engine logs a clean skip.
   Admin uploads the PRR CSV manually. Already implemented for code violations.
