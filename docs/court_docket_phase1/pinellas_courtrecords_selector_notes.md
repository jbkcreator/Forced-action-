# Pinellas Court Records — Selector & Flow Notes (Phase 1)

Reference for `src/scrappers/court_docket/pinellas/court_session.py` +
`court_scraper.py` (+ `court_agent_fallback.py`). Source:
`courtrecords.mypinellasclerk.gov` (Ken Burke, CPA — Pinellas Clerk of the
Circuit Court). Captured 2026-06-04 from live probes on real eviction cases.
**CONFIRMED** = verified live; **TO CONFIRM** = best-effort, validate later.

This is the Pinellas analogue of the Hillsborough HOVER scraper. Both are
per-case court-docket extractors returning the same-shaped dict; persistence is
the separate `court_*` enrichment stage. Key differences from HOVER are called
out throughout.

## Access (bot wall) — see `court_session.py`

- **CONFIRMED:** the portal's bot wall is **Google reCAPTCHA v2 on Submit**,
  solved via **2captcha** — NOT Cloudflare, NOT PerimeterX. So unlike HOVER
  (real-Edge profile + PerimeterX) and unlike the Pinellas *Official Records*
  ORI portal (Cloudflare + warmed Edge profile), this needs **no Edge profile**:
  plain stealth Chromium passes. Reuses `STEALTH_ARGS`/`STEALTH_UA` +
  `apply_stealth_to_page` + `_solve_recaptcha_2captcha` from the evictions
  engine (same portal, same captcha — imported, not duplicated).
- **CONFIRMED:** sitekey `6LcCmw0oAAAA…`. 2captcha returns a token (len ~2.2–2.4k)
  in ~2–20s; injected into `#g-recaptcha-response` + the widget callback.
- **CONFIRMED:** the captcha callback **auto-submits** the search — no manual
  re-submit needed for a single exact case-number match.
- **CONFIRMED:** prod server is US-hosted → `use_proxy=False` default;
  `--use-proxy` for local testing (`get_playwright_proxy()`, Oxylabs).
- **CONFIRMED:** runs **headless** fine (unlike HOVER, which must be headed for
  PerimeterX). `--headful` for debugging.
- **Cost:** each solve costs 2captcha credit → the session is designed to
  **solve once and reuse one warm context** across a batch (re-solve only if the
  captcha reappears). Verified: 3 cases in one session.
- **Requires** `TWOCAPTCHA_API_KEY` in `.env` and the `2captcha-python` package.

## Case-number format & search

- **CONFIRMED:** Pinellas dashed UCN is **type-LAST**: `YY-NNNNNN-XX[-suffix]`
  e.g. `26-004970-CO` (contrast HOVER's type-MIDDLE `26-CC-025949`). Optional
  party suffix e.g. `25-07679-CF-B`. Regex `UCN_RE` in `court_scraper.py`.
  Long undashed UCN also shown on the detail page: `522026CC004970XXCOCO`.
- **Input source = `legal_proceedings.case_number`.** ⚠️ **Only Pinellas
  *Eviction* rows carry a real dashed UCN** (37 rows as of 2026-06-04). Pinellas
  *Probate* (462) and *Divorce* (16) rows carry **10-digit ORI instrument
  numbers** (e.g. `2026148526`) sourced from the Official Records export, NOT
  court-docket case numbers — these fail the UCN regex and return
  `case_number_missing` (correct: they are not searchable here). So this scraper
  meaningfully serves **evictions** today.
- **CONFIRMED (2026-06-05):** search with the **base UCN, party suffix stripped**.
  OCR'd judgment case numbers carry a party/count designator (e.g. `26-02186-MM-L`);
  the portal's Case Number field returns **no results** for the suffixed form but
  matches `26-02186-MM`. `scrape_case` builds the search term as
  `{year}-{number}-{court_type}` from the parsed UCN (mirrors HOVER's "search base
  UCN, party=ALL"). Verified: judgment instrument `2026151201` → OCR `26-02186-MM-L`
  → docket `26-02186-MM` ok (2 parties, 19 events, 14 docs, defendant matches the
  OCR debtor).
- **CONFIRMED flow:** `/MyCr/Cases/Search?s=e` → tabs **Name / Case / Attorney /
  Calendar**; activate the **Case** tab → single **Case Number** text field →
  Submit. Selectors (best-effort, worked live):
  - Case tab: `a:has-text('Case')` (+ fallbacks)
  - Case Number field: **CONFIRMED** `id="caseNumber"` (lowercase),
    `name="CaseNumber"`, `class="case-number"`. NB CSS ids are case-sensitive →
    use `#caseNumber` / `input.case-number` (not `#CaseNumber`).
  - Submit: `input[type='submit'][value='Submit']` / `button:has-text('Submit')`.

## Results → detail

- **CONFIRMED:** a single exact case-number match **auto-navigates straight to
  the detail page** (`/MyCr/CaseDetails?caseId=<int>&caseIdEnc=<encrypted>`).
  `_ensure_on_detail` detects this via `#caseHeader` / `"CaseDetails" in url`.
- **CONFIRMED (critical):** the detail URL carries an **encrypted `caseIdEnc`
  token** → **NOT deep-linkable from the case number**. Multi-result searches
  must click the blue case# link (`a[href*='CaseDetails']`). The multi-result
  grid path is coded but **TO CONFIRM** (every eviction tested so far was a
  single match → auto-nav).
- **CONFIRMED:** the detail page **progressively AJAX-loads** — header/parties
  render first, attorney/events/financial fill in after. `_ensure_on_detail`
  waits for `networkidle` then `table[summary='case parties'] tbody tr` as the
  readiness signal before scraping.

## Detail sections (DOM-scrape — no per-tab CSV export, unlike HOVER)

### Case Header — `_scrape_header` (CONFIRMED)
`#headerCollapse .header-row` — each row is two cols (label / value).
Labels → keys: Case Type, Date Filed, Status, Court, Judicial Officer, UCN.
Style (`PLAINTIFF Vs. DEFENDANT`) parsed from `.search-bar-results`.

### Parties — `_scrape_parties` (CONFIRMED)
`table[summary='case parties']` → thead `Name | Type | Party Address | Attorney |
Lead Attorney Address`, tbody `tr.ptr`. **This is the high-value data** —
defendant **mailing address** (incl. unit, e.g. `APT 5108`) absent from the bulk
court export. Empty attorney cells normalized to `null` (not `","`).

### Events & Documents — `_scrape_events_and_documents` (CONFIRMED)
`table[summary='docket events']` (4 such tables exist — Events + Other Documents
views + responsive duplicates; iterate all, **dedupe** on `(date, title,
docket#)`). Rows carry **class-tagged cells** (extract by class, not position):
| Field | Selector within row |
|---|---|
| date | `td.dDate` |
| event/title | `td.dDescription` (fallback `td.cdDocLink a`) |
| comments | `td.ttd-col-sm` (first) |
| docket# | first `td.td-col-sm` whose text is all digits |
| pages | `td.hPageCount` (hidden helper cell) |
| doc_id | `td.hDocId` (hidden) |
| doc_url | `td.cdDocLink a[href*='DocView']` → `BASE_URL + href` (DocView/Doc?request=…&eCode=…) |
| doc_status | `td.cdDocLink span[class*='glyphicons-file']` color token |

A `documents[]` entry is emitted for rows with a DocView link / status icon.
`doc_url` is the **actual downloadable document viewer link** (useful for a later
document-download phase).

**Doc-status color tokens** (`_DOC_STATUS_BY_COLOR`):
- **CONFIRMED:** `gl-green` = `public` (→ `image_available=True`).
- **TO CONFIRM:** `gl-blue`=view_on_request, `gl-orange`=confidential,
  `gl-red`=sealed, `gl-grey`/`gl-gray`=pending — guessed from the on-page legend
  (Public / View On Request / Confidential / Sealed / Pending); confirm against a
  non-public case. Unknown tokens are stored raw.

### Financial — `_scrape_financial` (CONFIRMED)
`table:has(thead[aria-label='financialtableheader'])` → `Date | Description |
Amount | Actions`. Party-group label rows and the **Balance Due** footer use
`colspan` (skipped for line items; group label kept as `party`, balance parsed
to `balance_due`). Amount = 3rd column (Actions ignored).

## Return shape (`scrape_case`)

Mirrors HOVER semantically, but emits **parsed lists** (the portal has no CSV
export) rather than CSV paths:

```json
{
  "case_number": "26-004970-CO",
  "county": "pinellas",
  "status": "ok|case_number_missing|not_found|blocked|error",
  "ucn": {"year":"26","number":"004970","court_type":"CO","party_suffix":null,"case_type":"eviction"},
  "extraction_path": "playwright|browser_use",
  "header": {"case_type","date_filed","status","court","judicial_officer","uniform_case_number","style_plaintiff","style_defendant"},
  "parties": [{"name","party_type","party_address","attorney","lead_attorney_address"}],
  "events": [{"date","event","comments","docket_num","pages","doc_status"}],
  "documents": [{"title","doc_date","docket_num","pages","doc_id","doc_url","doc_status","image_available"}],
  "financial": [{"date","description","amount","party"}],
  "balance_due": "0.00",
  "detail_url": "https://courtrecords.mypinellasclerk.gov/MyCr/CaseDetails?caseId=…&caseIdEnc=…",
  "warnings": [], "error": null, "screenshots": []
}
```

## Browser-use fallback — `court_agent_fallback.py`

- Fires **only on extraction/navigation drift on the already-cleared page**
  (the deterministic session has already passed the captcha). Browser-use
  **cannot** solve the reCAPTCHA — it is a markup-drift safety net, not a
  bot-wall bypass.
- Sonnet (`BROWSER_MODEL`) agent jumps to the live `detail_url`, reads the four
  sections, returns the **same dict shape**; output is shape-validated before
  acceptance and tagged `extraction_path="browser_use"`. On any failure →
  `None` (caller records a warning + screenshot).
- **TO CONFIRM:** unexercised against real drift (Playwright path hasn't failed
  in testing).

## Test set (Pinellas eviction `CO`)
- `26-004970-CO` — 2 parties, 8 events/docs, balance 0.00 (the screenshot case)
- `26-004991-CO` — 2 parties, 6 events/docs (defendant 2649 Teakwood Dr, Lot 34)
- `26-004914-CO` — 2 parties, 7 events/docs (defendant 5074 Foxbridge Cir, Apt 286)

> Probate/divorce UCNs are not available here (ORI instrument numbers). When a
> judgment/eviction with a non-`CO` type code surfaces, confirm the
> `_TYPE_TO_CASE_TYPE` mapping (`CO`/`CC`=eviction, `DR`=divorce, `CP`/`GA`=probate,
> `CA`/`CF`/`CT`=judgment) against live data.

## Failure handling
Every failure path screenshots + dumps HTML to `data/court_docket/pinellas_debug/`
(the CLI probe mirrors to `scratch/`). Per-case errors are captured in the result
(`status`, `error`, `warnings`, `screenshots`) — a batch never crashes on one bad
case.

## How to run (probe)
```powershell
# one case, dumps DOM + screenshot + JSON to scratch/ (probe-first selector pinning)
python -m src.scrappers.court_docket.pinellas.court_scraper --case 26-004970-CO
python -m src.scrappers.court_docket.pinellas.court_scraper --case 26-004970-CO --headful --no-fallback
```
