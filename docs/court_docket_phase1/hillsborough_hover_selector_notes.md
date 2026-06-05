# Hillsborough HOVER — Selector & Flow Notes (Phase 1)

Reference for `src/scrappers/court_docket/hillsborough/hover_session.py` +
`hover_scraper.py`. Source: hover.hillsclerk.com. Captured 2026-06-04 from
live probes + reference screenshots. **CONFIRMED** = verified live;
**TO CONFIRM** = best-effort selector, validate on first past-PerimeterX run.

## Access (PerimeterX) — see `hover_session.py`

- **CONFIRMED:** plain headless Chromium → HTTP 403 PerimeterX block page
  (`px-cdn.net`, `/captcha/captcha.js`). Real **Edge binary + warmed persistent
  profile + headed + US IP** → HTTP 200, form reachable.
- **CONFIRMED:** reach Case Search by **clicking the on-page link**
  `a[href*='caseSearch']`. A direct `goto(caseSearch.html)` → `net::ERR_ABORTED`.
- **CONFIRMED:** a background cart XHR can 403 → "Error getting shopping cart!"
  modal; close via `button:has-text('Close')`. Harmless (we don't buy docs).
- **CONFIRMED:** "Press & Hold" challenge is risk-triggered / intermittent;
  often skipped on a trusted warmed session. Visible-only detection + hold in
  `HoverSession.maybe_press_and_hold` (div[role=button]:has-text('Press') /
  `#px-captcha`). Whether a programmatic hold passes is **TO CONFIRM**.
- **CONFIRMED (trap):** the persistent profile's `_pxvid` gets flagged if
  hammered → 403 on all IPs. Keep velocity low; rotate profile on hard block.
- Prod server is US-hosted → `use_proxy=False`. Local testing → `use_proxy=True`
  (Oxylabs US-Florida, flaky ~50/50).

## Search form — `/html/case/caseSearch.html`, "Search by Case Number" tab

**CONFIRMED** selectors:

| Field | Selector | Value |
|---|---|---|
| County Designator | `#txtCountyDesignator` | `29` (prefilled) |
| Year (2-digit) | `#txtYear` | UCN year |
| Court Type | `#ddlCourtType` (`select_option` by value) | UCN 2-letter code |
| Number | `#txtNumber` | UCN seq, zero-padded to 6 |
| Party Designator | `#ddlPartyDesignator` | default `ALL` |
| Location | `#txtLocation` | `HC` (prefilled) |
| Readiness signal | `#txtYear` visible | — |

Hidden: `#caseNumber`, `#captchaToken` (PX token), `#selectedCaseSearch`.

**Court Type values** (`#ddlCourtType`): `AF BD CA CC CF CL CM CT CP CJ CS CZ DC
DE DP DR FC IN JC MC MD MH NB PK PC PN PP TC TJ TR SW WD`.
Routing: **CC**=eviction (county civil), **DR**=divorce, **CP/WD/GA**=probate-family,
**CF/CT/CA/CC**=judgments.

**UCN decomposition** (`decompose_ucn`): regex `^(\d{2})-([A-Z]{2})-(\d+)$`
→ {county=29, year, court_type, number(zfill 6), location=HC}. Non-matching
values (e.g. 10-digit `2026149626`) → **`case_number_missing`** (no fuzzy
search in v1).

**Submit button — TO CONFIRM.** Tried in order: `#btnCaseSearch`,
`#nav-CaseNumber button:has-text('Search')`, `button:has-text('Search')`,
`input[type=submit]`; fallback = press Enter in `#txtNumber`. Capture the real
id on first live run and pin it.

## Results → detail — TO CONFIRM

- After submit, wait for `table tbody tr` (or a no-results state).
- Match row by case number / 6-digit number: `tr:has-text("<key>")`.
- **Zoom / view icon** opens the case. Exact selector unknown — `_open_result`
  tries `img[src*='magnif'|'zoom'|'view']`, `a[onclick*='ase']`,
  `a[href*='caseView'|'View']`, `[title*='View']`, then generic `button`/`a`.
  **Pin the real zoom control on first live run.**
- Detail may open in a **new tab** (handled via `context.pages`) or same page.
- **Deep-link opportunity (TO CONFIRM):** detail page shows the undashed
  Uniform Case Number (e.g. `292026CC025949000...`). If a viewer URL is
  constructible from it, skip search entirely → ~1 page-load/case (less PX
  surface). Capture the detail URL pattern on first run.

## Detail tabs — `_collect_tab_names`, `_activate_tab`

Tabs vary by case type. Seen: Icon Keys, Summary, Parties, Events, Case Options
& Payments, Charges, Hearings, Financial, Warrants, Bonds, Disposition, File
Location, Related Cases. Tab click selectors (TO CONFIRM exact markup):
`[role='tab']:has-text('<name>')` / `.nav-tabs >> text=<name>` /
`a.nav-link:has-text('<name>')`.

### Summary — `_scrape_summary` (TO CONFIRM labels)
Best-effort: read body `inner_text` and lift `Label: value` lines for —
Case Number, Citation Number, Case Category Description, Case Type Description,
Case Sub Type Description, Case Status, Case Filed On, Judge, Division,
Balance Due — plus header "Uniform Case Number". Replace with targeted
key/value selectors once the Summary DOM is inspected live.

### Parties — CSV export (CONFIRMED present)
DataTable with **Excel / CSV** export buttons + columns: Party Type, Name +
mailing address, Party Demographics, Attorney Name, Attorney Contact (phone +
address). `_download_tab_csv("Parties", …)` clicks `button:has-text('CSV')` and
captures the download. **Button selector TO CONFIRM** (DataTables buttons vary).

### Events — CSV export (CONFIRMED present)
DataTable + Excel/CSV export + date filter. Columns: Document Index, Clock-In
Event Date, Event Creation Date, Event Description, Comment, Image, Certify.
`_download_tab_csv("Events", …)`. Document/image availability captured from the
Image column via `_events_document_metadata` (count of rows with an icon; no
document downloads in v1).

## Return shape (`scrape_case`)

```json
{
  "case_number": "26-CC-025949",
  "county": "hillsborough",
  "status": "ok|case_number_missing|not_found|blocked|error",
  "ucn": {"county":"29","year":"26","court_type":"CC","number":"025949","location":"HC"},
  "summary": { "...": "..." },
  "parties_csv": "data/court_docket/hover_downloads/26_CC_025949_parties.csv",
  "events_csv":  "...events.csv",
  "tabs": ["Summary","Parties","Events", "..."],
  "documents": [ {"row_text": "...", "image_available": true} ],
  "events_image_available": 7,
  "warnings": [], "error": null, "screenshots": []
}
```

## Test set (5× each Hills doc type)

- Eviction (CC): 26-CC-025949, 26-CC-025942, 26-CC-025938, 26-CC-025899, 26-CC-025894
- Probate (WD): 26-WD-000969, 26-WD-000968, 26-WD-000967, 26-WD-000966, 26-WD-000965
- Divorce (DR): 26-DR-007183, 26-DR-007168, 26-DR-007167, 26-DR-007149, 26-DR-007142
- Judgment (from OCR): 25-CT-011762, 25-CF-016327-A*, 18-CA-000456, 25-CC-035465, 25-CF-018488-A*
  (*the `-A` party suffix — strip to base `YY-CF-NNNNNN` for the form; party designator stays ALL.)

> NOTE: judgment UCNs with a trailing party suffix (`-A`) do not match the
> base `YY-CT-NNNNNN` regex. Either extend the regex to accept an optional
> `-<suffix>` and ignore it for the form, or pre-strip it. Decide on first run.

## Failure handling
Every failure path screenshots + dumps HTML to `data/court_docket/hover_debug/`
(and the probe mirrors results to `scratch/hover_case_results.json`). Per-case
errors are captured in the result (`status`, `error`, `warnings`,
`screenshots`) — the batch never crashes on one bad case.
