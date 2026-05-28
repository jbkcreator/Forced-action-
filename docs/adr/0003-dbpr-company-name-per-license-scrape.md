# 0003 — DBPR company name scraped per-license from myfloridalicense.com

## Status
Accepted 2026-05-27.

## Context

`dbpr_contacts` is populated weekly by `src/scrappers/dbpr/dbpr_engine.py`,
which downloads the public CILB bulk CSV extracts
(`cilb_certified.csv`, `cilb_registered.csv`) and upserts on `license_number`.
This loader is fast, `requests`-only (no browser), and county-filtered.

We need a **company name** (the contractor's business / DBA) on each contact.
Inspection of the bulk extract confirms it carries only:

```
type_code, type_desc, license_number, full_name (LAST, FIRST — the individual
qualifier), address, blank, city/state/zip, expiry, + CE course columns
```

There is **no business/DBA name column**. DBPR contractor licenses are
qualifier licenses: the license belongs to an individual (`full_name`) who
qualifies a business. The business name only appears on the licensee record
served by the site at `myfloridalicense.com/wl11.asp`, keyed by license number.

On a license-number search, the results page lists multiple rows per license
distinguished by a **Name Type** column (`Primary` = the individual,
`DBA` = the business). The detail page consolidates both as
`<PRIMARY> (Primary Name)` and `<BUSINESS> (DBA Name)`. Not every license has
a DBA row.

## Decision

1. **Scrape the company name per-license** with Playwright (+ stealth + Oxylabs
   proxy when configured) — there is no bulk source. New standalone module
   `src/scrappers/dbpr/dbpr_company_scraper.py`, CLI-driven like the other DBPR
   jobs, **not** county-scoped (drains all pending rows table-wide).

2. **Parse the DBA name off the search-results table, cross-checking the
   license number printed in each row.** The flow: landing page (`mode=0`) →
   select the `SearchType=LicNbr` radio → submit (`SelectSearchType`) →
   license-number form (`mode=1`) → fill `LicNbr` → submit (`Search1`) →
   results page. Each results row renders tab-separated as
   `<License Type>  <Name>  <DBA|Primary>  <License Number>  <Status>`. We take
   the `DBA` row whose License Number equals the searched license — that
   equality is the cross-check against writing a name from the wrong record.

   We do **not** click through to the per-result detail page. Its links
   (`LicenseDetail.asp?SID=&id=…`) carry an empty SID and bounce back to the
   landing page in an automated session, so detail navigation is unreliable.
   The results table already exposes both the DBA name and the license number,
   so it is the source of truth and costs one round-trip instead of two.

   **Sole proprietors:** the site emits a DBA row literally named `INDIVIDUAL`
   when the licensee has no business name. That is a sentinel, not a company —
   it is treated as `none` (NULL company_name), not stored verbatim.

3. **Three new columns** (Alembic migration): `company_name VARCHAR(255)`,
   `company_name_status VARCHAR(20)` (default `pending`,
   CHECK ∈ `{pending, found, none, failed}`), and `company_name_scraped_at`.
   Status semantics:
   - matching DBA row with a real name → `found`
   - license matches but only Primary row(s) / DBA == `INDIVIDUAL` → NULL + `none`
   - no results row matches the license / zero records → NULL + `failed`

4. **NULL-/status-driven incremental**: the weekly job targets
   `company_name_status IN ('pending', 'failed')`, oldest-first. `found`/`none`
   are terminal — never re-scraped, even when the weekly CSV sync updates the
   row. Mirrors the existing `enrichment_status` pattern.

5. **Serial, unbounded, polite** (~1.5s/license; first backfill of ~5,945 rows
   ≈ 2.5 hrs in one session, steady-state deltas finish in minutes). Abort the
   run and `send_alert` after 20 consecutive failures (site down).

6. **Scheduled Sunday ~02:30 UTC**, after `dbpr_engine` (02:00) creates the new
   `pending` rows; independent of `dbpr_enrichment`. Writes a `scraper_run_stats`
   row each run (`scanned/found/none/failed/aborted`).

## Consequences

- A per-license gov-site crawl is real infrastructure with a failure mode the
  bulk loader doesn't have (site downtime, layout change, IP blocking). It is
  isolated in its own module so it cannot stall the fast CSV load.
- First full backfill spans one long Sunday session; acceptable off-peak.
- If DBPR ever adds the DBA name to the bulk extract, this whole job can be
  retired in favor of one more parsed column in `dbpr_engine.py`.
- Layout dependence: parsing keys off the results-row column order
  (`Name`, `Name Type` ∈ {DBA, Primary}, `License Number`) and the
  `SearchType=LicNbr` / `Search1` form control names. A site redesign breaks
  it — the abort-and-alert guard surfaces this within one run (mass `failed`).
