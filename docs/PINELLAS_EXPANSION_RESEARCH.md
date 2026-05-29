# Pinellas County Expansion — Research & Planning

**Status:** Research phase (Step 1 complete — sources identified)
**FIPS:** 12103 | **Bankruptcy Court:** FLMB Tampa Division (same as Hillsborough) | **NWS Zone:** FLZ050 (single zone)

---

## Step 1 — Signal Source Map (Complete)

### Direct Config Swaps — Zero Scraper Changes Needed

| Signal | Hillsborough URL | Pinellas URL | Notes |
|--------|-----------------|--------------|-------|
| Foreclosure auctions | `hillsborough.realforeclose.com/index.cfm` | `pinellas.realforeclose.com/index.cfm` | Same RealAuction platform, subdomain swap only |
| Tax delinquency | `hillsborough.county-taxes.com` | `pinellas.county-taxes.com/public/search/property_tax` | Same vendor. Certificate sales use LienHub separately (not needed for delinquency lookup) |
| Permits / Code enforcement | `aca-prod.accela.com/HCFL` | `aca-prod.accela.com/PINELLAS` | Same Accela platform, agency code swap: `HCFL` → `PINELLAS` |
| SunBiz | `search.sunbiz.org` | Same — statewide | No change |
| Bankruptcy | FLMB Tampa, division prefix `8:` | Same court | Pinellas is also Tampa Division, FLMB |
| FEMA / NWS APIs | FIPS 12057, zones FLZ151/FLZ251 | FIPS 12103, zone **FLZ050** | Single zone (Hillsborough is split). Same national API endpoints |

---

### Works But Different URL Pattern — Minor Scraper Adaptation

| Signal | Pinellas URL | Difference from Hillsborough | Work Required |
|--------|-------------|------------------------------|---------------|
| ORI — liens, judgments, deeds | `officialrecords.mypinellasclerk.gov` | Different domain/branding, same search function | Port scraper logic to new domain; verify field names match |
| Property master data | `pcpao.gov/tools-data/data-downloads/raw-database-files` | 15 separate CSV/JSON/Excel tables vs. Hillsborough's single DBF/XLS master file | New loader (see PCPAO section below) |

---

### Needs New Approach — Biggest Risk Items

| Signal | Problem | What's Available | Risk Level |
|--------|---------|-----------------|------------|
| Civil filings (evictions/divorce) | **No static daily filings page** — Hillsborough exposes flat HTML table at `/Civil/dailyfilings/`. Pinellas has none. | `courtrecords.mypinellasclerk.gov` — requires date-range POST/GET query | Medium — different scraping pattern, fields TBD |
| Probate filings | Same — no `/Probate/dailyfilings/` equivalent | Same portal as civil, filter by case type | Medium — same as above, plus some records have restricted access |
| Fire/Sheriff incidents | No confirmed bulk API | `egis.pinellas.gov/apps/CrimeViewer/` has CSV export. Validate against `new-pinellas-egis.opendata.arcgis.com` ArcGIS catalog | Medium — needs manual browser verification |

> **TODO before coding:** Manually open `courtrecords.mypinellasclerk.gov` and verify: (1) can you filter by filed-date range, (2) does it return case type, party name, address fields. Same check for CrimeViewer CSV.

---

## Step 2 — PCPAO Bulk Data Deep Dive (Complete)

Pinellas County Property Appraiser (`pcpao.gov`) provides 15 raw database tables, nightly refresh (~02:00–03:30 AM), in CSV / JSON / Excel (subset also XML). Primary join key across all tables: **STRAP** (parcel identifier — different format from Hillsborough folio, see gotchas).

### Hub Table: RP_PROPERTY_INFO (76 columns)

This is the Pinellas equivalent of Hillsborough's master file. All 10 Hillsborough key fields are present.

**All Hillsborough fields covered:**

| Hillsborough Field | PCPAO Column(s) |
|-------------------|-----------------|
| Parcel ID (folio) | `STRAP`, `PARCEL_NUMBER` |
| Site address | `SITE_ADDRESS`, `SITE_CITYZIP`, plus decomposed `STR_NUM`, `STR_NAME`, `STR_SFX`, `STR_CITY`, `STR_ZIP`, `STR_UNIT` |
| Owner name | `OWNER1`, `OWNER2` |
| Mailing address | `MAILING_ADDRESS_1`–`4`, `MAILING_CITY`, `MAILING_STATE`, `MAILING_ZIP` |
| Owner occupancy | `HX_CAP` / `HX_SAVINGS` as proxy; use `RP_EXEMPTIONS.HX_YN` for explicit flag |
| Property use code | `PROPERTY_USE`, `LAND_USE_CD`, `USE_CD` |
| Land value | `JUST_LAND` |
| Building value | `JUST_BUILDING` |
| Total assessed value | `CNTY_JST_VALUE`, `CNTY_ASD_VALUE`, `CNTY_TAXABLE_VALUE` |
| Year built | `YEAR_BUILT` |
| Living area (sqft) | `TOTAL_LIVING_SQFT` |
| Bedroom / bathroom count | **NOT here** — EAV rows in `RP_STRUCTURAL_ELEMENTS` (requires pivot) |

**High-value extras not in Hillsborough master:**

| Column | Distress / Scoring Value |
|--------|--------------------------|
| `LATITUDE` / `LONGITUDE` | Geocoords baked in — no geocoding step needed |
| `CONTAMINATION_YN` | Environmental contamination flag |
| `SUBSIDENCE_YN` | Sinkhole risk flag — Pinellas-specific, high relevance |
| `EVAC_ZONE` (A–F) | Hurricane evacuation zone — correlates with flood/wind insurance distress |
| `ELEVATION_CERT` | Flood insurance compliance indicator |
| `DLHL_YN` | "Denial of Homestead" — explicit failed homestead flag, stronger than address comparison |
| `TAX_AMOUNT_NO_EX` | Tax burden without exemptions — true carrying cost for investors |
| `SPECIAL_ASSESSMENT` | CDD / district charges — hidden carrying cost |
| `WATERFRONT_YN`, `SEAWALL`, `FRONTAGE`, `VIEWS` | Waterfront signals |
| `FUTURE_L1_USE`–`FUTURE_L4_USE` | Future land use from comp plan — rezoning potential |
| `TIF_CD` / `TIF_DSCR` | Tax Increment Financing district |
| `HX_SAVINGS` | Dollar value of homestead exemption — `$0` = strong absentee signal |

---

### All 15 PCPAO Tables — Summary

| Table | Key Columns | Use in Pipeline |
|-------|------------|-----------------|
| **RP_PROPERTY_INFO** | 76 cols — all master fields + extras above | Hub table replacement for Hillsborough master file |
| **RP_ALL_SITE_ADDRESSES** | `STRAP, SITE_ADDR, STR_NUM, STR_NAME, STR_SFX, STR_UNIT, CITY, ZIP, LN_NUM, SITE_BLD_NUM` | Multi-address parcels (condos) — one row per address; improves address matching for units |
| **RP_ALL_OWNERS** | `STRAP, OWNER_NAME, OWNER_NUMBER` | Multiple owners per parcel (joint tenants, LLC members) |
| **RP_EXEMPTIONS** | `STRAP, HX_YN, HX_YR, HX_USE, HX_STATUS, PROPERTY_EXEMPTION` | Primary absentee owner detection; `HX_YN = Y` = owner-occupied |
| **RP_BUILDING** | `STRAP, BUILDING_NUMBER, YEAR_BUILT, HEATED_AREA_SQFT, QUALITY, STORIES, LIVING_UNITS, FOUNDATION, EXTERIOR_WALLS` | Building characteristics; multi-building parcels have one row per structure |
| **RP_STRUCTURAL_ELEMENTS** | `STRAP, BUILDING_NUMBER, ATTRIBUTE_CD, ATTRIBUTE_DSCR, ATTRIBUTE_VALUE` | EAV table — bedrooms, bathrooms, roof type, floor finish, depreciation; requires pivot |
| **RP_LAND** | `STRAP, LAND_USE, LAND_SIZE, LAND_SQFT, VAL, METHOD` | Land valuation detail |
| **RP_LEGAL** | `STRAP, LONG_LEGAL_1, LONG_LEGAL_2` | Full legal description — HOA/condo association from plat references |
| **RP_EXTRA_FEATURES** | `STRAP, XFEAT_DESCRIPTION, VALUE_NEW, VALUE_DPR, YR_ADDED, UNITS` | Pool, dock, seawall — deferred maintenance from depreciation ratio |
| **RP_PERMITS** | `STRAP, PERMIT_NUMBER, PERMIT_TYPE, PERMIT_DSCR, AGENCY_NAME, ISSUE_DT, SIGN_OFF_DT, EST_VAL` | **Replaces Accela permit scraping**. Open permits (`SIGN_OFF_DT` null) = title issue signal. Demo permits = tear-down candidate |
| **RP_SALES** | `STRAP, SALE_DATE, PRICE, QUALIFIED_FLG, GRANTEE, GRANTOR, MULTI_SALES_YN, VACANT_IMPROVED` | Current year sales |
| **RP_SALES_HISTORY** | `STRAP, GRANTOR, GRANTEE, GRANTOR_ADDR_*`, `GRANTEE_ADDR_*`, `SALES_DATE, PRICE, QU_FLG, DOC_STAMPS, TRNS_CD` | **Replaces deed transfer scraping**. `TRNS_CD` = warranty deed / quit claim / foreclosure deed. `QU_FLG = U` = distress sale. Full address history for absentee detection across transactions |
| **RP_SUB_AREAS** | `STRAP, BLD_NUM, DESCRIPTION, LIVING_AREA_SQFT, GROSS_AREA_SQFT, FACTOR` | Sub-area types (porch, garage, utility) — detects unpermitted conversions |
| **RP_MILLAGE_RATES** | `MILL_CD, MILL_NAME, TAX_RATE_CALC` | Tax rate by district — low direct distress value |
| **RP_INACTIVE_PARCEL_LIST** | `STRAP, REASON_INACTIVE, INACTIVE_DATE, NEW_PARCEL_NUMBER` | Parcel splits/merges — critical for matching historical records to current STRAPs |

---

### Tables That Replace Scraping

| What currently gets scraped | PCPAO bulk replacement | Notes |
|----------------------------|------------------------|-------|
| Building permits (Accela) | `RP_PERMITS` | Nightly; covers all municipalities in Pinellas (county + St. Pete + Clearwater + Largo etc.) |
| Deed transfers (Clerk ORI) | `RP_SALES` + `RP_SALES_HISTORY` | Full transaction history with distress flags |
| Absentee owner detection | `RP_EXEMPTIONS` + `RP_PROPERTY_INFO.DLHL_YN` | More reliable than address comparison |

---

## Key Technical Gotchas

### 1. STRAP Format ≠ Hillsborough Folio
Pinellas STRAP: `SS-TT-RR-SSSSS-LLL-LLLL` (section-township-range-subdivision-lot)
Hillsborough folio: different format entirely.

The `parcel_id` normalizer in `src/utils/` needs a Pinellas-specific branch. This affects every join between distress signal records and the `properties` table.

### 2. Bedrooms/Bathrooms Are EAV Rows
`RP_STRUCTURAL_ELEMENTS` uses attribute rows, not columns:
- `ATTRIBUTE_DSCR = 'BEDROOMS'` → `ATTRIBUTE_VALUE`
- `ATTRIBUTE_DSCR = 'FULL BATHS'` → `ATTRIBUTE_VALUE`

The master loader will need a pivot/aggregation step. Copying the Hillsborough flat-file loader directly will break this.

### 3. Absentee Owner Detection Is Different
Hillsborough: compare mailing address to site address.
Pinellas: use `RP_EXEMPTIONS.HX_YN = 'N'` (no homestead) OR `RP_PROPERTY_INFO.DLHL_YN = 'Y'` (denied homestead). More reliable but requires the join to RP_EXEMPTIONS.

### 4. Multi-Address Parcels
`RP_ALL_SITE_ADDRESSES` has one row per address, not one row per parcel. Must deduplicate or handle the join carefully, especially for condo complexes.

### 5. No Excel for Some Tables
`RP_PERMITS`, `RP_SALES_HISTORY`, `RP_STRUCTURAL_ELEMENTS`, `RP_SUB_AREAS` — CSV/JSON only, no Excel. Existing CSV ingestion path handles this fine.

---

## Step 3 — ORI Lien Export Analysis (Complete)

Sample: `officialrecords.mypinellasclerk.gov` CSV export — 3,051 records from a single day (2026-05-04/05).

### CSV Column Structure

| Pinellas CSV Column | Loader Expects | Gap / Action |
|--------------------|---------------|--------------|
| `DirectName` | `Grantor` | Rename |
| `IndirectName` | `Grantee` | Rename |
| `RecordDate` | `RecordDate` | Same ✓ |
| `DocTypeDescription` | `document_type` | Rename + doc type mapping (see below) |
| `BookType` | `BookType` | Same ✓ (always "OR") |
| `BookPage` ("23544/1338") | `Book` + `Page` (separate cols) | Split on `/` |
| `Comments` | `Legal` | Rename |
| `InstrumentNumber` | `Instrument` | Rename |
| *(missing)* | `Filing Amt` | Not in Pinellas portal — no amount data available |

**59% of records have no `Comments` (no legal description)** — primary matching strategy must be owner name (`IndirectName`), same as Hillsborough.

### Document Type Distribution & Mapping

| Pinellas `DocTypeDescription` | Count | Internal Label | Action |
|-------------------------------|-------|---------------|--------|
| `NOTICE OF COMMENCEMENT` | 480 | — | Skip |
| `DEED` | 479 | `DEED` | DeedLoader |
| `SATISFACTION` | 420 | — | Skip |
| `JUDGEMENT LIEN` | 261 | `JUDGMENT` | LienLoader |
| `MORTGAGE` | 236 | — | Skip |
| `PROBATE` | 197 | `PROBATE` | **Feed ProbateLoader** (replaces separate scraper) |
| `AFFIDAVIT` | 159 | — | Skip |
| `COURT PAPER` | 99 | — | Skip |
| `JUDGEMENT` | 90 | `JUDGMENT` | LienLoader |
| `LIEN` | 79 | `LIEN` | LienLoader |
| `Marriage License` | 74 | — | Skip |
| `TERMINATION` | 56 | — | Skip |
| `DEATH CERTIFICATE` | 54 | — | Skip |
| `ASSIGNMENT` | 48 | — | Skip |
| `FINANCING STATEMENT` | 43 | `LIEN` | LienLoader |
| `RELEASE` | 28 | — | Skip |
| `LIS PENDENS` | 18 | `LIS PENDENS` | LisPendensLoader |
| `PROBATE REAL PROPERTY` | 15 | `PROBATE` | ProbateLoader |
| `LIEN (IRS)` | 13 | `TAX LIEN` | LienLoader (IRS branch) |
| `DOMESTIC RELATIONS JUDGMENT` | 22 | `JUDGMENT` | LienLoader |
| `CERTIFIED COPY OF A COURT JUDGMENT OR ORDER` | 9 | `JUDGMENT` | LienLoader |
| `CORPORATE LIEN` | 3 | `LIEN` | LienLoader |
| `RELEASE (IRS)` | 5 | — | Skip |
| `RELEASE OF LIS PENDENS` | 6 | — | Skip |

### Loader Gaps for Pinellas

**1. Column normalizer needed (scraper output layer):**
A thin normalization step before feeding to existing loaders:
- Rename `DirectName → Grantor`, `IndirectName → Grantee`, `InstrumentNumber → Instrument`, `Comments → Legal`, `DocTypeDescription → document_type`
- Split `BookPage` → `Book`, `Page` (split on `/`)
- Map full Pinellas doc type text → internal labels (table above)

**2. `_CITY_FILER_KEYWORDS` hardcoded for Hillsborough** (`src/loaders/liens.py:23-24`):
Currently: `{'CITY OF TAMPA', 'HILLSBOROUGH COUNTY'}`
Needs Pinellas: `{'PINELLAS COUNTY', 'CITY OF ST. PETERSBURG', 'CITY OF CLEARWATER', 'CITY OF LARGO', 'CITY OF PINELLAS PARK', 'CITY OF DUNEDIN', 'CITY OF TARPON SPRINGS'}`
→ Must be county-aware (passed in from county config, not hardcoded).

**3. `_CODE_LIEN_CITY_MAP` also Hillsborough-specific** (`src/loaders/liens.py:29`):
Maps `TCL` → Tampa, `CCL` → None. Pinellas code lien types TBD — depends on what Pinellas Accela enforcement exports use as type codes.

### Bonus: ORI Export Covers Signals That Need Separate Scrapers in Hillsborough

| Signal | Hillsborough Source | Pinellas via ORI |
|--------|--------------------|--------------------|
| Probate | Separate scraper (`/Probate/dailyfilings/`) | `PROBATE` + `PROBATE REAL PROPERTY` in ORI CSV |
| Divorce filings | Separate scraper (`/Civil/dailyfilings/` DR case type) | `DOMESTIC RELATIONS JUDGMENT` in ORI CSV |

**One Pinellas ORI scraper replaces three Hillsborough scrapers** (liens + probate + divorce).

---

## Step 4 — Accela Permit & Violation Export Analysis (Complete)

### Building Permits — `aca-prod.accela.com/PINELLAS` (Building module)

**Status: Zero changes needed to `BuildingPermitLoader`.**

Pinellas Accela export columns match exactly what the loader reads:

| CSV Column | Loader Field | Match |
|-----------|-------------|-------|
| `Date` | `issue_date` | ✓ |
| `Record Number` | `permit_number` | ✓ |
| `Record Type` | `permit_type` | ✓ |
| `Status` | `status` | ✓ |
| `Address` | address matching | ✓ |
| `Expiration Date` | `expire_date` | ✓ |
| `Project Name` | *(ignored)* | harmless extra |
| `Description` | *(ignored)* | harmless extra |

**One minor fix needed:** `"Waiting on Applicant"` appears in Pinellas Accela as a status. Functionally equivalent to Hillsborough's `"awaiting client reply"` (owner not responding, work stalled = distress signal). Add to `_ENFORCEMENT_STATUS_VALUES` in `src/loaders/permits.py:23`.

Sample record types seen: Express Building Permit, Residential Remodel/Repair/Renovation, Residential Electrical, Residential Demolition, Residential Solar, Residential Pools and Spas, Commercial Remodel, Commercial Signs.

### Code Violations — `aca-prod.accela.com/PINELLAS` (Enforcement module)

**Status: Correct module confirmed. Column structure identified. Record type variety TBD.**

Pinellas Enforcement export columns vs. `ViolationLoader` expectations:

| CSV Column | Loader Field | Match |
|-----------|-------------|-------|
| `Date` | `opened_date` | ✓ |
| `Record Number` | `record_number` | ✓ |
| `Record Type` | `violation_type` | ✓ |
| `Status` | `status` | ✓ |
| `Address` | address matching | ✓ |
| *(missing)* | `Description` | null on all records |
| *(missing)* | `Fine Amount` | null — fine-based severity thresholds won't fire |
| *(missing)* | `Is Lien` | null — lien-based severity thresholds won't fire |

Violation column structure is **simpler than permits** — no Description, Project Name, or Expiration Date in the export. Loader runs as-is; `classify_severity()` falls back to keyword matching on `Record Type` only.

**Fewer columns than permits export.** Not a blocker — the loader handles missing columns gracefully (all default to None/False).

**Record type variety unknown.** The sample export (3-day window) only returned Short Term Rental types. A 90-day export is needed to confirm what actual enforcement Record Types look like in Pinellas — e.g. `"Citizen Complaint"`, `"Housing Inspection"`, `"Unsafe Structure"`. These need to match (or be added to) `_CRITICAL_TYPE_KEYWORDS` / `_MAJOR_TYPE_KEYWORDS` in `src/loaders/violations.py:23–54`.

**Root cause confirmed (not a filter issue):** The Pinellas Accela Enforcement module is architecturally different from Hillsborough's. It primarily handles Short Term Rental licensing enforcement. Traditional code complaints (`Code Complaint`, `Code Enforcement Citation`) exist but at very low volume — only one non-STR record appeared across a 90-day export (`CFC-26-00001`, a code lien foreclosure).

Pinellas code enforcement appears to be split across multiple systems:
- **Accela Enforcement** — STR compliance + code lien foreclosures (`CFC-` prefix)
- **Separate system (TBD)** — traditional housing complaints, unsafe structure notices, ordinance violations. Municipalities (St. Petersburg, Clearwater, Largo) may each have their own code enforcement portals separate from the county Accela instance.
- **MuniReg (`munireg.com`)** — Foreclosure Property Registration for Pinellas (confirmed separate system, no Hillsborough equivalent)

**This is a blocker for code violations parity.** The `ViolationLoader` cannot be fed from Accela Enforcement for Pinellas — the data is not there in meaningful volume.

**Confirmed after full research:** No single bulk-downloadable code violations dataset exists for Pinellas. Data is fragmented across county + 24 municipalities, each with their own system. Full breakdown in Step 5 below.

**STR side note:** STR data (Short Term Rental Certificate of Use / Application) is a valid absentee owner signal — "Conditionally Active" STR + no homestead exemption = near-certain non-owner-occupied. Could feed absentee scoring in a future phase but is not a code violation signal.

---

## Step 5 — Code Violations Source Research (Complete)

**Verdict: Pinellas has no single code violations source. Data is split across county + 24 municipalities.**

Hillsborough has one Accela Enforcement instance covering the whole county. Pinellas does not — the county Accela only covers **unincorporated areas (~25% of population)**. The 24 municipalities each run their own enforcement system.

### Source Map by Jurisdiction

| Jurisdiction | System | Scrape Method | Bulk Export? | Notes |
|-------------|--------|--------------|-------------|-------|
| **Pinellas County (unincorporated)** | Accela `aca-prod.accela.com/PINELLAS` Enforcement module | Playwright date-range loop | No | Covers unincorporated areas only. Date-range search available. |
| **St. Petersburg** (265k pop) | Click2Gov `stpe-egov.aspgov.com/Click2GovCE/casesearch.html` | Playwright | No | Case search by address/case number. No date-range search confirmed. |
| **St. Petersburg** (API) | Socrata SODA API `stpete.data.socrata.com/resource/qdms-3kn3.json` | Direct API call | Yes — daily CSV/JSON | 311 service requests include code complaints. Complaint intake only, not full case resolution data. |
| **Clearwater** (117k pop) | Accela `aca-prod.accela.com/CLEARWATER` `CodeCompliance` module | Playwright | No | Module name differs from county (`CodeCompliance` not `Enforcement`). Record format: `PNU(YYYY)-(NNNNN)`. |
| **Largo, Pinellas Park, Dunedin, Tarpon Springs, etc.** | Unknown — each city has own system | TBD | TBD | 21 remaining municipalities. Lower population; may use county Accela or own legacy systems. |

### Historical Backfill Path (All Jurisdictions)

Florida Statute 119 (Public Records Law) mandates access. Pinellas County uses GovQA at `pinellas.govqa.us`.

- Request: "All code enforcement cases opened between Jan 1 2020 and present — case number, address, parcel ID, violation type, date opened, status, fine amount"
- Expected output: Accela CSV export (the system can generate this internally even if the public UI doesn't expose it)
- Timeline: Response acknowledgment within 5 business days
- Cost: Staff time at $20–35/hr; a well-scoped request for an Accela CSV export is typically low cost
- Same PRR process applies to Clearwater and St. Pete via their respective public records portals

### Practical Launch Strategy

For Pinellas v1, prioritise coverage over completeness:

| Priority | Source | Coverage | Effort |
|---------|--------|---------|--------|
| 1 | **St. Pete Socrata API** | St. Pete complaints (largest city, 265k) | Low — direct SODA API call, same pattern as FEMA API |
| 2 | **Pinellas County Accela Playwright scraper** | Unincorporated county | Medium — same Playwright pattern as Hillsborough scraper, different URL |
| 3 | **PRR to Pinellas County Code Enforcement** | Unincorporated — full history | Low effort to request, 5-day wait |
| 4 | **Clearwater Accela** | Clearwater (117k) | Medium — separate Accela instance, different module name |
| 5 | **St. Pete Click2Gov** | St. Pete cases (full detail) | High — no date-range search, must scrape by address |

**Gap accepted for v1:** Largo (85k), Pinellas Park (54k), and 19 smaller municipalities. Combined ~200k population. Can be added iteratively.

### Final Verdict — All Public Sources Exhausted

All three programmatic sources have been manually verified and are dead ends:

| Source | Status | Finding |
|--------|--------|---------|
| St. Pete Socrata API | ❌ Dead | Domain decommissioned — `{"code":"not_found","error":true,"message":"This domain has been decommissioned."}` |
| St. Pete Click2Gov (`stpe-egov.aspgov.com`) | ❌ No date range | Address or case number search only — cannot iterate systematically |
| Clearwater Accela `CodeCompliance` | ❌ No date range | Same — no date range on public UI |
| Pinellas County Accela `Enforcement` | ⚠️ Low yield | Date range available but output is ~99% STR records; actual code complaints are low volume |

**PRR is the only viable path for code violations in Pinellas.**

### Implementation Decision

| Phase | Approach |
|-------|---------|
| **Initial load** | PRR via `pinellas.govqa.us` — request all code enforcement cases from Pinellas County + separate PRRs to St. Pete and Clearwater. One-time effort, full history. |
| **Ongoing updates** | No public portal supports date-range scraping. Options: (a) Re-run PRR quarterly, (b) accept code violations as a static signal refreshed quarterly, (c) skip ongoing updates for this signal in Pinellas. |
| **Score impact** | Code violations is 1 of 14 signals. Properties with foreclosures, liens, and tax delinquency surface correctly without it. Restoration/Fix-Flip vertical loses some precision but product remains functional. |

**Accepted limitation for Pinellas v1:** Code violations = PRR-only, static dataset. Not a launch blocker.

### Key Structural Difference vs. Hillsborough

| | Hillsborough | Pinellas |
|--|-------------|---------|
| Code enforcement system | Single Accela instance, county-wide | County Accela (unincorporated only) + 24 separate municipal systems |
| Bulk export | No | No (PRR path available) |
| Programmatic API | None | St. Pete Socrata SODA API |
| Scraper count needed | 1 | 3–5 for meaningful coverage |

---

## Signal Coverage Summary — 18 Confirmed / 3 Partial / 1 Unknown

| # | Signal | Status | Source / Notes |
|---|--------|--------|---------------|
| 1 | `foreclosures` | ✅ Ready | `pinellas.realforeclose.com` — same RealAuction platform, subdomain swap |
| 2 | `tax_delinquencies` | ✅ Ready | `pinellas.county-taxes.com` — same vendor |
| 3 | `bankruptcy` | ✅ Ready | Same FLMB Tampa court, no change |
| 4 | `storm_damage` | ✅ Ready | NWS API — update zone to `FLZ050` |
| 5 | `flood_damage` | ✅ Ready | FEMA NFIP API — update FIPS to `12103` |
| 6 | `insurance_claim` | ✅ Ready | FEMA Housing Assistance API — update FIPS to `12103` |
| 7 | `sunbiz` | ✅ Ready | Statewide, zero changes |
| 8 | `probate` | ✅ Ready | ORI export `PROBATE` type — replaces separate Hillsborough scraper |
| 9 | `divorce_filings` | ✅ Ready | ORI export `DOMESTIC RELATIONS JUDGMENT` — replaces separate scraper |
| 10 | `judgment_liens` | ✅ Ready | ORI export `JUDGEMENT LIEN` + `JUDGEMENT` types |
| 11 | `irs_tax_liens` | ✅ Ready | ORI export `LIEN (IRS)` type |
| 12 | `hoa_liens` | ✅ Ready | ORI export `LIEN` type |
| 13 | `mechanics_liens` | ✅ Ready | ORI export `LIEN` type |
| 14 | `deed_transfers` | ✅ Ready | ORI export `DEED` + PCPAO `RP_SALES_HISTORY` |
| 15 | `building_permits` | ✅ Ready | Accela `PINELLAS` Building module — zero loader changes needed |
| 16 | `roofing_permits` | ✅ Ready | Same Accela Building export — filter by roofing keywords in `Description` |
| 17 | `master_data` / `absentee_owners` | ✅ Ready | PCPAO `RP_PROPERTY_INFO` + `RP_EXEMPTIONS` — richer than Hillsborough |
| 18 | `code_violations` | ⚠️ PRR only | All public portals exhausted: Socrata decommissioned, Click2Gov and Clearwater Accela have no date-range search. PRR via `pinellas.govqa.us` is the only path. Static dataset, refreshed quarterly. |
| 19 | `tampa_code_liens` / `county_code_liens` | ⚠️ Needs remap | Hillsborough-specific names. Pinellas equivalent = ORI `LIEN` where `DirectName` = county/city filer. `_CITY_FILER_KEYWORDS` and `_CODE_LIEN_CITY_MAP` must be updated for Pinellas jurisdictions. |
| 20 | `enforcement_permit` | ⚠️ Partial | PCPAO `RP_PERMITS` covers open/unsigned permits (nightly bulk). Stop-work enforcement flags unclear — Accela Enforcement is STR-dominated. Needs one more check. |
| 21 | `evictions` | ✅ Ready | `courtrecords.mypinellasclerk.gov` — 34 cases in 5 days. Needs `Style/Description` parser + column normalizer. Name-only matching (no address field). |
| 22 | `fire_incidents` | ❓ Unknown | `egis.pinellas.gov/apps/CrimeViewer/` — needs manual browser check |

**The 1 remaining unknown is a browser check only — fire incidents CSV.**
**The 3 partials are real build items — code violations is the most complex.**

---

## Steps Still TODO (Before Planning Scale-Up)

## Step 6 — Pinellas Clerk Court Records Analysis (Complete)

**Source:** `courtrecords.mypinellasclerk.gov` — date-range export (2026-05-01 to 05-06), 500 rows
**Format:** Excel (.xlsx), single row per case, 7 columns: `Case Type, Case #, Filed, Style/Description, Status, Judicial Officer, Charges`

### Structural Difference vs. Hillsborough

Hillsborough civil daily filings: **multi-row per case** (one row per party), with explicit `PartyType`, `FirstName`, `LastName/CompanyName`, `PartyAddress` columns.

Pinellas court records: **one row per case**, party names embedded in `Style/Description` as `"PLAINTIFF NAME\nVs.\nDEFENDANT NAME"`. **No address field.**

### Column Mapping

| Loader Expects | Pinellas Has | Action |
|---------------|-------------|--------|
| `Case Number` | `Case #` | Rename |
| `FilingDate` | `Filed` | Rename |
| `CaseTypeDescription` | `Case Type` | Rename |
| `Title` (status) | `Status` | Rename |
| `PartyType` | Not present | Parse from `Style/Description` — split on `\nVs.\n` |
| `FirstName` + `LastName/CompanyName` | Not present | Parse from split result |
| `PartyAddress` | **Not present at all** | ❌ No address. Name-only matching for all Pinellas court records. |

### Normalizer Required (pre-loader step)

A thin normalization function before handing to existing loaders:
1. Rename columns as above
2. Parse `Style/Description` on `\nVs.\n` → extract plaintiff (part 0, strip leading whitespace) and defendant (part 1, strip `.et al`)
3. Expand one row into two rows with explicit `PartyType` (`Plaintiff` / `Defendant`)
4. Set `LastName/CompanyName` from parsed name
5. Leave `PartyAddress` as `None` — loaders fall through to name matching (already implemented)

### Distress Signals Found

| Signal | Count (5 days) | Case Types | Action |
|--------|---------------|------------|--------|
| `evictions` | 34 | `RESIDENTIAL EVICTION *`, `UNLAWFUL DETAINER` | Feed `EvictionLoader` after normalizing |
| `divorce_filings` | 13 | `DISSOLUTION OF MARRIAGE - *` | Feed `DivorceLoader` after normalizing |
| Probate | 33 | `FORMAL ADMINISTRATION`, `SUMMARY ADMINISTRATION *`, `DEPOSIT WILL` | **Skip** — already covered by ORI export |
| Court foreclosures | 12 | `REAL PROP - MORTGAGE FORECLOSURE - *`, `REAL PROP/MTGE FRCL *` | **Skip** — HOA/condo foreclosures already captured via ORI `LIS PENDENS` + RealForeclose |

### Match Quality Note

No address = name-only matching for all records. `EvictionLoader` already falls back to plaintiff (landlord) name matching. `DivorceLoader` already falls back to petitioner name. `BankruptcyLoader` is name-only by design. Existing matching logic handles this — the normalizer is the only new piece needed.

---

**Code Violations — RESOLVED (PRR only):**
- [x] Socrata API — decommissioned, dead
- [x] St. Pete Click2Gov — no date-range search, not usable for scraping
- [x] Clearwater Accela CodeCompliance — no date-range search, not usable for scraping
- [x] Pinellas County Accela Enforcement — date range works but ~99% STR records, negligible code violation volume
- [ ] **Submit PRR** via `pinellas.govqa.us` at initial load time — request all code enforcement cases Jan 2020–present from Pinellas County, St. Pete, and Clearwater separately. Static dataset, refresh quarterly.

**Other signals still to verify:**
- [x] `courtrecords.mypinellasclerk.gov` — confirmed: evictions (34/5d) and divorce (13/5d) available. No address field — name-only matching. Normalizer needed. See Step 6.
- [ ] `egis.pinellas.gov/apps/CrimeViewer/` — does CSV export have address-level fire/incident data?
- [ ] Download sample `RP_PROPERTY_INFO` from PCPAO — confirm column names and STRAP format
- [ ] Check Pinellas active parcel count — sets DB size and scoring runtime expectations

---

## Scale-Up Build Plan

**Architecture principle:** Platform-level scrapers driven by county config. Most FL counties share the same 5–6 underlying platforms — only URLs, agency codes, and column names differ. Those differences live in `counties.json`, not in Python. Adding a new county = updating config only (unless the county uses a platform not yet supported).

```
src/scrappers/platforms/          ← one file per platform, works for any county
    real_auction.py               ← realforeclose.com (URL from config)
    accela.py                     ← Accela Building + Enforcement (agency_code from config)
    clerk_ori.py                  ← ORI portal (URL + column_map from config)
    clerk_court.py                ← Clerk court records (URL from config)
    county_taxes.py               ← county-taxes.com (URL from config)

src/scrappers/pcpao/              ← Pinellas-specific (no other county uses PCPAO)
    pcpao_downloader.py
```

Column differences between counties move into `counties.json`:
```json
"pinellas": {
  "ori_column_map": { "DirectName": "Grantor", "IndirectName": "Grantee", ... },
  "ori_book_page_col": "BookPage",   // combined → split on "/"
  "ori_doc_type_map": { "JUDGEMENT LIEN": "JUDGMENT LIEN", ... },
  "court_style_col": "Style/Description"  // parse on "\nVs.\n"
}
"hillsborough": {
  "ori_column_map": {},   // columns already match loader expectations — identity map
  "ori_book_page_col": null,   // Book and Page are separate columns already
  "ori_doc_type_map": {}
}
```

Adding Manatee County later = add a `"manatee"` block to `counties.json`. Zero new Python files unless Manatee uses an unsupported platform.

**Rule:** Every item below is either a new file, a modified file, or a config change. Nothing is "refactor later." Each task is scoped to the minimum required to ship Pinellas without breaking Hillsborough.

---

### Phase 0 — Foundation (Block Everything Else)

These two changes must land first. Every Pinellas loader and scraper depends on them.

#### 0-A: STRAP Normalizer — `src/utils/parcel_id.py` (new file)

Hillsborough folio format and Pinellas STRAP format are different. Every loader calls `normalize_parcel_id()` before matching to the `properties` table. Currently that function assumes Hillsborough format.

**What to build:**
```python
def normalize_parcel_id(raw: str, county: str) -> str:
    if county == "pinellas":
        # STRAP: "SS-TT-RR-SSSSS-LLL-LLLL" — strip spaces, validate hyphen segments
        return _normalize_strap(raw)
    else:
        # Hillsborough folio — existing logic
        return _normalize_folio(raw)
```

Move existing folio normalization out of wherever it currently lives (likely inline in loaders) into `_normalize_folio()`. Add `_normalize_strap()` that accepts `SS-TT-RR-SSSSS-LLL-LLLL`, strips whitespace, validates segment count (6 groups), and returns canonical form. Parcel lookup against `properties.parcel_id` uses this output.

**Files touched:** `src/utils/parcel_id.py` (new), any loaders with inline folio normalization.

---

#### 0-B: County-Aware `_CITY_FILER_KEYWORDS` — `src/loaders/liens.py`

Currently hardcoded at module level (line ~23–29). This breaks when the same loader processes Pinellas ORI output.

**What to build:** Replace the module-level constants with county-aware lookup:

```python
_CITY_FILER_KEYWORDS_BY_COUNTY = {
    "hillsborough": frozenset({
        "CITY OF TAMPA", "HILLSBOROUGH COUNTY",
    }),
    "pinellas": frozenset({
        "PINELLAS COUNTY", "CITY OF ST. PETERSBURG", "CITY OF CLEARWATER",
        "CITY OF LARGO", "CITY OF PINELLAS PARK", "CITY OF DUNEDIN",
        "CITY OF TARPON SPRINGS", "CITY OF SAFETY HARBOR",
    }),
}

_CODE_LIEN_CITY_MAP_BY_COUNTY = {
    "hillsborough": {"TCL": "TAMPA", "CCL": None},
    "pinellas": {},  # populate after first PRR export reveals Pinellas type codes
}
```

Pass `county` down from the loader's `__init__` (already receives config). Any place that reads `_CITY_FILER_KEYWORDS` or `_CODE_LIEN_CITY_MAP` switches to `_BY_COUNTY[self.county]`.

**Files touched:** `src/loaders/liens.py`.

---

#### 0-C: Pinellas County Config — `config/counties.json`

Add Pinellas block alongside the existing Hillsborough block:

```json
"pinellas": {
  "fips": "12103",
  "nws_zone": "FLZ050",
  "realforeclose_url": "https://pinellas.realforeclose.com/index.cfm",
  "county_taxes_url": "https://pinellas.county-taxes.com/public/search/property_tax",
  "accela_agency": "PINELLAS",
  "ori_url": "https://officialrecords.mypinellasclerk.gov",
  "court_records_url": "https://courtrecords.mypinellasclerk.gov",
  "pcpao_bulk_url": "https://www.pcpao.gov/tools-data/data-downloads/raw-database-files",
  "bankruptcy_division": "8",
  "parcel_id_format": "strap"
}
```

**Files touched:** `config/counties.json`, `src/utils/county_config.py` (add `parcel_id_format` reader).

---

### Phase 1 — Master Data / Property Hub

This is the biggest structural change. Hillsborough uses a single master file (DBF/XLS). Pinellas uses 15 CSV tables from PCPAO. The `properties` table is populated from the master file. A new loader is needed.

#### 1-A: PCPAO Master Loader — `src/loaders/pcpao_master.py` (new file)

Downloads (or reads from disk) and joins 5 PCPAO tables into the `properties` table format.

**Join chain:**
```
RP_PROPERTY_INFO (base)
  LEFT JOIN RP_EXEMPTIONS ON STRAP          → HX_YN for absentee flag
  LEFT JOIN RP_BUILDING ON STRAP            → YEAR_BUILT, HEATED_AREA_SQFT (per building, take max or primary)
  LEFT JOIN RP_STRUCTURAL_ELEMENTS ON STRAP → pivot BEDROOMS / FULL BATHS
  LEFT JOIN RP_ALL_SITE_ADDRESSES ON STRAP  → deduplicated site address (for condo units)
```

**Column mapping to `properties` table:**

| `properties` column | PCPAO source |
|--------------------|-|
| `parcel_id` | `STRAP` → `normalize_parcel_id(raw, "pinellas")` |
| `address` | `SITE_ADDRESS` from RP_PROPERTY_INFO (or primary row from RP_ALL_SITE_ADDRESSES) |
| `city` | `STR_CITY` |
| `zip_code` | `STR_ZIP` |
| `owner_name` | `OWNER1` (+ `OWNER2` if present, concatenated) |
| `mailing_address` | `MAILING_ADDRESS_1` + city/state/zip |
| `is_absentee` | `HX_YN = 'N'` (RP_EXEMPTIONS join) OR `DLHL_YN = 'Y'` |
| `property_use` | `PROPERTY_USE` |
| `land_value` | `JUST_LAND` |
| `building_value` | `JUST_BUILDING` |
| `assessed_value` | `CNTY_JST_VALUE` |
| `year_built` | `YEAR_BUILT` (RP_BUILDING primary building) |
| `living_area_sqft` | `TOTAL_LIVING_SQFT` (RP_PROPERTY_INFO) or `HEATED_AREA_SQFT` sum (RP_BUILDING) |
| `bedrooms` | pivot from RP_STRUCTURAL_ELEMENTS where `ATTRIBUTE_DSCR = 'BEDROOMS'` |
| `bathrooms` | pivot from RP_STRUCTURAL_ELEMENTS where `ATTRIBUTE_DSCR = 'FULL BATHS'` |
| `latitude` | `LATITUDE` |
| `longitude` | `LONGITUDE` |
| `county` | `"pinellas"` (hardcoded) |

**Pinellas-only extras to store (extend `properties` or `financials` table):**

| Column | Table | Scoring use |
|--------|-------|------------|
| `evac_zone` | RP_PROPERTY_INFO | Storm/flood distress weighting |
| `contamination_yn` | RP_PROPERTY_INFO | Environmental distress flag |
| `subsidence_yn` | RP_PROPERTY_INFO | Sinkhole risk flag |
| `special_assessment` | RP_PROPERTY_INFO | Hidden carrying cost |
| `hx_savings` | RP_PROPERTY_INFO | `$0` = absentee signal |

Add these as nullable columns in an Alembic migration (additive — no Hillsborough rows affected).

**Loader class signature:**
```python
class PcpaoMasterLoader(BaseLoader):
    def __init__(self, county="pinellas", data_dir=None):
        ...
    def load(self, session):
        # 1. Read 5 CSVs from data_dir (or download from PCPAO)
        # 2. Join chain above
        # 3. Pivot structural elements
        # 4. Upsert into properties table (keyed on parcel_id + county)
```

**Files touched:** `src/loaders/pcpao_master.py` (new), Alembic migration for extra columns.

---

#### 1-B: PCPAO Downloader — `src/scrappers/pcpao/pcpao_downloader.py` (new file)

The PCPAO raw files are static URLs on `pcpao.gov/tools-data/data-downloads/raw-database-files`. They refresh nightly.

**What to build:** Simple `requests`-based downloader (no Playwright needed — these are direct CSV links). Downloads the 5 tables needed for the master loader + `RP_PERMITS` + `RP_SALES_HISTORY` into `data/pinellas/pcpao/`. Verifies file sizes against prior run (detects failed refreshes). Runs once nightly before the master loader.

**Files touched:** `src/scrappers/pcpao/pcpao_downloader.py` (new), `src/scrappers/pcpao/__init__.py` (new).

---

### Phase 2 — Platform Scraper: Clerk ORI

One scraper file. Works for any county. Hillsborough and Pinellas (and every future county) both use it.

#### 2-A: `src/scrappers/platforms/clerk_ori.py` (new file)

**What to build:** Playwright scraper that takes a `county` string, loads config from `counties.json`, and runs the ORI date-range search against whatever URL is configured. Output: raw CSV saved to `data/{county}/ori/`.

```python
class ClerkOriScraper:
    def __init__(self, county: str):
        self.county = county
        cfg = get_county_config(county)
        self.url = cfg["ori_url"]
        self.column_map = cfg.get("ori_column_map", {})      # rename map
        self.book_page_col = cfg.get("ori_book_page_col")    # None = already split
        self.doc_type_map = cfg.get("ori_doc_type_map", {})  # full text → internal label

    def scrape(self, start_date, end_date) -> pd.DataFrame:
        # 1. Playwright: navigate to self.url, submit date-range form, download CSV
        # 2. Apply self.column_map renames
        # 3. If self.book_page_col: split "23544/1338" → Book="23544", Page="1338"
        # 4. Apply self.doc_type_map to document_type column
        # 5. Add "Filing Amt" = None if column absent
        # Returns: DataFrame with canonical columns (Grantor, Grantee, Instrument,
        #          Legal, document_type, BookType, Book, Page, RecordDate)
```

**`counties.json` additions:**

```json
"hillsborough": {
  "ori_url": "https://pubrec.hillsclerk.com",
  "ori_column_map": {},       // identity — columns already match loader expectations
  "ori_book_page_col": null,  // Book and Page are separate columns
  "ori_doc_type_map": {}
},
"pinellas": {
  "ori_url": "https://officialrecords.mypinellasclerk.gov",
  "ori_column_map": {
    "DirectName": "Grantor",
    "IndirectName": "Grantee",
    "InstrumentNumber": "Instrument",
    "Comments": "Legal",
    "DocTypeDescription": "document_type"
  },
  "ori_book_page_col": "BookPage",
  "ori_doc_type_map": {
    "JUDGEMENT LIEN": "JUDGMENT LIEN",
    "JUDGEMENT": "JUDGMENT",
    "LIEN (IRS)": "TAX LIEN",
    "DOMESTIC RELATIONS JUDGMENT": "JUDGMENT",
    "PROBATE REAL PROPERTY": "PROBATE",
    "CERTIFIED COPY OF A COURT JUDGMENT OR ORDER": "JUDGMENT",
    "CORPORATE LIEN": "LIEN"
  }
}
```

After scraping, dispatch normalized rows by `document_type` to existing loaders (same logic as today — no loader changes):
- `JUDGMENT*`, `LIEN*`, `TAX LIEN` → `LienLoader`
- `DEED` → `DeedLoader`
- `LIS PENDENS` → `LisPendensLoader`
- `PROBATE` → `ProbateLoader`
- Everything else → skip

**Hillsborough migration:** The existing Hillsborough ORI scraper keeps running until `ClerkOriScraper("hillsborough")` is validated in prod. Then swap and delete the old file.

**Files touched:** `src/scrappers/platforms/clerk_ori.py` (new), `src/scrappers/platforms/__init__.py` (new), `config/counties.json` (add ORI fields to both counties).

---

### Phase 3 — Platform Scraper: Clerk Court Records

#### 3-A: `src/scrappers/platforms/clerk_court.py` (new file)

**What to build:** Playwright scraper that takes a `county` string, queries the court records portal for a date range, downloads Excel/CSV. Config-driven for URL. Output saved to `data/{county}/court_records/`.

```python
class ClerkCourtScraper:
    def __init__(self, county: str):
        self.county = county
        cfg = get_county_config(county)
        self.url = cfg["court_records_url"]
        self.style_col = cfg.get("court_style_col")   # if set, parse "Plaintiff\nVs.\nDefendant"

    def scrape(self, start_date, end_date) -> pd.DataFrame:
        # 1. Playwright: navigate, submit date range, download Excel
        # 2. Rename columns to canonical names (Case Number, FilingDate, CaseTypeDescription, Title)
        # 3. If self.style_col is set: expand one-row-per-case into two rows with PartyType
        # Returns: DataFrame in Hillsborough multi-row format that existing loaders expect
```

The `Style/Description` expansion logic (Pinellas-specific — split on `\nVs.\n`, strip `et al`, emit Plaintiff/Defendant rows) lives inside `clerk_court.py`, triggered only when `court_style_col` is set in config. Hillsborough has separate party columns already — no expansion needed, `court_style_col` is null.

**`counties.json` additions:**

```json
"hillsborough": {
  "court_records_url": "https://court.hillsclerk.com/...",
  "court_style_col": null     // party columns already explicit
},
"pinellas": {
  "court_records_url": "https://courtrecords.mypinellasclerk.gov",
  "court_style_col": "Style/Description"   // triggers Vs. parser
}
```

After scraping, filter by `CaseTypeDescription` and dispatch:
- `RESIDENTIAL EVICTION*`, `UNLAWFUL DETAINER` → `EvictionLoader`
- `DISSOLUTION OF MARRIAGE*` → `DivorceLoader`
- All others → skip

**Files touched:** `src/scrappers/platforms/clerk_court.py` (new), `config/counties.json`.

---

### Phase 4 — Platform Scrapers: Accela, RealAuction, county-taxes.com

These are the same platform as Hillsborough — only the URL or agency code changes. Build platform scrapers, wire both counties via config.

#### 4-A: `src/scrappers/platforms/accela.py` (new file)

```python
class AccelaScraper:
    def __init__(self, county: str, module: str = "Building"):
        cfg = get_county_config(county)
        self.agency = cfg["accela_agency"]   # "HCFL" or "PINELLAS"
        self.module = module                 # "Building" or "Enforcement"
        # URL: f"https://aca-prod.accela.com/{self.agency}"
```

`counties.json`:
```json
"hillsborough": { "accela_agency": "HCFL" },
"pinellas":     { "accela_agency": "PINELLAS" }
```

**One loader fix for permits:** Add `"waiting on applicant"` to `_ENFORCEMENT_STATUS_VALUES` in `src/loaders/permits.py`. One line — Pinellas Accela uses this status where Hillsborough uses `"awaiting client reply"`.

#### 4-B: `src/scrappers/platforms/real_auction.py` (new file)

```python
class RealAuctionScraper:
    def __init__(self, county: str):
        cfg = get_county_config(county)
        self.url = cfg["realforeclose_url"]
        # "https://hillsborough.realforeclose.com/index.cfm"
        # "https://pinellas.realforeclose.com/index.cfm"
```

#### 4-C: `src/scrappers/platforms/county_taxes.py` (new file)

```python
class CountyTaxesScraper:
    def __init__(self, county: str):
        cfg = get_county_config(county)
        self.url = cfg["county_taxes_url"]
```

#### 4-D: National API scrapers (FEMA, NWS, Bankruptcy) — config only

These already hit national endpoints — only the FIPS / NWS zone / bankruptcy division parameter changes. No new files. Update `counties.json` Pinellas block with `fips`, `nws_zone`, `bankruptcy_division` and the existing scrapers read from config.

```json
"pinellas": {
  "fips": "12103",
  "nws_zone": "FLZ050",
  "bankruptcy_division": "8"
}
```

**SunBiz:** Statewide. Zero changes.

---

### Phase 5 — PCPAO Permit & Sales History Loaders

These replace Accela and Clerk ORI scraping for two signals, providing richer history.

#### 5-A: PCPAO Permit Loader — `src/loaders/pcpao_permits.py` (new file)

Reads `RP_PERMITS` (downloaded by pcpao_downloader) and feeds `BuildingPermitLoader`-compatible rows.

Column mapping:

| RP_PERMITS | `building_permits` table |
|-----------|--------------------------|
| `STRAP` | `parcel_id` (normalize_strap) |
| `PERMIT_NUMBER` | `permit_number` |
| `PERMIT_TYPE` | `permit_type` |
| `PERMIT_DSCR` | `description` |
| `AGENCY_NAME` | *(store as metadata)* |
| `ISSUE_DT` | `issue_date` |
| `SIGN_OFF_DT` | `finaled_date` (null = open permit) |
| `EST_VAL` | `estimated_value` |

Open permit flag: `SIGN_OFF_DT IS NULL AND ISSUE_DT IS NOT NULL`. This is the title-issue distress signal — no separate enforcement check needed.

**Note:** PCPAO permits come from all agencies (county + municipalities). This has wider coverage than Accela alone. For Pinellas, use PCPAO `RP_PERMITS` as primary; Accela Building scraper is optional supplemental.

---

#### 5-B: PCPAO Sales History Loader — `src/loaders/pcpao_sales.py` (new file)

Reads `RP_SALES_HISTORY` and feeds `DeedLoader`-compatible rows. Covers the distress deed transfer signal.

Key columns: `STRAP`, `GRANTOR`, `GRANTEE`, `SALES_DATE`, `PRICE`, `TRNS_CD`, `QU_FLG`.

Distress filter: `QU_FLG = 'U'` (unqualified = distress sale) OR `TRNS_CD` in (`FORECLOSURE DEED`, `QUIT CLAIM DEED`). These map to the existing deed distress signal.

---

### Phase 6 — Code Violations (PRR Path)

Not a scraper build — a data intake workflow.

#### 6-A: PRR Submission Checklist

Submit via `pinellas.govqa.us` before Pinellas data load:

1. **Pinellas County** — "All code enforcement cases opened Jan 1, 2020 to present. Fields: case number, site address, parcel ID, violation type, opened date, status, fine amount if any. Format: CSV export from Accela."
2. **City of St. Petersburg** — same request, their public records portal
3. **City of Clearwater** — same request, their public records portal

Response timeline: 5 business days acknowledgment, 10 business days delivery typical. Cost: $0–$150 depending on staff time.

#### 6-B: PRR Import Loader — `src/loaders/prr_violations.py` (new file)

Once PRR CSVs arrive, they will be Accela internal exports (same system — column names differ from public UI). Map whatever columns arrive to `ViolationLoader` format. Generic enough to handle slight column name variations across the three jurisdictions.

This file can be written after PRR data arrives. Placeholder only at build time.

---

### Phase 7 — Database Migration

One Alembic migration covering all additive changes. **No destructive changes to existing tables.**

```python
# New nullable columns on properties table (Pinellas extras)
op.add_column("properties", sa.Column("evac_zone", sa.String(2), nullable=True))
op.add_column("properties", sa.Column("contamination_yn", sa.Boolean, nullable=True))
op.add_column("properties", sa.Column("subsidence_yn", sa.Boolean, nullable=True))
op.add_column("properties", sa.Column("special_assessment", sa.Numeric(12,2), nullable=True))
op.add_column("properties", sa.Column("hx_savings", sa.Numeric(12,2), nullable=True))
op.add_column("properties", sa.Column("county", sa.String(50), nullable=True))  # if not already present

# New index for county-scoped queries
op.create_index("ix_properties_county", "properties", ["county"])
```

If `county` column already exists on `properties`, skip that line.

---

### Phase 8 — Scoring Config Updates

No new signals — Pinellas uses the same 22 signals. Two scoring config changes:

**8-A: County-gate county in CDS engine.** When scoring a Pinellas property, the engine should only look at distress signals where the record's county = `"pinellas"`. If the `distress_scores` table doesn't already carry a `county` column, add it (Alembic migration).

**8-B: Absentee bonus logic update.** Hillsborough: mailing address ≠ site address → `is_absentee = True`. Pinellas: `HX_YN = 'N'` or `DLHL_YN = 'Y'`. The PCPAO master loader sets `is_absentee` on the `properties` row directly. The CDS engine reads `properties.is_absentee` — no change to scoring weights needed.

---

### Build Order & Effort Estimates

| Order | Task | File(s) | Effort |
|-------|------|---------|--------|
| 1 | STRAP normalizer | `src/utils/parcel_id.py` | 2h |
| 2 | County-aware lien keywords | `src/loaders/liens.py` | 1h |
| 3 | Pinellas county config (all fields) | `config/counties.json`, `county_config.py` | 1h |
| 4 | DB migration (new columns) | Alembic migration | 1h |
| 5 | PCPAO downloader | `src/scrappers/pcpao/` | 3h |
| 6 | PCPAO master loader | `src/loaders/pcpao_master.py` | 6h |
| 7 | Platform: `clerk_ori.py` (replaces Hillsborough ORI scraper + Pinellas ORI) | `src/scrappers/platforms/clerk_ori.py` | 4h |
| 8 | Platform: `clerk_court.py` (replaces Hillsborough court scraper + Pinellas court) | `src/scrappers/platforms/clerk_court.py` | 3h |
| 9 | Platform: `accela.py` (Building + Enforcement, both counties) | `src/scrappers/platforms/accela.py` | 3h |
| 10 | Platform: `real_auction.py` | `src/scrappers/platforms/real_auction.py` | 2h |
| 11 | Platform: `county_taxes.py` | `src/scrappers/platforms/county_taxes.py` | 2h |
| 12 | PCPAO permits loader | `src/loaders/pcpao_permits.py` | 3h |
| 13 | PCPAO sales history loader | `src/loaders/pcpao_sales.py` | 2h |
| 14 | Permits loader fix (1 line) | `src/loaders/permits.py` | 0.25h |
| 15 | National APIs config update (FEMA/NWS) — config only | `config/counties.json` | 0.5h |
| 16 | PRR submission | External (manual) | 0.5h |
| 17 | PRR import loader (after data arrives) | `src/loaders/prr_violations.py` | 3h |
| 18 | Cron schedule updates | `scripts/cron/crontab.txt` | 0.5h |
| — | **Total (excluding PRR wait)** | | **~37h** |

Note: Steps 7–11 also migrate Hillsborough onto the platform scrapers. The old county-specific Hillsborough scrapers get deleted once the platform versions are validated in prod. No net increase in files — the platform files replace the old ones.

---

### New Files Created

```
src/utils/parcel_id.py                     # STRAP + folio normalizer
src/scrappers/platforms/__init__.py
src/scrappers/platforms/clerk_ori.py       # ORI scraper — any county, config-driven
src/scrappers/platforms/clerk_court.py     # Court records scraper — any county, config-driven
src/scrappers/platforms/accela.py          # Accela Building + Enforcement — any county
src/scrappers/platforms/real_auction.py    # RealAuction — any county
src/scrappers/platforms/county_taxes.py    # county-taxes.com — any county
src/scrappers/pcpao/__init__.py
src/scrappers/pcpao/pcpao_downloader.py    # nightly PCPAO CSV downloader (Pinellas-only)
src/loaders/pcpao_master.py               # PCPAO 5-table → properties loader
src/loaders/pcpao_permits.py              # RP_PERMITS → building_permits loader
src/loaders/pcpao_sales.py               # RP_SALES_HISTORY → deeds loader
src/loaders/prr_violations.py            # PRR CSV → violations loader (post-PRR)
```

### Modified Files (Existing)

```
src/loaders/liens.py                # county-aware _CITY_FILER_KEYWORDS
src/loaders/permits.py              # add "waiting on applicant" status
config/counties.json                # Pinellas block + ORI/court fields for Hillsborough
src/utils/county_config.py          # parcel_id_format reader
scripts/cron/crontab.txt            # Pinellas cron entries
alembic/versions/XXXX_pinellas.py   # additive DB migration
```

### Deleted (after platform scrapers validated)

```
src/scrappers/ori/hillsborough_ori.py          # replaced by platforms/clerk_ori.py
src/scrappers/court_records/hillsborough_*.py  # replaced by platforms/clerk_court.py
src/scrappers/foreclosures/foreclosure_engine.py → migrated to real_auction.py
```

---

### Launch Readiness Checklist

Before flipping Pinellas live in the scoring engine:

- [ ] PCPAO master load complete — check `SELECT COUNT(*) FROM properties WHERE county='pinellas'` (expect ~400–500k parcels)
- [ ] ORI load complete — spot-check 10 liens against `officialrecords.mypinellasclerk.gov` by instrument number
- [ ] Foreclosure load complete — cross-check count against `pinellas.realforeclose.com` auction list
- [ ] Tax delinquency load complete — spot-check against `pinellas.county-taxes.com`
- [ ] Bankruptcy load complete — PACER FLMB Tampa case count sanity check
- [ ] Scoring run complete — `SELECT tier, COUNT(*) FROM distress_scores WHERE county='pinellas' GROUP BY tier` — verify Ultra Platinum < 0.5%, Platinum < 5%, Gold < 15%
- [ ] Absentee flag sanity — `SELECT COUNT(*) FROM properties WHERE county='pinellas' AND is_absentee=TRUE` — expect 40–55% of parcels (vacation/rental county)
- [ ] GHL CRM push tested with 1 Pinellas lead before bulk push
- [ ] PRR submitted to Pinellas County, St. Pete, Clearwater — note submission date for follow-up
