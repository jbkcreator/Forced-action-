# A7 Macro Signal Sources — Assessment

Discovery sprint: 2026-06-25. No DB tables created. No CDS integration. Loaders only.

---

## Source Summary Table

| Source | Data available | Access method | API key? | Free? | Update freq | Geography | Build priority |
|---|---|---|---|---|---|---|---|
| **FRED** | Mortgage rates, CPI, unemployment, HPI (Case-Shiller), vacancy rates, Fed funds rate, GDP | REST API (GET) | Yes — free registration | Yes | Weekly / Monthly / Quarterly | National (some state/MSA series) | **1 — Mandatory** |
| **FHFA HPI** | House Price Index — purchase-only and all-transactions | CSV download (no API) | No | Yes | Monthly / Quarterly | National, division, state, MSA, county, ZIP5, tract | **2 — High** |
| **BLS** | Unemployment (national + MSA + county), CPI shelter/rent, job openings | REST API (POST v2 / GET v1) | Optional — free registration unlocks higher limits | Yes | Monthly | National (some MSA/county via LAUS) | **3 — Medium** |
| **Census ACS5** | Median income, vacancy, housing units, tenure, population, median rent, median home value | REST API (GET) | Yes — free registration | Yes | Annual (5-yr estimates) | Nation → state → county → tract → block group → ZIP (ZCTA) | **4 — Later** |

---

## FRED

**Base URL:** `https://api.stlouisfed.org/fred/`  
**Key endpoint:** `GET /fred/series/observations?series_id=MORTGAGE30US&api_key=KEY&file_type=json`  
**Rate limit:** 120 requests/minute. No daily cap.  
**Key signup:** https://fred.stlouisfed.org/docs/api/api_key.html (instant, free)  
**Config key:** `FRED_API_KEY` → `settings.fred_api_key`

**Implemented:** `fred_client.py`, `fred_rates_loader.py`

**Response shape:**
```json
{
  "observations": [
    {"date": "2024-01-04", "value": "6.62"},
    {"date": "2023-12-28", "value": "."}
  ]
}
```
Missing values use `"."` as sentinel — loader skips them automatically.

**Recommended series for A7:**

| Series ID | Signal key | Frequency | Notes |
|---|---|---|---|
| `MORTGAGE30US` | `mortgage_rate_30yr` | Weekly | Primary rate signal — implemented |
| `MORTGAGE15US` | `mortgage_rate_15yr` | Weekly | Implemented |
| `UNRATE` | `unemployment_rate_national` | Monthly | Duplicate of BLS LNS14000000 |
| `CPIAUCSL` | `cpi_all_urban_sa` | Monthly | SA version; BLS has NSA |
| `CSUSHPISA` | `case_shiller_hpi_national` | Monthly | S&P/Case-Shiller via FRED, SA |
| `HOUST` | `housing_starts` | Monthly | New construction activity |
| `USRVAC` | `rental_vacancy_rate` | Quarterly | National vacancy signal |
| `MSPUS` | `median_home_sale_price` | Quarterly | Census/HUD via FRED |
| `FEDFUNDS` | `fed_funds_rate` | Monthly | Benchmark for rate context |

**Limitations:** National-level only for most series. State/MSA variants exist but require series ID discovery via `/fred/series/search`.

---

## FHFA House Price Index

**Master CSV:** `https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv`  
**Auth:** None. No API. Static government file server.  
**Rate limit:** None.  
**File size:** ~20MB (all geographies, all series types, full history).

**Implemented:** `fhfa_hpi_loader.py`

**CSV columns:** `hpi_type`, `hpi_flavor`, `frequency`, `level`, `place_name`, `place_id`, `yr`, `period`, `index_nsa`, `index_sa`

**Index types:**
- `traditional / purchase-only` — Fannie/Freddie sales only (back to Jan 1991 monthly)
- `traditional / all-transactions` — sales + appraisals (back to 1975 quarterly at national)
- `expanded-data / purchase-only` — adds FHA + county recorder data

**Geography levels available:**

| Level value in CSV | geography_scope | Coverage |
|---|---|---|
| `USA or Census Division` | `national_or_division` | 1 national + 9 census divisions |
| `state` | `state` | 50 states + DC |
| `MSA` | `metro` | 400+ metro areas |
| `county` | `county` | County FIPS |
| `ZIP5` | `zip5` | Annual only |
| `3-Digit ZIP` | `zip3` | Developmental series |
| `census_tract` | `tract` | Annual only |

**Recommended use:** Pull the master CSV once per quarter (aligns with FHFA's quarterly release), parse and upsert into a `macro_signals` table filtered to `purchase-only / monthly` for state + county granularity. Tract/ZIP available annually for deeper geographic cuts.

**Limitations:** No real-time API. File must be re-downloaded on each refresh. `index_sa` is null for some series/geographies (NSA is always present).

---

## BLS (Bureau of Labor Statistics)

**v1 endpoint (no key):** `GET https://api.bls.gov/publicAPI/v1/timeseries/data/{seriesID}`  
**v2 endpoint (with key):** `POST https://api.bls.gov/publicAPI/v2/timeseries/data/`  
**Key signup:** https://www.bls.gov/developers/home.htm (email-based, free)  
**Config key:** Not yet added to settings — add `BLS_API_KEY` if extended history needed.

**Implemented:** `bls_client.py` (v2 POST, key optional)

**Rate limits:**

| | Without key | With key |
|---|---|---|
| Series per request | 25 | 50 |
| Queries per day | 25 | 500 |
| Years per query | 10 | 20 |

**Period format:** `M01`–`M12` (monthly), `M13` (annual average — skipped by loader), `Q01`–`Q04` (quarterly).

**Recommended series:**

| Series ID | Signal key | Notes |
|---|---|---|
| `LNS14000000` | `unemployment_rate_national` | Seasonally adjusted, national |
| `CUSR0000SA0` | `cpi_all_urban_sa` | CPI-U all items, SA |
| `CUSR0000SAH1` | `cpi_shelter` | CPI shelter component — housing cost proxy |
| `CUSR0000SEHA` | `cpi_rent_primary_residence` | Rent CPI — direct rental market signal |
| `CUSR0000SEHC` | `cpi_owners_equivalent_rent` | OER — complements rent CPI |

**County-level unemployment via LAUS:** Series ID format `LAUCN{state_fips}{county_fips}0000000003`. Example: Hillsborough County FL = `LAUCN120570000000003`. Useful for county-level macro context alongside property signals. Not yet implemented — add when county-level signals are needed.

**Limitations:** Series ID discovery requires navigating BLS ID format documentation. No bulk catalog endpoint — must know series IDs in advance. County LAUS IDs require constructing FIPS-based strings.

---

## Census ACS5

**Endpoint pattern:** `GET https://api.census.gov/data/{year}/acs/acs5?get={vars}&for=county:*&in=state:{fips}&key={KEY}`  
**Key signup:** https://api.census.gov/data/key_signup.html (instant, free)  
**Config key:** `CENSUS_API_KEY` → `settings.census_api_key`

**Implemented:** `census_client.py` (Florida counties, configurable variables)

**Response format:** JSON array-of-arrays. First row = headers. Suppressed cells use large negative values (e.g. `-666666666`) — loader skips values below `-1,000,000`.

**Key variables implemented:**

| Variable | Signal key | Unit |
|---|---|---|
| `B25001_001E` | `total_housing_units` | count |
| `B25002_002E` | `occupied_housing_units` | count |
| `B25002_003E` | `vacant_housing_units` | count |
| `B25003_002E` | `owner_occupied_units` | count |
| `B25003_003E` | `renter_occupied_units` | count |
| `B25064_001E` | `median_gross_rent` | dollars |
| `B25077_001E` | `median_home_value` | dollars |
| `B19013_001E` | `median_household_income` | dollars |
| `B23025_005E` | `unemployed_civilian` | count |
| `B01003_001E` | `total_population` | count |

**Geography granularity:** County is the implemented level. Finer geographies (tract, block group, ZCTA) are available by changing the `for=` parameter. Most useful future addition: ZIP Code Tabulation Areas (ZCTA) for `B25064_001E` (median rent) and `B25077_001E` (median home value) to align with property-level ZIP signals.

**Limitations:** Annual release with ~1 year lag (2022 ACS5 = most recent reliable). 5-year estimates smooth year-over-year volatility — good for structural signals, not current-market timing. Max 50 variables per request.

---

## What's Deferred (Out of Scope for A7 MVP)

- **DB table / Alembic migration** — no `macro_signals` table yet. Schema to be designed after reviewing actual normalized field shapes from sample outputs.
- **CDS scoring integration** — no weight mapping yet. A7 Phase 2 will add signal keys to `config/scoring.py` or a separate macro weight table.
- **LAUS county unemployment** — BLS LAUS series for Hillsborough/Pinellas are straightforward to add to `bls_client.py` once needed.
- **Census tract/ZIP** — `census_client.py` supports it via parameter change; not fetched yet.
- **FRED state/MSA series** — discoverable via `/fred/series/search?search_text=...&filter_value=States`. Add series IDs to `source_registry.py` as needed.
- **Production cron** — no scheduling added. Loaders are standalone and can be wired into the existing cron pattern once the DB table exists.
- **Intraday/real-time signals** — all four sources are daily-or-slower. Not a limitation for weekly CDS rescore cycles.

---

## Files Created

```
config/settings.py                                   ← FRED_API_KEY + CENSUS_API_KEY added
src/loaders/macro_signals/__init__.py
src/loaders/macro_signals/normalization.py           ← normalize_record(), REQUIRED_KEYS
src/loaders/macro_signals/source_registry.py         ← SOURCES dict (all 4 sources)
src/loaders/macro_signals/fred_client.py             ← FREDClient.fetch_observations()
src/loaders/macro_signals/fred_rates_loader.py       ← fetch_mortgage_rates(), save_sample()
src/loaders/macro_signals/bls_client.py              ← BLSClient.fetch_series()
src/loaders/macro_signals/census_client.py           ← CensusClient.fetch_county()
src/loaders/macro_signals/fhfa_hpi_loader.py         ← fetch_hpi(), parse_hpi_master()
tests/macro_signals/test_macro_signals.py            ← 43 tests, all passing
data/reference/macro_signals_samples/                ← output dir for sample JSON files
```

## Generating Sample Outputs

Once `FRED_API_KEY` is set in `.env`:

```powershell
# FRED mortgage rates (52 weeks)
PYTHONPATH=. .venv/Scripts/python.exe -m src.loaders.macro_signals.fred_rates_loader
# → data/reference/macro_signals_samples/fred_mortgage30us_sample.json

# FHFA HPI (national + state, monthly since 2020, no key needed)
PYTHONPATH=. .venv/Scripts/python.exe -m src.loaders.macro_signals.fhfa_hpi_loader
# → data/reference/macro_signals_samples/fhfa_hpi_sample.json
```
