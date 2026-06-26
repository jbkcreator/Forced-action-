# Sprint A7 — External Macro-Signal Ingestion Pipeline
**Status:** Complete  
**Completed:** 2026-06-26  
**Branch:** `feat/a7-macro-signals`

---

## Objective

Build a standardized ETL ingest system to consume external macroeconomic, real-estate trend, and title transfer signals. Fetch, normalize, persist, and make available to the CDS scoring layer the following signal families: mortgage rates (FRED), house price index (FHFA), labor/inflation indicators (BLS), and county-level demographic/housing data (Census ACS5).

---

## Deliverables

### 1. Source Assessment

**`docs/macro_signal_sources_assessment.md`**

Full evaluation of all candidate data sources: access method, key requirement, rate limits, geography levels, series IDs, relevance to CDS scoring, and sample output commands.

| Source | Access | Key | Rate Limit | Geography |
|--------|--------|-----|-----------|-----------|
| FRED | REST API | Free key | 120 req/min | National only |
| FHFA HPI | CSV download | None | None | National, state, MSA, county, ZIP |
| BLS | REST API (POST) | Optional | 25 req/day (no key) | National; county via LAUS series |
| Census ACS5 | REST API | Free key | Generous | County, tract, ZIP |

---

### 2. Normalization Contract

**`src/loaders/macro_signals/normalization.py`**  
**`src/loaders/macro_signals/__init__.py`**

All loaders produce dicts conforming to a single normalized schema:

```
REQUIRED_KEYS = (
    "source", "signal_key", "source_series_id", "value",
    "observed_at", "frequency", "geography_scope", "geography_id",
    "unit", "raw_payload"
)
```

Every loader calls `normalize_record(...)` — the service layer accepts this shape directly with no per-source translation needed.

---

### 3. Source Registry

**`src/loaders/macro_signals/source_registry.py`**

Central catalog of all sources, series IDs, and metadata:

```python
SOURCES = {
    "fred":        { series: MORTGAGE30US, MORTGAGE15US, DFF, T10YIE },
    "bls":         { series: LNS14000000, CUUR0000SA0, CUSR0000SEHA },
    "fhfa":        { series: HPI_master },
    "census_acs5": { series: B19013_001E, B25002_001E, B25002_003E, ... (10 vars) },
}
```

---

### 4. Loader Clients

#### FRED — `src/loaders/macro_signals/fred_client.py` + `fred_rates_loader.py`
- REST GET against `api.stlouisfed.org/fred/series/observations`
- Requires free `FRED_API_KEY` in `.env`
- Skips `"."` (missing value sentinel) and non-numeric values
- `fetch_mortgage_rates(series_ids, limit, observation_start)` → normalized records
- Default series: `MORTGAGE30US`, `MORTGAGE15US`
- Sample output: `data/reference/macro_signals_samples/fred_mortgage30us_sample.json` (52 weekly records, latest: 6.47% on 2026-06-18)

#### FHFA HPI — `src/loaders/macro_signals/fhfa_hpi_loader.py`
- Downloads single ~20MB CSV from `fhfa.gov/hpi/download/monthly/hpi_master.csv`
- No key, no rate limit — download once, parse in-memory
- Filters: `hpi_flavor`, `frequency`, geography `level`, `place_id`, `min_year`
- `LEVEL_SCOPE` map normalizes FHFA level strings → geography_scope values
- Sample output: `data/reference/macro_signals_samples/fhfa_hpi_sample.json` (200 of 750 national + state records since 2020)

#### BLS — `src/loaders/macro_signals/bls_client.py`
- POST to `api.bls.gov/publicAPI/v2/timeseries/data/`
- API key optional (basic: 25 req/day; registered: higher limits)
- Skips `M13` (annual average) entries — monthly only
- Raises `RuntimeError` on non-`REQUEST_SUCCEEDED` status
- Sample output: `data/reference/macro_signals_samples/bls_sample.json` (141 monthly records, 3 series, 2022–2025)

#### Census ACS5 — `src/loaders/macro_signals/census_client.py`
- REST GET against `api.census.gov/data/{year}/acs/acs5`
- Requires free `CENSUS_API_KEY` in `.env`
- Florida state FIPS = `12`; fetches all 67 FL counties by default
- Skips suppressed cells (value < −1,000)
- 10 ACS5 variables: total housing units, occupied/vacant, owner/renter, gross rent, home value, median income, unemployment, population
- Sample output: `data/reference/macro_signals_samples/census_sample.json` (200 of 670 records — all FL counties × 10 variables)

---

### 5. Sample Outputs Generated

| File | Records | Contents |
|------|---------|---------|
| `fred_mortgage30us_sample.json` | 52 | Weekly 30-yr mortgage rate, 2024–2026 |
| `fhfa_hpi_sample.json` | 200 of 750 | Monthly HPI, national + state divisions, 2020–2026 |
| `bls_sample.json` | 141 | Monthly unemployment rate, CPI-U, rent CPI, 2022–2025 |
| `census_sample.json` | 200 of 670 | All 67 FL counties × 10 ACS5 housing/demographic vars, 2022 vintage |

---

### 6. Settings

**`config/settings.py`** — two new optional secrets added:

```python
fred_api_key:    Optional[SecretStr] = Field(default=None, env="FRED_API_KEY")
census_api_key:  Optional[SecretStr] = Field(default=None, env="CENSUS_API_KEY")
```

---

### 7. Database Persistence Layer

#### ORM Model — `src/core/models.py`

`MacroSignal` appended:

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | `generate_uuidv7()` PK |
| `source` | VARCHAR(30) | e.g. `fred`, `bls`, `fhfa`, `census_acs5` |
| `signal_key` | VARCHAR(80) | e.g. `mortgage_30y_fixed`, `house_price_index` |
| `source_series_id` | VARCHAR(120) | e.g. `MORTGAGE30US`, `ACS5_2022_B19013_001E` |
| `value` | NUMERIC(18,6) | Normalized signal value |
| `unit` | VARCHAR(30) | e.g. `percent`, `index`, `dollars` |
| `observed_at` | DATE | Observation date |
| `frequency` | VARCHAR(20) | `weekly`, `monthly`, `annual` |
| `geography_scope` | VARCHAR(50) | `national`, `state`, `county`, etc. |
| `geography_id` | VARCHAR(30) | `US`, `FL`, FIPS code, CBSA code |
| `raw_payload` | JSONB | Full source response for traceability |
| `created_at` | TIMESTAMPTZ | Set on insert |
| `updated_at` | TIMESTAMPTZ | Refreshed on upsert |

**Unique constraint:** `(source, signal_key, source_series_id, observed_at, geography_scope, geography_id)`

**Indexes:** `source`, `signal_key`, `observed_at`, `(source, signal_key, observed_at)`, `(geography_scope, geography_id)`, `(source, geography_scope, geography_id)`

#### Migration

- **`alembic/versions/fa096_macro_signals.py`** — standard Alembic revision
- **`scripts/apply_fa096_macro_signals.py`** — idempotent DDL apply script (used due to multi-head Alembic tree)

```bash
PYTHONPATH=. python scripts/apply_fa096_macro_signals.py
```

#### Persistence Service — `src/services/macro_signal_service.py`

| Function | Description |
|----------|-------------|
| `upsert_macro_signal(session, record)` | Single-record upsert |
| `upsert_macro_signals(session, records)` | Batch upsert — one SQL round trip |
| `get_latest_macro_signal(session, signal_key, geography_scope, geography_id)` | Latest observation lookup |
| `get_latest_macro_signals_by_source(session, source)` | One row per series for a source |

Upsert uses `INSERT ... ON CONFLICT DO UPDATE` (Postgres-native). Detects inserts vs updates via `xmax` system column. Returns `{"inserted": N, "updated": N}`.

#### Sync Task — `src/tasks/sync_macro_signals.py`

- Runs all four source loaders sequentially
- Each source is isolated — failure in one does not block others
- Census skipped gracefully if `CENSUS_API_KEY` is absent
- FRED skipped gracefully if `FRED_API_KEY` is absent
- Logs per-source insert/update counts and a final summary
- Accepts `--sources` flag to target specific sources

```bash
PYTHONPATH=. python -m src.tasks.sync_macro_signals
PYTHONPATH=. python -m src.tasks.sync_macro_signals --sources fred bls
```

---

### 8. Tests

#### Loader Tests — `tests/macro_signals/test_macro_signals.py`
43 tests, all passing. Covers normalization, source registry, FRED client, BLS client, FHFA parser. All HTTP mocked.

#### DB Persistence Tests — `tests/test_macro_signals_db.py`
29 tests, all passing (verified against live Postgres).

| Class | Tests | Mechanism |
|-------|-------|-----------|
| `TestNormalizationContract` | 5 | Unit — loader output shape |
| `TestCDSUntouched` | 3 | Unit — no scoring imports in service/task |
| `TestSyncTaskBehaviour` | 5 | Unit (mock DB) — skip/error/filter behaviour |
| `TestPGUpsertSingleRecord` | 2 | Postgres — insert and queryability |
| `TestPGUpsertDuplicate` | 2 | Postgres — no duplicate rows on repeat |
| `TestPGUpsertValueUpdate` | 2 | Postgres — value + raw_payload refresh |
| `TestPGBatchUpsert` | 3 | Postgres — batch, empty batch, mixed insert/update |
| `TestPGLatestLookup` | 5 | Postgres — latest signal, geo filter, by-source |
| `TestPGIdempotentSync` | 2 | Postgres — repeated sync row count stability |

---

## Architecture Notes

- **Loaders are standalone.** They do not inherit `BaseLoader` — they are time-series economic clients, not property-matched data loaders.
- **FRED is national only.** County-specific equivalents require FHFA (county HPI), BLS LAUS series (`LAUCN{state}{county}0000000003`), and Census ACS5.
- **FHFA covers county level.** The master CSV includes ZIP5, MSA, county, state, and national rows — the loader filter can be scoped to county FIPS for local precision.
- **All 4 sources are aggregate/macro data.** None are property-level. They influence how much weight the CDS engine gives to property-level signals, not whether a distress signal exists on a property.

---

## CDS Distress Multiplier Integration

**Completed 2026-06-26 — `feat/a7-macro-signals`**

### How it works

`MultiVerticalScorer.__init__` calls `get_macro_distress_multipliers(session)` once per scorer instantiation. The service reads the latest `MORTGAGE30US` row from `macro_signals`, compares to the 6.5% threshold in `config/macro_signal_rules.json`, and returns a `dict[signal_type, multiplier]`. The CDS engine applies this multiplier in `_score_vertical` after the A3 additive delta and before recency/decay:

```
base = weights[sig_type] + A3_delta
base = apply_macro_multiplier(sig_type, base, _macro_multipliers)   # A7
recency = _recency_bonus(sig_date)
decay   = _age_decay(sig_date)
total   = base + recency + decay
```

When rate is below threshold or DB has no data, `_macro_multipliers` is `{}` (neutral — no scoring change).

### Multiplier config — `config/macro_signal_rules.json`

| Signal type | Multiplier | Condition |
|---|---|---|
| `foreclosures` | ×1.10 | mortgage rate ≥ 6.5% |
| `tax_delinquencies` | ×1.08 | mortgage rate ≥ 6.5% |
| `loan_lane_refi_risk` | ×1.15 | mortgage rate ≥ 6.5% |
| All others | ×1.00 (neutral) | always |

`max_multiplier: 1.15` — configured values are clamped so no single rule can exceed this cap.

### E2E test result (property 208833 — 10520 BENEVA DR)

| Vertical | Baseline (6.47% — neutral) | Boosted (7.50% synthetic) |
|---|---|---|
| wholesalers | 100.0 | 100.0 |
| fix_flip | 100.0 | 100.0 |
| attorneys | 82.0 | **87.5 (+5.5)** |
| restoration | 53.0 | **56.0 (+3.0)** |
| roofing | 50.0 | **53.0 (+3.0)** |
| public_adjusters | 50.0 | **53.0 (+3.0)** |

**Result: PASS** — multiplier activates correctly, boosts configured signal types only, leaves others unchanged.

### New files

| File | Purpose |
|---|---|
| `config/macro_signal_rules.json` | Threshold + per-signal multiplier config |
| `src/services/macro_signal_multiplier_service.py` | `get_macro_distress_multipliers`, `apply_macro_multiplier`, `get_latest_mortgage_rate_context` |
| `src/services/cds_engine.py` | Modified — load multipliers in `__init__`, apply in `_score_vertical` |
| `tests/test_macro_signal_multiplier.py` | 27 unit tests (all passing) |
| `scripts/e2e_macro_multiplier.py` | E2E test — synthetic row insert/score/cleanup/assert |

### Tests added

27 unit tests in `tests/test_macro_signal_multiplier.py`:

| Class | Coverage |
|---|---|
| `TestNoMacroData` | Empty DB → neutral |
| `TestRateBelowThreshold` | Rate < 6.5% → neutral |
| `TestRateAtThreshold` | Rate = 6.5% → multiplier activates |
| `TestRateAboveThreshold` | Rate > 6.5% → multiplier activates; clamped to max |
| `TestSignalSelectivity` | Only configured signal types boosted |
| `TestConfigFallback` | Missing/malformed config → neutral, never raises |
| `TestCDSIntegration` | Full chain: DB → multipliers → apply → score delta |

---

## Cron Schedule

Added to `scripts/cron/crontab.txt` — all jobs run at 06:40–06:41 UTC, after scrapers finish (06:30) and before CDS scoring (07:00).

| Source | Schedule | UTC |
|---|---|---|
| FRED (mortgage rate) | Every Friday | 06:40 |
| BLS (unemployment, CPI) | 1st of every month | 06:40 |
| FHFA (HPI) | 1st of Jan / Apr / Jul / Oct | 06:40 |
| Census ACS5 | 1st of Jan / Apr / Jul / Oct | 06:41 |

---

## Out of Scope (Deferred)

- Admin API / UI for signal inspection
- MLS inventory metrics (no free public API identified)
- County-level BLS LAUS unemployment series (separate series IDs, not in current registry)
- FHFA / BLS / Census multiplier rules (only FRED mortgage rate is wired to CDS today)

---

## Files Created / Modified

| File | Action |
|------|--------|
| `config/settings.py` | Modified — added `fred_api_key`, `census_api_key` |
| `src/loaders/macro_signals/__init__.py` | Created |
| `src/loaders/macro_signals/normalization.py` | Created |
| `src/loaders/macro_signals/source_registry.py` | Created |
| `src/loaders/macro_signals/fred_client.py` | Created |
| `src/loaders/macro_signals/fred_rates_loader.py` | Created |
| `src/loaders/macro_signals/bls_client.py` | Created |
| `src/loaders/macro_signals/fhfa_hpi_loader.py` | Created |
| `src/loaders/macro_signals/census_client.py` | Created |
| `src/core/models.py` | Modified — `MacroSignal` appended |
| `alembic/versions/fa096_macro_signals.py` | Created |
| `scripts/apply_fa096_macro_signals.py` | Created |
| `src/services/macro_signal_service.py` | Created |
| `src/tasks/sync_macro_signals.py` | Created |
| `data/reference/macro_signals_samples/fred_mortgage30us_sample.json` | Generated |
| `data/reference/macro_signals_samples/fhfa_hpi_sample.json` | Generated |
| `data/reference/macro_signals_samples/bls_sample.json` | Generated |
| `data/reference/macro_signals_samples/census_sample.json` | Generated |
| `docs/macro_signal_sources_assessment.md` | Created |
| `tests/macro_signals/test_macro_signals.py` | Created — 43 tests |
| `tests/test_macro_signals_db.py` | Created — 29 tests |
| `config/macro_signal_rules.json` | Created — multiplier threshold + config |
| `src/services/macro_signal_multiplier_service.py` | Created — multiplier service |
| `src/services/cds_engine.py` | Modified — A7 multiplier applied in `_score_vertical` |
| `tests/test_macro_signal_multiplier.py` | Created — 27 tests |
| `scripts/e2e_macro_multiplier.py` | Created — E2E test script |
| `scripts/cron/crontab.txt` | Modified — macro signal sync cron entries |

**Total: 99 tests passing (43 loader + 29 DB + 27 multiplier)**
