# Loader Optimization — Feasibility Analysis

Based on a full read of all 10 loaders and the DB schema (`models.py`).

Two loaders are already partially optimized: `MasterPropertyLoader` (chunked CSV, pre-loaded parcel set) and `TaxDelinquencyLoader` (pre-loaded `(property_id, tax_year)` map). Those are the blueprint for the rest.

The proposed optimizations split into **universally applicable**, **address-match loaders only**, and **cannot be applied** categories.

---

## Optimization 1 — Bulk Duplicate Pre-Check (temp table JOIN or in-memory set)

**Can apply to every loader.** TaxLoader and MasterLoader already do this. All others issue N individual `SELECT ... WHERE key = ?` queries.

| Loader | Unique key checked | Notes |
|---|---|---|
| ViolationLoader | `code_violations.record_number` | Straightforward |
| PermitLoader | `building_permits.permit_number` | Straightforward |
| DeedLoader | `deeds.instrument_number` | Inline query, not via `check_duplicate` |
| LienLoader | `legal_and_liens.instrument_number` | Straightforward |
| LisPendensLoader | `foreclosures.case_number` (LP-prefix) | Straightforward for dup check; merge logic is separate |
| Probate/Eviction/Bankruptcy/Divorce | `legal_proceedings.case_number` | These iterate groups not rows; pre-load works the same |
| ForeclosureLoader | `foreclosures.case_number` | **Partial** — the LP placeholder lookup (`case_number LIKE 'LP-%'` for the same `property_id`) depends on the matched property, which isn't known until after address matching. Can bulk the exact dup check, but not the LP merge lookup |

**Risk: LOW.** Read-only change. No schema changes. The in-memory set approach (like TaxLoader's `existing_map`) is simpler than the temp table JOIN and avoids temp table lifetime/transaction concerns.

---

## Optimization 2 — COPY into `tmp_violations`, Bulk Staging

**Can apply to**: `ViolationLoader`, `BuildingPermitLoader`.

**Cannot cleanly apply to name-based loaders** — `DeedLoader`, `LienLoader`, `LisPendensLoader`, `BankruptcyLoader`. Here's why:

The match logic for these loaders determines *which field* to use as the owner name on a per-row basis:
- `LienLoader`: `_party_is_filer()` scans both Grantor and Grantee to detect the governmental creditor and uses the other side. The logic differs for tax liens (always Grantee), code liens (detect which side has city keyword), mechanics liens (Grantee first), all others (Grantor first).
- `DeedLoader`: Tries legal description → Grantor → Grantee in order.
- `LisPendensLoader`: Splits a multi-party comma-separated Grantee string and filters noise parties (banks, unknowns, government agencies).

This field-selection logic is dynamic Python — it cannot be pushed into a SQL join without rewriting it as a CASE expression of 6+ conditions, and getting that wrong risks the same type of cascade incident documented in `liens.py` (the "113-record / 2-property-ID" comment).

**Partial apply to**: `ProbateLoader`, `EvictionLoader`, `DivorceLoader`. These group by `CaseNumber` first, so you'd need to deduplicate to case-representative rows before staging into tmp. Adds a preprocessing step but is feasible.

**Risk for applicable loaders: MEDIUM.** Requires dropping to raw psycopg2 `COPY` or a multi-row `INSERT INTO tmp ... VALUES (...)` via `session.execute(text(...), rows)`. Either approach mixes raw SQL with the ORM session — workable but needs care to stay within the same transaction.

---

## Optimization 3 — Pre-Computed `normalized_address` + `house_number` Columns on `properties`

**Requires**: Alembic migration + backfill script + master loader update.

**Current schema**: `properties` has only `address: String(255)`. No normalized form stored.

**Can apply for**: `ViolationLoader`, `BuildingPermitLoader`, `ForeclosureLoader`, `ProbateLoader`, `EvictionLoader`, `DivorceLoader`.

**Does not help**: `DeedLoader`, `LienLoader`, `LisPendensLoader` (use legal description or name match), `BankruptcyLoader` (name only), `TaxDelinquencyLoader` (parcel ID, no address match at all).

**Critical constraint**: The SQL backfill suggested (`lower(trim(address))`) is **not equivalent** to the Python `normalize_address()`. The Python version does ~20 replacements: strips city names from a list of 20 cities, strips zip, strips unit/apt/lot, abbreviates street/drive/road/avenue, rejects intersections and invalid patterns. If you backfill with simple SQL and query with Python-normalized input, the stored values will never match for non-trivial addresses.

The backfill must run the Python `normalize_address()` function over all ~522k property rows and write the results back. This requires a one-time Python script, not a SQL `UPDATE`. Once in place, `MasterPropertyLoader.load_from_dataframe()` must also populate `normalized_address` and `house_number` at insert/upsert time.

**Risk: MEDIUM.** Schema migration itself is safe (adding nullable columns). The backfill script is the risk: if it uses different normalization logic than incoming data, bulk exact-match queries will produce false negatives. If `normalize_address()` logic ever changes in the future, stored values go stale and need rebackfilling.

---

## Optimization 4 — Bulk Exact Address Match via SQL JOIN

**Depends on Optimization 3.**

```sql
SELECT v.record_number, p.id, 100 AS score
FROM tmp_violations v
JOIN properties p
  ON p.county_id = :county_id
 AND p.normalized_address = v.normalized_address
```

Once `normalized_address` is pre-stored and indexed, this resolves all exact matches in one query.

**Risk: LOW** once Opt 3 is done. The index `idx_properties_county_norm_addr (county_id, normalized_address)` is a standard btree — fast, safe.

---

## Optimization 5 — Bulk Fuzzy Match via pg_trgm + Blocking on `house_number`

**Depends on Optimization 3.**

Replaces the per-row pg_trgm savepoint strategy with:

```sql
SELECT v.record_number, p.id, similarity(p.normalized_address, v.normalized_address) AS score
FROM tmp_violations v                           -- leftovers from exact match
JOIN properties p
  ON p.county_id = :county_id
 AND p.house_number = v.house_number            -- blocking key
WHERE similarity(p.normalized_address, v.normalized_address) >= 0.80
```

This requires a GIN trigram index on `properties.normalized_address`. The pg_trgm extension is already in use (Strategy 2 in `find_property_by_address` at `base.py:468`), so the extension is already installed.

**Risk: LOW** once Opt 3 is in place. The blocking on `house_number` prevents cross-property fuzzy spurious matches, which is the same guard `strict_house_number` enforces in Strategy 3 today.

---

## Optimization 6 — Bulk `INSERT ... ON CONFLICT DO NOTHING`

Replaces per-row savepoint + flush in `safe_add`.

| Loader | Applicable? | Notes |
|---|---|---|
| ViolationLoader | **YES** | `record_number` UNIQUE — clean DO NOTHING |
| PermitLoader | **YES** | `permit_number` UNIQUE — clean DO NOTHING |
| DeedLoader | **YES** | `instrument_number` UNIQUE — clean DO NOTHING |
| LienLoader | **YES** | `instrument_number` UNIQUE — clean DO NOTHING |
| Probate/Eviction/Bankruptcy/Divorce | **YES** | `case_number` UNIQUE — clean DO NOTHING |
| LisPendensLoader | **PARTIAL** | New-row inserts can use DO NOTHING, but the merge path (update existing Foreclosure's `lis_pendens_date` + `plaintiff` when an auction row already exists) is a conditional UPDATE that cannot be expressed as DO NOTHING |
| ForeclosureLoader | **PARTIAL** | Case 3 (plain new insert) can use DO NOTHING. Case 2 (LP placeholder promotion — updates `case_number`, `auction_date`, `judgment_amount`, keeps `lis_pendens_date`) requires a targeted UPDATE. Replacing this with a bulk MERGE is high-risk (see below) |
| TaxDelinquencyLoader | **PARTIAL** | New inserts can use `ON CONFLICT DO NOTHING`; existing-row updates (amount, years_delinquent) can use `ON CONFLICT DO UPDATE SET ...`, which is a clean PostgreSQL upsert |

**Risk: LOW** for the DO NOTHING cases. **MEDIUM** for TaxLoader DO UPDATE. **HIGH** for Foreclosure/LP merge (see below).

---

## Optimization 7 — Chunked Processing + Commit per Chunk

`MasterPropertyLoader` already uses `chunksize=10000`. All row-based loaders can adopt the same pattern with low effort.

| Loader | Applicable? | Notes |
|---|---|---|
| ViolationLoader | **YES** | Row-based, independent rows |
| PermitLoader | **YES** | Row-based, independent rows |
| DeedLoader | **YES** | Row-based, independent rows |
| LienLoader | **YES** | Row-based; `stats_by_doc_type` dict resets per call — would need to be accumulated externally across chunks |
| BankruptcyLoader | **YES** | Row-based |
| Probate/Eviction/Divorce | **PARTIAL** | Use `df.groupby('CaseNumber')` — chunking on raw rows risks splitting a multi-row case group across chunks. Must chunk on unique case numbers, not raw rows |
| LisPendensLoader | **YES** | LP→Foreclosure merge requires prior chunk's committed LP rows to be visible when auction data arrives. Works fine because committed chunks are visible |
| ForeclosureLoader | **YES** | Same caveat as LisPendensLoader |

**Risk: LOW** for row-based loaders. **MEDIUM** for grouped loaders (case boundary splitting).

---

## High-Risk Items — Do Not Touch Without Dedicated Work

### 1. ForeclosureLoader LP Placeholder Merge → Bulk UPSERT
The three-way logic (exact dup → skip, LP placeholder → promote synthetic `case_number` to real + merge auction fields, new → insert) is the most fragile code in the loaders. It has a known incident history (`liens.py` references a "113-record / 2-property-ID cascade"). Rewriting this as a SQL MERGE/UPSERT would require encoding the "never overwrite `lis_pendens_date`, only fill null fields" logic in SQL, which is non-trivial and would be hard to test against edge cases.

### 2. Bulk Name-Matching for LienLoader / DeedLoader
The governmental filer detection (`_party_is_filer`) + geographic validation (`prop_city != required_city`) for code liens was added specifically to prevent the cascade incident. Any bulk SQL approach that skips this logic and does a flat owner-name join would reintroduce that risk.

### 3. normalized_address Backfill Divergence
If the backfill uses a different normalization than runtime `normalize_address()`, the `idx_properties_county_norm_addr` join will silently produce false negatives — violations get quarantined not because there's no matching property, but because the stored form doesn't match the incoming form. This would be hard to detect until match rates drop noticeably.

---

## Applicability Matrix

| Loader | Bulk dedup | Tmp staging | Norm addr col | Bulk addr match | Bulk fuzzy | Bulk INSERT | Chunked |
|---|---|---|---|---|---|---|---|
| ViolationLoader | YES | YES | YES | YES | YES | YES | YES |
| PermitLoader | YES | YES | YES | YES | YES | YES | YES |
| ForeclosureLoader | PARTIAL | YES | YES | YES | YES | PARTIAL | YES |
| DeedLoader | YES | NO | NO | NO | NO | YES | YES |
| LienLoader | YES | NO | NO | NO | NO | YES | YES |
| LisPendensLoader | YES | NO | NO | NO | NO | PARTIAL | YES |
| TaxDelinquencyLoader | DONE | NO | NO | NO | NO | PARTIAL | DONE |
| ProbateLoader | YES | PARTIAL | YES | YES | YES | YES | PARTIAL |
| EvictionLoader | YES | PARTIAL | YES | YES | YES | YES | PARTIAL |
| BankruptcyLoader | YES | NO | NO | NO | NO | YES | YES |
| DivorceLoader | YES | PARTIAL | YES | YES | YES | YES | PARTIAL |

> NO = name-based matching only; bulk SQL address match does not apply.
> PARTIAL = complex merge/upsert logic prevents full replacement.
> DONE = already implemented.

---

## Recommended Implementation Order

Lowest risk, highest impact first.

1. **Bulk duplicate pre-check (in-memory set)** — all loaders, no schema change, mirrors TaxLoader/MasterLoader pattern already in the codebase.
2. **Bulk INSERT ON CONFLICT DO NOTHING** — violations, permits, deeds, liens, legal proceedings. Replaces per-row savepoint + flush.
3. **Chunked processing** — violations, permits, deeds, liens. Copy the MasterLoader pattern directly.
4. **Fix O(N²) candidate dedup in `find_property_by_address`** (`base.py:491`) — one-line fix, set maintained outside the loop.
5. **`lru_cache` on `normalize_address`** — zero schema change, reduces per-candidate recomputation across rows.
6. **normalized_address column + backfill + bulk address match** — medium effort, high payoff for address-matched loaders. Requires Alembic migration, a Python backfill script using `normalize_address()`, master loader update, and new indexes.
7. **COPY into tmp table** — only after normalized_address is stable; only for violations and permits.

---

## ViolationLoader and BuildingPermitLoader — Detailed Plan

These two loaders are structurally identical and accept every optimization. All 7 apply. Work is split into two independent tracks: the **immediate** track (no schema changes, shippable now) and the **bulk pipeline** track (depends on the Opt 3 migration).

---

### Vectorized Field Computation

Both loaders have pure-Python classification functions called per row inside the loop. Both can be replaced with pandas vectorized operations that run on the full column at once before any DB work starts.

**`classify_severity()` → vectorized (ViolationLoader)**

The current logic uses `any(kw in string for kw in keyword_list)` — substring containment. `str.contains(pattern)` with a `|`-joined regex is the exact equivalent:

```python
import re

_crit_type_re = '|'.join(re.escape(kw) for kw in _CRITICAL_TYPE_KEYWORDS)
_crit_desc_re = '|'.join(re.escape(kw) for kw in _CRITICAL_DESC_KEYWORDS)
_maj_type_re  = '|'.join(re.escape(kw) for kw in _MAJOR_TYPE_KEYWORDS)
_maj_desc_re  = '|'.join(re.escape(kw) for kw in _MAJOR_DESC_KEYWORDS)

vtype  = df['Record Type'].fillna('').str.lower().str.strip()
desc   = df['Description'].fillna('').str.lower().str.strip()
status = df['Status'].fillna('').str.lower().str.strip()
fine   = pd.to_numeric(
    df['Fine Amount'].fillna('0').astype(str).str.replace(r'[$,]', '', regex=True).str.strip(),
    errors='coerce'
).fillna(0.0)
lien   = df['Is Lien'].fillna('').astype(str).str.lower().str.strip().isin({'true', 'yes', '1'})

is_critical = (
    vtype.str.contains(_crit_type_re, na=False) |
    desc.str.contains(_crit_desc_re, na=False) |
    (lien & (fine >= 5000)) |
    status.str.contains('condemned|emergency|imminent', na=False)
)
is_major = (
    vtype.str.contains(_maj_type_re, na=False) |
    desc.str.contains(_maj_desc_re, na=False) |
    (lien & (fine >= 1000)) |
    status.str.contains('hearing|board|escalated|non-compliant', na=False)
)

# Apply Major first, Critical second — Critical overwrites, preserving priority order
df['severity_tier'] = 'Minor'
df.loc[is_major,    'severity_tier'] = 'Major'
df.loc[is_critical, 'severity_tier'] = 'Critical'
```

**`_is_enforcement()` → vectorized (BuildingPermitLoader)**

```python
_enf_type_re = '|'.join(re.escape(kw) for kw in _ENFORCEMENT_TYPE_KEYWORDS)

permit_type = df['Record Type'].fillna('').str.lower()
status      = df['Status'].fillna('').str.lower().str.strip()

df['is_enforcement'] = (
    permit_type.str.contains(_enf_type_re, na=False) |
    status.isin(_ENFORCEMENT_STATUS_VALUES)
)
```

**`_SKIP_STATUS_VALUES` filter → vectorized (BuildingPermitLoader)**

The per-row `if raw_status in _SKIP_STATUS_VALUES: continue` becomes a single mask applied before any processing:

```python
status = df['Status'].fillna('').str.lower().str.strip()
df = df[~status.isin(_SKIP_STATUS_VALUES)].copy()
```

**Date and amount parsing → vectorized**

```python
df['opened_date'] = pd.to_datetime(df['Date'], errors='coerce', infer_datetime_format=True)
# fine already computed above during severity vectorization
# is_lien already computed above
```

These vectorized columns replace all the per-row NaN checks and field extraction inside the `if property_record:` block. After this preprocessing pass the DataFrame has `severity_tier`, `fine_amount`, `is_lien`, `opened_date`, `is_enforcement` fully populated with no NaN values.

---

### Track 1 — Immediate (no schema changes)

#### Step 1: Bulk duplicate pre-check

Load all existing keys into a set once at the start, replace per-row `check_duplicate` with a set lookup.

```python
# In load_from_dataframe, before the loop
existing = set()
if skip_duplicates:
    rows = self.session.query(CodeViolation.record_number).all()
    existing = {r[0] for r in rows}

# Inside the loop
if record_number in existing:
    skipped += 1
    continue
```

For permits, same pattern against `BuildingPermit.permit_number`.

#### Step 2: Vectorized preprocessing pass

Run all field computation before the loop (as shown above). The loop body shrinks to: address match → build model object → stage for insert.

#### Step 3: Chunked processing

Both loaders are flat row-by-row with no groupby. Direct copy of the MasterLoader pattern:

```python
for chunk in pd.read_csv(csv_path, dtype=str, chunksize=5000):
    # apply column mapping
    self.load_from_dataframe(chunk, skip_duplicates=False, _existing=existing)
```

`skip_duplicates=False` because duplicates are already filtered by the pre-loaded set in the caller. Pass `_existing` in so each chunk can check against the same set and also update it after inserts.

#### Step 4: Bulk INSERT ON CONFLICT DO NOTHING

Replace `safe_add()` per row with a batched insert at the end of each chunk:

```python
from sqlalchemy.dialects.postgresql import insert as pg_insert

records = [{ ...fields... } for row in matched_rows]
if records:
    stmt = pg_insert(CodeViolation.__table__).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=['record_number'])
    self.session.execute(stmt)
    self.session.flush()
```

This replaces N savepoints + N flushes with 1 statement.

---

### Track 2 — Bulk Pipeline (depends on Opt 3 migration)

This track cannot start until `properties.normalized_address` and `properties.house_number` columns exist and are backfilled.

#### Step 5: Opt 3 — Schema migration and backfill

Alembic migration adds two nullable text columns. Backfill script runs `normalize_address()` on every property row and writes results. `MasterPropertyLoader` is updated to populate both columns on insert.

#### Step 6: Opt 4 + 5 — Replace `find_property_by_address` with bulk SQL match

After vectorized preprocessing, `tmp_violations` (or `tmp_permits`) is loaded with `record_number`, `normalized_address`, `house_number`. Two SQL passes:

**Pass A — exact match:**
```sql
SELECT v.record_number, p.id AS property_id, 100 AS score
FROM tmp_violations v
JOIN properties p
  ON p.county_id = :county_id
 AND p.normalized_address = v.normalized_address
```

**Pass B — fuzzy match on leftovers:**
```sql
SELECT v.record_number, p.id AS property_id,
       similarity(p.normalized_address, v.normalized_address) AS score
FROM tmp_violations v
JOIN properties p
  ON p.county_id = :county_id
 AND p.house_number = v.house_number
WHERE similarity(p.normalized_address, v.normalized_address) >= 0.80
  AND v.record_number NOT IN (SELECT record_number FROM exact_matches)
```

Rows unresolved after both passes go to `quarantine_unmatched` in bulk.

Note for `BuildingPermitLoader`: it uses `strict_house_number=False` and passes a ZIP code extracted via regex. Pass B must include the ZIP filter when available:
```sql
AND (p.zip = v.zip OR v.zip IS NULL)
```

#### Step 7: Opt 2 — Full COPY pipeline

With Steps 5–6 in place, the full flow becomes:

```
Vectorized preprocessing (Python, no DB)
      ↓
COPY preprocessed rows → tmp_violations / tmp_permits
      ↓
Anti-join against code_violations / building_permits → filter already-existing
      ↓
Pass A: exact normalized_address JOIN → matched set
      ↓
Pass B: pg_trgm + house_number JOIN → fuzzy matched set
      ↓
INSERT INTO code_violations ... ON CONFLICT DO NOTHING (matched rows)
      ↓
Bulk UPSERT unmatched into unmatched_records
```

Use `session.execute(text("INSERT INTO tmp_violations ..."), rows)` (executemany via ORM) rather than raw psycopg2 COPY to stay within the ORM session. For 50k rows this is ~5–10s vs COPY's ~0.5s, but still orders of magnitude faster than the current per-row approach and avoids raw DBAPI coupling.

Temp table: no `ON COMMIT DROP` — use `TRUNCATE` at the start of each load call instead. This avoids the table disappearing if the calling engine commits mid-run.
