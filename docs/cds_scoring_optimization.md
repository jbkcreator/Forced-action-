# CDS Scoring Engine — Optimization Reference

Covers the data structures, query mechanisms, and algorithms introduced across
Phases 1–4. Metrics are projected to 10M properties to show scaling behaviour.

---

## Data Structures

### SimpleNamespace duck-typing
Replaces SQLAlchemy ORM object hydration for property and signal rows.

```python
from types import SimpleNamespace
bundle = SimpleNamespace(**dict(row._mapping))
```

- ORM hydration allocates a Python object per row, wires up lazy-load
  descriptors, and holds a reference back to the session.
- `SimpleNamespace` is a plain attribute bag — no descriptors, no session
  reference, no change-tracking overhead.
- At 500 properties × ~200 signal rows each, the difference is ~100k live
  ORM objects vs ~100k dumb structs per batch.

### Min-heap (top-10 accumulator)
Replaces an unbounded `scores[]` list that grew to N dicts for full runs.

```python
import heapq
# push: O(log 10)
heapq.heappush(heap, (score, pid, score_data))
# evict lowest when heap exceeds 10
if len(heap) > 10:
    heapq.heapreplace(heap, (score, pid, score_data))
```

- Heap size is capped at 10 at all times regardless of N.
- Total cost: O(N log 10) = O(N) — same asymptotic complexity as a single
  linear scan.
- At 10M properties: old approach held 10M dicts in RAM; new approach holds
  exactly 10 entries.

### `defaultdict(lambda: {...})` signal map
Used inside `_fetch_signals_for_batch` to group signal rows by property_id
without conditional key checks.

```python
from collections import defaultdict
signal_map = defaultdict(lambda: {
    "owner": None, "financial": None,
    "code_violations": [], ...
})
for row in rows:
    signal_map[row.property_id]["code_violations"].append(_ns(row))
```

- Single pass over each result set — O(rows) — with O(1) dict access per row.

### `Counter` (incremental stats)
Replaces post-run aggregation over the full scores list.

```python
from collections import Counter
stats_tier_counts    = Counter()
stats_signal_types   = Counter()
stats_top_vertical   = Counter()
```

- Each scored property updates counters in O(1).
- Eliminates the need to keep any score dicts in memory beyond the top-10 heap.

---

## Query Mechanisms

### Keyset pagination
Replaces `OFFSET`-based or full-table ORM loading.

```sql
SELECT id, ... FROM properties
WHERE id > :last_id
  AND county_id = :county   -- optional
ORDER BY id
LIMIT 500
```

- `OFFSET N` scans and discards N rows on every page — O(N) cumulative cost.
- Keyset (`WHERE id > :last_id`) always lands directly on the next page via
  the primary key B-tree — O(log N) per page, O(1) per row amortised.
- No RAM spike: only 500 rows are ever in flight at once.

### `WHERE id = ANY(:ids)` batch lookup
Replaces per-property SELECT loops for signal loading.

```sql
SELECT property_id, status, ... FROM code_violations
WHERE property_id = ANY(:ids)
```

- PostgreSQL expands the array into a bitmap index scan — one index traversal
  for the whole batch.
- Replaces 500 individual round trips per batch with 1 query per signal table.
- psycopg2 binding: `{"ids": list_of_ints}` — no string interpolation needed.

### `DISTINCT ON (property_id) ORDER BY property_id, score_date DESC`
Efficiently retrieves the latest score row per property using the composite
index `idx_score_property_date (property_id, score_date DESC)`.

```sql
SELECT DISTINCT ON (property_id)
    property_id, score_date, final_cds_score, lead_tier
FROM distress_scores
WHERE property_id = ANY(:ids)
ORDER BY property_id, score_date DESC
```

- PostgreSQL reads index entries in order and skips duplicate property_ids —
  equivalent to `ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY
  score_date DESC) = 1` but without the full sort.
- Old approach: `LEFT JOIN distress_scores` with no ordering — non-deterministic
  row selected, wrong comparison date for signal staleness checks.

### Date range filter (index-compatible)
Replaces `CAST(score_date AS DATE) = today` which prevented index use.

```python
today_start    = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
tomorrow_start = today_start + timedelta(days=1)
```

```sql
WHERE property_id = :pid
  AND score_date >= :today_start
  AND score_date  < :tomorrow_start
```

- `CAST(col AS DATE)` wraps the indexed column in a function — PostgreSQL
  cannot use a B-tree index on the transformed value.
- Equivalent range on the raw column is fully covered by
  `idx_score_property_date`.

### executemany batch UPDATE / INSERT
Collapses N individual DML statements into a single driver-level batch.

```python
session.execute(sa_text("""
    UPDATE distress_scores SET
        score_date      = :now,
        final_cds_score = :score,
        factor_scores   = CAST(:factor AS jsonb),
        ...
    WHERE id = :id
"""), list_of_param_dicts)
```

- psycopg2 sends all rows in one network round trip via the extended query
  protocol.
- `CAST(:param AS jsonb)` used instead of `:param::jsonb` — psycopg2
  translates `:name` to `%(name)s`; the `::` cast prefix starts with `:` and
  triggers a false parse as a named parameter.

### UNION ALL CTE with indexed LEFT JOINs (`--rescore-new-signals`)
Replaces 8 correlated `EXISTS` subqueries.

```sql
WITH latest_scores AS (
    SELECT DISTINCT ON (property_id)
        property_id, score_date
    FROM distress_scores
    ORDER BY property_id, score_date DESC
)
SELECT DISTINCT s.property_id FROM (
    SELECT t.property_id FROM code_violations t
    LEFT JOIN latest_scores ls ON ls.property_id = t.property_id
    WHERE ls.property_id IS NULL OR t.date_added > ls.score_date
    UNION ALL
    -- ... 7 more signal tables
) s
JOIN properties p ON p.id = s.property_id
WHERE p.county_id = :county
```

- Correlated `EXISTS`: for every row in `properties`, PostgreSQL re-executes
  the subquery — up to 8 seq scans per property row.
- UNION ALL + LEFT JOIN: each branch is a single indexed join on
  `(property_id, date_added)` from `fa005`. The CTE result is computed once,
  all branches share it.
- `DISTINCT` on the outer query deduplicates across tables.

---

## Algorithms

### Batch-level persistence (2-read / 2-write pattern)
Per batch of 500 properties:

1. **Read today's rows** — `WHERE property_id = ANY(:ids) AND score_date >= :start AND score_date < :end`
2. **Read latest historical rows** — `DISTINCT ON` for properties with no today row
3. **Classify** — today row exists → UPDATE; no today row + score changed → INSERT; unchanged → skip
4. **Write** — one executemany UPDATE + one executemany INSERT

Old pattern: 4 round trips × N properties = 4N total.
New pattern: 4 round trips per batch of 500 = 4 × (N/500) total.

### Scoring run commit cadence
- Commit once per 500-property batch, not per property.
- Reduces transaction overhead and WAL flush frequency by 500×.
- Partial failure (crash mid-run) loses at most one batch of 500, not the
  entire run.

---

## Indexes Added

| Index | Table | Columns | Purpose |
|---|---|---|---|
| `idx_score_property_date` | `distress_scores` | `(property_id, score_date DESC)` | Today/latest score lookups; DISTINCT ON |
| `idx_cv_pid_date_added` | `code_violations` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_lal_pid_date_added` | `legal_and_liens` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_deeds_pid_date_added` | `deeds` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_lp_pid_date_added` | `legal_proceedings` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_td_pid_date_added` | `tax_delinquencies` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_fc_pid_date_added` | `foreclosures` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_bp_pid_date_added` | `building_permits` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_inc_pid_date_added` | `incidents` | `(property_id, date_added)` | New-signal LEFT JOIN |
| `idx_properties_county_id` | `properties` | `(county_id)` | County filter JOIN in new-signal CTE |

---

## Metrics at 10M Properties

All figures assume: 500 properties/batch → 20,000 batches. Average 10 signal
rows per property per table.

### DB round trips — persistence

| | Old | New | Reduction |
|---|---|---|---|
| Round trips | 4 × 10,000,000 = **40,000,000** | 4 × 20,000 = **80,000** | **99.8%** |
| Commits | 10,000,000 | 20,000 | **99.8%** |

### RAM — property loading

| | Old | New | Reduction |
|---|---|---|---|
| Properties in RAM | 10,000,000 ORM objects | 500 SimpleNamespace structs | **~20,000×** |
| Signal rows in RAM | ~100M rows (all collections) | ~5,000 rows (one batch) | **~20,000×** |
| Approx peak RSS | ~80–120 GB (OOM on most servers) | ~50–100 MB | **Crash → stable** |

### Score lookups — `distress_scores`

| | Old | New | Factor |
|---|---|---|---|
| Filter type | `CAST(score_date AS DATE)` — seq scan | Date range on raw column — index scan | — |
| Rows examined per lookup | All rows for property (unbounded) | Index range: O(log N) | ~100–1000× |
| Index used | None | `idx_score_property_date` | ✓ |

### `--rescore-new-signals` collection

| | Old | New | Factor |
|---|---|---|---|
| Query pattern | 8 correlated EXISTS per properties row | UNION ALL CTE + indexed LEFT JOINs | — |
| Seq scans | Up to 8 × 10M = **80M row scans** | 8 index joins on `(property_id, date_added)` | ~50–200× |
| Correctness | Arbitrary `distress_scores` row (wrong date) | `DISTINCT ON` — always latest score date | Bug fixed |
| County support | None | `JOIN properties WHERE county_id = :county` | Added |
| Sessions opened | 2 (pre-main + main) | 1 (collection inside main session) | Simplified |

### End-to-end full run estimate (10M properties)

| Phase | Old wall time (est.) | New wall time (est.) |
|---|---|---|
| Property load into RAM | 15–30 min (+ OOM risk) | 8–12 min (streaming) |
| Signal load | Embedded in Cartesian product | 4–6 min (batched) |
| Scoring (CPU) | Same | Same |
| Persistence | 3–6 hours (40M round trips) | 4–8 min (80k round trips) |
| **Total** | **4–7 hours (if it completes)** | **~20–30 min** |

Persistence alone goes from the dominant cost to near-negligible.
