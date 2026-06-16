# Master Loader — Supplemental Table Merge (Planned)

> **Status:** PLANNED — not implemented. Sequenced AFTER the fa077 weekly-refresh
> go-live (Hillsborough backfill + cron install) and after the one-time rescore
> backlogs have drained.
>
> **Driving case:** Pinellas sale history. `RP_PROPERTY_INFO` (the master parcel
> file) contains no sale price/date columns, so `financials.last_sale_price` and
> `last_sale_date` are NULL for all ~437k Pinellas parcels. The data lives in a
> separate PCPAO bulk file, `RP_SALES_HISTORY`
> (https://www.pcpao.gov/tools-data/data-downloads/raw-database-files → Parcel Info).

---

## 1. Design principle

Per `MULTI_COUNTY_SCRAPING_ARCHITECTURE.md`: **no county-specific branches in
engine code**. The merge is a generic, config-driven engine capability; Pinellas
(and any future county) activates it through `county_sources.special_flags` and
its column mapping — DB rows only.

## 2. Source file facts (verified 2026-06-11)

`RP_SALES_HISTORY` columns include: `STRAP`, `PARCEL_NUMBER`, `SALES_DATE`,
`PRICE`, `QU_FLG`, `GRANTOR*`, `GRANTEE*`, `MULTI_SALES_YN`, `VACANT_IMPROVED`,
and **`IMPORTANCE`** — an integer ranking per parcel where `1` = most recent
sale. Filtering `IMPORTANCE = 1` yields exactly one latest-sale row per parcel,
matching the loader's one-row-per-parcel model.

`RP_BUILDING` was also checked: it does NOT contain bedroom/bathroom counts —
beds/baths are confirmed unavailable from any Pinellas raw file (closed; not a
mapping gap).

⚠️ Do NOT simply add `RP_SALES_HISTORY` to `bulk_tables` today: the current
engine loads each bulk table independently through the master loader, and a
sales file has no OWNER column — every row would be rejected as `no_owner` and
flood `unmatched_records` with hundreds of thousands of quarantine rows.

## 3. Config vocabulary (new `special_flags` keys, master_data source)

```json
{
  "bulk_tables": ["RP_PROPERTY_INFO", "RP_SALES_HISTORY"],
  "primary_table": "RP_PROPERTY_INFO",
  "merge_tables": {
    "RP_SALES_HISTORY": {
      "join_on": "PARCEL_NUMBER",
      "filter": {"column": "IMPORTANCE", "equals": "1"},
      "take": ["SALES_DATE", "PRICE"]
    }
  }
}
```

- `primary_table` — the parcel-shaped table handed to the loader (defaults to
  the sole/first bulk table for backward compatibility).
- `merge_tables` — per supplemental table: `join_on` (key column present in
  both files), optional `filter` (row predicate applied to the supplement),
  `take` (columns to attach to the primary rows).

A county without `merge_tables` behaves exactly as today.

## 4. Engine change (once, county-agnostic) — `master_engine.py`

New phase between file discovery and `load_to_database`:

1. For each entry in `merge_tables`: read the supplement CSV, apply `filter`,
   build an in-memory dict `{join_on value -> take columns}` (O(1) lookups;
   the filtered Pinellas sales table is ~437k small rows, a few hundred MB of
   headroom not required).
2. Stream the primary CSV in chunks, attach the taken columns by key, write
   one merged CSV to the staging dir.
3. Hand the merged CSV to the loader — **no loader changes**; the merged
   columns flow through ColumnMapper and the existing hash/update machinery.

## 5. Column mapping additions (Pinellas, source_id=21, approved mapping id=20)

Two new entries (admin UI or SQL):

```
"SALES_DATE": "SALE1_DATE",
"PRICE":      "SALE1_PRC"
```

## 6. Cron / automated download — the agent prompt must fetch BOTH files

The weekly cron runs the browser-use download phase (`build_agent_task`), whose
task prompt is generated from the master_data source row. Today it instructs the
agent to download only `RP_PROPERTY_INFO`. Required updates on the Pinellas
master_data `county_sources` row:

1. `special_flags.bulk_tables` gains `RP_SALES_HISTORY` (section 3) — the
   multi-table download/discovery path iterates this list.
2. `description` / `navigation_hint` updated to name both files explicitly,
   e.g. "Download BOTH raw database files from the Parcel Info section:
   RP_PROPERTY_INFO (CSV) and RP_SALES_HISTORY (CSV)." — these fields feed the
   LLM task generation.
3. Verify `build_agent_task` output (one supervised `--headful` run) actually
   yields both files in `data/reference/pinellas/` before trusting cron.
4. Failure handling: if `RP_SALES_HISTORY` is missing at merge time, the run
   must FAIL LOUDLY (abort before load), not silently load the primary without
   sale columns — otherwise every sale field would flip to NULL, stage ~400k
   "changed" rows, and flood needs_rescore with retractions.
   (`discover_and_convert`-style newest-file fallback must not paper over a
   missing supplement.)

## 7. Consequences to plan around

- **One-time enrichment wave:** first merged run flips `last_sale_date/price`
  NULL→value for every Pinellas parcel with a recorded sale → hash change →
  staged + material → a `needs_rescore` wave (est. 300k+). Schedule after the
  current backlogs drain. No HASH_VERSION bump needed (sale fields are already
  in `_HASH_FIELDS`; values merely go from NULL to real).
- **Doc upkeep:** add the section-3 keys to `MULTI_COUNTY_SCRAPING_ARCHITECTURE.md`
  §6 (special_flags known keys) when implemented.
- `RP_SALES_HISTORY` is already downloaded locally (2026-06-11) and ready for
  the implementation dry run.

## 8. Acceptance checklist

- [ ] Engine merge step implemented + unit tests (filter, join, missing-supplement abort)
- [ ] Pinellas `special_flags` updated (bulk_tables / primary_table / merge_tables)
- [ ] Mapping entries `SALES_DATE`/`PRICE` added to mapping id 20
- [ ] Source `description`/`navigation_hint` updated for two-file download
- [ ] Supervised headful download run fetches both files
- [ ] Dry run: staged count ≈ parcels-with-sales; drift confined to sale fields
- [ ] Live run + idempotency rerun (0 updated on rerun)
- [ ] Coverage check: `count(last_sale_date)` on Pinellas financials jumps from 0
- [ ] Architecture doc §6 updated with the new special_flags keys
