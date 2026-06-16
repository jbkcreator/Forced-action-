"""
Master property loader — weekly refresh with hash-based change detection.

Column normalization is handled by ColumnMapper (src/loaders/column_mapper.py).
On the first load for a new county the mapper calls the LLM to propose a mapping,
saves it as pending for admin review, and applies it optimistically.  On subsequent
loads the approved mapping is used directly (no LLM call).

Load semantics (one pass over the county bulk file):
  NEW        parcel not in DB           -> ORM insert (Property + Owner + Financial)
  UNCHANGED  source_row_hash matches    -> only last_seen_at is stamped
  CHANGED    hash differs or is NULL    -> staged, then set-based SQL UPDATE of
                                           scraper-sourced fields only

App-managed fields (HCPA enrichment, CRM ids/sync timestamps, skip-trace
phones/emails, Sunbiz data, lat/lon) are never in any UPDATE's SET list.
Downstream flags are guarded per-field with IS DISTINCT FROM so rows whose
stored hash is NULL (pre-fa077 backfill) update silently unless real drift
exists:
  properties.needs_rescore      -> consumed by the CDS engine
  properties.sync_status        -> 'pending_sync' only when already 'synced'
                                   (never-synced rows go through scoring first)
  owners.skip_trace_stale       -> owner_name changed but prior trace data kept

Hash contract (HASH_VERSION): the md5 is computed over the canonical, ordered
values in _HASH_FIELDS *after* column mapping and parsing — never raw CSV bytes
and never values read back from the DB — so both sides of every comparison were
produced by the same function.  Any change to _parse_row, _HASH_FIELDS, or
_canon REQUIRES bumping HASH_VERSION: stored hashes are only comparable within
one version, and a bump forces a one-off full re-stage (still flag-guarded).
"""

import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.exc import SQLAlchemyError

from src.loaders.base import BaseLoader
from src.loaders.dor_use_codes import translate_dor_code
from src.core.models import Property, Owner, Financial
from src.utils.address_normalize import normalize_street_address
from src.utils.time_tracker import TimeTracker

logger = logging.getLogger(__name__)

HASH_VERSION = "v1"

# Fixed-order, scraper-sourced fields covered by the change-detection hash.
# Derived columns (normalized_address, owner_type, absentee_status) are pure
# functions of these inputs and are excluded; app-managed columns must never
# participate.
_HASH_FIELDS = (
    "address", "city", "zip", "property_type", "year_built", "sq_ft", "beds",
    "baths", "lot_size", "legal_description",
    "owner_name", "mailing_address",
    "assessed_value_mkt", "assessed_value_tax", "last_sale_price", "last_sale_date",
)

# Numeric canonicalization scale must match the live column scale, otherwise a
# value round-trips through NUMERIC and re-hashes differently next week.
_TWO_DP_FIELDS = frozenset({
    "sq_ft", "lot_size", "assessed_value_mkt", "assessed_value_tax", "last_sale_price",
})
_ONE_DP_FIELDS = frozenset({"beds", "baths"})

_STAGE_FLUSH_SIZE = 5_000

# Staging-table columns shared by the CREATE TABLE DDL and the
# jsonb_to_recordset() flush — one definition so they can never drift.
_STAGE_COLUMNS: tuple = (
    ("property_id",        "BIGINT"),
    ("parcel_id",          "VARCHAR(100)"),
    ("address",            "VARCHAR(255)"),
    ("normalized_address", "VARCHAR(255)"),
    ("city",               "VARCHAR(100)"),
    ("zip",                "VARCHAR(10)"),
    ("property_type",      "VARCHAR(50)"),
    ("year_built",         "INTEGER"),
    ("sq_ft",              "NUMERIC(10,2)"),
    ("beds",               "NUMERIC(4,1)"),
    ("baths",              "NUMERIC(4,1)"),
    ("lot_size",           "NUMERIC(12,2)"),
    ("legal_description",  "TEXT"),
    ("owner_name",         "TEXT"),
    ("mailing_address",    "VARCHAR(255)"),
    ("owner_type",         "VARCHAR(50)"),
    ("absentee_status",    "VARCHAR(50)"),
    ("assessed_value_mkt", "NUMERIC(12,2)"),
    ("assessed_value_tax", "NUMERIC(12,2)"),
    ("last_sale_price",    "NUMERIC(12,2)"),
    ("last_sale_date",     "DATE"),
    ("source_row_hash",    "CHAR(32)"),
)


def _json_safe(value: Any) -> Any:
    """NaN floats serialize as the invalid-JSON token `NaN` — map to None
    (dates/Decimals are handled by json.dumps(default=str))."""
    if isinstance(value, float) and value != value:
        return None
    return value


# Names carrying any of these markers are real entities, not address text that
# spilled into the owner column — they bypass the address-shaped heuristics.
# Without this, legitimate owners like "17808 LEE AVENUE LLC" (address-named
# LLCs), "PINELLAS COUNTY", or "DIOCESE OF ST PETERSBURG" were silently
# dropped (~9.3k parcels per Pinellas file).
_ENTITY_MARKERS = (
    'LLC', 'LLP', 'PLLC', 'TRUST', 'TRUSTEE', 'ESTATE',
    'ASSN', 'ASSOCIATION', 'CHURCH', 'BANK', 'COUNTY', 'DIOCESE',
    ' INC', ' CORP', ' LTD', ' CO ',
)
_ENTITY_SUFFIXES = (' LP', ' INC', ' CORP', ' LTD', ' CO')


def _looks_like_entity(owner_name: str) -> bool:
    upper = owner_name.upper()
    return any(m in upper for m in _ENTITY_MARKERS) or upper.endswith(_ENTITY_SUFFIXES)


def _canon(field_name: str, value: Any) -> str:
    """Canonical string form of one parsed field value for hashing."""
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.split()).upper()
    if isinstance(value, datetime) or isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if field_name in _TWO_DP_FIELDS:
        try:
            return str(Decimal(str(value)).quantize(Decimal("0.01")))
        except InvalidOperation:
            return str(value)
    if field_name in _ONE_DP_FIELDS:
        try:
            return str(Decimal(str(value)).quantize(Decimal("0.1")))
        except InvalidOperation:
            return str(value)
    return str(value)


def compute_source_row_hash(parsed: dict) -> str:
    """md5 over the versioned, canonical, ordered _HASH_FIELDS values."""
    payload = "|".join(
        [HASH_VERSION, *(_canon(name, parsed.get(name)) for name in _HASH_FIELDS)]
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


@dataclass
class MasterLoadStats:
    """Outcome of one master file load."""
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    flagged_rescore: int = 0
    flagged_resync: int = 0
    flagged_stale_trace: int = 0
    skip_reasons: dict = field(default_factory=dict)

    def summary(self) -> str:
        return (
            f"{self.inserted:,} inserted | {self.updated:,} updated | "
            f"{self.unchanged:,} unchanged | {self.skipped:,} skipped | "
            f"flags: rescore={self.flagged_rescore:,} "
            f"resync={self.flagged_resync:,} stale_trace={self.flagged_stale_trace:,}"
        )


class MasterPropertyLoader(BaseLoader):
    """Loader for master property records (FOLIO/parcel data)."""

    @staticmethod
    def _is_valid_zip(value) -> Optional[str]:
        """Validate and extract ZIP code (5 or 5+4 format)."""
        if pd.isna(value):
            return None

        s = str(value).strip()
        # ZIP must be numeric or numeric with dash (5 digits or 5+4).
        # Always store only the 5-digit base — ZIP+4 suffix is discarded so that
        # territory exclusivity queries group correctly (33613-1371 → 33613).
        if re.match(r'^\d{5}(-\d{4})?$', s):
            return s[:5]

        # If it looks like a ZIP at the start, extract and truncate
        match = re.match(r'^(\d{5}(-\d{4})?)', s)
        if match:
            return match.group(1)[:5]

        return None

    @staticmethod
    def normalize_parcel_id(parcel_id: str) -> str:
        """
        Normalize parcel ID by removing hyphens.
        Example: 16-29-15-32292-019-0010 -> 162915322920190010
        """
        if not parcel_id:
            return ""

        return parcel_id.replace("-", "")

    @staticmethod
    def _is_valid_small_number(value, max_value: float = 999.9) -> Optional[float]:
        """Validate number fits in NUMERIC(4,1) format (max 999.9)."""
        if pd.isna(value):
            return None

        try:
            num = float(value)
            # Check if it's within valid range
            if 0 <= num <= max_value:
                return num
        except (ValueError, TypeError):
            pass

        return None

    @staticmethod
    def _parse_mailing_parts(mailing_address: str) -> tuple:
        """Extract (street, state_code) from mailing address blob.

        The loader stores mailing address as joined CSV parts: ADDR_1, CITY, STATE, ZIP.
        When some CSV columns are blank only ADDR_1 is stored (street-only format).
        This helper handles both cases and is only used by _determine_absentee_status.

        Returns:
            (mailing_street, mailing_state) — either may be None if not determinable.
        """
        parts = [p.strip() for p in mailing_address.split(", ") if p.strip()]
        n = len(parts)
        if n >= 4:
            # Full format: ADDR_1, CITY, STATE, ZIP
            # Join parts before last 3 to handle streets containing commas (e.g. "123 MAIN ST, APT 4")
            street = ", ".join(parts[:-3]) or None
            state  = parts[-2][:2].upper() or None
        elif n == 3:
            # ADDR_1, CITY, STATE — ZIP was blank in CSV
            street = parts[0] or None
            state  = parts[2][:2].upper() or None
        else:
            # Street only — CITY/STATE/ZIP were blank in original CSV
            street = parts[0] if parts else None
            state  = None
        return street, state

    @staticmethod
    def _determine_absentee_status(
        property_address: Optional[str],
        property_state: Optional[str],
        mailing_address: Optional[str],
    ) -> Optional[str]:
        """Determine owner occupancy by comparing situs and mailing addresses.

        Comparison is field-by-field against Property's structured columns:
          1. State mismatch  → Out-of-State  (owner mails from outside property's state)
          2. Street match    → In-County     (owner lives at the property)
          3. Fallback        → Out-of-County (different address, same state)

        NOTE: This is only for absentee_status at ingest time. It does not interact
        with the match waterfall (find_property_by_address) or Property.normalized_address.
        """
        if not mailing_address or not property_address:
            return None

        mailing_street, mailing_state = MasterPropertyLoader._parse_mailing_parts(mailing_address)

        # Step 1 — Out-of-State: compare mailing state against Property.state
        if mailing_state and property_state:
            if mailing_state.upper() != property_state.upper():
                return 'Out-of-State'

        # Step 2 — In-County: normalize and compare streets in-memory only
        if mailing_street:
            norm_mail = normalize_street_address(mailing_street) or mailing_street.strip().upper()
            norm_prop = normalize_street_address(property_address) or property_address.strip().upper()
            if norm_mail == norm_prop:
                return 'In-County'

        # Step 3 — fallback
        return 'Out-of-County'

    # ------------------------------------------------------------------
    # File encoding
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_encoding(csv_path: str) -> str:
        """utf-8 if the whole file validates, else cp1252.

        County portals ship cp1252 CSVs (RP_PROPERTY_INFO has bytes like 0xC9
        'É' in owner names) while our own XLS->CSV conversions are utf-8 —
        and a bad byte 400k rows into a chunked read would otherwise kill the
        load mid-run. One streaming pass over the raw bytes (~1-3s for a
        300-500MB file) settles it before any DB work starts. cp1252 maps
        every byte, so it is a safe fallback rather than a guess."""
        import codecs
        decoder = codecs.getincrementaldecoder("utf-8")()
        try:
            with open(csv_path, "rb") as f:
                for block in iter(lambda: f.read(1 << 20), b""):
                    decoder.decode(block)
                decoder.decode(b"", final=True)
            return "utf-8"
        except UnicodeDecodeError:
            return "cp1252"

    # ------------------------------------------------------------------
    # Row parsing
    # ------------------------------------------------------------------

    def _parse_row(self, row) -> tuple[Optional[dict], Optional[str]]:
        """
        Parse one CSV row into the canonical field dict consumed by BOTH the
        insert and update paths (so hashed values are identical by construction).

        Returns:
            (parsed, None) on success, (None, skip_reason) when the row must be
            skipped — same validation rules the insert path has always used.
        """
        folio_raw = row.get('FOLIO', '')
        parcel_id = self.normalize_parcel_id(
            str(folio_raw).strip() if pd.notna(folio_raw) else ''
        )
        if not parcel_id or parcel_id == 'nan':
            return None, 'no_folio'

        # OWNER is critical - skip if looks like an address or clearly wrong
        owner_name = str(row.get('OWNER', '')).strip()
        if not owner_name or owner_name == 'nan':
            return None, 'no_owner'

        # Skip if owner name looks like address text spilled into the owner
        # column — UNLESS it carries an entity marker (an address-named LLC,
        # a county, a church... is a real owner, not a spill):
        # - Starts with digits (likely an address)
        # - Is a single word with no spaces and all caps (likely a city name like "ODESSA")
        # - Contains typical address indicators
        if not _looks_like_entity(owner_name) and (
            re.match(r'^\d', owner_name) or  # Starts with number
            (len(owner_name.split()) == 1 and owner_name.isupper() and len(owner_name) < 15) or  # Single all-caps short word
            any(indicator in owner_name.upper() for indicator in [' STREET ', ' ST ', ' AVE ', ' AVENUE ', ' ROAD ', ' RD ', ' LANE ', ' LN ', ' DRIVE ', ' DR ', 'W COUNTY', 'E COUNTY', 'N COUNTY', 'S COUNTY'])):
            return None, 'invalid_owner'

        # Safely extract fields - use None if invalid
        site_zip = self._is_valid_zip(row.get('SITE_ZIP'))
        beds = self._is_valid_small_number(row.get('tBEDS'))
        baths = self._is_valid_small_number(row.get('tBATHS'))

        # Extract string fields - skip if they look wrong
        site_addr_raw = str(row.get('SITE_ADDR', '')) if pd.notna(row.get('SITE_ADDR')) else ''
        site_addr = site_addr_raw[:255] if site_addr_raw and len(site_addr_raw) > 5 else None
        # Canonical form for ingestion + match-time comparison; shares
        # the same normalizer used by BaseLoader.normalize_address.
        site_addr_normalized = (normalize_street_address(site_addr) or None) if site_addr else None
        if site_addr_normalized:
            site_addr_normalized = site_addr_normalized[:255]

        site_city_raw = str(row.get('SITE_CITY', '')) if pd.notna(row.get('SITE_CITY')) else ''
        # Skip if site_city looks like a legal description (too long or has degrees/minutes)
        site_city = None
        if site_city_raw and len(site_city_raw) < 100 and 'DEG' not in site_city_raw.upper() and 'SEC' not in site_city_raw.upper():
            site_city = site_city_raw[:100]

        # TYPE (county-provided label, e.g. Pinellas "0110 Single Family Home")
        # wins; counties that publish only the bare DOR use code (Hillsborough
        # DOR_C) get the same "code + label" format via translation. Safe for
        # existing hashes: any row that already produced a TYPE value is
        # untouched, so no HASH_VERSION bump is needed.
        type_raw = row.get('TYPE')
        prop_type = None
        if pd.notna(type_raw) and str(type_raw).strip():
            prop_type = str(type_raw)[:50]
        else:
            dor_raw = row.get('DOR_C')
            if pd.notna(dor_raw):
                translated = translate_dor_code(dor_raw)
                prop_type = translated[:50] if translated else None

        # Build legal description
        legal_parts = []
        for col in ['LEGAL1', 'LEGAL2', 'LEGAL3', 'LEGAL4']:
            val = row.get(col)
            if pd.notna(val) and str(val).strip():
                legal_parts.append(str(val).strip())
        legal_desc = ' '.join(legal_parts) if legal_parts else None

        # Year built — try common HCPA column names
        yr_raw = row.get('YR_BLT') or row.get('YEAR_BUILT') or row.get('YR_BUILT')
        year_built = None
        if pd.notna(yr_raw):
            try:
                y = int(float(yr_raw))
                if 1800 <= y <= date.today().year:
                    year_built = y
            except (ValueError, TypeError):
                pass

        owner_name_clean = owner_name[:255]

        # Build mailing address - include all non-empty fields
        mailing_parts = []
        for col in ['ADDR_1', 'CITY', 'STATE', 'ZIP']:
            val = str(row.get(col, '')).strip() if pd.notna(row.get(col)) else ''
            # Include if not empty and not too long (allow short fields like "FL")
            if val and len(val) < 200:
                mailing_parts.append(val)

        mailing_addr = ', '.join(mailing_parts)[:255] if mailing_parts else None

        # Determine absentee status by comparing situs vs mailing address
        absentee_status = self._determine_absentee_status(
            property_address=site_addr,
            property_state="FL",
            mailing_address=mailing_addr,
        )

        # Classify owner type from name
        name_upper = owner_name_clean.upper()
        if any(kw in name_upper for kw in ['LLC', 'LLP', 'PLLC']):
            owner_type = 'LLC'
        elif any(kw in name_upper for kw in [' INC', ' CORP', ' LTD', ' CO ']):
            owner_type = 'Corporate'
        elif any(kw in name_upper for kw in ['TRUST', 'TRUSTEE']):
            owner_type = 'Trust'
        elif 'ESTATE' in name_upper:
            owner_type = 'Estate'
        else:
            owner_type = 'Individual'

        # Last sale — try common HCPA column names
        sale_date = self.parse_date(
            row.get('SALE1_DATE') or row.get('SALE_DATE') or row.get('SALESDATE')
        )
        sale_price = self.parse_amount(
            row.get('SALE1_PRC') or row.get('SALE_PRC') or row.get('SALESPRICE')
        )

        return {
            'parcel_id': parcel_id,
            'address': site_addr,
            'normalized_address': site_addr_normalized,
            'city': site_city,
            'zip': site_zip,
            'property_type': prop_type,
            'year_built': year_built,
            'sq_ft': self.parse_amount(row.get('HEAT_AR')),
            'beds': beds,
            'baths': baths,
            'lot_size': self.parse_amount(row.get('ACREAGE')),
            'legal_description': legal_desc,
            'owner_name': owner_name_clean,
            'mailing_address': mailing_addr,
            'owner_type': owner_type,
            'absentee_status': absentee_status,
            'assessed_value_mkt': self.parse_amount(row.get('ASD_VAL')),
            'assessed_value_tax': self.parse_amount(row.get('TAX_VAL')),
            'last_sale_price': sale_price,
            # Date column in DB — drop any time component so hash and storage agree.
            'last_sale_date': sale_date.date() if isinstance(sale_date, datetime) else sale_date,
        }, None

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def load_from_csv(
        self,
        csv_path: str,
        chunksize: int = 10000,
        dry_run: bool = False,
    ) -> MasterLoadStats:
        """
        Load a master bulk file: insert new parcels, update changed ones,
        stamp last_seen_at for every parcel present in the file.

        Args:
            csv_path: Path to CSV file
            chunksize: Number of rows to read per chunk from CSV
            dry_run: Classify and stage everything, report real counts, but
                     roll back instead of committing (no inserts, no updates).

        Returns:
            MasterLoadStats with insert/update/flag counts.
        """
        logger.info(f"Loading data from: {csv_path} (dry_run={dry_run})")

        run_ts = datetime.now(timezone.utc)
        tracker = TimeTracker(
            component="master_loader",
            run_id=f"{self.county_id}_{run_ts:%Y%m%d_%H%M%S}",
            meta={"county": self.county_id, "dry_run": dry_run, "csv": str(csv_path)},
        )

        with tracker.span("detect_encoding") as s:
            encoding = self._detect_encoding(csv_path)
            s["encoding"] = encoding
        logger.info(f"CSV encoding: {encoding}")

        # Resolve column mapping for this county via ColumnMapper middleware.
        # Peek at the first chunk to get column names → look up approved mapping or LLM-map.
        with tracker.span("resolve_column_mapping"):
            col_mapping: Optional[dict] = self._resolve_column_mapping(csv_path, encoding)

        from src.utils.county_config import get_county_config as _gc
        county_cfg = _gc(self.county_id)

        stats = MasterLoadStats(skip_reasons={
            'no_folio': 0, 'no_owner': 0, 'invalid_owner': 0, 'duplicate': 0,
            'cross_county': 0,
        })

        with tracker.span("preload_existing") as s:
            existing = self._preload_existing()
            s["rows"] = len(existing)
        # parcel_id is globally UNIQUE but the preload above is county-scoped:
        # a parcel already owned by another county would crash the insert batch
        # with a UniqueViolation, so those rows are skipped explicitly.
        with tracker.span("preload_other_county_ids") as s:
            other_county_ids = self._preload_other_county_ids()
            s["rows"] = len(other_county_ids)
        with tracker.span("create_staging_tables"):
            self._create_staging_tables()

        # In-file dedup across chunks: first occurrence of a parcel wins,
        # matching the historical behavior of the pre-loaded id set.
        seen_in_file: set = set()
        changed_buf: list = []
        seen_buf: list = []
        no_owner_buf: list = []
        pending_inserts = 0
        changed_total = 0

        chunk_num = 0
        rows_done = 0
        loop_t0 = time.perf_counter()
        for chunk in pd.read_csv(csv_path, dtype=str, chunksize=chunksize, encoding=encoding):
            chunk_num += 1
            chunk_t0 = time.perf_counter()
            with tracker.span("csv_chunk", chunk=chunk_num, rows=len(chunk)) as span_meta:
                chunk.columns = chunk.columns.str.upper()
                if col_mapping:
                    from src.loaders.column_mapper import ColumnMapper
                    chunk = ColumnMapper.apply(chunk, col_mapping)

                for _, row in chunk.iterrows():
                    parsed, skip_reason = self._parse_row(row)
                    if parsed is None:
                        stats.skip_reasons[skip_reason] += 1
                        stats.skipped += 1
                        if skip_reason == 'no_owner':
                            # Parcel exists but owner is blank — quarantine the
                            # raw row so the parcel isn't silently invisible.
                            no_owner_buf.append(self._build_quarantine_record(row))
                        continue

                    parcel_id = parsed['parcel_id']
                    if parcel_id in seen_in_file:
                        stats.skip_reasons['duplicate'] += 1
                        stats.skipped += 1
                        continue
                    seen_in_file.add(parcel_id)

                    row_hash = compute_source_row_hash(parsed)
                    hit = existing.get(parcel_id)

                    if hit is None:
                        if parcel_id in other_county_ids:
                            stats.skip_reasons['cross_county'] += 1
                            stats.skipped += 1
                            continue
                        stats.inserted += 1
                        if not dry_run:
                            self._insert_new(parsed, row_hash, run_ts, county_cfg)
                            pending_inserts += 1
                            if pending_inserts % 1000 == 0:
                                self.session.commit()
                    elif hit[1] == row_hash:
                        stats.unchanged += 1
                        seen_buf.append({'parcel_id': parcel_id})
                    else:
                        # Hash differs OR stored hash is NULL (pre-fa077 backfill):
                        # stage the full canonical record for the set-based update.
                        changed_buf.append({
                            **parsed,
                            'property_id': hit[0],
                            'source_row_hash': row_hash,
                        })
                        changed_total += 1

                    if len(changed_buf) >= _STAGE_FLUSH_SIZE:
                        self._flush_stage(self._changed_table, changed_buf)
                    if len(seen_buf) >= _STAGE_FLUSH_SIZE:
                        self._flush_stage(self._seen_table, seen_buf)

                # Running totals on the span line so the live JSONL tail shows
                # classification progress, not just chunk durations.
                span_meta.update(
                    new=stats.inserted, changed=changed_total,
                    unchanged=stats.unchanged, skipped=stats.skipped,
                )

            rows_done += len(chunk)
            elapsed = time.perf_counter() - loop_t0
            rate = rows_done / elapsed if elapsed > 0 else 0.0
            logger.info(
                f"Chunk {chunk_num} done in {time.perf_counter() - chunk_t0:.1f}s | "
                f"{rows_done:,} rows total ({rate:,.0f} rows/s) | "
                f"new={stats.inserted:,} changed={changed_total:,} "
                f"unchanged={stats.unchanged:,} skipped={stats.skipped:,}"
            )

        with tracker.span("final_stage_flush", changed=len(changed_buf), seen=len(seen_buf)):
            self._flush_stage(self._changed_table, changed_buf)
            self._flush_stage(self._seen_table, seen_buf)
            if not dry_run and pending_inserts % 1000 != 0:
                self.session.commit()

        if no_owner_buf:
            quarantine_count = len(no_owner_buf)
            with tracker.span("quarantine_no_owner", rows=quarantine_count):
                self._quarantine_no_owner(no_owner_buf)
            logger.info(f"Quarantined {quarantine_count:,} no-owner rows to unmatched_records")

        try:
            self._apply_set_based_updates(run_ts, stats, dry_run, tracker)
            with tracker.span("rollback" if dry_run else "final_commit"):
                if dry_run:
                    self.session.rollback()
                    logger.info("[DRY RUN] All changes rolled back")
                else:
                    self.session.commit()
        finally:
            self._drop_staging_tables()

        tracker.finish(**asdict(stats))
        logger.info(f"Master load complete: {stats.summary()}")
        logger.info(f"Skip reasons: {stats.skip_reasons}")
        return stats

    def load_from_dataframe(self, df: pd.DataFrame, skip_duplicates: bool = True):
        """Satisfies the BaseLoader ABC. The master loader's insert-only
        DataFrame API was removed by the weekly-refresh rewrite (fa077) —
        change detection needs the whole file, so use load_from_csv()."""
        raise NotImplementedError(
            "MasterPropertyLoader.load_from_dataframe was removed — "
            "use load_from_csv(), which partitions rows by change detection."
        )

    def _resolve_column_mapping(self, csv_path: str, encoding: str = "utf-8") -> Optional[dict]:
        """
        Peek at the CSV header, look up the source_id for this county's master_data source,
        and return a mapping dict via ColumnMapper.  Returns None if source is not in DB
        (Hillsborough canonical pass-through) or if mapper raises SkipMapping.
        """
        from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
        from src.core.models import CountySource

        # Look up source_id
        src = (
            self.session.query(CountySource)
            .filter_by(county_id=self.county_id, signal_type="master_data")
            .first()
        )
        if src is None:
            logger.warning(
                "[MasterLoader] No county_sources row for %s/master_data — "
                "skipping column mapping (assuming columns already canonical)",
                self.county_id,
            )
            return None

        # Read just the first 5 rows to give the LLM sample data
        sample_df = pd.read_csv(csv_path, dtype=str, nrows=5, encoding=encoding)
        sample_df.columns = sample_df.columns.str.upper()

        try:
            mapper = ColumnMapper()
            return mapper.get_or_create("master_data", src.id, sample_df)
        except SkipMapping:
            return None
        except NeedsMappingError as e:
            logger.error(
                "[MasterLoader] Column mapping required but LLM failed — "
                "create a mapping via admin UI before loading. Error: %s", e
            )
            raise

    # ------------------------------------------------------------------
    # Insert path (new parcels)
    # ------------------------------------------------------------------

    def _insert_new(self, parsed: dict, row_hash: str, run_ts: datetime, county_cfg: dict) -> None:
        """Create Property + Owner + Financial for a parcel not yet in the DB."""
        property_record = Property(
            parcel_id=parsed['parcel_id'],
            address=parsed['address'],
            normalized_address=parsed['normalized_address'],
            city=parsed['city'],
            state="FL",
            zip=parsed['zip'],
            jurisdiction=county_cfg.get("display_name", self.county_id),
            county_id=self.county_id,
            property_type=parsed['property_type'],
            legal_description=parsed['legal_description'],
            lot_size=parsed['lot_size'],
            year_built=parsed['year_built'],
            beds=parsed['beds'],
            baths=parsed['baths'],
            sq_ft=parsed['sq_ft'],
            source_row_hash=row_hash,
            last_seen_at=run_ts,
        )
        owner_record = Owner(
            property=property_record,
            owner_name=parsed['owner_name'],
            mailing_address=parsed['mailing_address'],
            owner_type=parsed['owner_type'],
            absentee_status=parsed['absentee_status'],
            county_id=self.county_id,
        )
        financial_record = Financial(
            property=property_record,
            assessed_value_mkt=parsed['assessed_value_mkt'],
            assessed_value_tax=parsed['assessed_value_tax'],
            last_sale_date=parsed['last_sale_date'],
            last_sale_price=parsed['last_sale_price'],
            annual_tax_amount=None,
            county_id=self.county_id,
        )
        self.session.add_all([property_record, owner_record, financial_record])

    # ------------------------------------------------------------------
    # Update path (changed parcels) — pure SQL, set-based
    # ------------------------------------------------------------------

    @property
    def _changed_table(self) -> str:
        return f"_master_stage_changed_{self._stage_suffix()}"

    @property
    def _seen_table(self) -> str:
        return f"_master_stage_seen_{self._stage_suffix()}"

    def _stage_suffix(self) -> str:
        # Table names cannot be bound parameters; restrict to a safe identifier
        # derived from county_id so concurrent county runs cannot collide.
        suffix = re.sub(r'[^a-z0-9_]', '', str(self.county_id).lower())
        if not suffix:
            raise ValueError(f"Cannot derive staging suffix from county_id={self.county_id!r}")
        return suffix

    def _preload_existing(self) -> dict:
        """One query: {parcel_id: (property_id, source_row_hash | None)} for the county."""
        logger.info(f"Pre-loading existing parcels for {self.county_id}...")
        rows = self.session.execute(
            sa_text(
                "SELECT parcel_id, id, source_row_hash "
                "FROM properties WHERE county_id = :county"
            ),
            {"county": self.county_id},
        ).fetchall()
        existing = {r[0]: (r[1], r[2]) for r in rows if r[0]}
        logger.info(f"  Found {len(existing):,} existing parcels")
        return existing

    def _build_quarantine_record(self, row) -> dict:
        """Quarantine payload for a no-owner CSV row (parcel exists, owner blank)."""
        folio = row.get('FOLIO', '')
        addr = row.get('SITE_ADDR')
        return {
            'source_type': 'master_data',
            'county': self.county_id,
            'parcel': self.normalize_parcel_id(
                str(folio).strip() if pd.notna(folio) else ''
            ),
            'address': str(addr)[:500] if pd.notna(addr) and str(addr).strip() else None,
            'raw': json.dumps({k: _json_safe(v) for k, v in row.items()}, default=str),
        }

    def _quarantine_no_owner(self, buffer: list) -> None:
        """Insert no-owner rows into unmatched_records. Idempotent across weekly
        runs via the (instrument_number, source_type, county_id) unique index —
        the parcel id serves as the instrument number."""
        stmt = sa_text("""
            INSERT INTO unmatched_records
                (source_type, county_id, raw_data, instrument_number,
                 address_string, match_status)
            VALUES (:source_type, :county, CAST(:raw AS jsonb), :parcel,
                    :address, 'unmatched')
            ON CONFLICT (instrument_number, source_type, county_id)
                WHERE instrument_number IS NOT NULL
            DO NOTHING
        """)
        try:
            self.session.execute(stmt, buffer)
        except SQLAlchemyError:
            logger.error(
                f"Failed to quarantine {len(buffer)} no-owner rows", exc_info=True
            )
            raise
        buffer.clear()

    def _preload_other_county_ids(self) -> set:
        """parcel_ids owned by OTHER counties (global unique-constraint guard)."""
        rows = self.session.execute(
            sa_text(
                "SELECT parcel_id FROM properties "
                "WHERE county_id IS DISTINCT FROM :county"
            ),
            {"county": self.county_id},
        ).fetchall()
        return {r[0] for r in rows if r[0]}

    def _create_staging_tables(self) -> None:
        """
        UNLOGGED staging tables (not TEMP: the session returns its connection to
        the pool on every commit, and TEMP tables are connection-scoped — they
        could silently vanish between the per-batch insert commits and the
        update phase). UNLOGGED skips WAL; names are county-suffixed so an
        overlapping run for another county cannot collide.
        """
        chg, seen = self._changed_table, self._seen_table
        col_defs = ",\n                ".join(f"{name} {pgtype}" for name, pgtype in _STAGE_COLUMNS)
        self.session.execute(sa_text(f"DROP TABLE IF EXISTS {chg}"))
        self.session.execute(sa_text(f"DROP TABLE IF EXISTS {seen}"))
        self.session.execute(sa_text(f"""
            CREATE UNLOGGED TABLE {chg} (
                {col_defs},
                material_change    BOOLEAN,
                owner_name_changed BOOLEAN,
                PRIMARY KEY (property_id)
            )
        """))
        self.session.execute(sa_text(
            f"CREATE UNLOGGED TABLE {seen} (parcel_id VARCHAR(100) PRIMARY KEY)"
        ))

    def _drop_staging_tables(self) -> None:
        try:
            self.session.execute(sa_text(f"DROP TABLE IF EXISTS {self._changed_table}"))
            self.session.execute(sa_text(f"DROP TABLE IF EXISTS {self._seen_table}"))
            self.session.commit()
        except SQLAlchemyError:
            logger.warning("Failed to drop staging tables (next run recreates them)", exc_info=True)
            self.session.rollback()

    def _flush_stage(self, table: str, buffer: list) -> None:
        """Bulk-insert buffered rows into a staging table and clear the buffer.

        The whole batch travels as ONE jsonb bind parameter, expanded
        server-side by jsonb_to_recordset — a single network round trip per
        flush. The executemany alternative costs one round trip PER ROW with
        psycopg2, which at WAN latencies (~250ms) turns a 5k-row flush into
        ~20 minutes.
        """
        if not buffer:
            return
        if table == self._seen_table:
            stmt = sa_text(
                f"INSERT INTO {table} (parcel_id) "
                "SELECT jsonb_array_elements_text(CAST(:rows AS jsonb)) "
                "ON CONFLICT DO NOTHING"
            )
            payload = json.dumps([r['parcel_id'] for r in buffer])
        else:
            names = ", ".join(name for name, _ in _STAGE_COLUMNS)
            record_defs = ", ".join(f"{name} {pgtype}" for name, pgtype in _STAGE_COLUMNS)
            stmt = sa_text(
                f"INSERT INTO {table} ({names}) "
                f"SELECT {names} FROM jsonb_to_recordset(CAST(:rows AS jsonb)) "
                f"AS r({record_defs}) "
                "ON CONFLICT (property_id) DO NOTHING"
            )
            payload = json.dumps(
                [{name: _json_safe(r.get(name)) for name, _ in _STAGE_COLUMNS} for r in buffer],
                default=str,  # date -> 'YYYY-MM-DD', cast to DATE by the recordset
            )
        try:
            self.session.execute(stmt, {"rows": payload})
        except SQLAlchemyError:
            logger.error(f"Failed staging flush into {table} ({len(buffer)} rows)", exc_info=True)
            raise
        buffer.clear()

    def _apply_set_based_updates(
        self,
        run_ts: datetime,
        stats: MasterLoadStats,
        dry_run: bool,
        tracker: TimeTracker,
    ) -> None:
        """
        Apply staged changes with set-based SQL. Order matters:
          0. compute material_change / owner_name_changed flags on staging
             (per-field IS DISTINCT FROM — this is what keeps NULL-hash backfill
             rows from spuriously triggering downstream re-processing)
          1. update properties (+ needs_rescore / pending_sync / last_seen_at)
          2. update owners (guarded; keeps trace data, flags it stale)
          3. insert owners missing for staged properties
          4. update financials (guarded)
          5. insert financials missing for staged properties
          6. stamp last_seen_at for all unchanged-but-present parcels
        """
        chg, seen = self._changed_table, self._seen_table
        params = {"county": self.county_id, "run_ts": run_ts}

        # Owner IDENTITY comparison is punctuation/whitespace-insensitive:
        # "SCHULTZ RONALD L" vs "SCHULTZ, RONALD L" is the same person, and a
        # portal reformatting names must not mass-flag skip_trace_stale /
        # needs_rescore (the byte-level diff still updates the stored string).
        def _norm(col: str) -> str:
            return f"regexp_replace(upper(coalesce({col}, '')), '[^A-Z0-9]', '', 'g')"

        def _timed(name: str, sql: str, bind: Optional[dict] = None):
            """Execute one statement inside a tracker span; log rowcount +
            elapsed so long remote-DB statements are visible while they run."""
            t0 = time.perf_counter()
            with tracker.span(name) as s:
                result = (
                    self.session.execute(sa_text(sql), bind)
                    if bind is not None else self.session.execute(sa_text(sql))
                )
                rowcount = result.rowcount if result.rowcount is not None else -1
                if rowcount >= 0:
                    s["rowcount"] = rowcount
            logger.info(
                "  %s: %s in %.1fs",
                name,
                f"{rowcount:,} rows" if rowcount >= 0 else "done",
                time.perf_counter() - t0,
            )
            return result

        # Planner needs stats on freshly bulk-loaded staging tables: the backfill
        # run stages ~500k rows and a seqscan-vs-index misestimate there is costly.
        with tracker.span("analyze_staging"):
            self.session.execute(sa_text(f"ANALYZE {chg}"))
            self.session.execute(sa_text(f"ANALYZE {seen}"))

        _timed("stmt0_flag_compute", f"""
            UPDATE {chg} s SET
                owner_name_changed = (
                    {_norm('o.owner_name')} IS DISTINCT FROM {_norm('s.owner_name')}
                ),
                material_change = (
                       p.address            IS DISTINCT FROM s.address
                    OR p.zip                IS DISTINCT FROM s.zip
                    OR p.city               IS DISTINCT FROM s.city
                    OR p.property_type      IS DISTINCT FROM s.property_type
                    OR p.year_built         IS DISTINCT FROM s.year_built
                    OR p.sq_ft              IS DISTINCT FROM s.sq_ft
                    OR p.beds               IS DISTINCT FROM s.beds
                    OR p.baths              IS DISTINCT FROM s.baths
                    OR p.lot_size           IS DISTINCT FROM s.lot_size
                    OR p.legal_description  IS DISTINCT FROM s.legal_description
                    OR {_norm('o.owner_name')} IS DISTINCT FROM {_norm('s.owner_name')}
                    OR o.mailing_address    IS DISTINCT FROM s.mailing_address
                    OR o.absentee_status    IS DISTINCT FROM s.absentee_status
                    OR f.assessed_value_mkt IS DISTINCT FROM s.assessed_value_mkt
                    OR f.assessed_value_tax IS DISTINCT FROM s.assessed_value_tax
                    OR f.last_sale_price    IS DISTINCT FROM s.last_sale_price
                    OR f.last_sale_date     IS DISTINCT FROM s.last_sale_date
                )
            FROM properties p
            LEFT JOIN owners o     ON o.property_id = p.id
            LEFT JOIN financials f ON f.property_id = p.id
            WHERE p.id = s.property_id
        """)

        # Flag counts must be read BEFORE statements 1-2 mutate sync_status etc.
        with tracker.span("flag_counts"):
            counts = self.session.execute(sa_text(f"""
                SELECT
                count(*) AS staged,
                count(*) FILTER (WHERE s.material_change) AS material,
                count(*) FILTER (
                    WHERE s.material_change AND p.sync_status = 'synced'
                ) AS resync,
                count(*) FILTER (
                    WHERE s.owner_name_changed
                      AND (o.skip_trace_success IS TRUE
                           OR o.phone_1 IS NOT NULL OR o.email_1 IS NOT NULL)
                ) AS stale_trace
            FROM {chg} s
            JOIN properties p ON p.id = s.property_id
            LEFT JOIN owners o ON o.property_id = s.property_id
        """)).one()
        stats.updated = counts.staged
        stats.flagged_rescore = counts.material
        stats.flagged_resync = counts.resync
        stats.flagged_stale_trace = counts.stale_trace
        logger.info(
            f"Staged {counts.staged:,} changed rows "
            f"({counts.material:,} with material drift)"
        )
        if dry_run:
            # Counts are real; skip the mutating statements entirely so a dry
            # run cannot leave row versions behind even if rollback is bypassed.
            return

        # 1. properties — app-managed columns (HCPA, CRM ids, lat/lon) are never
        # in the SET list; updated_at deliberately bypasses the ORM onupdate so
        # no-material-change backfill rows keep their original timestamp.
        _timed("stmt1_properties", f"""
            UPDATE properties p SET
                address            = s.address,
                normalized_address = s.normalized_address,
                city               = s.city,
                zip                = s.zip,
                property_type      = s.property_type,
                year_built         = s.year_built,
                sq_ft              = s.sq_ft,
                beds               = s.beds,
                baths              = s.baths,
                lot_size           = s.lot_size,
                legal_description  = s.legal_description,
                source_row_hash    = s.source_row_hash,
                last_seen_at       = :run_ts,
                needs_rescore      = CASE WHEN s.material_change THEN TRUE
                                          ELSE p.needs_rescore END,
                sync_status        = CASE WHEN s.material_change AND p.sync_status = 'synced'
                                          THEN 'pending_sync' ELSE p.sync_status END,
                updated_at         = CASE WHEN s.material_change THEN :run_ts
                                          ELSE p.updated_at END
            FROM {chg} s
            WHERE p.id = s.property_id AND p.county_id = :county
        """, params)

        # 2. owners — WHERE guard means unchanged children produce zero row
        # versions on the backfill run; trace/sunbiz columns are kept untouched.
        _timed("stmt2_owners", f"""
            UPDATE owners o SET
                owner_name       = s.owner_name,
                mailing_address  = s.mailing_address,
                owner_type       = s.owner_type,
                absentee_status  = s.absentee_status,
                skip_trace_stale = CASE
                    WHEN s.owner_name_changed
                         AND (o.skip_trace_success IS TRUE
                              OR o.phone_1 IS NOT NULL OR o.email_1 IS NOT NULL)
                    THEN TRUE ELSE o.skip_trace_stale END
            FROM {chg} s
            WHERE o.property_id = s.property_id
              AND (   o.owner_name      IS DISTINCT FROM s.owner_name
                   OR o.mailing_address IS DISTINCT FROM s.mailing_address
                   OR o.owner_type      IS DISTINCT FROM s.owner_type
                   OR o.absentee_status IS DISTINCT FROM s.absentee_status)
        """, params)

        # 3. hub rows created by other ingest paths may lack an owners child
        _timed("stmt3_owners_insert", f"""
            INSERT INTO owners (property_id, owner_name, mailing_address,
                                owner_type, absentee_status, county_id)
            SELECT s.property_id, s.owner_name, s.mailing_address,
                   s.owner_type, s.absentee_status, :county
            FROM {chg} s
            LEFT JOIN owners o ON o.property_id = s.property_id
            WHERE o.id IS NULL
        """, params)

        # 4. financials — only master-sourced columns; equity/mortgage/HCPA tax
        # fields are app-managed and never touched.
        _timed("stmt4_financials", f"""
            UPDATE financials f SET
                assessed_value_mkt = s.assessed_value_mkt,
                assessed_value_tax = s.assessed_value_tax,
                last_sale_price    = s.last_sale_price,
                last_sale_date     = s.last_sale_date
            FROM {chg} s
            WHERE f.property_id = s.property_id
              AND (   f.assessed_value_mkt IS DISTINCT FROM s.assessed_value_mkt
                   OR f.assessed_value_tax IS DISTINCT FROM s.assessed_value_tax
                   OR f.last_sale_price    IS DISTINCT FROM s.last_sale_price
                   OR f.last_sale_date     IS DISTINCT FROM s.last_sale_date)
        """, params)

        # 5. insert missing financials children
        _timed("stmt5_financials_insert", f"""
            INSERT INTO financials (property_id, assessed_value_mkt, assessed_value_tax,
                                    last_sale_price, last_sale_date, county_id)
            SELECT s.property_id, s.assessed_value_mkt, s.assessed_value_tax,
                   s.last_sale_price, s.last_sale_date, :county
            FROM {chg} s
            LEFT JOIN financials f ON f.property_id = s.property_id
            WHERE f.id IS NULL
        """, params)

        # 6. presence stamp for unchanged parcels. The IS DISTINCT FROM guard
        # skips rows statement 1 / the insert path already stamped, keeping the
        # weekly full-county stamp idempotent on re-runs.
        _timed("stmt6_last_seen", f"""
            UPDATE properties p SET last_seen_at = :run_ts
            FROM {seen} m
            WHERE p.parcel_id = m.parcel_id
              AND p.county_id = :county
              AND p.last_seen_at IS DISTINCT FROM CAST(:run_ts AS timestamptz)
        """, params)
