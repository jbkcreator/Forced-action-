"""
Tax delinquency loader — optimised with bulk upsert and pre-loaded property maps.
Version 2.0 — 2024-06-10

Well-optimized loader for tax delinquency records with the following features:
- Flexible field mapping via FIELD_ALIASES to handle varying source schemas.
- County-specific parcel matching strategies with configurable transforms.
- Pre-loading of property IDs for all unique parcel candidates to avoid per-row DB queries.
- Bulk upsert of matched records with null-safe field updates to preserve existing data.
- Batch quarantine of unmatched records into a separate table for later review. 

"""

import json
import logging
import math
import re
from collections import OrderedDict
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional, Tuple

import pandas as pd
from sqlalchemy import Integer, String, bindparam, func, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.loaders.base import BaseLoader
from src.core.models import TaxDelinquency, UnmatchedRecord

logger = logging.getLogger(__name__)


class TaxDelinquencyLoader(BaseLoader):
    """Loader for tax delinquency records."""

    PARCEL_MATCH_STRATEGIES: dict[str, dict[str, str]] = {
        "default": {
            "source_field": "parcel_number",
            "transform": "none",
        },
        "hillsborough": {
            "source_field": "source_account_number",
            "transform": "strip_alpha_prefix",
        },
        "pinellas": {
            "source_field": "parcel_number",
            "transform": "none",
        },
    }

    # Registry of parcel transform functions. Add a new entry here to support
    # a new county — no changes to _parcel_match_candidate needed.
    _PARCEL_TRANSFORMS: dict[str, Callable[[str], Optional[str]]] = {
        "none": lambda v: v,
        "strip_alpha_prefix": lambda v: re.sub(r"^[A-Za-z]+", "", v).strip() or None,
    }

    FIELD_ALIASES: dict[str, tuple[str, ...]] = {
        "source_report": ("source_report", "Source Report"),
        "tax_year": ("tax_year", "Tax Yr", "Tax Year"),
        "years_delinquent": ("years_delinquent", "years_delinquent_scraped", "Years Delinquent"),
        "source_account_number": ("source_account_number", "Account Number"),
        "account_number": ("account_number", "Account Number"),
        "alternate_key": ("alternate_key", "Alternate Key"),
        "parcel_number": ("parcel_number", "Parcel Number"),
        "owner_name": ("owner_name", "Owner", "Owner Name", "Name"),
        "owner_address": ("owner_address", "Owner Address", "Mailing Address"),
        "property_address": ("property_address", "Property Address", "Address", "Site Address"),
        "certificate_number": ("certificate_number", "Certificate Number", "Cert Number", "Cert No"),
        "certificate_status": ("certificate_status", "Cert Status", "Certificate Status"),
        "issued_date": ("issued_date", "Issued Date", "Issue Date"),
        "bidder_number": ("bidder_number", "Bidder Number"),
        "certificate_buyer": ("certificate_buyer", "Certificate Buyer", "Buyer"),
        "certificate_buyer_address": ("certificate_buyer_address", "Certificate Buyer Address", "Buyer Address"),
        "face_amount": ("face_amount", "Face Amount"),
        "account_balance_amount": ("account_balance_amount", "Account Balance Amount", "Account Balance"),
        "total_amount_due": ("total_amount_due", "Total Due", "Amount Due"),
        "interest_rate": ("interest_rate", "Interest Rate"),
        "assessed_value": ("assessed_value", "Assessed Value"),
        "account_status": ("account_status", "Account Status"),
        "deed_status": ("deed_status", "Deed Status"),
        "deed_app_date": ("deed_app_date", "Deed App Date", "Deed Application Date"),
        "date_redeemed": ("date_redeemed", "Date Redeemed", "Redeemed Date"),
        "purchased_date": ("purchased_date", "Purchased Date", "Purchase Date"),
        "county_held": ("county_held", "County Held"),
        "standard_flags": ("standard_flags", "Standard Flags"),
        "custom_flags": ("custom_flags", "Custom Flags"),
        "use_code": ("use_code", "Use Code"),
    }

    INT_FIELDS = {"tax_year", "years_delinquent"}
    MONEY_FIELDS = {
        "face_amount",
        "account_balance_amount",
        "total_amount_due",
        "interest_rate",
        "assessed_value",
    }
    DATE_FIELDS = {
        "issued_date",
        "deed_app_date",
        "date_redeemed",
        "purchased_date",
    }
    BOOL_FIELDS = {"county_held"}

    # ── Fields that must never overwrite a non-null DB value with NULL ──────
    _NULL_SAFE_FIELDS: tuple[str, ...] = (
        "source_report",
        "years_delinquent",
        "source_account_number",
        "account_number",
        "alternate_key",
        "parcel_number",
        "owner_name",
        "owner_address",
        "property_address",
        "certificate_number",
        "certificate_status",
        "issued_date",
        "bidder_number",
        "certificate_buyer",
        "certificate_buyer_address",
        "face_amount",
        "account_balance_amount",
        "total_amount_due",
        "interest_rate",
        "assessed_value",
        "account_status",
        "deed_status",
        "deed_app_date",
        "date_redeemed",
        "purchased_date",
        "county_held",
        "standard_flags",
        "custom_flags",
        "use_code",
        "certificate_data",
    )
    _UPSERT_ALWAYS_FIELDS: tuple[str, ...] = (
        "property_id",
        "tax_year",
        "county_id",
        "date_added",
        "raw_source_data",
    )

    @staticmethod
    def _clean_value(value: Any) -> Any:
        if pd.isna(value):
            return None
        if isinstance(value, str):
            clean = value.strip()
            if not clean or clean.lower() in {"nan", "none", "null", "-- none --"}:
                return None
            return clean
        return value

    def _first_value(self, row: pd.Series, field_name: str) -> Any:
        for col in self.FIELD_ALIASES[field_name]:
            if col in row.index:
                value = self._clean_value(row.get(col))
                if value is not None:
                    return value
        return None

    def _parse_bool(self, value: Any) -> Optional[bool]:
        value = self._clean_value(value)
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "t", "yes", "y", "county", "county held"}:
            return True
        if normalized in {"0", "false", "f", "no", "n"}:
            return False
        return None

    def _parse_date_value(self, value: Any) -> Optional[date]:
        parsed = self.parse_date(value)
        if parsed is None:
            return None
        return parsed.date() if hasattr(parsed, "date") else parsed

    def _parcel_match_strategy(self) -> dict[str, str]:
        return self.PARCEL_MATCH_STRATEGIES.get(
            self.county_id,
            self.PARCEL_MATCH_STRATEGIES["default"],
        )

    def _parcel_match_candidate(self, values: dict[str, Any]) -> Optional[str]:
        """Derive the parcel-ID candidate from raw values using the county strategy."""
        strategy = self._parcel_match_strategy()
        source_field = strategy.get("source_field", "parcel_number")
        transform = strategy.get("transform", "none")
        candidate = self._clean_value(values.get(source_field))

        if candidate is None and source_field == "source_account_number":
            candidate = self._clean_value(values.get("account_number"))
        if candidate is None:
            return None

        candidate = str(candidate)
        transform_fn = self._PARCEL_TRANSFORMS.get(transform)
        if transform_fn is None:
            logger.warning(
                "Unknown tax parcel transform=%s for county=%s",
                transform,
                self.county_id,
            )
            return candidate
        return transform_fn(candidate)

    @staticmethod
    def _chunks(values: set[str], size: int = 5000):
        values_list = list(values)
        for idx in range(0, len(values_list), size):
            yield values_list[idx:idx + size]

    def _preload_property_ids(
        self,
        parcel_candidates: set[str],
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Bulk-load property IDs for all unique parcel candidates in one round-trip."""
        exact_map: dict[str, int] = {}
        normalized_map: dict[str, int] = {}
        parcel_candidates = {str(candidate) for candidate in parcel_candidates if candidate}
        if not parcel_candidates:
            return exact_map, normalized_map

        exact_stmt = text("""
            SELECT p.id, p.parcel_id
            FROM properties p
            JOIN unnest(:parcel_ids) AS candidates(parcel_id)
              ON p.parcel_id = candidates.parcel_id
            WHERE p.county_id = :county_id
        """).bindparams(bindparam("parcel_ids", type_=ARRAY(String)))

        normalized_candidates = {
            normalized
            for normalized in (self.normalize_parcel_id(candidate) for candidate in parcel_candidates)
            if normalized
        }
        normalized_stmt = text("""
            SELECT
                p.id,
                regexp_replace(p.parcel_id, '[^A-Za-z0-9]', '', 'g') AS normalized_parcel_id
            FROM properties p
            JOIN unnest(:parcel_ids) AS candidates(parcel_id)
              ON regexp_replace(p.parcel_id, '[^A-Za-z0-9]', '', 'g') = candidates.parcel_id
            WHERE p.county_id = :county_id
        """).bindparams(bindparam("parcel_ids", type_=ARRAY(String)))

        for batch in self._chunks(parcel_candidates):
            rows = self.session.execute(
                exact_stmt,
                {"county_id": self.county_id, "parcel_ids": batch},
            ).mappings()
            exact_map.update({row["parcel_id"]: row["id"] for row in rows})

        for batch in self._chunks(normalized_candidates):
            rows = self.session.execute(
                normalized_stmt,
                {"county_id": self.county_id, "parcel_ids": batch},
            ).mappings()
            normalized_map.update({row["normalized_parcel_id"]: row["id"] for row in rows})

        logger.info(
            "Pre-loaded %d exact and %d normalized property matches from %d tax parcel candidates",
            len(exact_map),
            len(normalized_map),
            len(parcel_candidates),
        )
        return exact_map, normalized_map

    def _build_tax_values(self, row: pd.Series) -> dict[str, Any]:
        """Extract typed values from a source row using FIELD_ALIASES."""
        values: dict[str, Any] = {}
        for field_name in self.FIELD_ALIASES:
            raw_value = self._first_value(row, field_name)
            if raw_value is None:
                continue
            if field_name in self.INT_FIELDS:
                value = self.parse_int(raw_value)
            elif field_name in self.MONEY_FIELDS:
                value = self.parse_amount(raw_value)
            elif field_name in self.DATE_FIELDS:
                value = self._parse_date_value(raw_value)
            elif field_name in self.BOOL_FIELDS:
                value = self._parse_bool(raw_value)
            else:
                value = raw_value
            if value is not None:
                values[field_name] = value

        # Ensure account_number / source_account_number symmetry
        if not values.get("source_account_number") and values.get("account_number"):
            values["source_account_number"] = values["account_number"]
        if not values.get("account_number") and values.get("source_account_number"):
            values["account_number"] = values["source_account_number"]

        # Synthetic certificate_data if no column was present
        if not values.get("certificate_data"):
            parts = []
            cert_status = values.get("certificate_status")
            deed_status = values.get("deed_status")
            if cert_status:
                parts.append(f"Cert: {cert_status}")
            if deed_status:
                parts.append(f"Deed: {deed_status}")
            if parts:
                values["certificate_data"] = ", ".join(parts)

        return values

    def _raw_source_data(self, row: pd.Series) -> dict[str, Any]:
        raw = {}
        for key, value in row.to_dict().items():
            clean = self._clean_value(value)
            raw[key] = clean
        return raw

    # ========================================================================
    # CSV loading
    # ========================================================================

    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """Load tax delinquencies from CSV, chunked for memory safety."""
        col_mapping = self._resolve_column_mapping(csv_path)
        total_matched = total_updated = total_unmatched = 0
        chunk_count = 0
        from src.loaders.column_mapper import ColumnMapper
        for chunk in pd.read_csv(csv_path, dtype=str, chunksize=5000):
            chunk_count += 1
            if col_mapping:
                chunk = ColumnMapper.apply(chunk, col_mapping)
            m, u, um = self.load_from_dataframe(chunk, skip_duplicates=skip_duplicates)
            total_matched += m
            total_updated += u
            total_unmatched += um
            logger.info(
                "Chunk %d done: +%d matched, +%d updated, +%d unmatched (cumulative: %d/%d/%d)",
                chunk_count, m, u, um, total_matched, total_updated, total_unmatched,
            )
        return total_matched, total_updated, total_unmatched

    def _resolve_column_mapping(self, csv_path: str) -> Optional[dict]:
        from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
        src_row = self.session.execute(text("""
            SELECT id FROM county_sources
            WHERE county_id = :county_id AND signal_type = 'tax_delinquency'
            LIMIT 1
        """), {"county_id": self.county_id}).mappings().first()
        if src_row is None:
            return None
        sample_df = pd.read_csv(csv_path, dtype=str, nrows=5)
        try:
            mapper = ColumnMapper()
            return mapper.get_or_create("tax_delinquency", src_row["id"], sample_df)
        except SkipMapping:
            return None
        except NeedsMappingError as e:
            logger.error("[TaxDelinquencyLoader] Column mapping required but LLM failed: %s", e)
            raise

    # ========================================================================
    # Core DataFrame load
    # ========================================================================

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """
        Load tax delinquencies from a DataFrame using bulk upsert.

        Phase 1 — Pre-load existing (property_id, tax_year) keys and property
                  parcel-ID maps so the row loop has zero per-row DB queries.

        Phase 2 — Row loop: build values, resolve property via pre-loaded maps,
                  classify as matched/updated/unmatched, collect for bulk ops.

        Phase 3 — Bulk upsert matched rows + bulk quarantine unmatched rows.
        """
        logger.info("Loading %d tax delinquency rows (county=%s)", len(df), self.county_id)

        # ── Phase 1a: Warn early if the county has zero properties ───────
        has_properties = self.session.execute(text("""
            SELECT EXISTS (SELECT 1 FROM properties WHERE county_id = :county_id)
        """), {"county_id": self.county_id}).scalar()
        if not has_properties:
            logger.warning(
                "[TaxDelinquencyLoader] No properties found for county=%s — "
                "all rows will be quarantined. Load master property data first.",
                self.county_id,
            )

        # ── Phase 1b: Pre-load property parcel-ID maps ────────────────────
        parcel_candidates: set[str] = set()
        prepared_rows: list[tuple] = []

        for row_idx, row in df.iterrows():
            values = self._build_tax_values(row)
            parcel_number = self._parcel_match_candidate(values)
            if parcel_number:
                parcel_candidates.add(parcel_number)
            prepared_rows.append((row_idx, row, values, parcel_number))

        exact_property_map, normalized_property_map = self._preload_property_ids(parcel_candidates)

        # ── Phase 2: Row loop (no per-row DB queries in the hot path) ─────
        records_to_upsert: list[dict[str, Any]] = []
        unmatched_to_quarantine: list[dict[str, Any]] = []
        unmatched = 0

        for idx, (_, row, values, parcel_number) in enumerate(prepared_rows):
            if idx > 0 and idx % 500 == 0:
                logger.info(
                    "Tax delinquency progress: %d/%d rows (unmatched=%d so far)",
                    idx, len(df), unmatched,
                )

            account_number = values.get("source_account_number") or values.get("account_number")
            if not account_number:
                unmatched_to_quarantine.append({
                    "source_type": "tax_delinquencies",
                    "raw_row": row.to_dict() if hasattr(row, "to_dict") else dict(row),
                    "instrument_number": None,
                })
                unmatched += 1
                continue

            # Stage 1 — Pre-loaded parcel-id match (fast path)
            property_id: Optional[int] = None
            if parcel_number:
                property_id = exact_property_map.get(parcel_number)
                if not property_id:
                    normalized = self.normalize_parcel_id(parcel_number)
                    property_id = normalized_property_map.get(normalized)

            # Stage 2 — Address / owner cascade fallback
            if not property_id:
                address = str(
                    values.get("property_address") or values.get("owner_address") or ""
                ).strip()
                owner = str(values.get("owner_name") or "").strip()
                zip_code = str(
                    row.get("Zip") or row.get("ZIP") or row.get("Zip Code") or ""
                ).strip()
                if address == "nan":
                    address = ""
                if owner == "nan":
                    owner = ""
                if zip_code == "nan":
                    zip_code = ""

                if address or owner:
                    prop, method, score = self.find_property_cascade(
                        address=address or None,
                        owner_name=owner or None,
                        zip_code=zip_code or None,
                        addr_threshold=self._thresholds.address_floor,
                        owner_threshold=self._thresholds.owner_name_floor,
                    )
                    if prop:
                        property_id = prop.id
                        logger.debug(
                            "Tax delinquency %s matched via cascade (%s, score=%s)",
                            account_number, method, score,
                        )

            if not property_id:
                logger.warning(
                    "No property match for parcel: %s (searched as: %s)",
                    account_number, parcel_number or account_number,
                )
                unmatched_to_quarantine.append({
                    "source_type": "tax_delinquencies",
                    "raw_row": row.to_dict() if hasattr(row, "to_dict") else dict(row),
                    "instrument_number": str(account_number),
                })
                unmatched += 1
                continue

            # ── Apply common metadata ─────────────────────────────────────
            tax_year = values.get("tax_year")
            if tax_year is None:
                logger.warning(
                    "No tax year for tax delinquency account: %s",
                    account_number,
                )
                unmatched_to_quarantine.append({
                    "source_type": "tax_delinquencies",
                    "raw_row": row.to_dict() if hasattr(row, "to_dict") else dict(row),
                    "instrument_number": str(account_number),
                })
                unmatched += 1
                continue
            values.update(
                property_id=property_id,
                tax_year=tax_year,
                county_id=self.county_id,
                date_added=date.today(),
                raw_source_data=self._raw_source_data(row),
            )

            records_to_upsert.append(values)

        records_to_upsert = self._dedupe_upsert_records(records_to_upsert)

        # ── Phase 2.5: Batch-query existing keys (batch-scoped, not full-county) ──
        existing_map: dict[tuple, int] = {}
        if records_to_upsert:
            property_ids = list({r["property_id"] for r in records_to_upsert})
            db_rows = self.session.execute(
                text("""
                    SELECT id, property_id, tax_year
                    FROM tax_delinquencies
                    WHERE property_id = ANY(:property_ids)
                      AND county_id = :county_id
                """).bindparams(bindparam("property_ids", type_=ARRAY(Integer))),
                {"property_ids": property_ids, "county_id": self.county_id},
            ).mappings()
            existing_map = {(r["property_id"], r["tax_year"]): r["id"] for r in db_rows}

        matched = sum(
            1 for rec in records_to_upsert
            if (rec["property_id"], rec["tax_year"]) not in existing_map
        )
        updated = len(records_to_upsert) - matched

        # ── Phase 3a: Bulk upsert matched records ─────────────────────────
        if records_to_upsert:
            self._bulk_upsert(records_to_upsert)
            logger.info(
                "Bulk upserted %d records (%d matched, %d updated)",
                len(records_to_upsert), matched, updated,
            )

        # ── Phase 3b: Batch quarantine unmatched records ─────────────────
        if unmatched_to_quarantine:
            self._batch_quarantine(unmatched_to_quarantine)
            logger.info("Batch-quarantined %d unmatched records", len(unmatched_to_quarantine))

        logger.info(
            "Tax delinquencies: %d inserted, %d updated, %d unmatched",
            matched, updated, unmatched,
        )
        return matched, updated, unmatched

    # ========================================================================
    # Bulk upsert helpers
    # ========================================================================

    def _dedupe_upsert_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Deduplicate by conflict key, merging non-null fields across duplicates."""
        deduped: OrderedDict[tuple, dict[str, Any]] = OrderedDict()
        for rec in records:
            key = (rec["property_id"], rec["tax_year"])
            if key not in deduped:
                deduped[key] = dict(rec)
                continue
            merged = deduped[key]
            for field, value in rec.items():
                if field in self._UPSERT_ALWAYS_FIELDS or value is not None:
                    merged[field] = value
        return list(deduped.values())

    def _normalize_upsert_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Give every bulk-insert row the same column set."""
        columns = self._UPSERT_ALWAYS_FIELDS + self._NULL_SAFE_FIELDS
        return [
            {column: rec.get(column) for column in columns}
            for rec in records
        ]

    def _bulk_upsert(self, records: list[dict[str, Any]]) -> None:
        """
        Execute a single bulk PostgreSQL upsert for all matched tax records.

        Always-overwrite fields: date_added, raw_source_data, updated_at
        Null-safe fields: all optional columns — excluded value only wins
        if it is NOT NULL (via func.coalesce).
        """
        if not records:
            return

        records = self._normalize_upsert_records(records)

        table = TaxDelinquency.__table__
        stmt = pg_insert(table).values(records)

        # Build the SET clause with null-safety for optional fields
        set_clause = {
            "date_added": stmt.excluded.date_added,
            "raw_source_data": stmt.excluded.raw_source_data,
            "updated_at": func.now(),
        }
        for field in self._NULL_SAFE_FIELDS:
            set_clause[field] = func.coalesce(
                getattr(stmt.excluded, field),
                getattr(table.c, field),
            )

        stmt = stmt.on_conflict_do_update(
            index_elements=["property_id", "tax_year"],
            set_=set_clause,
        )
        try:
            self.session.execute(stmt)
            self.session.flush()
        except Exception as e:
            logger.error("Bulk upsert failed for %d records: %s", len(records), e)
            raise

    def _batch_quarantine(self, records: list[dict[str, Any]]) -> None:
        """
        Bulk upsert unmatched records into unmatched_records.
        Deduplicates by (instrument_number, source_type, county_id) before
        executing so a re-encountered row refreshes timestamps instead of
        raising a unique-violation.
        """
        if not records:
            return

        # Keyed rows dedup by (instrument, source, county); None-instrument rows
        # each get a unique slot — the partial unique index doesn't cover NULLs
        # so they always do plain inserts and must not be collapsed.
        deduped: OrderedDict[Any, dict[str, Any]] = OrderedDict()
        anon_idx = 0
        for rec in records:
            county_id = rec.get("county_id", self.county_id)
            instrument = rec.get("instrument_number")
            if instrument is not None:
                key: Any = f"{instrument}|{rec['source_type']}|{county_id}"
                deduped[key] = rec
            else:
                deduped[anon_idx] = rec
                anon_idx += 1
        records = list(deduped.values())

        values_list: list[dict[str, Any]] = []
        for rec in records:
            raw_row = rec.get("raw_row", {})
            safe_raw = {}
            for k, v in raw_row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    safe_raw[k] = None
                else:
                    try:
                        json.dumps(v)
                        safe_raw[k] = v
                    except (TypeError, ValueError):
                        safe_raw[k] = str(v)

            instrument_val = rec.get("instrument_number")
            values_list.append({
                "source_type": rec.get("source_type", "tax_delinquencies"),
                "county_id": rec.get("county_id", self.county_id),
                "raw_data": safe_raw,
                "instrument_number": str(instrument_val) if instrument_val is not None else None,
                "match_status": "unmatched",
                "match_attempted_at": datetime.now(timezone.utc),
            })

        table = UnmatchedRecord.__table__
        stmt = pg_insert(table).values(values_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["instrument_number", "source_type", "county_id"],
            index_where=UnmatchedRecord.instrument_number.isnot(None),
            set_={
                "raw_data": stmt.excluded.raw_data,
                "match_status": stmt.excluded.match_status,
                "match_attempted_at": stmt.excluded.match_attempted_at,
            },
        )
        try:
            self.session.execute(stmt)
            self.session.flush()
        except Exception as e:
            logger.error("Batch quarantine failed for %d records: %s", len(records), e)
            raise
