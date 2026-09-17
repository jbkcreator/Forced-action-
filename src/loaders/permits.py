"""
Building permit loader.
"""

import logging
import re
from datetime import date
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import text

_COMPLETION_STATUS_MAP: dict[str, str] = {
    "issued": "issued",
    "active": "active",
    "open": "active",
    "in review": "active",
    "under review": "active",
    "approved": "issued",
    "expired": "expired",
    "revoked": "expired",
    "cancelled": "expired",
    "cancel": "expired",
    "completed": "completed",
    "complete": "completed",
    "finaled": "completed",
    "final": "completed",
    "closed": "completed",
    "co issued": "completed",
    "pending": "pending",
    "received": "pending",
    "submitted": "pending",
}

from src.loaders.base import BaseLoader
from src.core.models import BuildingPermit, CountySource, PermitStaging

# Hillsborough County permit_type substrings that indicate an enforcement permit
_ENFORCEMENT_TYPE_KEYWORDS = frozenset({
    "code compliance case",
})
# Status values that indicate enforcement (exact match, case-insensitive)
# "awaiting client reply" = owner not responding, work stalled — strong distress signal
# "expired" / "revoked" = permit lapsed without completion — owner stalled or non-compliant
_ENFORCEMENT_STATUS_VALUES = frozenset({
    "withdrawn", "cancel", "awaiting client reply", "waiting on applicant",
    "expired", "revoked",
})

def _is_enforcement(permit_type: str | None, status: str | None, expire_date) -> bool:
    """Return True if this permit qualifies as an enforcement permit."""
    pt = (permit_type or "").lower()
    st = (status or "").lower().strip()

    if any(kw in pt for kw in _ENFORCEMENT_TYPE_KEYWORDS):
        return True
    if st in _ENFORCEMENT_STATUS_VALUES:
        return True

    return False

logger = logging.getLogger(__name__)


def _clean_str(value) -> str | None:
    """Scraped-cell → clean str or None. Guards pandas NaN (read_csv(dtype=str)
    yields float NaN for blank cells; str(NaN) == 'nan', which would otherwise
    become a fake holder/contractor identity)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s or None


def _normalize_completion_status(raw: str | None) -> str | None:
    if not raw:
        return None
    return _COMPLETION_STATUS_MAP.get(raw.lower().strip())


def _parse_job_value(raw) -> float | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    try:
        cleaned = str(raw).replace("$", "").replace(",", "").strip()
        val = float(cleaned)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


class BuildingPermitLoader(BaseLoader):
    """Loader for building permits."""

    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """Load permits from CSV, applying ColumnMapper if a mapping exists."""
        col_mapping = self._resolve_column_mapping(csv_path)
        df = pd.read_csv(csv_path, dtype=str)
        if col_mapping:
            from src.loaders.column_mapper import ColumnMapper
            df = ColumnMapper.apply(df, col_mapping)
        return self.load_from_dataframe(df, skip_duplicates=skip_duplicates)

    def _resolve_column_mapping(self, csv_path: str) -> Optional[dict]:
        from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
        src = self.session.query(CountySource).filter_by(
            county_id=self.county_id, signal_type="permits"
        ).first()
        if src is None:
            return None
        sample_df = pd.read_csv(csv_path, dtype=str, nrows=5)
        try:
            mapper = ColumnMapper()
            return mapper.get_or_create("permits", src.id, sample_df)
        except SkipMapping:
            return None
        except NeedsMappingError as e:
            logger.error("[BuildingPermitLoader] Column mapping required but LLM failed: %s", e)
            raise

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load permits from DataFrame.
        
        Args:
            df: DataFrame with columns: Date, Record Number, Record Type, Address, Status, Expiration Date
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
            
        Note: BuildingPermit model has: permit_number, permit_type, issue_date, expire_date, status
        """
        logger.info(f"Loading {len(df)} building permits")
        
        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            record_number = str(row['Record Number']).strip()
            description_val = _clean_str(row.get('Description'))
            incoming_holder = _clean_str(row.get('Holder Name'))
            incoming_contractor = _clean_str(row.get('Contractor Name'))
            incoming_job_value = _parse_job_value(row.get('Job Value'))

            # Check for duplicates; backfill description + enrichment when NULL
            if skip_duplicates:
                existing_row = self.session.execute(
                    text("""
                        SELECT id, description, status, holder_name, contractor_name,
                               job_value, completion_status
                        FROM building_permits WHERE permit_number = :pnum LIMIT 1
                    """),
                    {"pnum": record_number},
                ).fetchone()
                if existing_row:
                    incoming_status = _clean_str(row.get('Status'))
                    incoming_completion = _normalize_completion_status(incoming_status)
                    description_changed = existing_row.description is None and description_val
                    status_changed = bool(incoming_status and incoming_status != existing_row.status)
                    # Backfill fires whenever incoming source data can fill a currently-NULL
                    # enrichment column — not only on description/status change (a later scrape
                    # can add holder/contractor/job_value with status unchanged).
                    enrichment_backfillable = (
                        (incoming_holder and existing_row.holder_name is None)
                        or (incoming_contractor and existing_row.contractor_name is None)
                        or (incoming_job_value is not None and existing_row.job_value is None)
                        or (incoming_completion and existing_row.completion_status is None)
                    )
                    if description_changed or status_changed or enrichment_backfillable:
                        self.session.execute(
                            text("""
                                UPDATE building_permits
                                SET description       = COALESCE(description, :desc),
                                    status            = CASE WHEN :status IS NOT NULL THEN :status ELSE status END,
                                    holder_name       = COALESCE(holder_name, :holder_name),
                                    contractor_name   = COALESCE(contractor_name, :contractor_name),
                                    job_value         = COALESCE(job_value, :job_value),
                                    completion_status = COALESCE(completion_status, :completion_status)
                                WHERE id = :id
                            """),
                            {
                                "desc": description_val,
                                "status": incoming_status,
                                "holder_name": incoming_holder,
                                "contractor_name": incoming_contractor,
                                "job_value": incoming_job_value,
                                "completion_status": incoming_completion,
                                "id": existing_row.id,
                            },
                        )
                        self.session.flush()
                    skipped += 1
                    continue
            

            # Match by address — extract ZIP from raw address if present
            property_record = None
            if pd.notna(row.get('Address')):
                raw_address = str(row['Address'])
                # Match zip after state abbreviation to avoid capturing 5-digit house numbers
                zip_match = re.search(r'\bFL\s+(\d{5})\b', raw_address, re.IGNORECASE)
                if not zip_match:
                    zip_match = re.search(r',\s*(\d{5})(?:-\d{4})?\s*$', raw_address)
                zip_code = zip_match.group(1) if zip_match else None
                match_result = self.find_property_by_address(
                    raw_address, zip_code=zip_code, strict_house_number=False,
                    threshold=self._thresholds.address_floor,
                )
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched permit by address (score: {score}%): {record_number}")
            
            # Enrichment fields shared by both branches (NaN-safe via _clean_str)
            permit_type_val = _clean_str(row.get('Record Type'))
            status_val = _clean_str(row.get('Status'))
            raw_holder = incoming_holder
            raw_contractor = incoming_contractor
            job_value_val = incoming_job_value
            completion_status_val = _normalize_completion_status(status_val)
            parsed_issue = self.parse_date(row.get('Date'))
            parsed_expire = self.parse_date(row.get('Expiration Date'))
            # Enforcement is computed BEFORE the property-match branch so unmatched
            # permits carry the flag into permit_staging (detectors must be able to
            # exclude enforcement records from the staging side too).
            enforcement = _is_enforcement(permit_type_val, status_val, parsed_expire)

            if property_record:
                try:
                    permit_record = BuildingPermit(
                        property_id=property_record.id,
                        permit_number=record_number,
                        permit_type=permit_type_val,
                        status=status_val,
                        issue_date=parsed_issue,
                        expire_date=parsed_expire,
                        is_enforcement_permit=enforcement,
                        county_id=self.county_id,
                        description=description_val,
                        holder_name=raw_holder,
                        contractor_name=raw_contractor,
                        job_value=job_value_val,
                        completion_status=completion_status_val,
                    )

                    if self.safe_add(permit_record):
                        matched += 1
                        # Promotion: this permit now has a real building_permits row.
                        # Remove any earlier permit_staging representation (and its
                        # buyer_entity_link) so detectors don't count the same permit
                        # twice — once via the matched property, once via staging.
                        self._promote_from_staging(record_number)
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building permit {record_number}: {e}")
                    unmatched += 1
            else:
                logger.info(f"Permit {record_number} unmatched — staging for builder engine")
                self._persist_to_staging(
                    permit_number=record_number,
                    permit_type=permit_type_val,
                    address=_clean_str(row.get('Address')) or '',
                    holder_name=raw_holder,
                    contractor_name=raw_contractor,
                    job_value=job_value_val,
                    completion_status=completion_status_val,
                    status=status_val,
                    description=description_val,
                    issue_date=parsed_issue,
                    expire_date=parsed_expire,
                    is_enforcement_permit=enforcement,
                )
                self.quarantine_unmatched(
                    source_type="permits",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    address_string=str(row.get('Address', '')),
                    instrument_number=str(record_number),
                )
                unmatched += 1
        
        logger.info(f"Building Permits: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped

    def _promote_from_staging(self, permit_number: str) -> None:
        """Remove a staged permit once it has a real building_permits row.

        Deletes the buyer_entity_link pointing at the staging row first (the
        principal will re-resolve against the building_permits row on the next
        resolver pass), then the staging row itself. Idempotent — a no-op when
        the permit was never staged.
        """
        staging = self.session.execute(
            text("SELECT id FROM permit_staging WHERE permit_number = :pn"),
            {"pn": permit_number},
        ).fetchone()
        if staging is None:
            return
        self.session.execute(
            text("DELETE FROM buyer_entity_links "
                 "WHERE source_table = 'permit_staging' AND source_id = :sid"),
            {"sid": staging.id},
        )
        self.session.execute(
            text("DELETE FROM permit_staging WHERE id = :sid"),
            {"sid": staging.id},
        )
        self.session.flush()

    def _persist_to_staging(
        self,
        *,
        permit_number: str,
        permit_type: str | None,
        address: str,
        holder_name: str | None,
        contractor_name: str | None,
        job_value: float | None,
        completion_status: str | None,
        status: str | None,
        description: str | None,
        issue_date,
        expire_date,
        is_enforcement_permit: bool = False,
    ) -> None:
        """Upsert an unmatched permit into permit_staging."""
        self.session.execute(
            text("""
                INSERT INTO permit_staging (
                    permit_number, permit_type, county_id, address,
                    holder_name, contractor_name, job_value,
                    completion_status, status, description,
                    issue_date, expire_date, is_enforcement_permit, date_added, matched
                ) VALUES (
                    :permit_number, :permit_type, :county_id, :address,
                    :holder_name, :contractor_name, :job_value,
                    :completion_status, :status, :description,
                    :issue_date, :expire_date, :is_enforcement_permit, CURRENT_DATE, FALSE
                )
                ON CONFLICT (permit_number) DO UPDATE SET
                    status                = EXCLUDED.status,
                    completion_status     = EXCLUDED.completion_status,
                    is_enforcement_permit = EXCLUDED.is_enforcement_permit,
                    holder_name           = COALESCE(EXCLUDED.holder_name, permit_staging.holder_name),
                    contractor_name       = COALESCE(EXCLUDED.contractor_name, permit_staging.contractor_name),
                    job_value             = COALESCE(EXCLUDED.job_value, permit_staging.job_value),
                    description           = COALESCE(EXCLUDED.description, permit_staging.description)
            """),
            {
                "permit_number": permit_number,
                "permit_type": permit_type,
                "county_id": self.county_id,
                "address": address,
                "holder_name": holder_name,
                "contractor_name": contractor_name,
                "job_value": job_value,
                "completion_status": completion_status,
                "status": status,
                "description": description,
                "issue_date": issue_date,
                "expire_date": expire_date,
                "is_enforcement_permit": is_enforcement_permit,
            },
        )


PermitLoader = BuildingPermitLoader
