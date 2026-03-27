"""
Code violation loader.
"""

import logging
from typing import Optional, Tuple

import pandas as pd

from src.loaders.base import BaseLoader
from src.core.models import CodeViolation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Severity classifier
# ---------------------------------------------------------------------------
# Priority order: Critical → Major → Minor.
# Rules are evaluated top-to-bottom; first match wins.
# Fields used: violation_type, description, fine_amount, is_lien, status.

_CRITICAL_TYPE_KEYWORDS = [
    "structural condemnation",
    "condemnation",
    "unsafe structure",
    "demolition order",
    "imminent danger",
    "emergency order",
    "fire damage",
    "flood damage",
]

_CRITICAL_DESC_KEYWORDS = [
    "unsafe",
    "condemned",
    "imminent",
    "emergency",
    "structural failure",
    "collapse",
    "uninhabitable",
]

_MAJOR_TYPE_KEYWORDS = [
    "citizen board",
    "enforcement complaint",
    "water enforcement complaint",
    "housing inspection",
    "generalized housing",
    "illegal",
    "unpermitted",
    "zoning violation",
    "commercial enforcement",
]

_MAJOR_DESC_KEYWORDS = [
    "unpermitted",
    "illegal",
    "sewage",
    "raw waste",
    "hazardous",
    "mold",
    "infestation",
    "electrical",
    "plumbing",
    "no permit",
]


def classify_severity(
    violation_type: Optional[str],
    description: Optional[str],
    fine_amount: Optional[float],
    is_lien: Optional[bool],
    status: Optional[str],
) -> str:
    """
    Classify a code violation into Critical / Major / Minor.

    Rules (first match wins):
      Critical — structural/condemnation types, unsafe/collapse keywords in
                 description, lien with fine ≥ $5,000, or status indicates
                 condemned/emergency.
      Major    — complaint-driven or board-referred types, hazardous material
                 keywords, lien with fine ≥ $1,000, or status is escalated.
      Minor    — everything else (proactive inspections, routine notices).
    """
    vtype = (violation_type or "").lower().strip()
    desc = (description or "").lower().strip()
    status_lower = (status or "").lower().strip()
    fine = fine_amount or 0.0
    lien = bool(is_lien)

    # --- Critical ---
    if any(kw in vtype for kw in _CRITICAL_TYPE_KEYWORDS):
        return "Critical"
    if any(kw in desc for kw in _CRITICAL_DESC_KEYWORDS):
        return "Critical"
    if lien and fine >= 5000:
        return "Critical"
    if any(word in status_lower for word in ("condemned", "emergency", "imminent")):
        return "Critical"

    # --- Major ---
    if any(kw in vtype for kw in _MAJOR_TYPE_KEYWORDS):
        return "Major"
    if any(kw in desc for kw in _MAJOR_DESC_KEYWORDS):
        return "Major"
    if lien and fine >= 1000:
        return "Major"
    if any(word in status_lower for word in ("hearing", "board", "escalated", "non-compliant")):
        return "Major"

    # --- Minor (default) ---
    return "Minor"


class ViolationLoader(BaseLoader):
    """Loader for code enforcement violations."""
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load violations from DataFrame.
        
        Args:
            df: DataFrame with columns: Record Number, Address, Status, etc.
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        # Drop intra-batch duplicates by record_number before processing.
        # The DB check_duplicate only queries committed rows, so two rows with
        # the same record_number in the same batch would both pass and cause a
        # UniqueViolation at commit time.
        before = len(df)
        df = df.drop_duplicates(subset=['Record Number'], keep='first')
        if len(df) < before:
            logger.warning(f"Dropped {before - len(df)} duplicate Record Number(s) within batch")

        logger.info(f"Loading {len(df)} violations")

        matched = 0
        unmatched = 0
        skipped = 0
        
        for _, row in df.iterrows():
            record_number = str(row['Record Number']).strip()
            
            # Check for duplicates
            if skip_duplicates and self.check_duplicate(CodeViolation, {'record_number': record_number}):
                logger.debug(f"Skipping duplicate violation: {record_number}")
                skipped += 1
                continue
            
            # Match by address
            property_record = None
            if pd.notna(row.get('Address')):
                match_result = self.find_property_by_address(row['Address'])
                if match_result:
                    property_record, score = match_result
                    logger.info(f"Matched violation by address (score: {score}%): {record_number}")
            
            if property_record:
                try:
                    # Map CSV columns to database fields:
                    # CSV 'Record Number' → record_number
                    # CSV 'Record Type' → violation_type  
                    # CSV 'Description' → description
                    # CSV 'Date' → opened_date
                    # CSV 'Status' → status
                    
                    # Handle NaN values from pandas - convert to None
                    description_val = row.get('Description')
                    if pd.isna(description_val):
                        description_val = None
                    
                    violation_type_val = row.get('Record Type')
                    if pd.isna(violation_type_val):
                        violation_type_val = None
                    
                    status_val = row.get('Status')
                    if pd.isna(status_val):
                        status_val = None
                    
                    fine_amount_val = None
                    raw_fine = row.get('Fine Amount')
                    if raw_fine is not None and not (isinstance(raw_fine, float) and pd.isna(raw_fine)):
                        try:
                            fine_amount_val = float(str(raw_fine).replace('$', '').replace(',', '').strip())
                        except (ValueError, TypeError):
                            fine_amount_val = None

                    is_lien_val = False
                    raw_lien = row.get('Is Lien')
                    if raw_lien is not None and not (isinstance(raw_lien, float) and pd.isna(raw_lien)):
                        is_lien_val = str(raw_lien).strip().lower() in ('true', 'yes', '1')

                    severity = classify_severity(
                        violation_type=violation_type_val,
                        description=description_val,
                        fine_amount=fine_amount_val,
                        is_lien=is_lien_val,
                        status=status_val,
                    )

                    violation_record = CodeViolation(
                        property_id=property_record.id,
                        record_number=record_number,
                        violation_type=violation_type_val,
                        description=description_val,
                        opened_date=self.parse_date(row.get('Date')),
                        status=status_val,
                        severity_tier=severity,
                        fine_amount=fine_amount_val,
                        is_lien=is_lien_val,
                    )
                    
                    if self.safe_add(violation_record):
                        matched += 1
                    else:
                        unmatched += 1

                except Exception as e:
                    logger.error(f"Error building violation {record_number}: {e}")
                    unmatched += 1
            else:
                logger.warning(f"No property match for violation: {record_number} at {row.get('Address')}")
                self.quarantine_unmatched(
                    source_type="violations",
                    raw_row=row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                    address_string=str(row.get('Address', '')),
                    instrument_number=str(record_number),
                )
                unmatched += 1
        
        logger.info(f"Violations: {matched} matched, {unmatched} unmatched, {skipped} skipped")
        return matched, unmatched, skipped
