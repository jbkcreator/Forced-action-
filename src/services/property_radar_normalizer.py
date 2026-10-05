"""PropertyRadar record → shared contract normalizer.

Converts a raw PropertyRadarRecord (as returned by the port's purchase() method)
into a typed PropertyRadarNormalized dataclass that the rest of the pipeline
(Dev 2 staging, Dev 3/4 matching) consumes.

Key decisions encoded here (all from task-analysis plan, not invented here):

  1. LONG-TERM EXCLUSION (20–30 year): FirstTermInYears is export-only (not a
     filterable API criterion — confirmed by criteria.json inspection). Records
     where FirstTermInYears parses as a number ≥ 20 are excluded here.
     Unknown/blank terms are kept (conservative — we don't penalise missing
     data on this filter; a record that doesn't declare its term is probably
     a short-term loan since conventional lenders almost always populate it).

  2. est_maturity_date: computed from FirstDate + FirstTermInYears when both
     are present and numeric. Null when either is missing or non-numeric
     (~44% of records per sample data). Dev 3/4 confirmed this is acceptable
     (open question #3 deferred; this null-ok decision is the safe default).

  3. loan_doc_number: always null — requires the transactions endpoint which
     is out of Dev 1's scope. Populated downstream if needed.

  4. principal_name: extracted from Persons[] where OwnershipRole=="Principal"
     AND PersonType=="Person". Null when no such person exists (~65% of
     records per sample). The API's company-person split means LLC-held
     properties often have only a Company-type Principal.

  5. county: taken from the raw County field (already uppercased by PR API).
     FIPS is looked up from config/property_radar_fips.py.

  6. mailing_address/city/state/zip: taken from PropertyRadar's OwnerAddress/
     OwnerCity/OwnerState/OwnerZipFive — this is the owner's mailing address,
     not necessarily the property's own address (isSameMailing distinguishes
     the two upstream but is not itself carried through here).

Field names below match src/services/property_radar/staging.py's _COLUMNS
exactly (property_address, zip, lender_name, loan_recorded_date, etc.) —
this was previously misaligned (address/zip_code/lender_original/loan_date)
and every renamed field silently landed as NULL in property_radar_records
once Dev 2's upsert path was actually wired up. Do not rename without
checking that module's _COLUMN_TYPES map first.

COMPLIANCE BOUNDARY: no borrower financial data (credit score, income, bank
statement, tax return, SSN) is stored or passed through this normalizer.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

from config.property_radar_fips import county_fips, state_fips as _state_fips_lookup
from src.services.property_radar_port import PropertyRadarRecord

logger = logging.getLogger(__name__)

# Long-term exclusion threshold — loans with term ≥ this many years are filtered
# out because they are conventional mortgages, not hard-money bridge loans.
_LONG_TERM_EXCLUSION_YEARS = 20


@dataclass
class PropertyRadarNormalized:
    """Shared contract record — the output of the Dev 1 pipeline stage.

    All fields are snake_case.  Nullable fields are typed Optional[...] and
    may be None (see module docstring for which ones and why).

    Open question #3 (null acceptability for est_maturity_date and
    loan_doc_number): deferred to Wednesday cross-developer integration
    meeting. This record is the source of truth for that decision; if the
    answer changes, only this dataclass and its consumers need updating.
    """

    # Field names below match src/services/property_radar/staging.py's
    # _COLUMNS exactly (Dev 2's §3 contract) — do not rename without
    # checking that module's _COLUMN_TYPES map.
    radar_id: str
    state_fips: str
    county_fips: str
    apn: str
    state: str
    county_name: str
    property_address: Optional[str]
    city: Optional[str]
    zip: Optional[str]
    property_type: Optional[str]
    owner_name: Optional[str]
    ownership_type: Optional[str]
    lender_name: Optional[str]
    loan_recorded_date: Optional[date]
    loan_amount: Optional[int]
    loan_term_years: Optional[int]       # None when "Unknown" or absent
    est_maturity_date: Optional[date]    # None when term or date is absent/unknown
    raw: dict                            # original PropertyRadar record, verbatim
    mailing_address: Optional[str] = None
    mailing_city: Optional[str] = None
    mailing_state: Optional[str] = None
    mailing_zip: Optional[str] = None
    loan_doc_number: None = None         # Always None — out of Dev 1 scope
    principal_name: Optional[str] = None
    campaign: str = ""


def normalize(
    record: PropertyRadarRecord,
    *,
    state: str,
    campaign: str,
) -> Optional[PropertyRadarNormalized]:
    """Normalise one raw PropertyRadar record into the shared contract.

    Returns None when the record is excluded by a hard filter (long-term
    exclusion) OR when any of the required fields Dev 2's storage layer
    depends on (radar_id, state_fips, county_fips, apn, state, county_name)
    can't be resolved — Dev 2's contract: "Records missing any of them are
    skipped." Callers should log/count exclusions but never treat None as an
    error — it is a normal outcome for a share of records based on sample data.
    """
    reason = exclusion_reason(record, state=state)
    if reason is not None:
        logger.debug("PropertyRadar %s excluded: %s", record.radar_id, reason)
        return None

    raw = record.raw
    state_upper = state.upper()
    county_raw = (raw.get("County") or "").strip().upper()
    fips = county_fips(state_upper, county_raw)
    st_fips = _state_fips_lookup(state_upper)
    apn = raw.get("APN") or None
    term_years = _parse_term(raw.get("FirstTermInYears"))
    loan_date = _parse_date(raw.get("FirstDate"))
    est_maturity = _compute_maturity(loan_date, term_years)
    principal_name = _extract_principal(raw.get("Persons") or [])

    return PropertyRadarNormalized(
        radar_id=record.radar_id,
        state_fips=st_fips,
        county_fips=fips,
        apn=apn,
        state=state_upper,
        county_name=county_raw,
        property_address=raw.get("Address") or None,
        city=raw.get("City") or None,
        zip=raw.get("ZipFive") or None,
        property_type=raw.get("PType") or None,
        owner_name=raw.get("Owner") or None,
        ownership_type=raw.get("OwnershipType") or None,
        mailing_address=raw.get("OwnerAddress") or None,
        mailing_city=raw.get("OwnerCity") or None,
        mailing_state=raw.get("OwnerState") or None,
        mailing_zip=raw.get("OwnerZipFive") or None,
        lender_name=raw.get("FirstLenderOriginal") or None,
        loan_recorded_date=loan_date,
        loan_amount=_parse_int(raw.get("FirstAmount")),
        loan_term_years=term_years,
        est_maturity_date=est_maturity,
        principal_name=principal_name,
        campaign=campaign,
        raw=raw,
    )


EXCLUDED_MISSING_RADAR_ID = "missing_radar_id"
EXCLUDED_UNKNOWN_STATE = "unknown_state"
EXCLUDED_MISSING_COUNTY = "missing_county"
EXCLUDED_UNMAPPED_COUNTY = "unmapped_county"
EXCLUDED_MISSING_APN = "missing_apn"
EXCLUDED_LONG_TERM = "long_term_loan"


def exclusion_reason(record: PropertyRadarRecord, *, state: str) -> Optional[str]:
    """Why normalize() would drop this record, or None when it is kept."""
    raw = record.raw
    state_upper = state.upper()
    county_raw = (raw.get("County") or "").strip().upper()
    if not record.radar_id:
        return EXCLUDED_MISSING_RADAR_ID
    if not state_upper or not _state_fips_lookup(state_upper):
        return EXCLUDED_UNKNOWN_STATE
    if not county_raw:
        return EXCLUDED_MISSING_COUNTY
    if not county_fips(state_upper, county_raw):
        return EXCLUDED_UNMAPPED_COUNTY
    if not raw.get("APN"):
        return EXCLUDED_MISSING_APN
    term_years = _parse_term(raw.get("FirstTermInYears"))
    if term_years is not None and term_years >= _LONG_TERM_EXCLUSION_YEARS:
        return EXCLUDED_LONG_TERM
    return None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _parse_term(value: object) -> Optional[int]:
    """Parse FirstTermInYears — may be int, float, str like "30", or "Unknown"."""
    if value is None:
        return None
    try:
        parsed = float(str(value))
        return int(parsed)
    except (ValueError, TypeError):
        return None


def _parse_date(value: object) -> Optional[date]:
    """Parse ISO date strings ("2025-12-09") as returned by the PropertyRadar API."""
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None


def _parse_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _compute_maturity(loan_date: Optional[date], term_years: Optional[int]) -> Optional[date]:
    """Estimate the loan maturity date. Null when either input is absent."""
    if loan_date is None or term_years is None:
        return None
    try:
        return loan_date.replace(year=loan_date.year + term_years)
    except ValueError:
        # e.g. Feb 29 in a non-leap-year — shift to Mar 1
        return loan_date.replace(year=loan_date.year + term_years, day=28)


def _extract_principal(persons: list[dict]) -> Optional[str]:
    """Return the first-listed Principal/Person's full name, or None.

    Persons[] entry qualifies when OwnershipRole=="Principal" AND
    PersonType=="Person" (i.e. a human, not a company entity). ~35% hit rate
    per sample data — LLC-held properties often have no Person-type Principal.
    """
    for person in persons:
        if (
            person.get("OwnershipRole") == "Principal"
            and person.get("PersonType") == "Person"
        ):
            parts = [
                person.get("FirstName"),
                person.get("MiddleName"),
                person.get("LastName"),
            ]
            name = " ".join(p for p in parts if p)
            return name or None
    return None
