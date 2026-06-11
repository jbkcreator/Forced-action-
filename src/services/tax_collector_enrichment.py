"""
Tax-collector enrichment post-processing step (ADR 0014).

Called after a tax-delinquency upload (both Hillsborough and Pinellas).
Operates on the full upload batch — including current-roll-year rows that
TaxDelinquencyLoader skips.

Responsibilities:
  1. Normalize billing address from the upload vs owners.mailing_address.
  2. Where they differ (or owner mailing is NULL), upsert enriched_contacts
     with source='tax_collector' (mailing_address only — never a phone/email).
  3. Classify absentee status (Out-of-State / Out-of-County / In-County)
     from billing address; write owners.absentee_status when NULL or when the
     billing address normalizes differently from the stored value. Never
     downgrade an existing status to NULL.
  4. Trigger CDS rescore for changed property_ids.

Note: owners.direct_mail_eligible is intentionally NOT set here — that flag is
owned by the skip-trace waterfall MISS hook (src/services/direct_mail.py),
which only flags owners with no resolvable phone (ADR 0013 / Phase 6).
enriched_contacts has no unique constraint on (property_id, source), so this
module does a manual select-then-insert/update keyed on property_id.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import Integer, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, insert as pg_insert
from sqlalchemy.orm import Session

from src.core.models import EnrichedContact
from src.loaders.tax import TaxDelinquencyLoader
from src.utils.address_normalize import normalize_street_address

logger = logging.getLogger(__name__)

# Per-county ZIP sets (from properties) — used for Out-of-County classification.
_COUNTY_ZIPS_CACHE: dict[str, set[str]] = {}

_STATE_ZIP_RE = re.compile(r"\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b")


def _load_county_zips(session: Session, county_id: str) -> set[str]:
    if county_id not in _COUNTY_ZIPS_CACHE:
        rows = session.execute(
            text("SELECT DISTINCT zip FROM properties WHERE county_id = :cid AND zip IS NOT NULL"),
            {"cid": county_id},
        ).scalars().all()
        _COUNTY_ZIPS_CACHE[county_id] = {str(z).strip()[:5] for z in rows if z}
    return _COUNTY_ZIPS_CACHE[county_id]


def _classify_absentee(
    billing_state: Optional[str],
    billing_zip: Optional[str],
    county_zips: set[str],
) -> Optional[str]:
    """Return 'Out-of-State', 'Out-of-County', 'In-County', or None.

    Values match the owners.check_absentee_status CHECK constraint exactly.
    """
    if not billing_state:
        return None
    state = billing_state.strip().upper()
    if state not in ("FL", "FLORIDA"):
        return "Out-of-State"
    if billing_zip:
        zip5 = str(billing_zip).strip()[:5]
        if zip5 and zip5 not in county_zips:
            return "Out-of-County"
    return "In-County"


def _comparison_key(normalized: Optional[str], raw: Optional[str]) -> str:
    """Address-equality key: normalized street if available, else collapsed raw.

    PO-box / non-street addresses normalize to empty, so fall back to a
    whitespace-collapsed, lowercased form of the raw string for comparison.
    """
    if normalized:
        return normalized.strip().lower()
    if raw:
        return re.sub(r"\s+", " ", str(raw)).strip().lower()
    return ""


def _extract_billing_fields(
    raw_addr: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (normalized_billing_addr, state, zip5) parsed from a billing address."""
    if not raw_addr:
        return None, None, None
    state: Optional[str] = None
    zip5: Optional[str] = None
    m = _STATE_ZIP_RE.search(str(raw_addr).upper())
    if m:
        state = m.group(1)
        zip5 = m.group(2)
    normalized = normalize_street_address(str(raw_addr))
    return (normalized or None), state, zip5


class TaxCollectorEnrichment:
    """Post-process a tax-delinquency upload batch for absentee/contact enrichment."""

    def __init__(self, session: Session, county_id: str) -> None:
        self.session = session
        self.county_id = county_id
        self._loader = TaxDelinquencyLoader(session, county_id=county_id)
        self._affected_property_ids: set[int] = set()

    def process_upload(self, df: pd.DataFrame) -> dict:
        """
        Enrich owners + contacts from tax billing addresses.

        Returns {enriched_contacts, absentee_updated}.
        """
        empty_result = {"enriched_contacts": 0, "absentee_updated": 0}
        if df is None or df.empty:
            return empty_result

        logger.info(
            "TaxCollectorEnrichment: processing %d rows for county=%s",
            len(df), self.county_id,
        )

        # Resolve property_ids using the tax loader's parcel pre-load
        parcel_candidates: set[str] = set()
        prepared: list[tuple] = []
        for _, row in df.iterrows():
            values = self._loader._build_tax_values(row)
            parcel = self._loader._parcel_match_candidate(values)
            if parcel:
                parcel_candidates.add(parcel)
            prepared.append((values, parcel))

        exact_map, norm_map = self._loader._preload_property_ids(parcel_candidates)

        # Build per-property billing payload (last row wins per property_id)
        billing_by_prop: dict[int, dict] = {}
        for values, parcel in prepared:
            prop_id: Optional[int] = None
            if parcel:
                prop_id = exact_map.get(parcel)
                if not prop_id:
                    prop_id = norm_map.get(self._loader.normalize_parcel_id(parcel))
            if not prop_id:
                continue
            raw_addr = values.get("owner_address")
            if not raw_addr:
                continue
            # billing_norm may be empty for PO-box / non-street mailing addresses —
            # those are still valid mailing destinations and common for absentee
            # owners, so we keep them. The normalized form is only used to compare
            # against the owner's mailing address.
            billing_norm, state, zip5 = _extract_billing_fields(raw_addr)
            billing_by_prop[prop_id] = {
                "raw_addr": str(raw_addr).strip(),
                "billing_norm": billing_norm,
                "state": state,
                "zip5": zip5,
                "billing_name": values.get("owner_name"),
                "tax_year": values.get("tax_year"),
            }

        if not billing_by_prop:
            logger.info("TaxCollectorEnrichment: no matched properties with billing address")
            return empty_result

        prop_ids = list(billing_by_prop.keys())

        # Bulk-load owners for affected properties
        owner_rows = self.session.execute(
            text("""
                SELECT id, property_id, mailing_address, absentee_status
                FROM owners
                WHERE property_id = ANY(:pids)
            """).bindparams(bindparam("pids", type_=ARRAY(Integer))),
            {"pids": prop_ids},
        ).mappings().all()
        owners_by_prop: dict[int, dict] = {r["property_id"]: dict(r) for r in owner_rows}

        # Pre-load existing tax_collector contacts (no unique constraint → manual upsert)
        existing_contacts: dict[int, int] = {}
        for er in self.session.execute(
            text("""
                SELECT property_id, MAX(id) AS id
                FROM enriched_contacts
                WHERE source = 'tax_collector' AND property_id = ANY(:pids)
                GROUP BY property_id
            """).bindparams(bindparam("pids", type_=ARRAY(Integer))),
            {"pids": prop_ids},
        ).mappings():
            existing_contacts[er["property_id"]] = er["id"]

        county_zips = _load_county_zips(self.session, self.county_id)

        contact_inserts: list[dict] = []
        contact_updates: list[dict] = []
        owner_updates: list[dict] = []

        for prop_id, payload in billing_by_prop.items():
            owner = owners_by_prop.get(prop_id)
            raw_addr = payload["raw_addr"]
            raw_response = {
                "billing_name": payload["billing_name"],
                "tax_year": payload["tax_year"],
                "raw_address": raw_addr,
            }

            owner_mailing = owner.get("mailing_address") if owner else None
            billing_key = _comparison_key(payload["billing_norm"], raw_addr)
            owner_key = (
                _comparison_key(normalize_street_address(owner_mailing), owner_mailing)
                if owner_mailing else ""
            )

            # Store a tax_collector contact when billing differs from owner mailing
            # (or owner mailing is unknown). mailing_address holds the full raw
            # billing line (street/PO box + city/state/zip) for direct mail.
            if not owner_key or billing_key != owner_key:
                stored_addr = raw_addr[:255]
                if prop_id in existing_contacts:
                    contact_updates.append({
                        "id": existing_contacts[prop_id],
                        "mailing_address": stored_addr,
                        "confidence": 0.90,
                        "raw_response": json.dumps(raw_response),
                    })
                else:
                    contact_inserts.append({
                        "property_id": prop_id,
                        "county_id": self.county_id,
                        "source": "tax_collector",
                        "mailing_address": stored_addr,
                        "match_success": True,
                        "confidence": 0.90,
                        "raw_response": raw_response,
                        "enriched_at": datetime.now(timezone.utc),
                    })
                self._affected_property_ids.add(prop_id)

            # Absentee classification (only when we have an owner row)
            if owner:
                new_status = _classify_absentee(payload["state"], payload["zip5"], county_zips)
                current_status = owner.get("absentee_status")
                if new_status and new_status != current_status:
                    owner_updates.append({
                        "oid": owner["id"],
                        "status": new_status,
                    })
                    self._affected_property_ids.add(prop_id)

        if contact_inserts:
            self.session.execute(pg_insert(EnrichedContact.__table__).values(contact_inserts))
            self.session.flush()
        if contact_updates:
            self.session.execute(
                text("""
                    UPDATE enriched_contacts
                    SET mailing_address = :mailing_address,
                        confidence = :confidence,
                        raw_response = CAST(:raw_response AS JSONB),
                        match_success = true,
                        enriched_at = now()
                    WHERE id = :id
                """),
                contact_updates,
            )
            self.session.flush()
        if owner_updates:
            self.session.execute(
                text("UPDATE owners SET absentee_status = :status WHERE id = :oid"),
                owner_updates,
            )
            self.session.flush()

        if self._affected_property_ids:
            self._trigger_rescore()

        result = {
            "enriched_contacts": len(contact_inserts) + len(contact_updates),
            "absentee_updated": len(owner_updates),
        }
        logger.info("TaxCollectorEnrichment: %s", result)
        return result

    def _trigger_rescore(self) -> None:
        pid_list = list(self._affected_property_ids)
        try:
            from src.services.cds_engine import MultiVerticalScorer
            scorer = MultiVerticalScorer(self.session)
            scorer.score_properties_by_ids(
                pid_list, save_to_db=True, county_id=self.county_id
            )
            logger.info(
                "TaxCollectorEnrichment: triggered rescore for %d properties",
                len(pid_list),
            )
        except Exception as e:
            logger.warning("TaxCollectorEnrichment: rescore trigger failed: %s", e)
