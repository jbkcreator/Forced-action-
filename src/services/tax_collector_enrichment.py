"""
Tax-collector enrichment post-processing step (ADR 0014).

Called after a tax-delinquency upload (both Hillsborough and Pinellas).
Operates on the full upload batch — including current-roll-year rows that
TaxDelinquencyLoader skips.

Responsibilities:
  1. Normalize billing address from the upload vs owners.mailing_address.
  2. Where they differ (or owner mailing is NULL), upsert enriched_contacts
     with source='tax_collector'.
  3. Classify absentee status (Out-of-State / Out-of-County / In-County)
     from billing address; write owners.absentee_status if NULL or billing
     address normalizes differently from the stored appraiser value.
     Never downgrade existing status to NULL.
  4. Set owners.direct_mail_eligible = true when a usable mailing address
     exists but skip-trace hasn't produced a phone (handled by direct_mail
     resolver — this module only flags the billing address as available).
  5. Trigger CDS rescore for changed property_ids.
"""

import logging
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import Integer, String, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, insert as pg_insert
from sqlalchemy.orm import Session

from src.core.models import EnrichedContact, Owner
from src.loaders.base import BaseLoader
from src.loaders.tax import TaxDelinquencyLoader
from src.utils.address_normalize import normalize_street_address

logger = logging.getLogger(__name__)

# ZIP codes that belong to each county — used for Out-of-County classification.
# Loaded lazily from the DB (properties table).
_COUNTY_ZIPS_CACHE: dict[str, set[str]] = {}


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
    """Return 'Out-of-State', 'Out-of-County', 'In-County', or None."""
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


def _extract_billing_fields(
    row: pd.Series,
    loader: TaxDelinquencyLoader,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (normalized_billing_addr, state, zip5) from a tax row."""
    raw_addr = loader._first_value(row, "owner_address") if "owner_address" in loader.FIELD_ALIASES else None
    if not raw_addr:
        return None, None, None

    # Try to parse state/zip from the address — common format: "123 Main St, Tampa FL 33601"
    state: Optional[str] = None
    zip5: Optional[str] = None
    import re
    m = re.search(r'\b([A-Z]{2})\s+(\d{5})\b', str(raw_addr).upper())
    if m:
        state = m.group(1)
        zip5 = m.group(2)

    normalized = normalize_street_address(str(raw_addr))
    return normalized or None, state, zip5


class TaxCollectorEnrichment:
    """Post-process a tax-delinquency upload batch for absentee/contact enrichment."""

    def __init__(self, session: Session, county_id: str) -> None:
        self.session = session
        self.county_id = county_id
        self._loader = TaxDelinquencyLoader(session, county_id=county_id)
        self._affected_property_ids: set[int] = set()

    def process_upload(self, df: pd.DataFrame) -> dict:
        """
        Enrich owners and contacts from the tax billing address data.

        Returns counts: {enriched_contacts, absentee_updated, direct_mail_flagged}.
        """
        if df.empty:
            return {"enriched_contacts": 0, "absentee_updated": 0, "direct_mail_flagged": 0}

        logger.info(
            "TaxCollectorEnrichment: processing %d rows for county=%s",
            len(df), self.county_id,
        )

        # Pre-load property IDs for the batch (reuse loader's parcel matching)
        parcel_candidates: set[str] = set()
        prepared: list[tuple] = []
        for _, row in df.iterrows():
            values = self._loader._build_tax_values(row)
            parcel = self._loader._parcel_match_candidate(values)
            if parcel:
                parcel_candidates.add(parcel)
            prepared.append((row, values, parcel))

        exact_map, norm_map = self._loader._preload_property_ids(parcel_candidates)

        # Resolve property_ids for all rows
        property_rows: list[tuple[int, pd.Series, dict]] = []
        for row, values, parcel in prepared:
            prop_id: Optional[int] = None
            if parcel:
                prop_id = exact_map.get(parcel)
                if not prop_id:
                    prop_id = norm_map.get(self._loader.normalize_parcel_id(parcel))
            if prop_id:
                property_rows.append((prop_id, row, values))

        if not property_rows:
            logger.info("TaxCollectorEnrichment: no matched properties in batch")
            return {"enriched_contacts": 0, "absentee_updated": 0, "direct_mail_flagged": 0}

        # Bulk-load current owners for affected property_ids
        prop_ids = list({pid for pid, _, _ in property_rows})
        owner_rows = self.session.execute(
            text("""
                SELECT o.id, o.property_id, o.mailing_address, o.absentee_status,
                       o.skip_trace_success, o.direct_mail_eligible
                FROM owners o
                WHERE o.property_id = ANY(:pids)
            """).bindparams(bindparam("pids", type_=ARRAY(Integer))),
            {"pids": prop_ids},
        ).mappings().all()
        owners_by_prop: dict[int, dict] = {r["property_id"]: dict(r) for r in owner_rows}

        county_zips = _load_county_zips(self.session, self.county_id)

        contacts_to_upsert: list[dict] = []
        owners_to_update: list[dict] = []

        for prop_id, row, values in property_rows:
            owner = owners_by_prop.get(prop_id)
            if not owner:
                continue

            billing_norm, billing_state, billing_zip = _extract_billing_fields(row, self._loader)
            if not billing_norm:
                continue

            owner_mailing = owner.get("mailing_address") or ""
            owner_mailing_norm = normalize_street_address(owner_mailing) or ""

            # Only store enriched contact if billing differs from known mailing
            if billing_norm.lower() != owner_mailing_norm.lower() or not owner_mailing_norm:
                contacts_to_upsert.append({
                    "property_id": prop_id,
                    "county_id": self.county_id,
                    "source": "tax_collector",
                    "mailing_address": billing_norm,
                    "confidence_score": 0.90,
                    "meta_data": {
                        "billing_name": values.get("owner_name"),
                        "tax_year": values.get("tax_year"),
                        "raw_address": values.get("owner_address"),
                    },
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                })
                self._affected_property_ids.add(prop_id)

            # Classify absentee
            new_status = _classify_absentee(billing_state, billing_zip, county_zips)
            current_status = owner.get("absentee_status")

            # Never downgrade existing classification; never write NULL
            status_changed = new_status and new_status != current_status
            if status_changed:
                owners_to_update.append({
                    "owner_id": owner["id"],
                    "absentee_status": new_status,
                    "direct_mail_eligible": True,  # billing address is usable
                })
                self._affected_property_ids.add(prop_id)
            elif not current_status and new_status:
                owners_to_update.append({
                    "owner_id": owner["id"],
                    "absentee_status": new_status,
                    "direct_mail_eligible": True,
                })
                self._affected_property_ids.add(prop_id)

        # Bulk upsert enriched contacts
        if contacts_to_upsert:
            self._upsert_contacts(contacts_to_upsert)

        # Bulk update owners
        if owners_to_update:
            self._update_owners(owners_to_update)

        # Trigger CDS rescore for changed properties
        if self._affected_property_ids:
            self._trigger_rescore()

        result = {
            "enriched_contacts": len(contacts_to_upsert),
            "absentee_updated": len(owners_to_update),
            "direct_mail_flagged": sum(1 for o in owners_to_update if o.get("direct_mail_eligible")),
        }
        logger.info("TaxCollectorEnrichment: %s", result)
        return result

    def _upsert_contacts(self, rows: list[dict]) -> None:
        stmt = pg_insert(EnrichedContact.__table__).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["property_id", "source"],
            set_={
                "mailing_address":  stmt.excluded.mailing_address,
                "confidence_score": stmt.excluded.confidence_score,
                "meta_data":        stmt.excluded.meta_data,
                "updated_at":       text("now()"),
            },
        )
        try:
            self.session.execute(stmt)
            self.session.flush()
        except Exception as e:
            logger.error("TaxCollectorEnrichment: contact upsert failed: %s", e)
            raise

    def _update_owners(self, rows: list[dict]) -> None:
        for rec in rows:
            self.session.execute(
                text("""
                    UPDATE owners
                    SET absentee_status = :status,
                        direct_mail_eligible = :dme
                    WHERE id = :oid
                """),
                {
                    "status": rec["absentee_status"],
                    "dme": rec["direct_mail_eligible"],
                    "oid": rec["owner_id"],
                },
            )
        self.session.flush()

    def _trigger_rescore(self) -> None:
        pid_list = list(self._affected_property_ids)
        try:
            from src.services.cds_engine import rescore_properties
            rescore_properties(pid_list)
            logger.info(
                "TaxCollectorEnrichment: triggered rescore for %d properties",
                len(pid_list),
            )
        except Exception as e:
            logger.warning("TaxCollectorEnrichment: rescore trigger failed: %s", e)
