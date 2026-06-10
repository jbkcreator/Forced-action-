"""
VoterRegistryLoader — loads Supervisor of Elections bulk voter files.

Supports two formats:
  (a) Hillsborough SOE quoted-CSV with header row (monthly MediaFire drop)
  (b) FL DOS statewide tab-delimited .txt, no header (38-field layout)

Contact-enrichment only. No CDS rescore. Phones isolated from auto-send (ADR 0013).
Upsert key: (county_id, source_voter_id).
"""

import logging
from datetime import date
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.core.models import Voter
from src.loaders.base import BaseLoader
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

# FL DOS 38-field tab-delimited layout (no header in the file).
# Source: Florida DOS Voter Registration layout specification.
_FL_DOS_HEADER = [
    "county_code", "voter_id", "voter_status", "last_name", "name_suffix",
    "first_name", "middle_name", "requested_public_exemption",
    "residential_address_line_1", "residential_address_line_2",
    "residential_city", "residential_state", "residential_zip",
    "residential_country", "mailing_address_line_1", "mailing_address_line_2",
    "mailing_city", "mailing_state", "mailing_zip", "mailing_country",
    "gender", "race", "birth_date", "registration_date",
    "party_affiliation", "precinct", "precinct_group", "precinct_split",
    "precinct_suffix", "voter_status_reason", "congressional_district",
    "house_district", "senate_district", "county_commission_district",
    "school_board_district", "area_1", "area_2", "area_3",
]

_CHUNK_SIZE = 1000

_STATUS_MAP = {
    "a": "ACT", "active": "ACT", "act": "ACT",
    "i": "INA", "inactive": "INA", "ina": "INA",
}


class VoterRegistryLoader(BaseLoader):
    """Load county SOE voter files into the voters spoke table."""

    _LLM_MAX_CALLS: int = 0  # contact-only; LLM tiebreak not needed

    @staticmethod
    def inject_fl_dos_header(df: pd.DataFrame) -> pd.DataFrame:
        """Apply the 38-field FL DOS header to a headerless DataFrame."""
        if len(df.columns) == len(_FL_DOS_HEADER):
            df.columns = pd.Index(_FL_DOS_HEADER)
        else:
            logger.warning(
                "FL DOS header has %d fields but file has %d columns — applying best-effort",
                len(_FL_DOS_HEADER), len(df.columns),
            )
            df.columns = pd.Index(_FL_DOS_HEADER[: len(df.columns)])
        return df

    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True,
    ) -> Tuple[int, int, int]:
        """
        Load voter records into the voters table.

        Returns (inserted, updated, quarantined).
        No rescore — contact-only per ADR 0013.
        """
        logger.info("Loading %d voter rows (county=%s)", len(df), self.county_id)

        inserted = updated = quarantined = 0
        upsert_rows: list[dict] = []
        quarantine_rows: list[dict] = []

        for _, row in df.iterrows():
            record = self._extract_voter_record(row)
            if not record.get("source_voter_id"):
                quarantined += 1
                continue

            address = record.get("residential_address") or ""
            zip_code = record.get("residential_zip") or ""

            result = None
            if address:
                result = self.find_property_by_address(
                    address,
                    threshold=75,
                    zip_code=zip_code or None,
                )

            if result is None:
                quarantine_rows.append({
                    "raw_row": row.to_dict(),
                    "address_string": address,
                    "grantor": record.get("voter_name"),
                })
                quarantined += 1
                continue

            prop, _score = result
            record["property_id"] = prop.id
            upsert_rows.append(record)

            if len(upsert_rows) >= _CHUNK_SIZE:
                ins, upd = self._bulk_upsert(upsert_rows)
                inserted += ins
                updated += upd
                upsert_rows = []

        if upsert_rows:
            ins, upd = self._bulk_upsert(upsert_rows)
            inserted += ins
            updated += upd

        for rec in quarantine_rows:
            self.quarantine_unmatched(
                source_type="voter_registry",
                raw_row=rec["raw_row"],
                address_string=rec.get("address_string"),
                grantor=rec.get("grantor"),
            )

        logger.info(
            "Voter registry: %d inserted, %d updated, %d quarantined",
            inserted, updated, quarantined,
        )
        return inserted, updated, quarantined

    # ── Private helpers ───────────────────────────────────────────────────────

    def _extract_voter_record(self, row: pd.Series) -> dict:
        def _v(*cols: str) -> Optional[str]:
            for col in cols:
                val = row.get(col)
                if val is None:
                    continue
                if isinstance(val, float) and pd.isna(val):
                    continue
                s = str(val).strip()
                if s and s.lower() not in {"nan", "none", "null"}:
                    return s
            return None

        voter_id = _v(
            "source_voter_id", "voter_id", "Voter ID",
            "VOTER_ID", "Voter Registration Number",
        )
        first = _v("first_name", "First Name", "FIRST_NAME") or ""
        middle = _v("middle_name", "Middle Name", "MIDDLE_NAME") or ""
        last = _v("last_name", "Last Name", "LAST_NAME") or ""
        voter_name = _v("voter_name") or " ".join(filter(None, [first, middle, last])) or None

        res_addr = _v(
            "residential_address", "Residential Address",
            "residential_address_line_1", "Residence Address Line 1",
        )
        res_city = _v("residential_city", "Residence City", "City")
        res_zip = _v(
            "residential_zip", "Residence Zip Code",
            "residential_zip5", "Zip",
        )

        mail_line1 = _v("mailing_address", "mailing_address_line_1", "Mailing Address Line 1")
        mail_city = _v("mailing_city", "Mailing City")
        mail_state = _v("mailing_state", "Mailing State")
        mail_zip = _v("mailing_zip", "Mailing Zip")
        mailing_full = (
            ", ".join(filter(None, [mail_line1, mail_city, mail_state, mail_zip]))
            if mail_line1
            else None
        )

        status_raw = (_v("registration_status", "Voter Status", "voter_status") or "").lower()
        status = _STATUS_MAP.get(status_raw)

        reg_date_raw = _v("registration_date", "Registration Date", "REGISTRATION_DATE")
        reg_date: Optional[date] = None
        if reg_date_raw:
            parsed = self.parse_date(reg_date_raw)
            if parsed:
                reg_date = parsed.date() if hasattr(parsed, "date") else parsed

        raw_phone = _v("phone_1", "Phone", "PHONE_NUMBER") or ""
        phone_normalized = normalize_phone(raw_phone) if raw_phone else None

        email = _v("email", "Email", "EMAIL_ADDRESS")

        return {
            "county_id": self.county_id,
            "source_voter_id": voter_id or "",
            "voter_name": voter_name,
            "first_name": first or None,
            "middle_name": middle or None,
            "last_name": last or None,
            "residential_address": res_addr,
            "residential_city": res_city,
            "residential_zip": res_zip,
            "mailing_address": mailing_full,
            "registration_status": status,
            "registration_date": reg_date,
            "phone_1": phone_normalized,
            "phones": [phone_normalized] if phone_normalized else [],
            "email": email,
        }

    def _bulk_upsert(self, rows: list[dict]) -> Tuple[int, int]:
        if not rows:
            return 0, 0

        voter_ids = [r["source_voter_id"] for r in rows]
        existing_phones: dict[str, list] = {}
        for er in self.session.execute(
            text("""
                SELECT source_voter_id, phones FROM voters
                WHERE county_id = :cid AND source_voter_id = ANY(:ids)
            """),
            {"cid": self.county_id, "ids": voter_ids},
        ).mappings():
            existing_phones[er["source_voter_id"]] = list(er["phones"] or [])

        existing_set = set(existing_phones.keys())
        n_inserted = sum(1 for r in rows if r["source_voter_id"] not in existing_set)
        n_updated = len(rows) - n_inserted

        values: list[dict] = []
        for rec in rows:
            vid = rec["source_voter_id"]
            hist = list(existing_phones.get(vid, []))
            new_phone = rec.get("phone_1")
            if new_phone and new_phone not in hist:
                hist.append(new_phone)
            values.append({**rec, "phones": hist})

        stmt = pg_insert(Voter.__table__).values(values)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_voter_county_source_id",
            set_={
                "voter_name":          stmt.excluded.voter_name,
                "first_name":          stmt.excluded.first_name,
                "middle_name":         stmt.excluded.middle_name,
                "last_name":           stmt.excluded.last_name,
                "residential_address": stmt.excluded.residential_address,
                "residential_city":    stmt.excluded.residential_city,
                "residential_zip":     stmt.excluded.residential_zip,
                "mailing_address":     stmt.excluded.mailing_address,
                "registration_status": stmt.excluded.registration_status,
                "registration_date":   stmt.excluded.registration_date,
                "phone_1":             stmt.excluded.phone_1,
                "phones":              stmt.excluded.phones,
                "email":               stmt.excluded.email,
                "updated_at":          text("now()"),
            },
        )
        self.session.execute(stmt)
        self.session.flush()
        return n_inserted, n_updated
