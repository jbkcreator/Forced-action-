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

    @staticmethod
    def read_voter_dataframe(content: str, nrows: Optional[int] = None) -> pd.DataFrame:
        """Parse raw voter-file text into a DataFrame, auto-detecting the format.

        Three shapes are handled, decided by sniffing the first non-empty line:
          • comma-delimited WITH header — Hillsborough SOE "All Eligible Voters"
            export (note: distributed as a misleadingly-named .txt). 67 quoted
            columns incl. VoterID, Residence_Address, Telephone_Number.
          • tab-delimited WITH header — alternate SOE exports.
          • tab-delimited, NO header — FL DOS statewide extract (38 fixed fields);
            the canonical header is injected.
        """
        import io as _io

        first = next((ln for ln in content.splitlines() if ln.strip()), "")
        commas = first.count(",")
        tabs = first.count("\t")

        if tabs > commas and tabs > 0:
            header_tokens = ("voter", "name", "address", "county", "registration")
            has_header = any(tok in first.lower() for tok in header_tokens)
            if has_header:
                return pd.read_csv(_io.StringIO(content), dtype=str, sep="\t", nrows=nrows)
            df = pd.read_csv(_io.StringIO(content), dtype=str, sep="\t", header=None, nrows=nrows)
            return VoterRegistryLoader.inject_fl_dos_header(df)

        # Comma-delimited with header (Hillsborough SOE). pandas honors quoting,
        # so embedded commas inside quoted fields are safe.
        return pd.read_csv(_io.StringIO(content), dtype=str, nrows=nrows)

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

        # Alias lists cover, in order: canonical (post-ColumnMapper) names,
        # Hillsborough SOE export names (e.g. VoterID, Residence_Address — verified
        # against the real "All Eligible Voters" file), and FL DOS extract names.
        voter_id = _v(
            "source_voter_id", "voter_id", "VoterID", "Voter ID",
            "VOTER_ID", "Voter Registration Number",
        )
        first = _v("first_name", "First_Name", "First Name", "FIRST_NAME") or ""
        middle = _v("middle_name", "Middle_Name", "Middle Name", "MIDDLE_NAME") or ""
        last = _v("last_name", "Last_Name", "Last Name", "LAST_NAME") or ""
        voter_name = (
            _v("voter_name", "Voter_Name")
            or " ".join(filter(None, [first, middle, last]))
            or None
        )

        res_addr = _v(
            "residential_address", "Residence_Address", "Formatted_Address",
            "Residential Address", "residential_address_line_1",
            "Residence Address Line 1",
        )
        res_city = _v("residential_city", "City_Name", "Residence City", "City")
        res_zip = _v(
            "residential_zip", "Zip_Code", "Residence Zip Code",
            "residential_zip5", "Zip",
        )

        mail_line1 = _v(
            "mailing_address", "Mailing_Address_1",
            "mailing_address_line_1", "Mailing Address Line 1",
        )
        mail_city = _v("mailing_city", "Mailing_City", "Mailing City")
        mail_state = _v("mailing_state", "Mailing_State", "Mailing State")
        mail_zip = _v("mailing_zip", "Mailing_zip", "Mailing Zip")
        mailing_full = (
            ", ".join(filter(None, [mail_line1, mail_city, mail_state, mail_zip]))
            if mail_line1
            else None
        )

        status_raw = (
            _v("registration_status", "Voter Status", "voter_status") or ""
        ).lower()
        status = _STATUS_MAP.get(status_raw)

        reg_date_raw = _v(
            "registration_date", "Registration_Date", "Registration Date",
            "REGISTRATION_DATE",
        )
        reg_date: Optional[date] = None
        if reg_date_raw:
            parsed = self.parse_date(reg_date_raw)
            if parsed:
                reg_date = parsed.date() if hasattr(parsed, "date") else parsed

        raw_phone = _v(
            "phone_1", "Telephone_Number", "Phone", "PHONE_NUMBER",
        ) or ""
        phone_normalized = normalize_phone(raw_phone) if raw_phone else None

        email = _v("email", "Public_Email_Address", "Email", "EMAIL_ADDRESS")

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


# ─────────────────────────────────────────────────────────────────────────────
# Set-based bulk path — full-county loads (~1M rows in ~4 min vs ~0.9 s/row).
#
# Streams the Hillsborough SOE CSV in 50k chunks: vectorized extract +
# address normalization in pandas, COPY into a temp table, one SQL join on
# properties.normalized_address (+zip), bulk upsert. Exact-match only (no
# fuzzy); ~82% match rate measured on the real April-2026 file. Per-chunk
# commits make a killed run resumable (upsert is idempotent). Unmatched rows
# are counted, not quarantined (~180k rows would bloat unmatched_records).
# Unlike the per-row path, the upsert refreshes property_id, so voters who
# moved are reassigned to their new property.
# ─────────────────────────────────────────────────────────────────────────────

_BULK_CHUNK = 50_000

_BULK_USECOLS = [
    "VoterID", "Voter_Name", "First_Name", "Middle_Name", "Last_Name",
    "Residence_Address", "City_Name", "Zip_Code",
    "Mailing_Address_1", "Mailing_City", "Mailing_State", "Mailing_zip",
    "Voter Status", "Registration_Date", "Telephone_Number", "Public_Email_Address",
]

_BULK_STAGE_DDL = """
    CREATE TEMP TABLE _voter_stage (
        source_voter_id text, voter_name text, first_name text,
        middle_name text, last_name text, residential_address text,
        residential_city text, residential_zip text, mailing_address text,
        registration_status text, registration_date date,
        phone_1 text, email text, norm_addr text
    ) ON COMMIT DROP
"""

_BULK_STAGE_COLS = [
    "source_voter_id", "voter_name", "first_name", "middle_name", "last_name",
    "residential_address", "residential_city", "residential_zip",
    "mailing_address", "registration_status", "registration_date",
    "phone_1", "email", "norm_addr",
]

_BULK_UPSERT_SQL = """
WITH matched AS (
    SELECT DISTINCT ON (s.source_voter_id)
        s.*, p.id AS property_id
    FROM _voter_stage s
    JOIN properties p
      ON p.county_id = :county
     AND p.normalized_address = s.norm_addr
     AND (s.residential_zip = '' OR p.zip = s.residential_zip)
    ORDER BY s.source_voter_id, p.id
)
INSERT INTO voters (
    property_id, county_id, source_voter_id, voter_name, first_name,
    middle_name, last_name, residential_address, residential_city,
    residential_zip, mailing_address, registration_status,
    registration_date, phones, phone_1, email, created_at
)
SELECT
    m.property_id, :county, m.source_voter_id,
    NULLIF(m.voter_name,''), NULLIF(m.first_name,''), NULLIF(m.middle_name,''),
    NULLIF(m.last_name,''), NULLIF(m.residential_address,''),
    NULLIF(m.residential_city,''), NULLIF(m.residential_zip,''),
    NULLIF(m.mailing_address,''), m.registration_status,
    m.registration_date,
    CASE WHEN m.phone_1 IS NULL THEN '[]'::jsonb
         ELSE jsonb_build_array(m.phone_1) END,
    m.phone_1, NULLIF(m.email,''), now()
FROM matched m
ON CONFLICT ON CONSTRAINT uq_voter_county_source_id DO UPDATE SET
    property_id          = EXCLUDED.property_id,
    voter_name           = EXCLUDED.voter_name,
    first_name           = EXCLUDED.first_name,
    middle_name          = EXCLUDED.middle_name,
    last_name            = EXCLUDED.last_name,
    residential_address  = EXCLUDED.residential_address,
    residential_city     = EXCLUDED.residential_city,
    residential_zip      = EXCLUDED.residential_zip,
    mailing_address      = EXCLUDED.mailing_address,
    registration_status  = EXCLUDED.registration_status,
    registration_date    = EXCLUDED.registration_date,
    phone_1              = EXCLUDED.phone_1,
    phones = CASE
        WHEN EXCLUDED.phone_1 IS NOT NULL
             AND NOT (COALESCE(voters.phones,'[]'::jsonb) @> EXCLUDED.phones)
        THEN COALESCE(voters.phones,'[]'::jsonb) || EXCLUDED.phones
        ELSE COALESCE(voters.phones,'[]'::jsonb) END,
    email      = EXCLUDED.email,
    updated_at = now()
"""


def _bulk_prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized field extraction + address normalization for one CSV chunk."""
    from src.utils.address_normalize import normalize_street_address

    out = pd.DataFrame()
    out["source_voter_id"] = df["VoterID"].astype(str).str.strip()
    out["voter_name"] = df["Voter_Name"].fillna("").str.strip().str.slice(0, 255)
    out["first_name"] = df["First_Name"].fillna("").str.strip().str.slice(0, 100)
    out["middle_name"] = df["Middle_Name"].fillna("").str.strip().str.slice(0, 100)
    out["last_name"] = df["Last_Name"].fillna("").str.strip().str.slice(0, 100)
    out["residential_address"] = df["Residence_Address"].fillna("").str.strip().str.slice(0, 500)
    out["residential_city"] = df["City_Name"].fillna("").str.strip().str.slice(0, 100)
    out["residential_zip"] = df["Zip_Code"].fillna("").astype(str).str.strip().str.slice(0, 5)

    mail = df["Mailing_Address_1"].fillna("").str.strip()
    mc = df["Mailing_City"].fillna("").str.strip()
    ms = df["Mailing_State"].fillna("").str.strip()
    mz = df["Mailing_zip"].fillna("").astype(str).str.strip()
    tail = (mc + ", " + ms + ", " + mz).str.strip(", ").str.replace(r"(, )+", ", ", regex=True)
    out["mailing_address"] = (mail + ", " + tail).str.strip(", ").where(mail != "", "")
    out["mailing_address"] = out["mailing_address"].str.slice(0, 500)

    out["registration_status"] = (
        df["Voter Status"].fillna("").str.strip().str.lower().map(_STATUS_MAP)
    )
    out["registration_date"] = pd.to_datetime(
        df["Registration_Date"], format="%m/%d/%Y", errors="coerce"
    ).dt.date

    out["phone_1"] = df["Telephone_Number"].map(
        lambda v: normalize_phone(str(v)) if pd.notna(v) and str(v).strip() else None
    )
    out["email"] = df["Public_Email_Address"].fillna("").str.strip().str.slice(0, 255)

    out["norm_addr"] = out["residential_address"].map(
        lambda a: normalize_street_address(a) if a else ""
    )
    out = out[(out["source_voter_id"] != "") & (out["norm_addr"] != "")]
    return out.drop_duplicates(subset=["source_voter_id"], keep="last")


def _bulk_load_one_chunk(db, prep: pd.DataFrame, county_id: str) -> int:
    """Stage + upsert one prepared chunk in its own committed transaction."""
    import io as _io

    with db.session_scope() as session:
        conn = session.connection()
        conn.execute(text(_BULK_STAGE_DDL))
        buf = _io.StringIO()
        prep[_BULK_STAGE_COLS].to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur = conn.connection.cursor()
        cur.copy_expert(
            "COPY _voter_stage FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
            buf,
        )
        res = conn.execute(text(_BULK_UPSERT_SQL), {"county": county_id})
        return res.rowcount


def bulk_load_voters_csv(
    fh,
    county_id: str = "hillsborough",
    limit: Optional[int] = None,
) -> Tuple[int, int, int]:
    """Bulk-load a Hillsborough-SOE-format voter CSV stream into voters.

    Args:
        fh:        File handle (binary or text) of the CSV — e.g.
                   zipfile.ZipFile(...).open('All Eligible Voters.txt').
        county_id: Target county.
        limit:     Optional row cap (testing).

    Returns:
        (rows_read, matched_upserted, unmatched)
    """
    import time as _time

    from src.core.database import Database

    t0 = _time.time()
    db = Database()
    total = staged = upserted = 0

    reader = pd.read_csv(fh, dtype=str, usecols=_BULK_USECOLS, chunksize=_BULK_CHUNK)
    for i, chunk in enumerate(reader):
        if limit and total >= limit:
            break
        if limit:
            chunk = chunk.head(limit - total)
        total += len(chunk)

        prep = _bulk_prepare_chunk(chunk)
        staged += len(prep)
        upserted += _bulk_load_one_chunk(db, prep, county_id)

        logger.info(
            "[VoterBulk] chunk %d: read=%d staged=%d upserted_total=%d elapsed=%.0fs",
            i + 1, total, staged, upserted, _time.time() - t0,
        )

    unmatched = staged - upserted
    logger.info(
        "[VoterBulk] DONE rows_read=%d matched_upserted=%d unmatched=%d elapsed=%.0fs",
        total, upserted, unmatched, _time.time() - t0,
    )
    return total, upserted, unmatched
