"""PropertyRadar staging writer.

Upserts normalized PropertyRadar records (§3 contract) into
property_radar_records. Change detection flags ownership and loan transitions
so downstream suppression can remove in-flight outreach when the underlying
situation changes.

Design: records are deduplicated by (state_fips, county_fips, apn).
radar_id is also unique; a collision on a different row is a data error —
we log and skip rather than overwrite.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.property_radar import OWNER_ABBREVIATIONS

logger = logging.getLogger(__name__)

# ── owner normalization ──────────────────────────────────────────────────────

_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def normalize_owner(name: str | None) -> str:
    """Upper-case, strip punctuation, collapse spaces, expand PR abbreviations."""
    if not name:
        return ""
    s = _PUNCT_RE.sub(" ", name.upper())
    s = _SPACE_RE.sub(" ", s).strip()
    return " ".join(OWNER_ABBREVIATIONS.get(w, w) for w in s.split())


# ── change detection ─────────────────────────────────────────────────────────

def _loan_changed(incoming: dict[str, Any], existing: dict[str, Any]) -> bool:
    """True when the loan appears to have changed since last staging."""
    new_doc = incoming.get("loan_doc_number") or ""
    old_doc = existing.get("loan_doc_number") or ""
    if new_doc and old_doc:
        return new_doc != old_doc
    # Fallback: lender name + recorded date together
    new_pair = (incoming.get("lender_name") or "", incoming.get("loan_recorded_date") or "")
    old_pair = (existing.get("lender_name") or "", existing.get("loan_recorded_date") or "")
    if any(new_pair) and any(old_pair):
        return new_pair != old_pair
    return False


# ── SQL ─────────────────────────────────────────────────────────────────────

_FETCH_EXISTING_SQL = """
    SELECT id, radar_id, owner_name, lender_name, loan_recorded_date,
           loan_doc_number, status, state_fips, county_fips, apn
    FROM property_radar_records
    WHERE (state_fips || '|' || county_fips || '|' || apn) = ANY(:keys)
"""

_FETCH_RADAR_IDS_SQL = """
    SELECT radar_id, state_fips, county_fips, apn
    FROM property_radar_records
    WHERE radar_id = ANY(:ids)
"""

_INSERT_SQL = """
    INSERT INTO property_radar_records (
        radar_id, state_fips, county_fips, apn,
        state, county_name,
        property_address, city, zip, property_type,
        owner_name, ownership_type,
        mailing_address, mailing_city, mailing_state, mailing_zip,
        principal_name,
        lender_name, loan_amount, loan_recorded_date,
        loan_term_years, est_maturity_date, loan_doc_number,
        campaign, status, raw,
        first_seen_at, last_seen_at
    ) VALUES (
        :radar_id, :state_fips, :county_fips, :apn,
        :state, :county_name,
        :property_address, :city, :zip, :property_type,
        :owner_name, :ownership_type,
        :mailing_address, :mailing_city, :mailing_state, :mailing_zip,
        :principal_name,
        :lender_name, :loan_amount, :loan_recorded_date,
        :loan_term_years, :est_maturity_date, :loan_doc_number,
        :campaign, 'active', :raw,
        now(), now()
    )
"""

_UPDATE_SQL = """
    UPDATE property_radar_records SET
        radar_id           = :radar_id,
        state              = :state,
        county_name        = :county_name,
        property_address   = :property_address,
        city               = :city,
        zip                = :zip,
        property_type      = :property_type,
        owner_name         = :owner_name,
        ownership_type     = :ownership_type,
        mailing_address    = :mailing_address,
        mailing_city       = :mailing_city,
        mailing_state      = :mailing_state,
        mailing_zip        = :mailing_zip,
        principal_name     = :principal_name,
        lender_name        = :lender_name,
        loan_amount        = :loan_amount,
        loan_recorded_date = :loan_recorded_date,
        loan_term_years    = :loan_term_years,
        est_maturity_date  = :est_maturity_date,
        loan_doc_number    = :loan_doc_number,
        campaign           = :campaign,
        status             = :status,
        change_flags       = :change_flags,
        changed_at         = :changed_at,
        prior              = :prior,
        raw                = :raw,
        last_seen_at       = now()
    WHERE state_fips = :state_fips
      AND county_fips = :county_fips
      AND apn = :apn
"""


# ── public API ───────────────────────────────────────────────────────────────

def upsert_records(
    session: Session,
    records: list[dict[str, Any]],
) -> tuple[int, int, int]:
    """Upsert a batch of §3-contract PropertyRadar records.

    Returns (inserted, updated, skipped). Skipped means radar_id collision on
    a different (state_fips, county_fips, apn) row — logged, not raised.
    """
    if not records:
        return 0, 0, 0

    inserted = updated = skipped = 0
    now = datetime.now(tz=timezone.utc)

    # Batch-fetch existing rows by dedup key
    key_strs = list({
        f"{r['state_fips']}|{r['county_fips']}|{r['apn']}"
        for r in records
    })
    rows = session.execute(text(_FETCH_EXISTING_SQL), {"keys": key_strs}).mappings().all()
    existing_by_key: dict[tuple[str, str, str], dict[str, Any]] = {
        (row["state_fips"], row["county_fips"], row["apn"]): dict(row)
        for row in rows
    }

    # Batch-fetch radar_id → row key for collision detection
    all_radar_ids = list({r["radar_id"] for r in records})
    rid_rows = session.execute(
        text(_FETCH_RADAR_IDS_SQL), {"ids": all_radar_ids}
    ).mappings().all()
    radar_id_to_key: dict[str, tuple[str, str, str]] = {
        row["radar_id"]: (row["state_fips"], row["county_fips"], row["apn"])
        for row in rid_rows
    }

    for record in records:
        key = (record["state_fips"], record["county_fips"], record["apn"])
        existing = existing_by_key.get(key)
        incoming_rid = record["radar_id"]

        if existing is None:
            if incoming_rid in radar_id_to_key and radar_id_to_key[incoming_rid] != key:
                logger.warning(
                    "radar_id %s already on row %s, skipping insert for %s",
                    incoming_rid, radar_id_to_key[incoming_rid], key,
                )
                skipped += 1
                continue
            session.execute(text(_INSERT_SQL), _base_params(record))
            inserted += 1

        else:
            existing_rid = existing["radar_id"]
            if incoming_rid != existing_rid:
                collision_key = radar_id_to_key.get(incoming_rid)
                if collision_key and collision_key != key:
                    logger.warning(
                        "radar_id %s collision: exists on %s, cannot reassign to %s",
                        incoming_rid, collision_key, key,
                    )
                    skipped += 1
                    continue

            change_flags: list[str] = []
            prior: dict[str, Any] = {}
            status: str = existing["status"]

            if normalize_owner(record.get("owner_name")) != normalize_owner(existing.get("owner_name")):
                change_flags.append("owner_changed")
                prior["owner_name"] = existing["owner_name"]
                status = "sold"

            if _loan_changed(record, existing):
                change_flags.append("loan_changed")
                prior["lender_name"] = existing.get("lender_name")
                prior["loan_recorded_date"] = existing.get("loan_recorded_date")
                prior["loan_doc_number"] = existing.get("loan_doc_number")
                if "owner_changed" not in change_flags:
                    status = "refinanced"

            params = _base_params(record)
            params["status"] = status
            params["change_flags"] = change_flags or None
            params["prior"] = prior or None
            params["changed_at"] = now if change_flags else None
            session.execute(text(_UPDATE_SQL), params)
            updated += 1

    session.flush()
    logger.info(
        "PropertyRadar upsert: inserted=%d updated=%d skipped=%d",
        inserted, updated, skipped,
    )
    return inserted, updated, skipped


def _base_params(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "radar_id": r["radar_id"],
        "state_fips": r["state_fips"],
        "county_fips": r["county_fips"],
        "apn": r["apn"],
        "state": r.get("state"),
        "county_name": r.get("county_name"),
        "property_address": r.get("property_address"),
        "city": r.get("city"),
        "zip": r.get("zip"),
        "property_type": r.get("property_type"),
        "owner_name": r.get("owner_name"),
        "ownership_type": r.get("ownership_type"),
        "mailing_address": r.get("mailing_address"),
        "mailing_city": r.get("mailing_city"),
        "mailing_state": r.get("mailing_state"),
        "mailing_zip": r.get("mailing_zip"),
        "principal_name": r.get("principal_name"),
        "lender_name": r.get("lender_name"),
        "loan_amount": r.get("loan_amount"),
        "loan_recorded_date": r.get("loan_recorded_date"),
        "loan_term_years": r.get("loan_term_years"),
        "est_maturity_date": r.get("est_maturity_date"),
        "loan_doc_number": r.get("loan_doc_number"),
        "campaign": r.get("campaign"),
        "raw": r.get("raw"),
    }
