"""PropertyRadar staging writer.

Upserts normalized PropertyRadar records (§3 contract) into
property_radar_records. Records never go into `properties`: parcel_id is
globally unique there, but an APN is only unique within a county, so a
statewide/multi-state feed would collide or merge the wrong property. The
future path is a per-county uniqueness migration on `properties`, then a
backfill from this table (see PropertyRadarRecord docstring).

Dedupe key is (state_fips, county_fips, apn); radar_id is also unique. A
radar_id already held by a different key is a data error — logged and
skipped, never overwritten.

Change detection: an owner change marks the row `sold`, a loan change marks
it `refinanced`. Flags, prior values and changed_at persist until the next
real change, so a later unchanged re-stage never erases the signal.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.property_radar import OWNER_ABBREVIATIONS

logger = logging.getLogger(__name__)

Key = tuple[str, str, str]

STATUS_ACTIVE = "active"
STATUS_SOLD = "sold"
STATUS_REFINANCED = "refinanced"
FLAG_OWNER_CHANGED = "owner_changed"
FLAG_LOAN_CHANGED = "loan_changed"

_COLUMNS: tuple[str, ...] = (
    "radar_id", "state_fips", "county_fips", "apn",
    "state", "county_name",
    "property_address", "city", "zip", "property_type",
    "owner_name", "ownership_type",
    "mailing_address", "mailing_city", "mailing_state", "mailing_zip",
    "principal_name",
    "lender_name", "loan_amount", "loan_recorded_date",
    "loan_term_years", "est_maturity_date", "loan_doc_number",
    "campaign", "raw",
)
_KEY_COLUMNS = ("state_fips", "county_fips", "apn")

_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def normalize_owner(name: str | None) -> str:
    """Upper-case, remove punctuation, collapse spaces, expand PR abbreviations."""
    if not name:
        return ""
    s = _PUNCT_RE.sub("", name.upper())  # remove, not replace: "Smith's" -> "SMITHS"
    s = _SPACE_RE.sub(" ", s).strip()
    return " ".join(OWNER_ABBREVIATIONS.get(w, w) for w in s.split())


def _key(r: dict[str, Any]) -> Key:
    return (r["state_fips"], r["county_fips"], r["apn"])


def _owner_changed(incoming: dict[str, Any], existing: dict[str, Any]) -> bool:
    new = normalize_owner(incoming.get("owner_name"))
    if not new:  # missing owner in the feed is not evidence of a sale
        return False
    return new != normalize_owner(existing.get("owner_name"))


def _loan_changed(incoming: dict[str, Any], existing: dict[str, Any]) -> bool:
    new_doc = incoming.get("loan_doc_number") or ""
    old_doc = existing.get("loan_doc_number") or ""
    if new_doc and old_doc:
        return new_doc != old_doc
    new_pair = (normalize_owner(incoming.get("lender_name")), incoming.get("loan_recorded_date") or "")
    old_pair = (normalize_owner(existing.get("lender_name")), existing.get("loan_recorded_date") or "")
    if all(new_pair) and all(old_pair):
        return new_pair != old_pair
    return False


def _detect_change(
    incoming: dict[str, Any], existing: dict[str, Any]
) -> tuple[list[str], dict[str, Any], str]:
    flags: list[str] = []
    prior: dict[str, Any] = {}
    status: str = existing["status"]
    if _owner_changed(incoming, existing):
        flags.append(FLAG_OWNER_CHANGED)
        prior["owner_name"] = existing.get("owner_name")
        status = STATUS_SOLD
    if _loan_changed(incoming, existing):
        flags.append(FLAG_LOAN_CHANGED)
        prior["lender_name"] = existing.get("lender_name")
        prior["loan_recorded_date"] = existing.get("loan_recorded_date")
        prior["loan_doc_number"] = existing.get("loan_doc_number")
        if status != STATUS_SOLD:
            status = STATUS_REFINANCED
    return flags, prior, status


_FETCH_EXISTING_SQL = """
    SELECT r.radar_id, r.state_fips, r.county_fips, r.apn, r.owner_name,
           r.lender_name, r.loan_recorded_date, r.loan_doc_number, r.status
    FROM property_radar_records r
    JOIN unnest(CAST(:sf AS text[]), CAST(:cf AS text[]), CAST(:apn AS text[]))
         AS k(state_fips, county_fips, apn)
      ON r.state_fips = k.state_fips AND r.county_fips = k.county_fips AND r.apn = k.apn
"""

_FETCH_RADAR_IDS_SQL = """
    SELECT radar_id, state_fips, county_fips, apn
    FROM property_radar_records
    WHERE radar_id = ANY(:ids)
"""

_INSERT_SQL = f"""
    INSERT INTO property_radar_records ({", ".join(_COLUMNS)}, status, first_seen_at, last_seen_at)
    VALUES ({", ".join(":" + c for c in _COLUMNS)}, '{STATUS_ACTIVE}', now(), now())
"""

_UPDATE_SQL = f"""
    UPDATE property_radar_records SET
        {", ".join(f"{c} = :{c}" for c in _COLUMNS if c not in _KEY_COLUMNS)},
        status       = :status,
        change_flags = CASE WHEN :has_change THEN CAST(:change_flags AS text[]) ELSE change_flags END,
        prior        = CASE WHEN :has_change THEN CAST(:prior AS jsonb) ELSE prior END,
        changed_at   = CASE WHEN :has_change THEN now() ELSE changed_at END,
        last_seen_at = now()
    WHERE state_fips = :state_fips AND county_fips = :county_fips AND apn = :apn
"""


def _base_params(r: dict[str, Any]) -> dict[str, Any]:
    params = {c: r.get(c) for c in _COLUMNS}
    params["raw"] = json.dumps(r["raw"]) if r.get("raw") is not None else None
    return params


def _dedupe_batch(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Last record wins per key; a radar_id reused by a second key is dropped."""
    by_key: dict[Key, dict[str, Any]] = {}
    for r in records:
        by_key[_key(r)] = r
    kept: list[dict[str, Any]] = []
    rid_owner: dict[str, Key] = {}
    for k, r in by_key.items():
        rid = r["radar_id"]
        if rid in rid_owner:
            logger.warning("radar_id %s repeated in batch for %s and %s, skipping latter", rid, rid_owner[rid], k)
            continue
        rid_owner[rid] = k
        kept.append(r)
    return kept, len(records) - len(kept)


def upsert_records(session: Session, records: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Upsert a batch of §3-contract records. Returns (inserted, updated, skipped)."""
    if not records:
        return 0, 0, 0

    batch, skipped = _dedupe_batch(records)
    keys = [_key(r) for r in batch]

    existing_rows = session.execute(
        text(_FETCH_EXISTING_SQL),
        {"sf": [k[0] for k in keys], "cf": [k[1] for k in keys], "apn": [k[2] for k in keys]},
    ).mappings().all()
    existing_by_key: dict[Key, dict[str, Any]] = {_key(row): dict(row) for row in existing_rows}

    rid_rows = session.execute(
        text(_FETCH_RADAR_IDS_SQL), {"ids": [r["radar_id"] for r in batch]}
    ).mappings().all()
    radar_id_to_key: dict[str, Key] = {row["radar_id"]: _key(row) for row in rid_rows}

    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    for record in batch:
        key = _key(record)
        holder = radar_id_to_key.get(record["radar_id"])
        if holder is not None and holder != key:
            logger.warning("radar_id %s already on %s, skipping %s", record["radar_id"], holder, key)
            skipped += 1
            continue

        existing = existing_by_key.get(key)
        if existing is None:
            inserts.append(_base_params(record))
            continue

        flags, prior, status = _detect_change(record, existing)
        params = _base_params(record)
        params.update(
            status=status,
            has_change=bool(flags),
            change_flags=flags or None,
            prior=json.dumps(prior) if prior else None,
        )
        updates.append(params)

    if inserts:
        session.execute(text(_INSERT_SQL), inserts)
    if updates:
        session.execute(text(_UPDATE_SQL), updates)
    session.flush()

    logger.info(
        "PropertyRadar upsert: inserted=%d updated=%d skipped=%d",
        len(inserts), len(updates), skipped,
    )
    return len(inserts), len(updates), skipped
