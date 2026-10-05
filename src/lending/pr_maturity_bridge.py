"""Bridge PropertyRadar maturity records into the dialer's calling pool.

The PropertyRadar pull lands records in ``property_radar_records`` and the live
trace writes any phones/emails into ``property_radar_trace_contacts`` (keyed by
``trace_key`` = normalized address + zip). Neither table is read by the dialer,
which loads only from ``lending.calling_pool_staging``. This bridge is the
missing link: it reads the staged PropertyRadar records, joins their traced
contacts, and writes them as a sixth pool (``pr_maturity``) into the same
staging table the dialer reads.

Georgia note: GA cold-calling of natural persons is prohibited, so the
compliance floor (``src/lending/compliance.py:_georgia_blocked``) drops any GA
row whose ``entity_status`` is not an allowed business type. ``entity_status``
is therefore mapped from PropertyRadar ownership here — a GA record left NULL
would be gated out of the pool entirely.

Pure mapping functions take no database and are unit-tested directly; the two
``_*`` database helpers and ``extract_pr_maturity_pool`` do the I/O.
"""
from __future__ import annotations

import logging
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_dialer import POOL_CAMPAIGN_TAGS
from src.services.phone_utils import normalize as normalize_phone
from src.services.skip_trace_ledger import trace_key

logger = logging.getLogger(__name__)

POOL_NAME = "pr_maturity"
SOURCE_TABLE = "property_radar_records"
# PropertyRadar maturity leads are the "Verified maturity" dialer queue (Go Live Brief 2.5).
CAMPAIGN_TAG = POOL_CAMPAIGN_TAGS["verified_maturity"]

# entity_status values the staging CHECK accepts. GA-allowed business types are
# LLC / CORPORATION (config.lending_compliance.GEORGIA_ALLOWED_ENTITY_TYPES minus
# LP, which the staging CHECK does not permit); TRUST and NATURAL_PERSON are kept
# so FL records carry a truthful status and GA fails closed on them.
_LLC_TOKENS = ("LLC", "L.L.C", "L L C")
_CORP_TOKENS = ("CORP", "INCORPORATED", " INC", "INC.", "COMPANY", "CORPORATION", "CORPORATE", "LP", "LLP", "PARTNERS")
_TRUST_TOKENS = ("TRUST", "TRUSTEE", "LIVING TR")


def map_entity_status(ownership_type: Optional[str], owner_name: Optional[str]) -> Optional[str]:
    """Map PropertyRadar ownership to a staging entity_status.

    Checks the owner name first (``ACME LLC`` is unambiguous) then the coarser
    PropertyRadar ownership_type. Returns None only when there is nothing to
    classify — a None status fails the GA gate closed, which is the safe default.
    """
    haystack = " ".join(p for p in (owner_name, ownership_type) if p).upper()
    if not haystack.strip():
        return None
    if any(tok in haystack for tok in _LLC_TOKENS):
        return "LLC"
    if any(tok in haystack for tok in _TRUST_TOKENS):
        return "TRUST"
    if any(tok in haystack for tok in _CORP_TOKENS):
        return "CORPORATION"
    ot = (ownership_type or "").strip().upper()
    if ot in ("COMPANY", "BUSINESS", "FINANCIAL", "CORPORATE"):
        return "CORPORATION"
    return "NATURAL_PERSON"


def _compose_address(
    address: Optional[str], city: Optional[str], state: Optional[str], zip_code: Optional[str]
) -> Optional[str]:
    parts = [p.strip() for p in (address, city, state, zip_code) if p and p.strip()]
    return ", ".join(parts) or None


def _to_decimal(loan_amount: Optional[int]) -> Optional[Decimal]:
    if loan_amount is None:
        return None
    try:
        return Decimal(int(loan_amount))
    except (InvalidOperation, ValueError, TypeError):
        return None


def build_pool_row(
    record: Mapping[str, Any],
    *,
    run_id: str,
    phone: Optional[str],
    email: Optional[str],
) -> dict[str, Any]:
    """Build one ``lending.calling_pool_staging`` row dict from a PR record.

    Pure: ``phone`` is already normalized (or None), ``email`` already chosen.
    """
    owner_name = record.get("owner_name")
    principal = record.get("principal_name")
    entity_status = map_entity_status(record.get("ownership_type"), owner_name)
    return {
        "run_id": run_id,
        "pool_name": POOL_NAME,
        "county_id": None,  # PR records are statewide; no FA county_id for most counties
        "county_name": record.get("county_name"),
        "borrower_name": principal or owner_name,
        "entity_name": owner_name if entity_status in ("LLC", "CORPORATION", "TRUST") else None,
        "target_property_address": _compose_address(
            record.get("property_address"), record.get("city"),
            record.get("state"), record.get("zip"),
        ),
        "recent_permit_details": None,
        "entity_status": entity_status,
        "parcel_id": record.get("apn"),
        "zip": record.get("zip"),
        "state": record.get("state"),
        "normalized_phone": phone,
        "phone_available": bool(phone),
        "line_type": None,
        "email": email,
        "financing_intent_score": None,
        "intent_tier": "unscored",
        "recommended_product": None,
        "estimated_loan_value": _to_decimal(record.get("loan_amount")),
        "aircall_campaign_tag": CAMPAIGN_TAG,
        "buyer_entity_id": None,
        "permit_number": None,
        "dbpr_license_number": None,
        "source_property_id": record.get("property_id"),
        "source_table": SOURCE_TABLE,
        "source_tag": record.get("campaign"),
        "homestead_exempt": None,
    }


_RECORDS_SQL = """
    SELECT radar_id, apn, state, county_name, property_address, city, zip,
           owner_name, ownership_type, principal_name, loan_amount, campaign,
           est_maturity_date, property_id
    FROM property_radar_records
    WHERE status = 'active'
    {state_filter}
    ORDER BY id
"""


def _load_records(session: Session, *, state: Optional[str]) -> list[dict[str, Any]]:
    clause = "AND state = :state" if state else ""
    rows = session.execute(
        text(_RECORDS_SQL.format(state_filter=clause)),
        {"state": state.upper()} if state else {},
    ).mappings().all()
    return [dict(r) for r in rows]


def _load_trace_contacts(session: Session, keys: list[str]) -> dict[str, dict[str, list]]:
    """Map trace_key -> {'phones': [...], 'emails': [...]} for the given keys."""
    if not keys:
        return {}
    rows = session.execute(
        text(
            "SELECT trace_key, phones, emails FROM property_radar_trace_contacts "
            "WHERE trace_key = ANY(:keys)"
        ),
        {"keys": keys},
    ).mappings().all()
    return {r["trace_key"]: {"phones": r["phones"] or [], "emails": r["emails"] or []} for r in rows}


def _first_phone(phones: list) -> Optional[str]:
    for raw in phones:
        norm = normalize_phone(str(raw))
        if norm:
            return norm
    return None


def extract_pr_maturity_pool(
    session: Session, *, dry_run: bool = False, state: Optional[str] = None
) -> dict[str, Any]:
    """Build the pr_maturity pool from PropertyRadar records + traced contacts.

    One staging run (``run_id``) per call. With ``dry_run`` the rows are built
    and counted but nothing is written. Records with no traced phone are still
    written (phone_available=False) so a later trace can be joined without a
    re-extract — matching the WP-W0-1 O15 convention.
    """
    run_id = str(uuid.uuid4())
    records = _load_records(session, state=state)
    keys = list({trace_key(r.get("property_address"), r.get("zip")) for r in records})
    contacts = _load_trace_contacts(session, keys)

    rows: list[dict[str, Any]] = []
    for r in records:
        key = trace_key(r.get("property_address"), r.get("zip"))
        c = contacts.get(key, {"phones": [], "emails": []})
        phone = _first_phone(c["phones"])
        email = (c["emails"][0].lower() if c["emails"] else None)
        rows.append(build_pool_row(r, run_id=run_id, phone=phone, email=email))

    with_phone = sum(1 for row in rows if row["phone_available"])
    ga = sum(1 for row in rows if (row["state"] or "").upper() == "GA")
    ga_dialable = sum(
        1 for row in rows
        if (row["state"] or "").upper() == "GA"
        and row["entity_status"] in ("LLC", "CORPORATION")
        and row["phone_available"]
    )
    summary = {
        "run_id": run_id,
        "dry_run": dry_run,
        "state_filter": state,
        "total_records": len(rows),
        "phone_available": with_phone,
        "ga_records": ga,
        "ga_dialable_est": ga_dialable,
    }

    if not dry_run and rows:
        written = _write_rows(session, rows)
        summary["rows_written"] = written
        logger.info("pr_maturity_bridge: run_id=%s wrote %d rows (%d with phone)", run_id, written, with_phone)
    else:
        logger.info(
            "pr_maturity_bridge: run_id=%s dry_run=%s built %d rows (%d with phone) — no write",
            run_id, dry_run, len(rows), with_phone,
        )
    return summary


def _write_rows(session: Session, rows: list[dict[str, Any]]) -> int:
    from sqlalchemy import insert

    from src.core.models import LendingCallingPoolStaging

    session.execute(insert(LendingCallingPoolStaging), rows)
    return len(rows)
