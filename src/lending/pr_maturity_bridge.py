"""Bridge PropertyRadar maturity records into the dialer's calling pool.

The PropertyRadar pull lands records in ``property_radar_records`` and the live
trace writes any phones/emails into ``property_radar_traced_contacts`` (keyed by
``radar_id``). Neither table is read by the dialer,
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
import re
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_dialer import POOL_CAMPAIGN_TAGS
from src.lending.pool_source import latest_run_id
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

POOL_NAME = "pr_maturity"
SOURCE_TABLE = "property_radar_records"
# PropertyRadar maturity leads are the "Verified maturity" dialer queue (Go Live Brief 2.5).
CAMPAIGN_TAG = POOL_CAMPAIGN_TAGS["verified_maturity"]

# PropertyRadar campaign -> Go Live Brief source list (list_1..list_9). The dialer's
# queue assignment (src/lending/queues.py:assign_queue) keys off these list tags, not
# the raw campaign name, so an unmapped campaign would be dropped before load.
#   list_1 = FL maturity, list_5 = GA maturity, list_8 = private-money maturity
#     -> Verified maturity (rank 1)
#   list_9 = stalled flips, list_6 = auction winners -> Transaction ready (rank 2)
# maturity_target_lender carries both FL and GA records, so it splits by state
# (list_1 FL / list_5 GA); List 5 is the Georgia maturity list, not a catch-all.
CAMPAIGN_SOURCE_TAG = {
    "private_maturity": "list_8",
    "stalled_flip": "list_9",
    "auction_winner": "list_6",
}


def _source_tag(campaign: Optional[str], state: Optional[str]) -> Optional[str]:
    if campaign == "maturity_target_lender":
        return "list_5" if (state or "").strip().upper() == "GA" else "list_1"
    return CAMPAIGN_SOURCE_TAG.get(campaign, campaign)

# entity_status values the staging CHECK accepts. GA-allowed business types are
# LLC / CORPORATION (config.lending_compliance.GEORGIA_ALLOWED_ENTITY_TYPES minus
# LP, which the staging CHECK does not permit); TRUST and NATURAL_PERSON are kept
# so FL records carry a truthful status and GA fails closed on them.
#
# Matched on WORD BOUNDARIES, never as substrings: this status drives the GA
# cold-calling gate, so "RALPH SMITH" must not match "LP" and become CORPORATION
# (which would fail the gate open on a natural person — exactly what GA forbids).
_LLC_RE = re.compile(r"\bL\.?\s?L\.?\s?C\b")
_TRUST_RE = re.compile(r"\b(?:TRUST|TRUSTEE|LIVING TR)\b")
_CORP_RE = re.compile(
    r"\b(?:CORP|CORPORATION|CORPORATE|INC|INCORPORATED|COMPANY|LP|LLP|LTD|PARTNERS)\b"
)


def map_entity_status(ownership_type: Optional[str], owner_name: Optional[str]) -> Optional[str]:
    """Map PropertyRadar ownership to a staging entity_status.

    Checks the owner name first (``ACME LLC`` is unambiguous) then the coarser
    PropertyRadar ownership_type. Returns None only when there is nothing to
    classify — a None status fails the GA gate closed, which is the safe default.

    Entity tokens match on word boundaries so a natural-person name is never
    misread as a business (which, for GA, would fail the compliance gate open).
    """
    haystack = " ".join(p for p in (owner_name, ownership_type) if p).upper()
    if not haystack.strip():
        return None
    if _LLC_RE.search(haystack):
        return "LLC"
    if _TRUST_RE.search(haystack):
        return "TRUST"
    if _CORP_RE.search(haystack):
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
        "source_tag": _source_tag(record.get("campaign"), record.get("state")),
        "homestead_exempt": None,
    }


# Keyset-paged so a statewide maturity table is never materialized in one fetch
# (CLAUDE.md "stream large result sets / never fetchall an unbounded result").
RECORD_PAGE_SIZE = 1000

_RECORDS_SQL = """
    SELECT id, radar_id, apn, state, county_name, property_address, city, zip,
           owner_name, ownership_type, principal_name, loan_amount, campaign,
           est_maturity_date, property_id
    FROM property_radar_records
    WHERE status = 'active' AND id > :after_id
    {state_filter}
    ORDER BY id
    LIMIT :limit
"""


def _iter_record_pages(
    session: Session, *, state: Optional[str], page_size: int = RECORD_PAGE_SIZE
) -> Iterator[list[dict[str, Any]]]:
    """Yield active PropertyRadar records in id order, one bounded page at a time."""
    clause = "AND state = :state" if state else ""
    sql = text(_RECORDS_SQL.format(state_filter=clause))
    after_id = 0
    while True:
        params: dict[str, Any] = {"after_id": after_id, "limit": page_size}
        if state:
            params["state"] = state.upper()
        rows = session.execute(sql, params).mappings().all()
        if not rows:
            return
        after_id = rows[-1]["id"]
        yield [dict(r) for r in rows]


def _load_trace_contacts(session: Session, radar_ids: list[str]) -> dict[str, dict[str, list]]:
    """Map radar_id -> {'phones': [...], 'emails': [...]} for the given records."""
    if not radar_ids:
        return {}
    rows = session.execute(
        text(
            "SELECT radar_id, phones, emails FROM property_radar_traced_contacts "
            "WHERE radar_id = ANY(:ids)"
        ),
        {"ids": radar_ids},
    ).mappings().all()
    return {r["radar_id"]: {"phones": r["phones"] or [], "emails": r["emails"] or []} for r in rows}


def _first_phone(phones: list) -> Optional[str]:
    for raw in phones:
        norm = normalize_phone(str(raw))
        if norm:
            return norm
    return None


def _pick_phone(phones: list, taken: set[str]) -> Optional[str]:
    """First normalized phone not already in this run; reserves it. The dialer load
    refuses a run where one phone sits on two records, so each phone goes to one row."""
    for raw in phones:
        norm = normalize_phone(str(raw))
        if norm and norm not in taken:
            taken.add(norm)
            return norm
    return None


def _phones_in_other_pools(session: Session, run_id: str, state: Optional[str] = None) -> set[str]:
    """Phones already staged in this run that this extract will not replace: every other
    pool, plus (state-scoped run) this pool's rows for other states."""
    clause = "pool_name <> :pool"
    params: dict[str, Any] = {"run_id": run_id, "pool": POOL_NAME}
    if state:
        clause = "(pool_name <> :pool OR state <> :state)"
        params["state"] = state.upper()
    rows = session.execute(
        text(
            "SELECT normalized_phone FROM lending.calling_pool_staging "
            f"WHERE run_id = CAST(:run_id AS uuid) AND {clause}"
        ),
        params,
    ).scalars()
    return {n for n in (normalize_phone(str(p)) for p in rows if p) if n}


def _clear_previous_rows(session: Session, run_id: str, *, state: Optional[str]) -> None:
    """Drop this pool's earlier rows in the run so a re-run replaces them."""
    clause = " AND state = :state" if state else ""
    params: dict[str, Any] = {"run_id": run_id, "pool": POOL_NAME}
    if state:
        params["state"] = state.upper()
    session.execute(
        text(
            "DELETE FROM lending.calling_pool_staging "
            "WHERE run_id = CAST(:run_id AS uuid) AND pool_name = :pool" + clause
        ),
        params,
    )


def _is_ga(row: Mapping[str, Any]) -> bool:
    return (row["state"] or "").upper() == "GA"


def _is_ga_dialable(row: Mapping[str, Any]) -> bool:
    """A GA row that will survive _georgia_blocked and has a number to call."""
    return _is_ga(row) and row["entity_status"] in ("LLC", "CORPORATION") and row["phone_available"]


def extract_pr_maturity_pool(
    session: Session, *, dry_run: bool = False, state: Optional[str] = None
) -> dict[str, Any]:
    """Build the pr_maturity pool from PropertyRadar records + traced contacts.

    Rows join the NEWEST existing staging run rather than starting their own:
    the dialer load reads only the newest run, so a pr_maturity-only run would
    crowd every other pool out of the load. Our previous rows in that run are
    replaced, so re-running is idempotent. Records are read and written in
    bounded pages, so a statewide maturity table is never fully materialized.
    With ``dry_run`` the rows are built and counted but nothing is written.
    Records with no traced phone are still written (phone_available=False) so a
    later trace can be joined without a re-extract (WP-W0-1 O15 convention).
    Each phone goes to one row only: a phone already in the run (any pool) is skipped
    and the record gets its next traced phone, or none.
    """
    run_id = latest_run_id(session) or str(uuid.uuid4())
    if not dry_run:
        _clear_previous_rows(session, run_id, state=state)
    taken = _phones_in_other_pools(session, run_id, state)
    total = with_phone = ga = ga_dialable = written = 0

    for records in _iter_record_pages(session, state=state):
        contacts = _load_trace_contacts(session, [r["radar_id"] for r in records])
        rows: list[dict[str, Any]] = []
        for r in records:
            c = contacts.get(r["radar_id"], {"phones": [], "emails": []})
            email = c["emails"][0].lower() if c["emails"] else None
            rows.append(build_pool_row(r, run_id=run_id, phone=_pick_phone(c["phones"], taken), email=email))

        total += len(rows)
        with_phone += sum(1 for row in rows if row["phone_available"])
        ga += sum(1 for row in rows if _is_ga(row))
        ga_dialable += sum(1 for row in rows if _is_ga_dialable(row))
        if not dry_run and rows:
            written += _write_rows(session, rows)

    summary = {
        "run_id": run_id,
        "dry_run": dry_run,
        "state_filter": state,
        "total_records": total,
        "phone_available": with_phone,
        "ga_records": ga,
        "ga_dialable_est": ga_dialable,
    }
    if not dry_run:
        summary["rows_written"] = written
        logger.info("pr_maturity_bridge: run_id=%s wrote %d rows (%d with phone)", run_id, written, with_phone)
    else:
        logger.info(
            "pr_maturity_bridge: run_id=%s dry_run=True built %d rows (%d with phone) — no write",
            run_id, total, with_phone,
        )
    return summary


def _write_rows(session: Session, rows: list[dict[str, Any]]) -> int:
    from sqlalchemy import insert

    from src.core.models import LendingCallingPoolStaging

    session.execute(insert(LendingCallingPoolStaging), rows)
    return len(rows)
