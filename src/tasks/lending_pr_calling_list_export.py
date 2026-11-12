"""Write a calling list's load-ready people as a dialer-load file (the JSON ``lending_dialer_load --input`` reads).

Load-ready = a qualifying record on the list, one per person (``contact_key``, confirmed match first), with a
traced phone whose newest stored scrub is clean (the first such phone; a second phone is checked only when the
first was blocked). People with a phone
not yet checked are counted but left out. No Tracerfy, no dialer, no writes: it reads and writes a file.

Verified maturity records go to the Verified maturity queue through the existing source tags (``list_1``
named hard-money lender, ``list_8`` private money); cash buyers go to Transaction ready as ``list_2``
(config/lending_queues.py).

Usage:
    python -m src.tasks.lending_pr_calling_list_export --list verified_maturity
    python -m src.tasks.lending_dialer_load --input <printed file>            # dry run of the load
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_calling_lists import (
    CASH_BUYER, HARD_MONEY_LENDER_NAMES, LANDING_ROOT, VERIFIED_MATURITY, WHOLESALER,
)
from config.lending_queues import SOURCE_TAG_QUEUES
from src.core.database import get_db_context
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

_HARD_MONEY = {name.upper() for name in HARD_MONEY_LENDER_NAMES}
WHOLESALER_SOURCE_TAG = "wholesaler_propertyradar"

_PEOPLE_SQL = text("""
    SELECT DISTINCT ON (r.contact_key)
           r.contact_key, r.radar_id, r.list_key, r.qualifies, r.match_strength, r.owner_name, r.ownership_type,
           r.principal_name, r.property_address, r.city, r.state, r.zip, r.county, r.apn, r.lender_name,
           r.loan_amount, r.est_maturity_date, t.phones
      FROM lending.pr_list_records r
      JOIN lending.pr_list_records r2 ON r2.list_key = r.list_key AND r2.contact_key = r.contact_key
      JOIN property_radar_traced_contacts t ON t.radar_id = r2.radar_id AND cardinality(t.phones) > 0
     WHERE r.list_key = :k AND r.qualifies AND r.contact_key IS NOT NULL
     ORDER BY r.contact_key, (r.match_strength = 'confirmed') DESC, r.id
""")


# Wholesalers: the person traced is the trace subject (the seller, or the person behind a company seller).
_SUBJECT_PEOPLE_SQL = text("""
    SELECT s.contact_key, s.radar_id, r.list_key, r.qualifies, r.match_strength, s.first_name, s.last_name,
           s.source, r.last_transfer_seller, r.property_address, r.city, r.state, r.zip, r.county, r.apn, t.phones
      FROM lending.pr_list_trace_subjects s
      JOIN lending.pr_list_records r ON r.list_key = s.list_key AND r.radar_id = s.radar_id
      JOIN property_radar_traced_contacts t ON t.radar_id = s.radar_id AND cardinality(t.phones) > 0
     WHERE s.list_key = :k AND s.untraceable_reason IS NULL AND r.qualifies
""")


def _phones(phones) -> list[str]:
    return [p for p in dict.fromkeys(normalize_phone(x) for x in phones or []) if p]


def _is_clean(scrub) -> bool:
    return scrub is not None and not (scrub.national_dnc or scrub.state_dnc or scrub.litigator)


def _phone_to_load(phones: list[str], scrubs: dict) -> Optional[str]:
    """The first phone whose newest scrub is clean; else the first phone not checked yet; else the first."""
    return (next((p for p in phones if _is_clean(scrubs.get(p))), None)
            or next((p for p in phones if p not in scrubs), None)
            or (phones[0] if phones else None))


def _source_tag(lender_name: Optional[str]) -> str:
    return "list_1" if (lender_name or "").strip().upper() in _HARD_MONEY else "list_8"


def wholesaler_record(person: dict[str, Any], phone: str) -> dict[str, Any]:
    from src.tasks.lending_pr_list_sunbiz import is_company

    seller = person["last_transfer_seller"]
    company = is_company(seller)
    address = ", ".join(p for p in (person["property_address"], person["city"]) if p)
    return {
        "source_record_ref": f"pr_list:{person['list_key']}:{person['radar_id']}",
        "source_tag": WHOLESALER_SOURCE_TAG,
        "pool": "wholesalers",
        "phone": phone,
        "borrower_name": f"{person['first_name']} {person['last_name']}".title(),
        "entity_name": seller if company else None,
        "property_address": address or None,  # the property they bought and resold inside 30 days
        "zip": person["zip"],
        "county": person["county"],
        "state": person["state"],
        "parcel_id": person["apn"],
        "entity_status": "LLC" if company else None,
        "match_strength": person["match_strength"],
        "trace_source": person["source"],
    }


def dialer_record(person: dict[str, Any], phone: str) -> dict[str, Any]:
    company = person["ownership_type"] in ("Corporate", "Trust")
    address = ", ".join(p for p in (person["property_address"], person["city"]) if p)
    return {
        "source_record_ref": f"pr_list:{person['list_key']}:{person['radar_id']}",
        "source_tag": _source_tag(person["lender_name"]),
        "pool": "verified_maturity",
        "phone": phone,
        "borrower_name": person["principal_name"] or (None if company else person["owner_name"]),
        "entity_name": person["owner_name"] if company else None,
        "property_address": address or None,
        "zip": person["zip"],
        "county": person["county"],
        "state": person["state"],
        "parcel_id": person["apn"],
        "entity_status": "LLC" if person["ownership_type"] == "Corporate" else None,
        "estimated_loan_value": float(person["loan_amount"]) if person["loan_amount"] is not None else None,
        "match_strength": person["match_strength"],
        "est_maturity_date": person["est_maturity_date"].isoformat() if person["est_maturity_date"] else None,
    }


def _first_last(owner_name: Optional[str]) -> Optional[str]:
    """County ``LAST,FIRST & CO-OWNER`` as ``First Last`` (the name the trace matched), for the dialer card."""
    from src.services.property_radar.live_trace import _split_name

    first, last = _split_name(owner_name)
    return " ".join(p for p in (first, last) if p).title() or None


def cash_buyer_record(person: dict[str, Any], phone: str) -> dict[str, Any]:
    """An all-cash buyer of several properties: the property is the recent cash purchase; no loan on it."""
    company = person["ownership_type"] in ("Corporate", "Trust")
    address = ", ".join(p for p in (person["property_address"], person["city"]) if p)
    return {
        "source_record_ref": f"pr_list:{person['list_key']}:{person['radar_id']}",
        "source_tag": "list_2",
        "pool": "transaction_ready",
        "phone": phone,
        "borrower_name": person["principal_name"] or (None if company else _first_last(person["owner_name"])),
        "entity_name": person["owner_name"] if company else None,
        "property_address": address or None,
        "zip": person["zip"],
        "county": person["county"],
        "state": person["state"],
        "parcel_id": person["apn"],
        "entity_status": "LLC" if person["ownership_type"] == "Corporate" else None,
        "match_strength": person["match_strength"],
    }


# list -> (people query, record builder, the queue its records must land in)
LIST_EXPORTS = {
    VERIFIED_MATURITY: (_PEOPLE_SQL, dialer_record, "verified_maturity"),
    WHOLESALER: (_SUBJECT_PEOPLE_SQL, wholesaler_record, "wholesalers"),
    CASH_BUYER: (_PEOPLE_SQL, cash_buyer_record, "transaction_ready"),
}


def build(db, list_key: str) -> tuple[list[dict], dict[str, int]]:
    sql, make_record, queue = LIST_EXPORTS[list_key]
    people = [dict(r._mapping) for r in db.execute(sql, {"k": list_key})]
    all_phones = {p["contact_key"]: _phones(p["phones"]) for p in people}
    scrubs = {r.phone: r for r in db.execute(text(
        "SELECT DISTINCT ON (phone) phone, national_dnc, state_dnc, litigator FROM lending.dnc_scrubs "
        "WHERE phone = ANY(:p) ORDER BY phone, checked_at DESC"),
        {"p": list({p for ps in all_phones.values() for p in ps})})}
    phones = {key: _phone_to_load(ps, scrubs) for key, ps in all_phones.items()}
    records, counts = [], {"people_with_phone": len(people), "not_checked": 0, "blocked": 0, "load_ready": 0}
    for person in people:
        phone = phones[person["contact_key"]]
        scrub = scrubs.get(phone) if phone else None
        if scrub is None:
            counts["not_checked"] += 1
        elif scrub.national_dnc or scrub.state_dnc or scrub.litigator:
            counts["blocked"] += 1
        else:
            # Every record must be a qualifying record of this list and land in its queue.
            assert person["list_key"] == list_key and person["qualifies"]
            record = make_record(person, phone)
            assert SOURCE_TAG_QUEUES[record["source_tag"]] == queue == record["pool"]
            records.append(record)
    # One record per phone: the dialer load refuses a phone that would load for two people (the same
    # person owning under two names or mailing addresses). Confirmed maturity wins.
    records.sort(key=lambda r: r["match_strength"] != "confirmed")
    by_phone: dict[str, dict] = {}
    for record in records:
        by_phone.setdefault(record["phone"], record)
    counts["same_phone_dropped"] = len(records) - len(by_phone)
    records = list(by_phone.values())
    counts["load_ready"] = len(records)
    counts["load_ready_confirmed"] = sum(1 for r in records if r["match_strength"] == "confirmed")
    return records, counts


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Write a calling list's load-ready people as a dialer-load file.")
    parser.add_argument("--list", dest="list_key", choices=sorted(LIST_EXPORTS), required=True)
    args = parser.parse_args(argv)
    with get_db_context() as db:
        db.execute(text("SET TRANSACTION READ ONLY"))
        records, counts = build(db, args.list_key)
        db.rollback()
    path = LANDING_ROOT / args.list_key / "load" / f"{args.list_key}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=1), encoding="utf-8")
    logger.info("[list-export] %s: %s -> %s", args.list_key, json.dumps(counts), path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
