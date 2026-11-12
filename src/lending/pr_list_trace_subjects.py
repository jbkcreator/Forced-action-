"""Trace subjects for a calling list whose people come as a name only (Wholesalers: the seller).

One subject per person (``contact_key``): the name and address Tracerfy is sent, and where they came from,
kept in ``lending.pr_list_trace_subjects`` so every trace can be explained afterwards.

- Individual seller ("LAST FIRST M"): their name + the address of the property they resold. Tracerfy's
  normal trace matches the name together with the address, so a hit is that person, not the new owner.
- Company seller: the first managing member or officer who is a person on its Sunbiz record, with that
  member's address (or the company's principal address). Sunbiz is matched on the exact normalized name.
- Anything else (no Sunbiz record, no person on it, trusts, estates, organisations) is kept with an
  ``untraceable_reason`` and never sent.

Pure helpers first; ``build_subjects`` does the I/O and is idempotent (upsert per list and person).
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from sqlalchemy import text

_ADDRESS_LAST_LINE = re.compile(r"^(?P<city>.+?),\s*(?P<state>[A-Z]{2})\s+(?P<zip>\d{5})")
_SUFFIXES = {"JR", "SR", "II", "III", "IV", "MD", "ESQ", "PA"}
# Names that are neither a person nor a Sunbiz company: never traced as a person.
_NOT_A_PERSON = re.compile(
    r"\b(TRUST|TRUSTEE|TTEE|TR|ESTATE|EST|CEMETERY|CHURCH|MINISTRIES|CITY|COUNTY|STATE|SCHOOL|UNIVERSITY|"
    r"CMNTY|COMMUNITY|ASSOC|ASSOCIATION|HOA|BANK|CREDIT UNION|FEDERAL|AUTHORITY|DISTRICT|FOUNDATION)\b")

SELLER_PROPERTY = "seller_property"
SUNBIZ_MEMBER = "sunbiz_member"
NONE = "none"


@dataclass(frozen=True)
class TraceSubject:
    list_key: str
    contact_key: str
    radar_id: str
    subject_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]
    source: str
    sunbiz_doc_number: Optional[str] = None
    untraceable_reason: Optional[str] = None


def _words(value: str) -> list[str]:
    return [w for w in re.sub(r"[^A-Z0-9' -]", " ", value.upper()).split() if w]


def split_property_name(name: str) -> tuple[Optional[str], Optional[str]]:
    """PropertyRadar's "LAST FIRST M [& SPOUSE]" -> (first, last). The first owner only."""
    words = _words(name.split("&")[0])
    words = [w for w in words if w not in _SUFFIXES]
    if len(words) < 2:
        return None, None
    return words[1], words[0]


def split_sunbiz_name(name: str) -> tuple[Optional[str], Optional[str]]:
    """Sunbiz's "LAST, FIRST M, JR." -> (first, last)."""
    parts = [p.strip() for p in name.split(",") if p.strip()]
    if len(parts) < 2:
        return None, None
    first = [w for w in _words(parts[1]) if w not in _SUFFIXES]
    last = _words(parts[0])
    return (first[0] if first else None), (" ".join(last) if last else None)


def parse_sunbiz_address(value: Optional[str]) -> Optional[dict[str, str]]:
    """'8100 ULMERTON RD., #1\\nLARGO, FL 33771' -> street, city, state, zip (None if it doesn't parse)."""
    lines = [line.strip() for line in (value or "").splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    match = _ADDRESS_LAST_LINE.match(lines[-1].upper())
    if not match:
        return None
    return {"address": " ".join(lines[:-1]), "city": match["city"].strip(), "state": match["state"], "zip": match["zip"]}


def _is_person_entry(entry: Mapping[str, Any]) -> bool:
    from src.tasks.lending_pr_list_sunbiz import is_company

    name = entry.get("name") or ""
    return "," in name and not is_company(name) and not _NOT_A_PERSON.search(name.upper())


def subject_for_individual(list_key: str, contact_key: str, record: Mapping[str, Any]) -> TraceSubject:
    seller = record["last_transfer_seller"] or ""
    first, last = split_property_name(seller)
    reason = None
    if _NOT_A_PERSON.search(seller.upper()):
        reason = "not_a_person"
    elif not first or not last:
        reason = "name_unparseable"
    elif not record.get("property_address"):
        reason = "no_address"
    return TraceSubject(list_key, contact_key, record["radar_id"], seller, first, last,
                        record.get("property_address"), record.get("city"), record.get("state"), record.get("zip"),
                        SELLER_PROPERTY if reason is None else NONE, untraceable_reason=reason)


def subject_for_company(list_key: str, contact_key: str, record: Mapping[str, Any],
                        sunbiz: Optional[Mapping[str, Any]]) -> TraceSubject:
    """The first person on the company's Sunbiz record, with their address (else the principal address)."""
    def untraceable(reason: str, doc: Optional[str] = None) -> TraceSubject:
        return TraceSubject(list_key, contact_key, record["radar_id"], record["last_transfer_seller"], None, None,
                            None, None, None, None, NONE, sunbiz_doc_number=doc, untraceable_reason=reason)

    if not sunbiz:
        return untraceable("not_on_sunbiz")
    doc = sunbiz.get("doc_number")
    people = [e for e in (sunbiz.get("managing_members") or []) + (sunbiz.get("officers") or []) if _is_person_entry(e)]
    if not people:
        return untraceable("no_person_on_sunbiz", doc)
    person = people[0]
    first, last = split_sunbiz_name(person["name"])
    address = parse_sunbiz_address(person.get("address")) or parse_sunbiz_address(sunbiz.get("principal_address"))
    if not first or not last:
        return untraceable("name_unparseable", doc)
    if not address:
        return untraceable("no_address", doc)
    return TraceSubject(list_key, contact_key, record["radar_id"], person["name"], first, last, address["address"],
                        address["city"], address["state"], address["zip"], SUNBIZ_MEMBER, sunbiz_doc_number=doc)


# --- I/O -------------------------------------------------------------------------------------------------

_SELLERS_SQL = text("""
    SELECT DISTINCT ON (contact_key) contact_key, radar_id, last_transfer_seller, property_address, city, state, zip
      FROM lending.pr_list_records
     WHERE list_key = :k AND qualifies AND contact_key IS NOT NULL
     ORDER BY contact_key, last_transfer_date DESC NULLS LAST, id
""")

_UPSERT = text("""
    INSERT INTO lending.pr_list_trace_subjects
        (list_key, contact_key, radar_id, subject_name, first_name, last_name, address, city, state, zip,
         source, sunbiz_doc_number, untraceable_reason)
    VALUES (:list_key, :contact_key, :radar_id, :subject_name, :first_name, :last_name, :address, :city, :state,
            :zip, :source, :sunbiz_doc_number, :untraceable_reason)
    ON CONFLICT (list_key, contact_key) DO UPDATE SET
        radar_id = EXCLUDED.radar_id, subject_name = EXCLUDED.subject_name, first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name, address = EXCLUDED.address, city = EXCLUDED.city, state = EXCLUDED.state,
        zip = EXCLUDED.zip, source = EXCLUDED.source, sunbiz_doc_number = EXCLUDED.sunbiz_doc_number,
        untraceable_reason = EXCLUDED.untraceable_reason, built_at = now()
""")


def sunbiz_by_name(lookups_file: Path, snapshot_rows: Iterable[Mapping[str, Any]]) -> dict[str, dict]:
    """Normalized company name -> parsed Sunbiz record: this list's lookups first, then stored snapshots."""
    from src.scrappers.sunbiz.sunbiz_engine import _normalize
    from src.tasks.lending_pr_list_sunbiz import _SUNBIZ_ENTITY_PREFIX

    found: dict[str, dict] = {}
    for row in snapshot_rows:
        data = row if isinstance(row, dict) else json.loads(row)
        found.setdefault(_normalize(_SUNBIZ_ENTITY_PREFIX.sub("", (data.get("name") or "").upper())), data)
    if lookups_file.exists():
        for line in lookups_file.read_text(encoding="utf-8").splitlines():
            entry = json.loads(line) if line.strip() else {}
            if entry.get("status") == "found" and entry.get("snapshot"):
                found[entry["normalized_name"]] = entry["snapshot"]
    return found


def build_subjects(db, list_key: str, lookups_file: Path) -> dict[str, int]:
    """One subject per person on the list, written idempotently. Returns counts by source / reason."""
    from src.scrappers.sunbiz.sunbiz_engine import _normalize
    from src.tasks.lending_pr_list_sunbiz import is_company

    sunbiz = sunbiz_by_name(lookups_file, [r[0] for r in db.execute(text("SELECT raw_jsonb FROM sunbiz_snapshots"))])
    subjects = []
    for record in (dict(r._mapping) for r in db.execute(_SELLERS_SQL, {"k": list_key})):
        seller = record["last_transfer_seller"] or ""
        if is_company(seller):
            subjects.append(subject_for_company(list_key, record["contact_key"], record, sunbiz.get(_normalize(seller))))
        else:
            subjects.append(subject_for_individual(list_key, record["contact_key"], record))
    if subjects:
        db.execute(_UPSERT, [asdict(s) for s in subjects])
    counts: dict[str, int] = {}
    for s in subjects:
        key = s.source if s.untraceable_reason is None else f"untraceable:{s.untraceable_reason}"
        counts[key] = counts.get(key, 0) + 1
    return counts
