"""Daily FA export (Cora brief P3.1): the day's new PropertyRadar leads as one CSV and as GHL contacts.

Leads are read from both PropertyRadar stores: the daily pull's ``property_radar_records`` (Florida and
Georgia) and the calling lists in ``lending.pr_list_records`` (qualifying Verified maturity and Cash buyer
rows; the Wholesaler list is not a P3.1 signal). A property found in both is one row with its signals joined.

  maturing_loan       a maturity campaign or Verified maturity list record inside the 5.5-month trigger
  cash_purchase       a Cash buyer list record, or a cash_purchase campaign record
  investor_portfolio  an investor_portfolio campaign record, or any company-owned record whose
                      owner holds INVESTOR_PORTFOLIO_MIN_PROPERTIES or more leads across both stores

The daily export is a delta, decided from the record alone (nothing is stored): a record is exported on the
ET day it was first stored, and a daily-pull maturing loan stored before its trigger is exported on the day
it crosses it. A re-run of a day therefore builds the same file. The backfill is every lead that qualifies
as of a day, for the one-time starter file.

Pure functions take no database; ``collect`` does the reads.
"""
from __future__ import annotations

import csv
import io
import logging
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any, Callable, Iterator, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_compliance import GHL_TAG_COLD_LEAD
from config.lending_wholesalers import GHL_TAG_SOURCE_PREFIX, GHL_TAG_STATE_PREFIX
from config.property_radar_campaigns import (
    ASSUMED_TERM_DAYS,
    ENABLED_STATES,
    INVESTOR_PORTFOLIO_MIN_PROPERTIES,
    MATURITY_CAMPAIGNS,
    MATURITY_TRIGGER_DAYS,
)
from config.property_radar_fips import county_fips
from src.lending.compliance import suppressed_phones
from src.lending.ledger_export import ET, day_window
from src.lending.pr_maturity_bridge import _first_phone, _load_trace_contacts, map_entity_status

logger = logging.getLogger(__name__)

SIGNAL_MATURING = "maturing_loan"
SIGNAL_CASH = "cash_purchase"
SIGNAL_PORTFOLIO = "investor_portfolio"
SIGNAL_SEPARATOR = ";"

GHL_TAG_NDL = "NDL"
GHL_TAG_SOURCE = GHL_TAG_SOURCE_PREFIX + "property-radar"
GHL_TAG_SIGNAL_PREFIX = "ndl-signal-"
# Georgia: natural persons are never cold-called, so only business owners become contacts.
GA_CONTACT_ENTITY_TYPES = frozenset({"LLC", "CORPORATION"})

PAGE_SIZE = 1000

SOURCE_DAILY = "pr_daily"
SOURCE_LIST_PREFIX = "pr_list:"
# lending.pr_list_records list keys (config/lending_calling_lists.py on the calling-lists branch).
LIST_VERIFIED_MATURITY = "verified_maturity"
LIST_CASH_BUYER = "cash_buyer"
EXPORTED_LISTS = (LIST_VERIFIED_MATURITY, LIST_CASH_BUYER)
# The Verified maturity pull takes loans maturing 90-180 days out under an assumed 1, 2 or 3 year term.
LIST_MATURITY_DAYS_OUT = (90, 180)
LIST_ASSUMED_TERMS_YEARS = (1, 2, 3)
MATURING_CAMPAIGNS = MATURITY_CAMPAIGNS | {LIST_VERIFIED_MATURITY}
CASH_CAMPAIGNS = frozenset({SIGNAL_CASH, LIST_CASH_BUYER})

COLUMNS = (
    "export_date", "signals", "state", "county", "county_fips", "property_address", "city", "zip",
    "owner_name", "principal_name", "ownership_type", "portfolio_properties",
    "mailing_address", "mailing_city", "mailing_state", "mailing_zip",
    "lender_name", "loan_amount", "loan_recorded_date", "maturity_date", "maturity_estimated",
    "days_to_maturity", "campaign", "source", "phone", "email", "fa_property_id", "radar_id",
)

GhlPush = Callable[[Mapping[str, Any], list[str]], Any]


def file_name(day: date, *, backfill: bool = False) -> str:
    return f"fa_export_{'backfill_' if backfill else ''}{day.isoformat()}.csv"


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _add_years(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year + years)
    except ValueError:  # Feb 29
        return day.replace(year=day.year + years, day=28)


def _list_maturity(recorded: date, pulled: Optional[date]) -> date:
    """A Verified maturity record with no term: the assumed term that put it in the pull's 90-180 day window."""
    if pulled:
        near, far = (pulled + timedelta(days=d) for d in LIST_MATURITY_DAYS_OUT)
        for years in LIST_ASSUMED_TERMS_YEARS:
            if near <= _add_years(recorded, years) <= far:
                return _add_years(recorded, years)
    return recorded + timedelta(days=ASSUMED_TERM_DAYS)


def maturity(record: Mapping[str, Any]) -> tuple[Optional[date], bool]:
    """(maturity date, estimated?). PropertyRadar's term when it gave one, else an assumed term."""
    known = _parse_date(record.get("est_maturity_date"))
    if known:
        return known, False
    recorded = _parse_date(record.get("loan_recorded_date"))
    if not recorded:
        return None, False
    if record.get("campaign") == LIST_VERIFIED_MATURITY:
        return _list_maturity(recorded, _parse_date(record.get("first_seen"))), True
    return recorded + timedelta(days=ASSUMED_TERM_DAYS), True


def due_on(record: Mapping[str, Any], day: date, first_seen: date) -> bool:
    """Is this record part of ``day``'s export?"""
    if record.get("campaign") not in MATURITY_CAMPAIGNS:
        return first_seen == day
    matures, _ = maturity(record)
    if matures is None:
        return False
    trigger = matures - timedelta(days=MATURITY_TRIGGER_DAYS)
    return trigger == day if first_seen < trigger else first_seen == day


def in_backfill(record: Mapping[str, Any], day: date) -> bool:
    """Does this record qualify as of ``day``? A daily-pull loan still ahead of its trigger does not yet."""
    if record.get("campaign") not in MATURITY_CAMPAIGNS:
        return True
    matures, _ = maturity(record)
    return matures is not None and matures - timedelta(days=MATURITY_TRIGGER_DAYS) <= day


def signals(record: Mapping[str, Any], portfolio_size: int) -> list[str]:
    campaign = record.get("campaign")
    found = []
    if campaign in MATURING_CAMPAIGNS:
        found.append(SIGNAL_MATURING)
    if campaign in CASH_CAMPAIGNS:
        found.append(SIGNAL_CASH)
    if campaign == SIGNAL_PORTFOLIO or portfolio_size >= INVESTOR_PORTFOLIO_MIN_PROPERTIES:
        found.append(SIGNAL_PORTFOLIO)
    return found


def build_row(record: Mapping[str, Any], *, day: date, portfolio_size: int, phone: Optional[str],
              email: Optional[str]) -> Optional[dict[str, Any]]:
    found = signals(record, portfolio_size)
    if not found:
        return None
    matures, estimated = maturity(record)
    return {
        "export_date": day.isoformat(),
        "signals": SIGNAL_SEPARATOR.join(found),
        "state": record.get("state"),
        "county": record.get("county_name"),
        "county_fips": record.get("county_fips"),
        "property_address": record.get("property_address"),
        "city": record.get("city"),
        "zip": record.get("zip"),
        "owner_name": record.get("owner_name"),
        "principal_name": record.get("principal_name"),
        "ownership_type": record.get("ownership_type"),
        "portfolio_properties": portfolio_size or "",
        "mailing_address": record.get("mailing_address"),
        "mailing_city": record.get("mailing_city"),
        "mailing_state": record.get("mailing_state"),
        "mailing_zip": record.get("mailing_zip"),
        "lender_name": record.get("lender_name"),
        "loan_amount": record.get("loan_amount"),
        "loan_recorded_date": _parse_date(record.get("loan_recorded_date")) or "",
        "maturity_date": matures.isoformat() if matures else "",
        "maturity_estimated": ("yes" if estimated else "no") if matures else "",
        "days_to_maturity": (matures - day).days if matures else "",
        "campaign": record.get("campaign"),
        "source": record.get("source") or SOURCE_DAILY,
        "phone": phone or "",
        "email": email or "",
        "fa_property_id": record.get("property_id") or "",
        "radar_id": record.get("radar_id"),
    }


def to_csv(rows: list[dict[str, Any]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows({k: ("" if v is None else v) for k, v in row.items()} for row in rows)
    return buffer.getvalue().encode("utf-8")


def ghl_tags(row: Mapping[str, Any]) -> list[str]:
    tags = [GHL_TAG_NDL, GHL_TAG_COLD_LEAD, GHL_TAG_SOURCE]
    if row.get("state"):
        tags.append(GHL_TAG_STATE_PREFIX + str(row["state"]).lower())
    tags.extend(GHL_TAG_SIGNAL_PREFIX + s.replace("_", "-") for s in str(row["signals"]).split(SIGNAL_SEPARATOR))
    return tags


def ghl_skip_reason(row: Mapping[str, Any], suppressed: set[str]) -> Optional[str]:
    if not row.get("phone"):
        return "no_phone"
    if row["phone"] in suppressed:
        return "suppressed"
    entity = map_entity_status(row.get("ownership_type"), row.get("owner_name"))
    if str(row.get("state") or "").upper() == "GA" and entity not in GA_CONTACT_ENTITY_TYPES:
        return "ga_natural_person"
    return None


def push_to_ghl(rows: list[dict[str, Any]], push: GhlPush, suppressed: set[str]) -> Counter:
    """One GHL contact per phone. A refused push is counted and the rest still go."""
    outcome: Counter = Counter()
    done: set[str] = set()
    for row in rows:
        reason = ghl_skip_reason(row, suppressed) or ("duplicate_phone" if row["phone"] in done else None)
        if reason:
            outcome[reason] += 1
            continue
        done.add(row["phone"])
        entity = map_entity_status(row.get("ownership_type"), row.get("owner_name"))
        contact = {
            "phone": row["phone"],
            "borrower_name": row.get("principal_name") or row.get("owner_name"),
            "entity_name": row.get("owner_name") if entity in ("LLC", "CORPORATION", "TRUST") else None,
            "email": row.get("email") or None,
        }
        try:
            push(contact, ghl_tags(row))
            outcome["pushed"] += 1
        except Exception as exc:  # class only: GHL errors can embed contact values
            logger.warning("[fa-export] GHL push failed for radar_id=%s (%s)", row.get("radar_id"), type(exc).__name__)
            outcome["failed"] += 1
    return outcome


def merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per property (radar_id): signals and sources joined, blanks filled from the later row."""
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        kept = merged.setdefault(row["radar_id"], row)
        if kept is row:
            continue
        for column in ("signals", "source", "campaign"):
            parts = str(kept[column]).split(SIGNAL_SEPARATOR)
            parts += [p for p in str(row[column]).split(SIGNAL_SEPARATOR) if p not in parts]
            kept[column] = SIGNAL_SEPARATOR.join(parts)
        for column, value in row.items():
            if kept.get(column) in (None, "") and value not in (None, ""):
                kept[column] = value
    return list(merged.values())


_COLUMNS_SQL = """
           property_address, city, zip, owner_name, principal_name, ownership_type, lender_name, loan_amount,
           loan_recorded_date, est_maturity_date, first_seen_at"""

_DAILY_SQL = """
    SELECT id, radar_id, state, county_name, county_fips, mailing_address, mailing_city, mailing_state,
           mailing_zip, campaign, property_id,""" + _COLUMNS_SQL + """
    FROM property_radar_records
    WHERE status = 'active' AND id > :after_id AND state = ANY(:states) {window}
    ORDER BY id
    LIMIT :limit
"""
_DAILY_WINDOW = "AND ((first_seen_at >= :start AND first_seen_at < :end) OR campaign = ANY(:maturity))"

_LIST_SQL = """
    SELECT id, radar_id, state, county AS county_name, owner_mailing_address AS mailing_address,
           owner_mailing_city AS mailing_city, owner_mailing_state AS mailing_state,
           owner_mailing_zip AS mailing_zip, list_key AS campaign,""" + _COLUMNS_SQL + """
    FROM lending.pr_list_records
    WHERE qualifies AND list_key = ANY(:lists) AND id > :after_id AND state = ANY(:states) {window}
    ORDER BY id
    LIMIT :limit
"""
_LIST_WINDOW = "AND first_seen_at >= :start AND first_seen_at < :end"

_PORTFOLIO_SQL = """
    SELECT owner, count(DISTINCT radar_id) AS properties FROM (
        SELECT upper(owner_name) AS owner, radar_id FROM property_radar_records
        WHERE status = 'active' AND ownership_type = 'Corporate' AND upper(owner_name) = ANY(:owners)
        {list_part}
    ) leads GROUP BY 1
"""
_PORTFOLIO_LIST_PART = """UNION
        SELECT upper(owner_name), radar_id FROM lending.pr_list_records
        WHERE qualifies AND list_key = ANY(:lists) AND ownership_type = 'Corporate'
          AND upper(owner_name) = ANY(:owners)"""


def _has_lists(session: Session) -> bool:
    """The calling-list table is created by its own migration; without it only the daily pull is exported."""
    return session.execute(text("SELECT to_regclass('lending.pr_list_records')")).scalar() is not None


def _pages(session: Session, sql: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
    statement, after_id = text(sql), 0
    while True:
        page = session.execute(statement, {**params, "after_id": after_id, "limit": PAGE_SIZE}).mappings().all()
        if not page:
            return
        after_id = page[-1]["id"]
        yield from (dict(r) for r in page)


def _first_seen(record: Mapping[str, Any]) -> date:
    return record["first_seen_at"].astimezone(ET).date()


def _iter_leads(session: Session, day: date, *, backfill: bool, lists: bool) -> Iterator[dict[str, Any]]:
    start, end = day_window(day)
    states = sorted(ENABLED_STATES)
    daily = _DAILY_SQL.format(window="" if backfill else _DAILY_WINDOW)
    daily_params = {"states": states} if backfill else {
        "states": states, "start": start, "end": end, "maturity": sorted(MATURITY_CAMPAIGNS)}
    for record in _pages(session, daily, daily_params):
        if in_backfill(record, day) if backfill else due_on(record, day, _first_seen(record)):
            yield {**record, "source": SOURCE_DAILY}
    if not lists:
        return
    listed = _LIST_SQL.format(window="" if backfill else _LIST_WINDOW)
    list_params = {"states": states, "lists": list(EXPORTED_LISTS)}
    if not backfill:
        list_params.update(start=start, end=end)
    for record in _pages(session, listed, list_params):
        yield {**record, "source": SOURCE_LIST_PREFIX + record["campaign"], "first_seen": _first_seen(record),
               "county_fips": county_fips(record["state"], str(record.get("county_name") or "").upper())}


def _chunks(items: list, size: int = PAGE_SIZE) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _portfolio_sizes(session: Session, records: list[dict[str, Any]], *, lists: bool) -> dict[str, int]:
    owners = sorted({str(r["owner_name"]).upper() for r in records
                     if r.get("owner_name") and r.get("ownership_type") == "Corporate"})
    statement = text(_PORTFOLIO_SQL.format(list_part=_PORTFOLIO_LIST_PART if lists else ""))
    sizes: dict[str, int] = {}
    for chunk in _chunks(owners):
        params = {"owners": chunk, **({"lists": list(EXPORTED_LISTS)} if lists else {})}
        sizes.update((r["owner"], r["properties"]) for r in session.execute(statement, params).mappings())
    return sizes


def collect(session: Session, day: date, *, backfill: bool = False) -> list[dict[str, Any]]:
    """The day's export rows (or, with ``backfill``, every lead that qualifies as of ``day``), one per property."""
    lists = _has_lists(session)
    records = list(_iter_leads(session, day, backfill=backfill, lists=lists))
    sizes = _portfolio_sizes(session, records, lists=lists)
    contacts: dict[str, dict[str, list]] = {}
    for chunk in _chunks(sorted({r["radar_id"] for r in records})):
        contacts.update(_load_trace_contacts(session, chunk))
    rows = []
    for record in records:
        contact = contacts.get(record["radar_id"], {"phones": [], "emails": []})
        corporate = record.get("ownership_type") == "Corporate"
        size = sizes.get(str(record.get("owner_name") or "").upper(), 0) if corporate else 0
        row = build_row(record, day=day, portfolio_size=size, phone=_first_phone(contact["phones"]),
                        email=contact["emails"][0].lower() if contact["emails"] else None)
        if row:
            rows.append(row)
    return merge_rows(rows)


def suppressed_among(session: Session, rows: list[dict[str, Any]]) -> set[str]:
    return suppressed_phones(session, sorted({r["phone"] for r in rows if r["phone"]}))
