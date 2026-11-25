"""Readers for the morning list: one function per tier, each returning ``ListItem`` candidates (D2, D5, D15, D17).

``read_candidates`` runs every reader in its own savepoint (the digest's ``_section`` rule, D12): a missing table or
failed query drops that tier only and names it in ``unavailable``. Line types and stored emails are added in one
batch read of ``lending.contacts`` (Tracerfy / 0.11 line type), and Clay results from ``lending.clay_enrichments``.
Test rows (``is_test``) are never listed.
"""
from __future__ import annotations

import logging
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Optional, Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from config.lending_closing import OWNER_LABELS
from config.lending_morning_list import (
    ACTIVE_DEAL, ACTIVE_STAGES_LATEST_FIRST, BOOKED_TODAY, CALLBACK, CALLBACK_WINDOW, CLOSING_BLOCKER, COLD_TOP,
    NEW_WARM_LEAD,
)
from config.lending_queues import DIAL_ORDER
from src.lending.borrower_score import facts_from_lendingflow_lead, facts_from_web_lead, score_borrower
from src.lending.morning_list.items import ListItem
from src.lending.morning_list.time import ET, et_day_start
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CALLBACK_CODE = "CALLBACK_REQUESTED"

_NAME_BY_PHONE = """COALESCE(
    (SELECT NULLIF(trim(coalesce(l.first_name, '') || ' ' || coalesce(l.last_name, '')), '')
       FROM lending.lendingflow_leads l WHERE l.phone = {phone} ORDER BY l.received_at DESC LIMIT 1),
    (SELECT NULLIF(trim(w.name), '') FROM lending.web_leads w WHERE w.phone = {phone}
      ORDER BY w.received_at DESC LIMIT 1),
    (SELECT coalesce(NULLIF(trim(d.borrower_name), ''), d.entity_name) FROM lending.dialer_load_records d
      WHERE d.phone = {phone} ORDER BY d.id DESC LIMIT 1))"""

_ACTIVE_LOANS = text("""
    SELECT id, borrower_first_name, borrower_last_name, borrower_phone, borrower_email, product, loan_amount,
           property_city, stage, status_at
      FROM lending.loan_files
     WHERE closed_at IS NULL AND NOT is_test AND stage IN :stages
""").bindparams(bindparam("stages", expanding=True))
_BOOKED_TODAY = text(f"""
    SELECT c.booking_ref, c.first_name, c.phone, c.slot_start, {_NAME_BY_PHONE.format(phone="c.phone")} AS full_name
      FROM (SELECT booking_ref, max(first_name) AS first_name, max(contact_phone) AS phone,
                   max(slot_start_utc) AS slot_start
              FROM lending.booking_messages
             WHERE slot_start_utc >= :start AND slot_start_utc < :end
             GROUP BY booking_ref) c
""")
_CALLBACKS = text(f"""
    SELECT c.id, c.phone, c.caller_name, coalesce(c.call_started_at, c.created_at) AS called_at,
           {_NAME_BY_PHONE.format(phone="c.phone")} AS full_name
      FROM lending.call_dispositions c
     WHERE c.disposition = :code AND c.phone IS NOT NULL AND coalesce(c.call_started_at, c.created_at) >= :since
       AND NOT EXISTS (SELECT 1 FROM lending.call_dispositions later
                        WHERE later.phone = c.phone
                          AND coalesce(later.call_started_at, later.created_at)
                              > coalesce(c.call_started_at, c.created_at))
""")
_WARM_LENDINGFLOW = text("""
    SELECT l.id, l.first_name, l.last_name, l.phone, l.email, l.loan_type, l.loan_amount, l.loan_amount_range,
           l.property_city, l.property_state, l.received_at, e.fit, e.routing_tag
      FROM lending.lendingflow_leads l LEFT JOIN lending.lendingflow_enrichment e ON e.lead_id = l.id
     WHERE l.received_at >= :since AND l.handled_in_ghl_at IS NULL AND NOT l.suppressed AND NOT l.is_test
       AND l.phone IS NOT NULL
""")
_WARM_WEB = text("""
    SELECT id, name, phone, email, deal_type, loan_amount, property_city, property_state, credit_band,
           completed_projects_3y, received_at
      FROM lending.web_leads
     WHERE received_at >= :since AND handled_in_ghl_at IS NULL AND NOT suppressed AND NOT is_test
       AND phone IS NOT NULL
""")
_COLD_TOP = text("""
    SELECT d.id, d.pool, d.phone, d.borrower_name, d.entity_name, d.property_address, d.estimated_loan_value
      FROM lending.dialer_load_records d
      JOIN lending.contacts ct ON ct.phone = d.phone
     WHERE d.active AND d.pool IN :pools AND lower(coalesce(ct.line_type, '')) = 'mobile'
       AND NOT ct.do_not_contact AND NOT ct.is_test
       AND NOT EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = d.phone)
     ORDER BY CASE d.pool {when} END, d.id
     LIMIT :limit
""".format(when=" ".join(f"WHEN :pool{n} THEN {n}" for n in range(len(DIAL_ORDER))))).bindparams(
    bindparam("pools", expanding=True))
_CONTACTS = text("SELECT phone, line_type, email FROM lending.contacts WHERE phone IN :phones"
                 ).bindparams(bindparam("phones", expanding=True))
_CLAY_FOUND = text("""
    SELECT lead_ref, field, value FROM lending.clay_enrichments
     WHERE status = 'found' AND value IS NOT NULL AND lead_ref IN :refs
""").bindparams(bindparam("refs", expanding=True))

_CITY = re.compile(r",\s*([^,]+?)(?:\s+[A-Z]{2}\s+\d{5})?\s*$")


def _aware(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _name(*parts: Optional[str]) -> Optional[str]:
    joined = " ".join(p.strip() for p in parts if p and p.strip())
    return joined or None


def _money(amount: Any) -> Optional[str]:
    try:
        value = int(amount)
    except (TypeError, ValueError):
        return None
    return f"${value / 1000:,.0f}k" if value >= 1000 else f"${value:,}"


def _brief(*parts: Optional[str]) -> str:
    return ", ".join(p for p in parts if p)


def _days(since: Optional[datetime], now: datetime) -> int:
    return max(0, (now - since).days) if since else 0


def blockers_from_stuck_deals(deals: Iterable[Any]) -> list[ListItem]:
    """P2.4 stuck deals (``closing.blockers.stuck_deals``), oldest first, as one short line each (D14)."""
    items = []
    for deal in deals:
        owner = OWNER_LABELS.get(getattr(deal.owner, "value", str(deal.owner)), str(deal.owner))
        items.append(ListItem(
            ref=f"opp:{deal.opportunity_id}", reason=CLOSING_BLOCKER, name=deal.first_name,
            phone=normalize(deal.phone) if deal.phone else None, line_type=None, email=deal.email,
            sort_key=(-deal.days,),
            brief=_brief(f"{deal.stage_name}, stuck {deal.days} days", f"waiting on {owner}",
                         "see the 7 AM blocker card")))
    return items


def read_active_deals(db: Session, now: datetime) -> list[ListItem]:
    rows = db.execute(_ACTIVE_LOANS, {"stages": list(ACTIVE_STAGES_LATEST_FIRST)}).mappings().all()
    items = []
    for row in rows:
        since = _aware(row["status_at"])
        stage = str(row["stage"])
        items.append(ListItem(
            ref=f"loan:{row['id']}", reason=ACTIVE_DEAL,
            name=_name(row["borrower_first_name"], row["borrower_last_name"]),
            phone=normalize(row["borrower_phone"]) if row["borrower_phone"] else None, line_type=None,
            email=row["borrower_email"],
            sort_key=(ACTIVE_STAGES_LATEST_FIRST.index(stage), -_days(since, now)),
            brief=_brief(f"{stage.replace('_', ' ').capitalize()}, {_days(since, now)} days in stage",
                         str(row["product"]).replace("_", " ") if row["product"] else None,
                         _money(row["loan_amount"]), row["property_city"])))
    return items


def read_booked_today(db: Session, now: datetime) -> list[ListItem]:
    start = et_day_start(now)
    rows = db.execute(_BOOKED_TODAY, {"start": start, "end": start + timedelta(days=1)}).mappings().all()
    items = []
    for row in rows:
        slot = _aware(row["slot_start"])
        local = slot.astimezone(ET) if slot else None
        items.append(ListItem(
            ref=f"bk:{row['booking_ref']}", reason=BOOKED_TODAY, name=row["full_name"] or row["first_name"],
            phone=normalize(row["phone"]) if row["phone"] else None, line_type=None,
            sort_key=(0, slot.timestamp() if slot else 0),  # tier 3: today's booked calls by time, then callbacks
            brief=f"call at {local.hour % 12 or 12}:{local:%M %p}" if local else "call today"))
    return items


def read_callbacks(db: Session, now: datetime) -> list[ListItem]:
    rows = db.execute(_CALLBACKS, {"code": CALLBACK_CODE, "since": now - CALLBACK_WINDOW}).mappings().all()
    items = []
    for row in rows:
        called = _aware(row["called_at"])
        items.append(ListItem(
            ref=f"cb:{row['id']}", reason=CALLBACK, name=row["full_name"], phone=normalize(row["phone"]),
            line_type=None, caller_name=row["caller_name"], sort_key=(1, called.timestamp() if called else 0),
            brief=f"asked for a callback {_days(called, now)} days ago" if _days(called, now) else
            "asked for a callback today"))
    return items


def _fit_text(fit: Any) -> Optional[str]:
    score = fit.get("score") if isinstance(fit, dict) else None
    return f"lender fit {score}%" if isinstance(score, (int, float)) else None


def read_warm_leads(db: Session, now: datetime, *, lead_days: int) -> list[ListItem]:
    since = now - timedelta(days=lead_days)
    items = []
    for row in db.execute(_WARM_LENDINGFLOW, {"since": since}).mappings():
        received = _aware(row["received_at"])
        score = score_borrower(facts_from_lendingflow_lead(row)).score
        items.append(ListItem(
            ref=f"lf:{row['id']}", reason=NEW_WARM_LEAD, name=_name(row["first_name"], row["last_name"]),
            phone=normalize(row["phone"]), line_type=None, email=row["email"], received_at=received,
            sort_key=(-score, -(received.timestamp() if received else 0)),
            brief=_brief(row["loan_type"], row["loan_amount_range"] or _money(row["loan_amount"]),
                         row["property_city"], _fit_text(row["fit"]),
                         "address and close date in" if row["routing_tag"] == "FULL_MACHINE" else None)))
    for row in db.execute(_WARM_WEB, {"since": since}).mappings():
        received = _aware(row["received_at"])
        score = score_borrower(facts_from_web_lead(row)).score
        place = ", ".join(p for p in (row["property_city"], row["property_state"]) if p) or None
        items.append(ListItem(
            ref=f"web:{row['id']}", reason=NEW_WARM_LEAD, name=row["name"], phone=normalize(row["phone"]),
            line_type=None, email=row["email"], received_at=received,
            sort_key=(-score, -(received.timestamp() if received else 0)),
            brief=_brief(row["deal_type"], _money(row["loan_amount"]), place,
                         f"credit {row['credit_band']}" if row["credit_band"] else None,
                         f"projects: {row['completed_projects_3y']}" if row["completed_projects_3y"] else None)))
    return items


def _city(address: Optional[str]) -> Optional[str]:
    found = _CITY.search(address or "")
    return found.group(1).strip().title() if found else None


def read_cold_top(db: Session, *, limit: int) -> list[ListItem]:
    """Padding: active BatchDialer records with a Mobile number, queue order first (D5)."""
    order = {f"pool{n}": pool for n, pool in enumerate(DIAL_ORDER)}
    rows = db.execute(_COLD_TOP, {"pools": list(DIAL_ORDER), "limit": limit, **order}).mappings().all()
    items = []
    for row in rows:
        pool = str(row["pool"])
        items.append(ListItem(
            ref=f"dlr:{row['id']}", reason=COLD_TOP, name=row["borrower_name"] or None,
            entity_name=row["entity_name"], phone=normalize(row["phone"]), line_type="mobile",
            property_address=row["property_address"],
            sort_key=(DIAL_ORDER.index(pool), row["id"]),
            brief=_brief(pool.replace("_", " "), row["entity_name"] if row["borrower_name"] else None,
                         _city(row["property_address"]), _money(row["estimated_loan_value"]))))
    return items


def _with_contacts(db: Session, items: list[ListItem]) -> list[ListItem]:
    """Line type (and a stored email when the lead has none) from ``lending.contacts``, one batch read."""
    phones = sorted({i.phone for i in items if i.phone})
    known = {row["phone"]: row for row in db.execute(_CONTACTS, {"phones": phones}).mappings()} if phones else {}
    out = []
    for item in items:
        contact = known.get(item.phone or "")
        if contact is None:
            out.append(item)
            continue
        out.append(replace(item, line_type=item.line_type or contact["line_type"],
                           email=item.email or contact["email"] or None))
    return out


def _with_clay(db: Session, items: list[ListItem]) -> list[ListItem]:
    """What Clay found (``lending.clay_enrichments``): an email for an item with none, and the verified owner's name
    for an LLC line with no person."""
    refs = [i.ref for i in items if not i.email or not i.name]
    found: dict[tuple[str, str], str] = {(row["lead_ref"], row["field"]): row["value"] for row in db.execute(
        _CLAY_FOUND, {"refs": refs}).mappings()} if refs else {}
    out = []
    for item in items:
        email = None if item.email else found.get((item.ref, "email"))
        owner = None if item.name else found.get((item.ref, "llc_owner"))
        out.append(replace(item, email=email or item.email, email_source="clay" if email else item.email_source,
                           name=owner or item.name))
    return out


def _tier(db: Session, name: str, read: Callable[[], list[ListItem]], unavailable: list[str]) -> list[ListItem]:
    try:
        with db.begin_nested():
            return read()
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.warning("[morning-list] %s not available (%s)", name, type(exc).__name__)
        unavailable.append(name)
        return []


def read_candidates(db: Session, now: datetime, *, lead_days: int, size: int,
                    stuck_deals: Callable[[Session, datetime], Sequence[Any]]) -> tuple[list[ListItem], list[str]]:
    """Every tier's candidates plus line types and emails, and the names of the tiers that could not be read."""
    unavailable: list[str] = []
    items = (_tier(db, "stuck deals", lambda: blockers_from_stuck_deals(stuck_deals(db, now)), unavailable)
             + _tier(db, "active deals", lambda: read_active_deals(db, now), unavailable)
             + _tier(db, "booked calls", lambda: read_booked_today(db, now), unavailable)
             + _tier(db, "callbacks", lambda: read_callbacks(db, now), unavailable)
             + _tier(db, "new leads", lambda: read_warm_leads(db, now, lead_days=lead_days), unavailable)
             + _tier(db, "calling queue", lambda: read_cold_top(db, limit=size), unavailable))
    for name, step in (("contact details", _with_contacts), ("Clay results", _with_clay)):
        try:
            with db.begin_nested():
                items = step(db, items)
        except Exception as exc:  # class only
            logger.warning("[morning-list] %s not available (%s)", name, type(exc).__name__)
            if step is _with_contacts:  # Clay is optional (D9): its absence is logged, not shown to the client
                unavailable.append(name)
    return items, unavailable
