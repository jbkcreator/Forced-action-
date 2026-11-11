"""Nightly ledger export to Google Drive (Cora brief P1.2): the agents' outcome feed for their nightly learning loop.

One CSV per table goes to Drive ``NDL Ledger Exports/{ET date}/``: contacts, consents, touches, dispositions, outcomes,
scores. No full phone number or email and no phone hash is ever written: a row is keyed by ``ghl_contact_id`` (when a
lead carries one), the internal ``contact_id`` and the last 4 digits of the phone.

* contacts, consents, scores: full snapshot each night.
* touches, dispositions, outcomes: the ET calendar day being exported (yesterday, run after midnight ET).
* scores: the borrower score (``borrower_score.py``) worked out at export time per LendingFlow / website lead, with
  the inputs it was missing. ``lead_scoring.score_lead`` is not wired to stored inputs, so it is not exported.
* Test rows (``is_test``) are left out.

Read-only on the database. The Drive upload is a seam (``Uploader``); ``src/lending/drive_upload.py`` is the default.
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.lending.borrower_score import facts_from_lendingflow_lead, facts_from_web_lead, score_borrower
from src.services.phone_utils import normalize

ET = ZoneInfo("America/New_York")
KEY_COLUMNS = ("ghl_contact_id", "contact_id", "phone_last4")

TABLES: dict[str, tuple[str, ...]] = {
    "contacts": ("do_not_contact", "nurture", "line_type", "last_dnc_scrub", "created_at"),
    "consents": ("kind", "detail", "at", "revoked_at"),
    "touches": ("at", "kind", "direction", "outcome"),
    "dispositions": ("dialer_call_id", "direction", "disposition", "disposition_raw", "talk_duration_sec",
                     "call_source", "queue", "call_started_at"),
    "outcomes": ("at", "kind", "outcome", "ref"),
    "scores": ("lead_source", "lead_id", "borrower_score", "missing", "created_at"),
}

Uploader = Callable[[str, str, str, bytes], None]  # (parent folder id, dated subfolder, file name, bytes)


@dataclass(frozen=True)
class ContactKey:
    contact_id: Optional[int]
    ghl_contact_id: Optional[str]


@dataclass
class LedgerData:
    keys: dict[str, ContactKey] = field(default_factory=dict)  # normalised phone -> ids
    contacts: list[dict] = field(default_factory=list)
    consents: list[dict] = field(default_factory=list)
    touches: list[dict] = field(default_factory=list)
    dispositions: list[dict] = field(default_factory=list)
    outcomes: list[dict] = field(default_factory=list)
    scores: list[dict] = field(default_factory=list)


def export_day(now: datetime) -> date:
    """The ET day that just ended: the run is after midnight ET."""
    return now.astimezone(ET).date() - timedelta(days=1)


def day_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time(), ET)
    return start.astimezone(timezone.utc), (start + timedelta(days=1)).astimezone(timezone.utc)


def key_columns(phone: Optional[str], keys: Mapping[str, ContactKey]) -> dict[str, Any]:
    if not phone:
        return {"ghl_contact_id": "", "contact_id": "", "phone_last4": ""}
    key = keys.get(phone)
    return {
        "ghl_contact_id": (key.ghl_contact_id or "") if key else "",
        "contact_id": key.contact_id if key and key.contact_id is not None else "",
        "phone_last4": phone[-4:],
    }


def _cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (list, tuple)):
        return ";".join(str(v) for v in value)
    return value


def _csv(columns: Sequence[str], rows: Iterable[Mapping[str, Any]], keys: Mapping[str, ContactKey]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow((*KEY_COLUMNS, *columns))
    for row in rows:
        ids = key_columns(row.get("phone"), keys)
        writer.writerow([*(ids[c] for c in KEY_COLUMNS), *(_cell(row.get(c)) for c in columns)])
    return out.getvalue().encode("utf-8")


def build_files(data: LedgerData) -> dict[str, bytes]:
    """File name -> CSV bytes, one per table, header always present."""
    return {f"{name}.csv": _csv(columns, getattr(data, name), data.keys) for name, columns in TABLES.items()}


def publish(files: Mapping[str, bytes], *, day: date, folder_id: str, upload: Uploader) -> list[str]:
    if not folder_id:
        raise ValueError("no Drive folder")
    for name, data in files.items():
        upload(folder_id, day.isoformat(), name, data)
    return list(files)


# ── database reads ──────────────────────────────────────────────────────────

_KEYS = text("""
    SELECT c.phone, c.id, g.ghl_contact_id
      FROM lending.contacts c
      LEFT JOIN LATERAL (
          SELECT ghl_contact_id FROM (
              SELECT ghl_contact_id, created_at FROM lending.web_leads
               WHERE phone = c.phone AND ghl_contact_id IS NOT NULL
              UNION ALL
              SELECT ghl_contact_id, created_at FROM lending.lendingflow_leads
               WHERE phone = c.phone AND ghl_contact_id IS NOT NULL
          ) x ORDER BY created_at DESC LIMIT 1) g ON true
     WHERE NOT c.is_test
""")
_LEAD_GHL = text("""
    SELECT phone, ghl_contact_id FROM (
        SELECT phone, ghl_contact_id, created_at FROM lending.web_leads WHERE ghl_contact_id IS NOT NULL AND NOT is_test
        UNION ALL
        SELECT phone, ghl_contact_id, created_at FROM lending.lendingflow_leads
         WHERE ghl_contact_id IS NOT NULL AND NOT is_test
    ) x ORDER BY created_at
""")
_CONTACTS = text("""
    SELECT phone, do_not_contact, nurture, line_type, last_dnc_scrub, created_at
      FROM lending.contacts WHERE NOT is_test ORDER BY id
""")
_CONSENTS = text("""
    SELECT phone, 'text_consent' AS kind, source AS detail, captured_at AS at, revoked_at
      FROM lending.text_consents
    UNION ALL
    SELECT phone, 'suppression', reason || ':' || source_channel, created_at, NULL
      FROM lending.suppression_list WHERE NOT is_test AND phone IS NOT NULL
    ORDER BY 4
""")
_TOUCHES = text("""
    SELECT phone, at, kind, direction, outcome FROM (
        SELECT phone, COALESCE(call_started_at, call_ended_at, created_at) AS at, 'call' AS kind, direction,
               COALESCE(disposition, disposition_raw) AS outcome
          FROM lending.call_dispositions
        UNION ALL
        SELECT phone, COALESCE(decided_at, created_at), 'text', 'outbound', status
          FROM lending.missed_call_events
    ) t WHERE at >= :start AND at < :end ORDER BY at
""")
_DISPOSITIONS = text("""
    SELECT phone, dialer_call_id, direction, disposition, disposition_raw, talk_duration_sec, call_source, queue,
           call_started_at
      FROM lending.call_dispositions
     WHERE COALESCE(call_started_at, created_at) >= :start AND COALESCE(call_started_at, created_at) < :end
     ORDER BY COALESCE(call_started_at, created_at)
""")
_OUTCOMES = text("""
    SELECT phone, at, kind, outcome, ref FROM (
        SELECT phone, event_at AS at, 'stage' AS kind, stage_name AS outcome, ghl_opportunity_id AS ref
          FROM lending.ghl_stage_events
        UNION ALL
        SELECT l.phone, e.created_at, 'routing', e.action, e.lead_id::text
          FROM lending.file_routing_events e JOIN lending.lendingflow_leads l ON l.id = e.lead_id
         WHERE NOT l.is_test
    ) t WHERE at >= :start AND at < :end ORDER BY at
""")
_SCORE_LEADS = text("""
    SELECT 'lendingflow' AS lead_source, id AS lead_id, phone, property_state, loan_amount,
           NULL AS completed_projects_3y, created_at
      FROM lending.lendingflow_leads WHERE NOT is_test
    UNION ALL
    SELECT 'web', id, phone, property_state, loan_amount, completed_projects_3y, created_at
      FROM lending.web_leads WHERE NOT is_test
    ORDER BY 1, 2
""")


def _rows(db, query, **params) -> list[dict]:
    rows = []
    for row in db.execute(query, params).mappings():
        item = dict(row)
        item["phone"] = normalize(item["phone"]) if item.get("phone") else None
        rows.append(item)
    return rows


def _keys(db) -> dict[str, ContactKey]:
    keys: dict[str, ContactKey] = {}
    for phone, ghl in db.execute(_LEAD_GHL):  # leads with no contacts row still get their GHL id
        if phone and (p := normalize(phone)):
            keys[p] = ContactKey(contact_id=None, ghl_contact_id=ghl)
    for phone, contact_id, ghl in db.execute(_KEYS):
        if phone and (p := normalize(phone)):
            keys[p] = ContactKey(contact_id=contact_id, ghl_contact_id=ghl or (keys[p].ghl_contact_id if p in keys
                                                                               else None))
    return keys


def _score(lead: dict) -> dict:
    facts = facts_from_lendingflow_lead(lead) if lead["lead_source"] == "lendingflow" else facts_from_web_lead(lead)
    result = score_borrower(facts)
    return {"phone": lead["phone"], "lead_source": lead["lead_source"], "lead_id": lead["lead_id"],
            "borrower_score": result.score, "missing": list(result.missing), "created_at": lead["created_at"]}


def collect(db, day: date) -> LedgerData:
    start, end = day_window(day)
    return LedgerData(
        keys=_keys(db),
        contacts=_rows(db, _CONTACTS),
        consents=_rows(db, _CONSENTS),
        touches=_rows(db, _TOUCHES, start=start, end=end),
        dispositions=_rows(db, _DISPOSITIONS, start=start, end=end),
        outcomes=_rows(db, _OUTCOMES, start=start, end=end),
        scores=[_score(lead) for lead in _rows(db, _SCORE_LEADS)],
    )
