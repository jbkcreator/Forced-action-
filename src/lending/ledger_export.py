"""Nightly ledger export to Google Drive (Cora brief P1.2): the agents' outcome feed for their nightly learning loop.

One CSV per table goes to Drive ``NDL Ledger Exports/{ET date}/``: contacts, consents, touches, dispositions, outcomes,
scores. No full phone number or email and no phone hash is ever written: a row is keyed by ``ghl_contact_id`` (when a
lead carries one), the internal ``contact_id`` and the last 4 digits of the phone.

* contacts, consents, scores: full snapshot each night.
* touches, dispositions, outcomes: the ET calendar day being exported (yesterday, run after midnight ET).
* scores: the borrower score (``borrower_score.py``) worked out at export time per LendingFlow / website lead, with
  the inputs it was missing. ``lead_scoring.score_lead`` is not wired to stored inputs, so it is not exported.
* touches include Cora's own sends (``pending_actions`` sent / failed: SMS, email, GHL actions), keyed by the GHL
  id the draft names when there is no phone (an email-only send).
* GHL ids come from website and LendingFlow leads and from dialer loads the GHL sync pushed.
* Test rows are left out: an ``is_test`` row, and on tables with no such flag (calls, texts, consents, stage events)
  any row whose phone belongs to a test contact or test lead.

Read-only on the database. The Drive upload is a seam (``Uploader``); ``src/lending/drive_upload.py`` is the default.
"""
from __future__ import annotations

import csv
import io
import re
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
    "scores": ("lead_source", "lead_id", "borrower_score", "missing", "received_at"),
}

# Columns that hold text a dialer, GHL or a person typed (not our own codes or ids): phone-like digit runs and email
# addresses in them are redacted, since nothing upstream whitelists these values.
FREE_TEXT_COLUMNS = frozenset({"disposition_raw", "outcome", "detail"})
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PHONE_LIKE = re.compile(r"\+?\d[\d\s().-]{5,}\d")
REDACTED = "[redacted]"

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


def key_columns(phone: Optional[str], keys: Mapping[str, ContactKey], ghl_contact_id: Optional[str] = None
                ) -> dict[str, Any]:
    """``ghl_contact_id`` is the row's own GHL id (a Cora send names its contact), else the phone's."""
    if not phone:
        return {"ghl_contact_id": ghl_contact_id or "", "contact_id": "", "phone_last4": ""}
    key = keys.get(phone)
    return {
        "ghl_contact_id": ghl_contact_id or ((key.ghl_contact_id or "") if key else ""),
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


def redact(value: Any) -> Any:
    """Free text with emails and runs of 7+ digits (phone numbers, however formatted) replaced."""
    if not isinstance(value, str):
        return value
    text_ = _EMAIL.sub(REDACTED, value)
    return _PHONE_LIKE.sub(lambda m: REDACTED if sum(c.isdigit() for c in m.group()) >= 7 else m.group(), text_)


def _csv(columns: Sequence[str], rows: Iterable[Mapping[str, Any]], keys: Mapping[str, ContactKey]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow((*KEY_COLUMNS, *columns))
    for row in rows:
        ids = key_columns(row.get("phone"), keys, row.get("ghl_contact_id"))
        writer.writerow([*(ids[c] for c in KEY_COLUMNS), *(_cell(redact(row.get(c)) if c in FREE_TEXT_COLUMNS else row.get(c))
                                                       for c in columns)])
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

_KEYS = text("SELECT phone, id FROM lending.contacts WHERE NOT is_test")
_LEAD_GHL = text("""
    SELECT phone, ghl_contact_id FROM (
        SELECT phone, ghl_contact_id, received_at FROM lending.web_leads WHERE ghl_contact_id IS NOT NULL AND NOT is_test
        UNION ALL
        SELECT phone, ghl_contact_id, received_at FROM lending.lendingflow_leads
         WHERE ghl_contact_id IS NOT NULL AND NOT is_test
        UNION ALL
        SELECT phone, ghl_contact_id, COALESCE(ghl_synced_at, loaded_at) FROM lending.dialer_load_records
         WHERE ghl_contact_id IS NOT NULL
    ) x ORDER BY received_at
""")
_TEST_PHONES = text("""
    SELECT phone FROM lending.contacts WHERE is_test
    UNION SELECT phone FROM lending.web_leads WHERE is_test
    UNION SELECT phone FROM lending.lendingflow_leads WHERE is_test
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
    SELECT phone, at, kind, direction, outcome, ghl_contact_id FROM (
        SELECT phone, COALESCE(call_started_at, call_ended_at, created_at) AS at, 'call' AS kind, direction,
               COALESCE(disposition, disposition_raw) AS outcome, NULL::text AS ghl_contact_id
          FROM lending.call_dispositions
        UNION ALL
        SELECT phone, COALESCE(decided_at, created_at), 'text', 'outbound', status, NULL
          FROM lending.missed_call_events
        UNION ALL
        SELECT recipient_phone, COALESCE(executed_at, updated_at), 'cora_' || channel, 'outbound', status,
               COALESCE(payload->>'ghl_contact_id',
                        CASE WHEN contact_ref LIKE 'ghl:%' THEN substr(contact_ref, 5) END)
          FROM lending.pending_actions WHERE status IN ('sent', 'failed')
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
           NULL AS completed_projects_3y, received_at
      FROM lending.lendingflow_leads WHERE NOT is_test
    UNION ALL
    SELECT 'web', id, phone, property_state, loan_amount, completed_projects_3y, received_at
      FROM lending.web_leads WHERE NOT is_test
    ORDER BY 1, 2
""")


def _rows(db, query, test_phones: frozenset[str] = frozenset(), **params) -> list[dict]:
    rows = []
    for row in db.execute(query, params).mappings():
        item = dict(row)
        item["phone"] = normalize(item["phone"]) if item.get("phone") else None
        if item["phone"] not in test_phones:
            rows.append(item)
    return rows


def _test_phones(db) -> frozenset[str]:
    return frozenset(p for (raw,) in db.execute(_TEST_PHONES) if raw and (p := normalize(raw)))


def _keys(db) -> dict[str, ContactKey]:
    """Phone -> contact id and the newest non-test lead's GHL id (a lead with no contacts row still gets its id)."""
    ghl: dict[str, str] = {}
    for phone, ghl_id in db.execute(_LEAD_GHL):  # oldest first: the newest lead wins
        if phone and (p := normalize(phone)):
            ghl[p] = ghl_id
    ids = {p: cid for phone, cid in db.execute(_KEYS) if phone and (p := normalize(phone))}
    return {p: ContactKey(contact_id=ids.get(p), ghl_contact_id=ghl.get(p)) for p in ghl.keys() | ids.keys()}


def _score(lead: dict) -> dict:
    facts = facts_from_lendingflow_lead(lead) if lead["lead_source"] == "lendingflow" else facts_from_web_lead(lead)
    result = score_borrower(facts)
    return {"phone": lead["phone"], "lead_source": lead["lead_source"], "lead_id": lead["lead_id"],
            "borrower_score": result.score, "missing": list(result.missing), "received_at": lead["received_at"]}


def collect(db, day: date) -> LedgerData:
    start, end = day_window(day)
    test = _test_phones(db)
    return LedgerData(
        keys=_keys(db),
        contacts=_rows(db, _CONTACTS, test),
        consents=_rows(db, _CONSENTS, test),
        touches=_rows(db, _TOUCHES, test, start=start, end=end),
        dispositions=_rows(db, _DISPOSITIONS, test, start=start, end=end),
        outcomes=_rows(db, _OUTCOMES, test, start=start, end=end),
        scores=[_score(lead) for lead in _rows(db, _SCORE_LEADS, test)],
    )
