"""Cora's tools over the lending data (read-only), her standing-rule memory, and gated drafts.

Read tools run inside a ``READ ONLY`` transaction with a statement timeout, so even a bad query
cannot change or stall the shared database. Phone numbers leave as the last four digits only
(the tool registry also masks any long digit run in a result). Draft tools never contact anyone:
they resolve the recipient from the database by ``lead_ref`` (the model never types a phone
number or address) and return an :class:`EgressDraft` for the send gate.
"""
from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.lending_compliance import DEFAULT_TZ
from packages.agent_core.governance import SafetyLevel
from packages.agent_core.memory import AgentMemory
from packages.agent_core.tools import EgressDraft, Tool, ToolContext, ToolInputError
from src.lending.contracts import BorrowerProfile, LoanRequest
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.scoreboard import build_scoreboard
from src.services.phone_utils import normalize

STATEMENT_TIMEOUT = "5s"
MAX_ROWS = 50
_TZ = ZoneInfo(DEFAULT_TZ)
_LEAD_REF = re.compile(r"^(web_lead|contact):(\d+)$")

# Free-form SQL guard. The READ ONLY transaction is the hard stop for writes; these checks keep the
# query inside the lending schema and away from server functions.
_SQL_START = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_SQL_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|grant|revoke|truncate|copy|call|do|vacuum|analyze|"
    r"set|reset|lock|refresh|comment|listen|notify|prepare|execute|discard)\b"
    r"|\bpg_\w+\s*\(|\bdblink\b|\blo_\w+\s*\(|\binto\b",
    re.IGNORECASE,
)
_SQL_RELATION = re.compile(r"\b(?:from|join)\s+([a-zA-Z_\"][\w\".]*)", re.IGNORECASE)

LENDING_TABLES_HELP = (
    "Tables (schema lending): call_dispositions (one row per dialer call: caller_name, phone, disposition, "
    "direction, talk_duration_sec, call_started_at, call_ended_at, dialer_campaign_id, campaign_tag), "
    "web_leads (website form: received_at, name, phone, email, property_city, deal_type, completed_projects_3y, "
    "sms_consent, suppressed, ghl_status, ghl_contact_id), contacts (phone, email, do_not_contact, nurture), "
    "text_consents (phone, source, captured_at, revoked_at), suppression_list (phone, email, reason, created_at), "
    "ghl_stage_events (ghl_opportunity_id, stage_name, phone, booked_by, event_at), booking_messages "
    "(booking_ref, kind, send_at, status, first_name, slot_start_utc, booked_by), confirmation_tasks "
    "(booking_ref, assignee, due_date, completed_at, cancelled_at), missed_call_events (phone, event_date_et, status), "
    "pending_actions, agent_memory."
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DayInput(_Input):
    day: Optional[date] = Field(None, description="Calendar day in Eastern time (YYYY-MM-DD). Defaults to today.")


class CallActivityInput(DayInput):
    caller: Optional[str] = Field(None, max_length=100, description="Filter to callers whose name contains this text.")
    limit: int = Field(20, ge=1, le=MAX_ROWS, description="How many recent calls to list.")


class FindLeadInput(_Input):
    query: str = Field(..., min_length=2, max_length=120, description="A name, phone number or email address.")


class WebLeadsInput(_Input):
    since_days: int = Field(7, ge=1, le=90, description="Look back this many days.")
    ghl_status: Optional[Literal["pending", "synced", "contact_only", "failed"]] = None
    limit: int = Field(25, ge=1, le=MAX_ROWS)


class BookingsInput(_Input):
    days: int = Field(2, ge=1, le=14, description="Bookings starting within this many days from now.")


class LenderFitInput(_Input):
    borrower: BorrowerProfile = Field(default_factory=BorrowerProfile)
    loan: LoanRequest


class QueryInput(_Input):
    sql: str = Field(..., min_length=8, max_length=4000,
                     description="One read-only SELECT (or WITH ... SELECT) over lending.* tables, always "
                                 "schema-qualified (lending.web_leads). Use date_part()/date_trunc() rather than "
                                 f"EXTRACT(... FROM ...). At most {MAX_ROWS} rows come back. " + LENDING_TABLES_HELP)


class SaveRuleInput(_Input):
    category: str = Field(..., min_length=2, max_length=64, pattern=r"^[a-z][a-z0-9_]*$",
                          description="Short snake_case topic, e.g. messaging, underwriting, scheduling.")
    rule_text: str = Field(..., min_length=5, max_length=500, description="The rule exactly as the approver meant it.")


class NoInput(_Input):
    pass


class DeactivateRuleInput(_Input):
    memory_id: str = Field(..., pattern=r"^[0-9a-fA-F-]{36}$")


class DraftSmsInput(_Input):
    lead_ref: str = Field(..., pattern=_LEAD_REF.pattern, description="From find_lead, e.g. web_lead:12.")
    body: str = Field(..., min_length=1, max_length=600)


class DraftEmailInput(_Input):
    lead_ref: str = Field(..., pattern=_LEAD_REF.pattern)
    subject: str = Field(..., min_length=1, max_length=150)
    body: str = Field(..., min_length=1, max_length=5000)


class DraftPipelineMoveInput(_Input):
    lead_ref: str = Field(..., pattern=r"^web_lead:\d+$", description="Only website leads are in GHL today.")
    stage: str = Field(..., min_length=2, max_length=100, description="Target stage name in the Booked Calls pipeline.")


@dataclass(frozen=True)
class _Lead:
    ref: str
    name: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    ghl_contact_id: Optional[str]


def _last4(phone: Optional[str]) -> Optional[str]:
    return f"…{phone[-4:]}" if phone else None


def _today() -> date:
    return datetime.now(_TZ).date()


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (datetime, date)) else value


def check_read_only_sql(sql: str) -> str:
    """Return the statement to run, or raise ToolInputError explaining why it is refused."""
    statement = sql.strip().rstrip(";").strip()
    if ";" in statement:
        raise ToolInputError("only one statement is allowed")
    if not _SQL_START.match(statement):
        raise ToolInputError("the query must start with SELECT or WITH")
    if _SQL_FORBIDDEN.search(statement):
        raise ToolInputError("the query uses a keyword or function that is not allowed in a read-only lookup")
    cte_names = {name.lower() for name in re.findall(r"\b(\w+)\s+as\s*\(", statement, re.IGNORECASE)}
    for relation in _SQL_RELATION.findall(statement):
        cleaned = relation.replace('"', "").lower()
        if cleaned in cte_names or cleaned.startswith("lending."):
            continue
        raise ToolInputError(f"only lending.* tables can be queried (found {relation})")
    return statement


class CoraToolkit:
    def __init__(self, engine: Engine, memory: AgentMemory,
                 campaign_names: Optional[Mapping[str, str]] = None) -> None:
        self._engine = engine
        self._memory = memory
        self._campaign_names = dict(campaign_names or {})

    @contextmanager
    def _read_only(self) -> Iterator[Session]:
        with Session(self._engine) as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            session.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'"))
            try:
                yield session
            finally:
                session.rollback()

    # -- read-only -----------------------------------------------------------------------

    def pipeline_summary(self, args: DayInput, _: ToolContext) -> dict[str, Any]:
        day = args.day or _today()
        with self._read_only() as session:
            data = build_scoreboard(session, day, self._campaign_names)

        def rows(items) -> list[dict[str, Any]]:
            return [{"name": r.name, "dials": r.dials, "live": r.live, "gated": r.gated, "booked": r.booked,
                     "showed": r.showed, "nurture": r.nurture} for r in items]

        return {"day": day.isoformat(), "total": rows([data.total])[0], "by_caller": rows(data.by_caller),
                "by_campaign": rows(data.by_campaign),
                "by_number": [{"number": _last4(n.number), "dials": n.dials, "answered": n.answered}
                              for n in data.by_number]}

    def call_activity(self, args: CallActivityInput, _: ToolContext) -> dict[str, Any]:
        day = args.day or _today()
        start = datetime.combine(day, datetime.min.time(), tzinfo=_TZ)
        params = {"start": start, "end": start + timedelta(days=1),
                  "caller": f"%{args.caller}%" if args.caller else None, "limit": args.limit}
        where = ("call_ended_at >= :start AND call_ended_at < :end "
                 "AND (CAST(:caller AS text) IS NULL OR caller_name ILIKE :caller)")
        with self._read_only() as session:
            by_disposition = session.execute(
                text(f"SELECT COALESCE(disposition, disposition_raw, '(none)') AS disposition, count(*) AS calls "
                     f"FROM lending.call_dispositions WHERE {where} GROUP BY 1 ORDER BY calls DESC"), params).all()
            recent = session.execute(
                text(f"SELECT call_ended_at, caller_name, direction, phone, COALESCE(disposition, disposition_raw) AS d, "
                     f"talk_duration_sec FROM lending.call_dispositions WHERE {where} "
                     "ORDER BY call_ended_at DESC LIMIT :limit"), params).all()
        return {"day": day.isoformat(), "by_disposition": [{"disposition": d, "calls": n} for d, n in by_disposition],
                "recent_calls": [{"ended_at": _iso(r[0].astimezone(_TZ) if r[0] else None), "caller": r[1],
                                  "direction": r[2], "phone": _last4(r[3]), "disposition": r[4], "talk_sec": r[5]}
                                 for r in recent]}

    def find_lead(self, args: FindLeadInput, _: ToolContext) -> dict[str, Any]:
        query = args.query.strip()
        digits = re.sub(r"\D", "", query)
        phone = normalize(query) if len(digits) >= 7 else None
        email = query.lower() if "@" in query else None
        name = None if (phone or email) else f"%{query}%"
        with self._read_only() as session:
            leads = session.execute(text("""
                SELECT 'web_lead:' || w.id AS lead_ref, w.name, w.phone, w.email, w.received_at, w.deal_type,
                       w.property_city, w.ghl_status, w.ghl_contact_id IS NOT NULL AS in_ghl
                  FROM lending.web_leads w
                 WHERE (CAST(:phone AS text) IS NOT NULL AND w.phone = :phone)
                    OR (CAST(:email AS text) IS NOT NULL AND w.email = :email)
                    OR (CAST(:name AS text) IS NOT NULL AND w.name ILIKE :name)
                UNION ALL
                SELECT 'contact:' || c.id, NULL, c.phone, c.email, c.created_at, NULL, NULL, NULL, false
                  FROM lending.contacts c
                 WHERE (CAST(:phone AS text) IS NOT NULL AND c.phone = :phone)
                    OR (CAST(:email AS text) IS NOT NULL AND lower(c.email) = :email)
                 ORDER BY 5 DESC NULLS LAST LIMIT 5
            """), {"phone": phone, "email": email, "name": name}).mappings().all()
            phones = sorted({lead["phone"] for lead in leads if lead["phone"]})
            status = {row["phone"]: row for row in session.execute(text("""
                SELECT p.phone,
                       EXISTS (SELECT 1 FROM lending.text_consents t WHERE t.phone = p.phone AND t.revoked_at IS NULL) AS text_consent,
                       EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = p.phone) AS suppressed,
                       EXISTS (SELECT 1 FROM lending.contacts c WHERE c.phone = p.phone AND c.do_not_contact) AS do_not_contact,
                       (SELECT json_build_object('ended_at', d.call_ended_at, 'caller', d.caller_name,
                                                 'disposition', COALESCE(d.disposition, d.disposition_raw))
                          FROM lending.call_dispositions d WHERE d.phone = p.phone
                         ORDER BY d.call_ended_at DESC NULLS LAST LIMIT 1) AS last_call
                  FROM unnest(CAST(:phones AS text[])) AS p(phone)
            """), {"phones": phones}).mappings()} if phones else {}
        results = []
        for lead in leads:
            flags = status.get(lead["phone"], {})
            results.append({
                "lead_ref": lead["lead_ref"], "name": lead["name"], "phone": _last4(lead["phone"]),
                "email": lead["email"], "first_seen": _iso(lead["received_at"]), "deal_type": lead["deal_type"],
                "property_city": lead["property_city"], "ghl_status": lead["ghl_status"], "in_ghl": lead["in_ghl"],
                "text_consent": flags.get("text_consent", False), "suppressed": flags.get("suppressed", False),
                "do_not_contact": flags.get("do_not_contact", False), "last_call": flags.get("last_call"),
            })
        return {"matches": results} if results else {"matches": [], "note": "no lead matched"}

    def list_web_leads(self, args: WebLeadsInput, _: ToolContext) -> dict[str, Any]:
        with self._read_only() as session:
            rows = session.execute(text("""
                SELECT id, received_at, name, phone, deal_type, property_city, completed_projects_3y,
                       sms_consent, suppressed, ghl_status, ghl_last_error
                  FROM lending.web_leads
                 WHERE received_at >= now() - make_interval(days => :days)
                   AND (CAST(:status AS text) IS NULL OR ghl_status = :status)
                 ORDER BY received_at DESC LIMIT :limit
            """), {"days": args.since_days, "status": args.ghl_status, "limit": args.limit}).mappings().all()
        return {"web_leads": [{"lead_ref": f"web_lead:{r['id']}", "received_at": _iso(r["received_at"]),
                               "name": r["name"], "phone": _last4(r["phone"]), "deal_type": r["deal_type"],
                               "property_city": r["property_city"], "completed_projects_3y": r["completed_projects_3y"],
                               "sms_consent": r["sms_consent"], "suppressed": r["suppressed"],
                               "ghl_status": r["ghl_status"], "ghl_last_error": r["ghl_last_error"]} for r in rows]}

    def upcoming_bookings(self, args: BookingsInput, _: ToolContext) -> dict[str, Any]:
        with self._read_only() as session:
            bookings = session.execute(text("""
                SELECT booking_ref, max(first_name) AS first_name, max(booked_by) AS booked_by,
                       max(slot_start_utc) AS slot_start,
                       json_object_agg(kind, status) AS messages
                  FROM lending.booking_messages
                 WHERE slot_start_utc >= now() AND slot_start_utc < now() + make_interval(days => :days)
                 GROUP BY booking_ref ORDER BY slot_start LIMIT :limit
            """), {"days": args.days, "limit": MAX_ROWS}).mappings().all()
            tasks = session.execute(text("""
                SELECT booking_ref, assignee, due_date FROM lending.confirmation_tasks
                 WHERE completed_at IS NULL AND cancelled_at IS NULL ORDER BY due_date LIMIT :limit
            """), {"limit": MAX_ROWS}).mappings().all()
        return {"bookings": [{"booking_ref": b["booking_ref"], "first_name": b["first_name"], "booked_by": b["booked_by"],
                              "starts_at": _iso(b["slot_start"].astimezone(_TZ)), "messages": b["messages"]}
                             for b in bookings],
                "open_confirmation_tasks": [{"booking_ref": t["booking_ref"], "assignee": t["assignee"],
                                             "due_date": _iso(t["due_date"])} for t in tasks]}

    def lender_fit(self, args: LenderFitInput, _: ToolContext) -> dict[str, Any]:
        result = evaluate_lender_fit(args.borrower, args.loan)
        return {"internal_only": "Never quote rates, terms or approval to a borrower from this.",
                **result.model_dump(mode="json")}

    def query_lending_data(self, args: QueryInput, _: ToolContext) -> dict[str, Any]:
        statement = check_read_only_sql(args.sql)
        with self._read_only() as session:
            result = session.execute(text(f"SELECT * FROM ({statement}) AS cora_query LIMIT {MAX_ROWS}"))
            columns = list(result.keys())
            rows = [{column: _iso(value) for column, value in zip(columns, row)} for row in result.all()]
        return {"columns": columns, "rows": rows, "row_count": len(rows), "row_cap": MAX_ROWS}

    # -- memory --------------------------------------------------------------------------

    def save_rule(self, args: SaveRuleInput, context: ToolContext) -> dict[str, Any]:
        rule, created = self._memory.save_rule(category=args.category, rule_text=args.rule_text,
                                               source_thread_ts=context.thread_ts)
        return {"memory_id": rule.memory_id, "saved": created,
                "note": "saved; confirm it to the user" if created else "an identical active rule already exists"}

    def list_rules(self, _: NoInput, __: ToolContext) -> dict[str, Any]:
        return {"rules": [{"memory_id": r.memory_id, "category": r.category, "rule_text": r.rule_text}
                          for r in self._memory.active_rules()]}

    def deactivate_rule(self, args: DeactivateRuleInput, _: ToolContext) -> dict[str, Any]:
        if not self._memory.deactivate_rule(args.memory_id):
            raise ToolInputError("no active rule has that memory_id")
        return {"memory_id": args.memory_id, "deactivated": True}

    # -- drafts (gated) ------------------------------------------------------------------

    def _lead(self, lead_ref: str) -> _Lead:
        match = _LEAD_REF.match(lead_ref)
        if not match:
            raise ToolInputError("lead_ref must look like web_lead:12 or contact:34")
        kind, lead_id = match.group(1), int(match.group(2))
        sql = ("SELECT name, phone, email, ghl_contact_id FROM lending.web_leads WHERE id = :id" if kind == "web_lead"
               else "SELECT NULL AS name, phone, email, NULL AS ghl_contact_id FROM lending.contacts WHERE id = :id")
        with self._read_only() as session:
            row = session.execute(text(sql), {"id": lead_id}).mappings().first()
        if row is None:
            raise ToolInputError(f"{lead_ref} does not exist; use find_lead first")
        return _Lead(lead_ref, row["name"], normalize(row["phone"]) if row["phone"] else None,
                     (row["email"] or "").strip().lower() or None, row["ghl_contact_id"])

    def draft_sms(self, args: DraftSmsInput, _: ToolContext) -> EgressDraft:
        lead = self._lead(args.lead_ref)
        if not lead.phone:
            raise ToolInputError(f"{lead.ref} has no phone number on record")
        return EgressDraft(channel="ghl_sms", payload={"lead_ref": lead.ref, "to_phone": lead.phone, "body": args.body},
                           summary=f"Text to {lead.name or 'lead'} ({_last4(lead.phone)})",
                           recipient_phone=lead.phone, recipient_email=lead.email, contact_ref=lead.ref)

    def draft_email(self, args: DraftEmailInput, _: ToolContext) -> EgressDraft:
        lead = self._lead(args.lead_ref)
        if not lead.email:
            raise ToolInputError(f"{lead.ref} has no email address on record")
        return EgressDraft(channel="ghl_email",
                           payload={"lead_ref": lead.ref, "to_email": lead.email, "subject": args.subject,
                                    "body": args.body},
                           summary=f"Email to {lead.name or 'lead'} <{lead.email}>: {args.subject}",
                           recipient_phone=lead.phone, recipient_email=lead.email, contact_ref=lead.ref)

    def draft_pipeline_move(self, args: DraftPipelineMoveInput, _: ToolContext) -> EgressDraft:
        lead = self._lead(args.lead_ref)
        if not lead.ghl_contact_id:
            raise ToolInputError(f"{lead.ref} has not reached GoHighLevel yet, so it has no pipeline card to move")
        return EgressDraft(channel="ghl_stage",
                           payload={"lead_ref": lead.ref, "ghl_contact_id": lead.ghl_contact_id, "stage": args.stage},
                           summary=f"Move {lead.name or 'lead'} to '{args.stage}' in GoHighLevel",
                           recipient_phone=lead.phone, recipient_email=lead.email, contact_ref=lead.ref)

    def tools(self) -> list[Tool]:
        read, write, egress = SafetyLevel.READ_ONLY, SafetyLevel.INTERNAL_WRITE, SafetyLevel.EXTERNAL_EGRESS
        return [
            Tool("get_pipeline_summary", "Calls, live conversations, gated, booked, showed and nurture counts for a "
                 "day, per caller, campaign and outbound number (the 7pm scoreboard numbers).", DayInput, read,
                 self.pipeline_summary),
            Tool("get_call_activity", "Calls on a day grouped by disposition, plus the most recent calls; optionally "
                 "for one caller.", CallActivityInput, read, self.call_activity),
            Tool("find_lead", "Look up a lead by name, phone or email. Returns lead_ref (needed for drafts), last "
                 "call, text consent, opt-out and do-not-contact status.", FindLeadInput, read, self.find_lead),
            Tool("list_web_leads", "Recent nextdeallending.com form leads and whether they reached GoHighLevel.",
                 WebLeadsInput, read, self.list_web_leads),
            Tool("list_upcoming_bookings", "Booked calls starting soon with their reminder status, and open "
                 "confirmation-call tasks.", BookingsInput, read, self.upcoming_bookings),
            Tool("check_lender_fit", "Run a borrower and loan through the five-lender rules matrix. Internal "
                 "analysis only: never present it to a borrower as a rate, term or approval.", LenderFitInput, read,
                 self.lender_fit),
            Tool("query_lending_data", "Fallback for questions the other tools cannot answer: one read-only SQL "
                 "SELECT over lending.* tables.", QueryInput, read, self.query_lending_data),
            Tool("save_standing_rule", "Save a standing rule an approver stated, so it applies to every future "
                 "conversation.", SaveRuleInput, write, self.save_rule, approver_only=True),
            Tool("list_standing_rules", "List the standing rules currently in force.", NoInput, read, self.list_rules),
            Tool("deactivate_standing_rule", "Stop applying a standing rule (by memory_id from list_standing_rules).",
                 DeactivateRuleInput, write, self.deactivate_rule, approver_only=True),
            Tool("draft_sms", "Draft a text message to a lead. It is held for approval; nothing is sent until an "
                 "approver clicks Approve.", DraftSmsInput, egress, self.draft_sms),
            Tool("draft_email", "Draft an email to a lead. Held for approval; nothing is sent until approved.",
                 DraftEmailInput, egress, self.draft_email),
            Tool("draft_pipeline_move", "Draft moving a website lead to another GoHighLevel pipeline stage. Held for "
                 "approval; nothing changes until approved.", DraftPipelineMoveInput, egress, self.draft_pipeline_move),
        ]
