"""SQLite with an attached ``lending`` schema holding the columns the R1 1.1 contact spine reads and writes."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

SOURCE_DDL = (
    "CREATE TABLE lending.borrower_profiles (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, phone TEXT,"
    " email TEXT)",
    "CREATE TABLE lending.entities (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE lending.loan_files (id INTEGER PRIMARY KEY, borrower_first_name TEXT, borrower_last_name TEXT,"
    " borrower_phone TEXT, borrower_email TEXT, borrower_id INTEGER, entity_id INTEGER)",
    "CREATE TABLE lending.lendingflow_leads (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, phone TEXT,"
    " email TEXT, ghl_contact_id TEXT)",
    "CREATE TABLE lending.web_leads (id INTEGER PRIMARY KEY, name TEXT, phone TEXT, email TEXT, ghl_contact_id TEXT)",
    "CREATE TABLE lending.after_hours_conversations (id INTEGER PRIMARY KEY, first_name TEXT, phone TEXT, email TEXT,"
    " ghl_contact_id TEXT)",
    "CREATE TABLE lending.dialer_load_records (id INTEGER PRIMARY KEY, phone TEXT, borrower_name TEXT,"
    " entity_name TEXT, ghl_contact_id TEXT, dialer_contact_id TEXT)",
    "CREATE TABLE lending.call_dispositions (id INTEGER PRIMARY KEY, dialer_call_id TEXT, phone TEXT,"
    " dialer_contact_id TEXT, direction TEXT, disposition TEXT, call_source TEXT, call_started_at TIMESTAMP,"
    " created_at TIMESTAMP)",
    "CREATE TABLE lending.instantly_replies (id INTEGER PRIMARY KEY, dedup_key TEXT, lead_email TEXT, first_name TEXT,"
    " intent TEXT, received_at TIMESTAMP)",
    "CREATE TABLE lending.sunbiz_llc_contacts (document_number TEXT PRIMARY KEY, entity_name TEXT, subject_name TEXT,"
    " phones TEXT, emails TEXT)",
    "CREATE TABLE lending.contacts (id INTEGER PRIMARY KEY, phone TEXT, email TEXT, line_type TEXT,"
    " do_not_contact BOOLEAN NOT NULL DEFAULT 0)",
)

SPINE_DDL = (
    "CREATE TABLE lending.contact_spine (id INTEGER PRIMARY KEY AUTOINCREMENT, seed_key TEXT NOT NULL UNIQUE,"
    " display_name TEXT, merged_into INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP)",
    "CREATE TABLE lending.contact_identifiers (id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL,"
    " kind TEXT NOT NULL, value TEXT NOT NULL, merge_key BOOLEAN NOT NULL, first_seen_at TIMESTAMP,"
    " last_seen_at TIMESTAMP, UNIQUE (kind, value, contact_id))",
    "CREATE UNIQUE INDEX lending.uq_ci_merge_key ON contact_identifiers (kind, value) WHERE merge_key",
    "CREATE TABLE lending.contact_sources (source TEXT NOT NULL, source_ref TEXT NOT NULL, contact_id INTEGER NOT NULL,"
    " seen_at TIMESTAMP, PRIMARY KEY (source, source_ref))",
    "CREATE TABLE lending.contact_spine_merges (id INTEGER PRIMARY KEY AUTOINCREMENT, survivor_id INTEGER NOT NULL,"
    " merged_id INTEGER NOT NULL, merged_at TIMESTAMP)",
)

TOUCH_DDL = (
    "CREATE TABLE lending.missed_call_events (id INTEGER PRIMARY KEY, phone TEXT, status TEXT,"
    " provider_message_id TEXT, created_at TIMESTAMP, decided_at TIMESTAMP)",
    "CREATE TABLE lending.pending_actions (id INTEGER PRIMARY KEY, channel TEXT, status TEXT, provider_ref TEXT,"
    " recipient_phone TEXT, recipient_email TEXT, executed_at TIMESTAMP, created_at TIMESTAMP)",
    "CREATE TABLE lending.ghl_stage_events (id INTEGER PRIMARY KEY, phone TEXT, stage_name TEXT, event_at TIMESTAMP)",
    "CREATE TABLE lending.file_routing_events (id INTEGER PRIMARY KEY, lead_id INTEGER, loan_file_id INTEGER,"
    " action TEXT, created_at TIMESTAMP)",
    "CREATE TABLE lending.loan_file_status_events (id INTEGER PRIMARY KEY, loan_file_id INTEGER, status TEXT,"
    " at TIMESTAMP)",
)


@contextmanager
def spine_session(*groups: tuple[str, ...]) -> Iterator[Session]:
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        for ddl in groups or (SOURCE_DDL, SPINE_DDL, TOUCH_DDL):
            for statement in ddl:
                conn.execute(text(statement))
    with Session(eng) as session:
        yield session
    eng.dispose()
