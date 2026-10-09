"""T-10 uncalled-lead alarms: pickup, call classification (CDR + GHL), once-only alarms, wording."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_uncalled_alarms import apply_to

ARRIVED = datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)
LEAD_PHONE = "+17275550100"
JOSH = "+18135550199"
OPS = "C-OPS"

# Used only when T-11's real table is absent. The shared DB already has it (with rows), so CREATE IF NOT EXISTS
# is a no-op there and lead() fills the real table's NOT NULL columns.
_T11_LEADS = """
CREATE TABLE IF NOT EXISTS lending.lendingflow_leads (
    id SERIAL PRIMARY KEY,
    lead_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    vendor_lead_id VARCHAR(100) NOT NULL UNIQUE,
    dedupe_hash VARCHAR(64) NOT NULL UNIQUE,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_name VARCHAR(80),
    phone VARCHAR(20) NOT NULL,
    property_state VARCHAR(20),
    suppressed BOOLEAN NOT NULL DEFAULT false,
    ghl_contact_id VARCHAR(64)
)"""


class Sms:
    def __init__(self, error: Exception | None = None):
        self.sent, self.error = [], error

    def __call__(self, to, body, first_name=None, *, deadline=None):
        if self.error:
            raise self.error
        self.sent.append((to, body))
        return "msg"


class Slack:
    def __init__(self, error: Exception | None = None):
        self.posts, self.error = [], error

    def chat_postMessage(self, channel, text):
        if self.error:
            raise self.error
        self.posts.append((channel, text))


@pytest.fixture
def db(lending_db):
    conn = lending_db.connection()
    conn.execute(text(_T11_LEADS))
    apply_to(conn)  # existing shared-DB leads become 'preexisting' and never alarm in tests
    return lending_db


def lead(db, *, arrived=ARRIVED, phone=LEAD_PHONE, suppressed=False, ghl_contact_id=None,
         first_name="Jane", state="FL") -> int:
    unique = uuid.uuid4().hex
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, received_at, first_name, "
        "phone, property_state, suppressed, ghl_contact_id) "
        "VALUES (:v, :h, '{}'::jsonb, :r, :n, :p, :st, :s, :g) RETURNING id"),
        {"v": f"t10-{unique}", "h": unique + unique, "r": arrived, "n": first_name, "p": phone, "st": state,
         "s": suppressed, "g": ghl_contact_id}).scalar()


def call(db, *, started, phone=LEAD_PHONE, direction="outbound", disposition=None, talk_seconds=60, call_id=None):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, call_ended_at, "
        "disposition, talk_duration_sec, raw_event) VALUES (:id, :d, :p, :s, :s, :disp, :talk, '{}')"),
        {"id": call_id or f"t10-{uuid.uuid4().hex}", "d": direction, "p": phone, "s": started,
         "disp": disposition, "talk": talk_seconds})


def alarm(db, lead_id):
    return db.execute(text("SELECT * FROM lending.uncalled_alarms WHERE lendingflow_lead_id = :i"),
                      {"i": lead_id}).mappings().first()


def test_migration_is_idempotent(db):
    apply_to(db.connection())
    assert db.execute(text("SELECT to_regclass('lending.uncalled_alarms')")).scalar() is not None


def test_migration_marks_existing_leads_preexisting(db):
    lead_id = lead(db)
    apply_to(db.connection())  # the deploy step re-runs the migration right before the flag goes on
    row = alarm(db, lead_id)
    assert row["resolved_reason"] == "preexisting" and row["fired_120_at"] is None


from src.lending.uncalled_alarm_worker import pick_up_new_leads  # noqa: E402


def test_pickup_starts_the_stopwatch_at_received_at(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    row = alarm(db, lead_id)
    assert row["arrived_at"] == ARRIVED and row["phone"] == LEAD_PHONE and row["resolved_at"] is None


def test_pickup_twice_makes_one_alarm(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    pick_up_new_leads(db)
    assert db.execute(text("SELECT count(*) FROM lending.uncalled_alarms WHERE lendingflow_lead_id = :i"),
                      {"i": lead_id}).scalar() == 1


def test_suppressed_lead_is_never_picked_up(db):
    lead_id = lead(db, suppressed=True)
    pick_up_new_leads(db)
    assert alarm(db, lead_id) is None


def test_old_lead_is_still_picked_up(db):
    lead_id = lead(db, arrived=ARRIVED - timedelta(hours=3))
    pick_up_new_leads(db)
    assert alarm(db, lead_id)["resolved_at"] is None
