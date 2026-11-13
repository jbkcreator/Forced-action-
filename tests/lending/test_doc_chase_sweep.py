"""P2.4: the document chase sweep (request on day 0, reminders on day 2, 5 and 8)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from packages.agent_core.tools import QueuedDraft
from src.lending.closing.doc_chase import run_chase_sweep, start_chase
from src.lending.closing.drafts import Recipient

ACCEPTED = datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)  # 10 AM ET
MARK = Recipient("Mark", "+18135550101", "mark@example.com", "c1")


class Gate:
    def __init__(self):
        self.drafts = {}

    def submit(self, draft, *, tool_name, context, ttl=None):
        self.drafts.setdefault(context.tool_use_id, draft)
        return QueuedDraft(list(self.drafts).index(context.tool_use_id) + 1, True)


@pytest.fixture
def link_base(monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", "https://api.example.com", raising=False)


@pytest.fixture
def inbox(monkeypatch):
    events = []
    monkeypatch.setattr("src.lending.closing.doc_chase.add_to_inbox_safely", lambda db, **e: events.append(e))
    return events


@pytest.fixture
def chase(closing_db, link_base):
    return start_chase(closing_db, MARK, lead_ref="web_lead:7", accepted_by="slack:U1", now=ACCEPTED).chase_id


def _sweep(db, gate, days, consent=True):
    return run_chase_sweep(db, gate, approvals_channel="C_APPROVALS", now=ACCEPTED + timedelta(days=days),
                           text_consent=lambda phone: consent)


def test_day_0_request_then_day_2_5_8(closing_db, chase, inbox):
    gate = Gate()
    for day in (0, 1, 2, 5, 8, 9):
        _sweep(closing_db, gate, day)
    assert list(gate.drafts) == [f"doc_chase:{chase}:d{d}" for d in (0, 2, 5, 8)]
    versions = [d.payload["template_version"] for d in gate.drafts.values()]
    assert versions == ["DOCS-REQUEST-SMS-v1", "DOCS-CHASE-D2-SMS-v1", "DOCS-CHASE-D5-SMS-v1", "DOCS-CHASE-D8-SMS-v1"]
    assert all("https://api.example.com/api/lending/docs/" in d.payload["body"] for d in gate.drafts.values())
    touches = closing_db.execute(text("SELECT chase_day, action_id FROM lending.doc_chase_touches ORDER BY 1")).all()
    assert touches == [(0, 1), (2, 2), (5, 3), (8, 4)]


def test_every_minute_sweep_sends_once_per_step(closing_db, chase, inbox):
    gate = Gate()
    assert [_sweep(closing_db, gate, 0) for _ in range(3)] == [1, 0, 0]


def test_outage_sends_only_latest_step(closing_db, chase, inbox):
    gate = Gate()
    _sweep(closing_db, gate, 0)
    _sweep(closing_db, gate, 6)
    assert list(gate.drafts)[-1] == f"doc_chase:{chase}:d5" and len(gate.drafts) == 2


def test_all_received_completes_and_revokes(closing_db, chase, inbox):
    closing_db.execute(text("UPDATE lending.doc_chase_items SET received_at = now()"))
    gate = Gate()
    _sweep(closing_db, gate, 2)
    assert gate.drafts == {}
    assert closing_db.execute(text("SELECT status FROM lending.doc_chases")).scalar() == "complete"
    assert inbox[-1]["event_type"] == "doc_chase_complete"


def test_lost_stage_cancels_and_revokes_links(closing_db, chase, inbox):
    gate = Gate()
    _sweep(closing_db, gate, 0)
    closing_db.execute(text("INSERT INTO lending.ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, "
                            "entered_at, phone) VALUES ('opp-1', 'Lost', 'lost', now(), '+18135550101')"))
    closing_db.execute(text("UPDATE lending.doc_chases SET ghl_opportunity_id = 'opp-1'"))
    _sweep(closing_db, gate, 2)
    assert closing_db.execute(text("SELECT status, status_reason FROM lending.doc_chases")).one() == ("cancelled", "stage:lost")
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chase_links WHERE revoked_at IS NULL")).scalar() == 0
    assert len(gate.drafts) == 1


def test_email_when_no_text_consent(closing_db, chase, inbox):
    gate = Gate()
    _sweep(closing_db, gate, 0, consent=False)
    draft, = gate.drafts.values()
    assert draft.channel == "ghl_email" and "Signed purchase agreement" in draft.payload["body"]


def test_no_channel_raises_inbox_item_once(closing_db, link_base, inbox):
    start_chase(closing_db, Recipient("Ana", "+18135550102", None, None), lead_ref="web_lead:8",
                accepted_by="slack:U1", now=ACCEPTED)
    gate = Gate()
    _sweep(closing_db, gate, 0, consent=False)
    _sweep(closing_db, gate, 0, consent=False)
    assert gate.drafts == {} and [e["event_type"] for e in inbox] == ["doc_chase_no_channel"]
    assert closing_db.execute(text("SELECT count(*) FROM lending.doc_chase_links")).scalar() == 0


class RecoveringGate(Gate):
    """Keeps every draft by key like the real queue; ``find`` is PendingActionQueue.find_by_idempotency_key."""

    def find(self, key):
        draft = self.drafts.get(key)
        return None if draft is None else type("A", (), {"action_id": list(self.drafts).index(key) + 1,
                                                          "payload": draft.payload})()


def test_link_survives_a_rollback_after_the_draft_was_queued(closing_db, chase, inbox):
    from src.lending.closing.doc_chase import chase_for_token
    gate = RecoveringGate()
    savepoint = closing_db.begin_nested()
    run_chase_sweep(closing_db, gate, approvals_channel="C_APPROVALS", now=ACCEPTED,
                    text_consent=lambda phone: True, find_draft=gate.find)
    savepoint.rollback()  # the sweep's transaction is lost after the gate committed the draft
    run_chase_sweep(closing_db, gate, approvals_channel="C_APPROVALS", now=ACCEPTED,
                    text_consent=lambda phone: True, find_draft=gate.find)
    draft, = gate.drafts.values()
    token = draft.payload["upload_link"].rsplit("/", 1)[1]
    assert chase_for_token(closing_db, token, ACCEPTED) is not None
    assert closing_db.execute(text("SELECT action_id FROM lending.doc_chase_touches")).scalar() == 1
