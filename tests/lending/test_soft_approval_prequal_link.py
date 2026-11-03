"""Q2 (client list Nov 2): the pre-qual letter is queued after the call, once the caller has entered the deal
figures and they fit a lender; never at intake."""
from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
from src.lending import soft_approval_webhook as webhook
from src.lending.models import LendingContact
from src.lending.soft_approval import prequal_link
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.lending.soft_approval.prequal_link import send_prequal_after_call

PHONE = "+17275550103"
FACTS = SoftApprovalFacts(property_address="12 Palm Ave, Tampa, FL", purchase_price=D("250000"),
                          rehab_budget=D("40000"), arv=D("400000"))


@pytest.fixture
def link_db(lending_db):
    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)  # lendingflow_leads.contact_id references it
    apply_lendingflow(conn)
    return lending_db


@contextmanager
def _session_over(db):
    with db.begin_nested():
        yield db


@pytest.fixture
def queued(link_db, monkeypatch):
    """Letters on; the queue call recorded instead of run."""
    calls = []
    monkeypatch.setattr(prequal_link, "lending_session", lambda: _session_over(link_db))
    monkeypatch.setattr(prequal_link, "get_settings", lambda: SimpleNamespace(lending_prequal_pdf_enabled=True))
    monkeypatch.setattr("src.lending.prequal_letters.queue_and_send_in_background", lambda **kw: calls.append(kw))
    return calls


def _lead(db, *, vendor, received, suppressed=False, contact="ghl_9") -> str:
    """Insert a LendingFlow lead; returns its ``lead_uuid`` (what a soft approval stores as ``lead_ref``)."""
    return str(db.execute(
        text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, suppressed, received_at, "
             "raw_payload, ghl_contact_id, credit_band, loan_amount, property_state, loan_type) "
             "VALUES (:v, :v, :p, :s, :r, '{}'::jsonb, :c, '700+', 300000, 'FL', 'FIX_AND_FLIP') RETURNING lead_uuid"),
        {"v": vendor, "p": PHONE, "s": suppressed, "r": received, "c": contact}).scalar_one())


def test_the_assessed_leads_letter_is_queued_with_its_four_core_fields(link_db, queued):
    _lead(link_db, vendor="old", received="2026-10-01 10:00:00+00")
    new = _lead(link_db, vendor="new", received="2026-10-02 10:00:00+00")
    send_prequal_after_call("call-1", PHONE, new)
    (call,) = queued
    assert (call["lead_source"], call["lead_ref"], call["contact_id"]) == ("lendingflow", "new", "ghl_9")
    lead = call["lead"]
    assert (lead.credit_band, lead.loan_amount, lead.property_state, lead.loan_type) == ("700+", 300000, "FL",
                                                                                          "FIX_AND_FLIP")


@pytest.mark.parametrize("suppressed, contact", [(True, "ghl_9"), (False, None)])
def test_a_suppressed_lead_or_one_not_in_ghl_gets_no_letter(link_db, queued, suppressed, contact):
    v1 = _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00", suppressed=suppressed, contact=contact)
    send_prequal_after_call("call-1", PHONE, v1)
    assert queued == []


def test_the_hand_off_skips_a_lead_after_which_a_suppressed_lead_arrived(link_db, queued):
    old = _lead(link_db, vendor="old", received="2026-10-01 10:00:00+00")
    _lead(link_db, vendor="new", received="2026-10-02 10:00:00+00", suppressed=True)
    send_prequal_after_call("call-1", PHONE, old)
    assert queued == []


def test_no_lead_and_letters_off_queue_nothing(link_db, queued, monkeypatch):
    send_prequal_after_call("call-1", PHONE, None)
    v1 = _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")
    monkeypatch.setattr(prequal_link, "get_settings", lambda: SimpleNamespace(lending_prequal_pdf_enabled=False))
    send_prequal_after_call("call-1", PHONE, v1)
    assert queued == []


def test_a_queue_failure_is_swallowed(link_db, monkeypatch):
    v1 = _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")
    monkeypatch.setattr(prequal_link, "lending_session", lambda: _session_over(link_db))
    monkeypatch.setattr(prequal_link, "get_settings", lambda: SimpleNamespace(lending_prequal_pdf_enabled=True))

    def boom(**kwargs):
        raise RuntimeError("queue down")

    monkeypatch.setattr("src.lending.prequal_letters.queue_and_send_in_background", boom)
    send_prequal_after_call("call-1", PHONE, v1)  # must not raise: the soft approval never depends on it


class _FakeSlack:
    def __init__(self):
        self.posts = []

    def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        return {"ts": "1.1"}


@pytest.mark.parametrize("status, sent", [("generated", True), ("no_fit", False), ("no_lead", False),
                                          ("out_of_scope", False), ("render_failed", False)])
def test_only_a_submission_whose_figures_fit_a_lender_sends_the_letter(link_db, monkeypatch, status, sent):
    link_db.execute(
        text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, caller_name, talk_duration_sec, disposition, raw_event) "
             "VALUES ('call-1', :p, 'Sam', 120, NULL, '{}'::jsonb)"), {"p": PHONE})
    letters = []
    monkeypatch.setattr(webhook, "lending_session", lambda: _session_over(link_db))
    monkeypatch.setattr(webhook, "slack_client", lambda: _FakeSlack())
    monkeypatch.setattr(webhook, "generate_soft_approval",
                        lambda db, **kw: SimpleNamespace(status=status, reason=None, approval_id=None,
                                                         lead_ref="lead-uuid-1"))
    monkeypatch.setattr(webhook, "_status_text", lambda outcome: "ok")
    monkeypatch.setattr(webhook, "_outcome_key", lambda outcome: "generated")
    monkeypatch.setattr(webhook, "_refresh_card", lambda *a, **kw: None)
    monkeypatch.setattr(webhook, "share_facts_with_enrichment", lambda *args: None)
    monkeypatch.setattr(webhook, "send_prequal_after_call", lambda *args: letters.append(args))
    webhook._process_submission("call-1", FACTS, "U1", "CDIAL", "1.1")
    assert letters == ([("call-1", PHONE, "lead-uuid-1")] if sent else [])


# --- A hand-off that did not land is recovered by the pre-qual sweep, without resubmitting the form ---

@pytest.fixture
def sweep_db(link_db):
    from migrations.apply_lending_prequal_letters import apply_to as apply_letters
    from migrations.apply_lending_soft_approvals import apply_to as apply_soft

    conn = link_db.get_bind()
    apply_soft(conn)
    apply_letters(conn)
    return link_db


def _approval(db, *, lead_ref, status="generated", updated="now()"):
    db.execute(text(
        "INSERT INTO lending.soft_approvals (phone, dialer_call_id, lead_ref, status, facts, updated_at) "
        f"VALUES (:p, 'call-1', :ref, :s, '{{}}'::jsonb, {updated})"), {"p": PHONE, "ref": lead_ref, "s": status})


def _letters(db):
    return db.execute(text("SELECT lead_ref, ghl_contact_id FROM lending.prequal_letters "
                           "WHERE lead_source = 'lendingflow' AND lead_ref IN ('v1', 'old', 'new')")).all()


def test_a_failed_hand_off_is_queued_by_the_sweep_once(sweep_db, monkeypatch):
    from datetime import datetime, timezone

    from src.lending.soft_approval.prequal_link import queue_missed_letters

    v1 = _lead(sweep_db, vendor="v1", received="2026-10-01 10:00:00+00")
    _approval(sweep_db, lead_ref=v1)
    monkeypatch.setattr(prequal_link, "lending_session", lambda: _session_over(sweep_db))
    monkeypatch.setattr(prequal_link, "get_settings", lambda: SimpleNamespace(lending_prequal_pdf_enabled=True))

    def failing_queue(**kwargs):
        raise RuntimeError("database blip")

    monkeypatch.setattr("src.lending.prequal_letters.queue_and_send_in_background", failing_queue)
    send_prequal_after_call("call-1", PHONE, v1)  # the immediate hand-off fails: no letter row
    assert _letters(sweep_db) == []

    now = datetime.now(timezone.utc)
    assert queue_missed_letters(sweep_db, now) == 1
    assert _letters(sweep_db) == [("v1", "ghl_9")]
    assert queue_missed_letters(sweep_db, now) == 0 and len(_letters(sweep_db)) == 1  # never a second letter


@pytest.mark.parametrize("status, updated", [("no_fit", "now()"), ("generated", "now() - interval '25 hours'")])
def test_the_sweep_skips_approvals_that_did_not_fit_or_are_too_old(sweep_db, status, updated):
    from datetime import datetime, timezone

    from src.lending.soft_approval.prequal_link import queue_missed_letters

    v1 = _lead(sweep_db, vendor="v1", received="2026-10-01 10:00:00+00")
    _approval(sweep_db, lead_ref=v1, status=status, updated=updated)
    assert queue_missed_letters(sweep_db, datetime.now(timezone.utc)) == 0 and _letters(sweep_db) == []


def test_the_sweep_skips_a_suppressed_latest_lead(sweep_db):
    from datetime import datetime, timezone

    from src.lending.soft_approval.prequal_link import queue_missed_letters

    old = _lead(sweep_db, vendor="old", received="2026-10-01 10:00:00+00")
    _lead(sweep_db, vendor="new", received="2026-10-02 10:00:00+00", suppressed=True)
    _approval(sweep_db, lead_ref=old)
    assert queue_missed_letters(sweep_db, datetime.now(timezone.utc)) == 0 and _letters(sweep_db) == []


def test_the_sweep_never_gives_a_newer_unassessed_lead_an_earlier_approval(sweep_db):
    """Review finding (PR 398): lead A is approved and has its letter; lead B arrives for the same phone. The
    sweep must not queue B's letter on A's approval, since no call has assessed B."""
    from datetime import datetime, timezone

    from src.lending.prequal_letters import enqueue
    from src.lending.prequal import PrequalLead
    from src.lending.soft_approval.prequal_link import queue_missed_letters

    old = _lead(sweep_db, vendor="old", received="2026-10-01 10:00:00+00")
    _approval(sweep_db, lead_ref=old)
    enqueue(sweep_db, lead_source="lendingflow", lead_ref="old", contact_id="ghl_9",
            lead=PrequalLead(credit_band="700+", loan_amount=300000, property_state="FL", loan_type="FIX_AND_FLIP"))
    _lead(sweep_db, vendor="new", received="2026-10-02 10:00:00+00")
    assert queue_missed_letters(sweep_db, datetime.now(timezone.utc)) == 0
    assert _letters(sweep_db) == [("old", "ghl_9")]


def test_the_sweep_skips_an_approval_saved_without_a_lead_ref(sweep_db):
    from datetime import datetime, timezone

    from src.lending.soft_approval.prequal_link import queue_missed_letters

    _lead(sweep_db, vendor="v1", received="2026-10-01 10:00:00+00")
    _approval(sweep_db, lead_ref=None)
    assert queue_missed_letters(sweep_db, datetime.now(timezone.utc)) == 0 and _letters(sweep_db) == []
