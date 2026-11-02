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


def _lead(db, *, vendor, received, suppressed=False, contact="ghl_9"):
    db.execute(
        text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, suppressed, received_at, "
             "raw_payload, ghl_contact_id, credit_band, loan_amount, property_state, loan_type) "
             "VALUES (:v, :v, :p, :s, :r, '{}'::jsonb, :c, '700+', 300000, 'FL', 'FIX_AND_FLIP')"),
        {"v": vendor, "p": PHONE, "s": suppressed, "r": received, "c": contact})


def test_the_latest_leads_letter_is_queued_with_its_four_core_fields(link_db, queued):
    _lead(link_db, vendor="old", received="2026-10-01 10:00:00+00")
    _lead(link_db, vendor="new", received="2026-10-02 10:00:00+00")
    send_prequal_after_call("call-1", PHONE)
    (call,) = queued
    assert (call["lead_source"], call["lead_ref"], call["contact_id"]) == ("lendingflow", "new", "ghl_9")
    lead = call["lead"]
    assert (lead.credit_band, lead.loan_amount, lead.property_state, lead.loan_type) == ("700+", 300000, "FL",
                                                                                          "FIX_AND_FLIP")


@pytest.mark.parametrize("suppressed, contact", [(True, "ghl_9"), (False, None)])
def test_a_suppressed_lead_or_one_not_in_ghl_gets_no_letter(link_db, queued, suppressed, contact):
    _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00", suppressed=suppressed, contact=contact)
    send_prequal_after_call("call-1", PHONE)
    assert queued == []


def test_no_lead_and_letters_off_queue_nothing(link_db, queued, monkeypatch):
    send_prequal_after_call("call-1", PHONE)
    _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")
    monkeypatch.setattr(prequal_link, "get_settings", lambda: SimpleNamespace(lending_prequal_pdf_enabled=False))
    send_prequal_after_call("call-1", PHONE)
    assert queued == []


def test_a_queue_failure_is_swallowed(link_db, monkeypatch):
    _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")
    monkeypatch.setattr(prequal_link, "lending_session", lambda: _session_over(link_db))
    monkeypatch.setattr(prequal_link, "get_settings", lambda: SimpleNamespace(lending_prequal_pdf_enabled=True))

    def boom(**kwargs):
        raise RuntimeError("queue down")

    monkeypatch.setattr("src.lending.prequal_letters.queue_and_send_in_background", boom)
    send_prequal_after_call("call-1", PHONE)  # must not raise: the soft approval never depends on it


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
                        lambda db, **kw: SimpleNamespace(status=status, reason=None, approval_id=None))
    monkeypatch.setattr(webhook, "_status_text", lambda outcome: "ok")
    monkeypatch.setattr(webhook, "_outcome_key", lambda outcome: "generated")
    monkeypatch.setattr(webhook, "_refresh_card", lambda *a, **kw: None)
    monkeypatch.setattr(webhook, "share_facts_with_enrichment", lambda *args: None)
    monkeypatch.setattr(webhook, "send_prequal_after_call", lambda *args: letters.append(args))
    webhook._process_submission("call-1", FACTS, "U1", "CDIAL", "1.1")
    assert letters == ([("call-1", PHONE)] if sent else [])
