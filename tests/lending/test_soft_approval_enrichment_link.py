"""T-12: the soft approval form's address and close date reach the LendingFlow enrichment card."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enrichment
from src.lending import soft_approval_webhook as webhook
from src.lending.models import LendingContact
from src.lending.soft_approval import enrichment_link
from src.lending.soft_approval.enrichment_link import share_facts_with_enrichment
from src.lending.soft_approval.facts import SoftApprovalFacts

PHONE = "+17275550102"
FACTS = SoftApprovalFacts(property_address="12 Palm Ave, Tampa, FL", purchase_price=D("250000"),
                          rehab_budget=D("40000"), arv=D("400000"), target_close_date=date(2026, 11, 1))


@pytest.fixture
def link_db(lending_db):
    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)  # lendingflow_leads.contact_id references it
    apply_lendingflow(conn)
    apply_enrichment(conn)
    return lending_db


@contextmanager
def _session_over(db):
    with db.begin_nested():  # like lending_session: an error rolls the unit of work back
        yield db


@pytest.fixture
def rebuilds(link_db, monkeypatch):
    """Enrichment on, the card rebuild recorded instead of run."""
    runs = []
    monkeypatch.setattr(enrichment_link, "lending_session", lambda: _session_over(link_db))
    monkeypatch.setattr(enrichment_link, "enrichment_enabled", lambda: True)
    monkeypatch.setattr(enrichment_link, "enrich_lead", lambda db, lead_id: runs.append(lead_id))
    return runs


def _lead(db, *, vendor, received, suppressed=False, phone=PHONE) -> int:
    return db.execute(
        text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, suppressed, received_at, raw_payload) "
             "VALUES (:v, :v, :p, :s, :r, '{}'::jsonb) RETURNING id"),
        {"v": vendor, "p": phone, "s": suppressed, "r": received},
    ).scalar()


def _enrichment_rows(db):
    return db.execute(text("SELECT lead_id, status, captured_address, target_close_date, facts_source "
                           "FROM lending.lendingflow_enrichment ORDER BY lead_id")).mappings().all()


def test_the_form_address_and_close_date_reach_the_enrichment_card(link_db, rebuilds):
    lead_id = _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")
    share_facts_with_enrichment("call-1", PHONE, FACTS)
    (row,) = _enrichment_rows(link_db)
    assert (row["lead_id"], row["captured_address"], str(row["target_close_date"]), row["facts_source"]) == (
        lead_id, "12 Palm Ave, Tampa, FL", "2026-11-01", "slack_form")
    assert rebuilds == [lead_id]  # and the card is rebuilt from those facts


def test_the_newest_lead_gets_the_facts_when_a_phone_has_several(link_db, rebuilds):
    _lead(link_db, vendor="old", received="2026-10-01 10:00:00+00")
    newest = _lead(link_db, vendor="new", received="2026-10-02 10:00:00+00")
    share_facts_with_enrichment("call-1", PHONE, FACTS)
    assert [row["lead_id"] for row in _enrichment_rows(link_db)] == [newest] and rebuilds == [newest]


def test_a_suppressed_latest_lead_leaves_an_older_lead_untouched(link_db, rebuilds):
    older = _lead(link_db, vendor="old", received="2026-10-01 10:00:00+00")
    _lead(link_db, vendor="new", received="2026-10-02 10:00:00+00", suppressed=True)
    link_db.execute(text("INSERT INTO lending.lendingflow_enrichment (lead_id, status, captured_address) "
                         "VALUES (:i, 'ready', 'OLD ADDRESS')"), {"i": older})
    share_facts_with_enrichment("call-1", PHONE, FACTS)
    (row,) = _enrichment_rows(link_db)  # the soft approval refuses this lead, so the facts go nowhere
    assert (row["lead_id"], row["status"], row["captured_address"]) == (older, "ready", "OLD ADDRESS")
    assert rebuilds == []


def test_nothing_is_shared_while_enrichment_is_off(link_db, rebuilds, monkeypatch):
    _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")
    monkeypatch.setattr(enrichment_link, "enrichment_enabled", lambda: False)
    share_facts_with_enrichment("call-1", PHONE, FACTS)
    assert _enrichment_rows(link_db) == [] and rebuilds == []


def test_a_phone_with_no_lendingflow_lead_shares_nothing(link_db, rebuilds):
    share_facts_with_enrichment("call-1", PHONE, FACTS)
    assert _enrichment_rows(link_db) == [] and rebuilds == []


def test_an_enrichment_failure_is_swallowed(link_db, rebuilds, monkeypatch):
    _lead(link_db, vendor="v1", received="2026-10-01 10:00:00+00")

    def boom(*args, **kwargs):
        raise RuntimeError("enrichment down")

    monkeypatch.setattr(enrichment_link, "record_deal_facts", boom)
    share_facts_with_enrichment("call-1", PHONE, FACTS)  # must not raise: the soft approval never depends on it
    assert rebuilds == []


class _FakeSlack:
    def __init__(self):
        self.posts = []

    def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        return {"ts": "1.1"}


def test_the_submission_hands_the_calls_phone_to_the_enrichment(link_db, monkeypatch):
    link_db.execute(
        text("INSERT INTO lending.call_dispositions (dialer_call_id, phone, caller_name, talk_duration_sec, disposition, raw_event) "
             "VALUES ('call-1', :p, 'Sam', 120, NULL, '{}'::jsonb)"), {"p": PHONE})
    shared, slack = [], _FakeSlack()
    monkeypatch.setattr(webhook, "lending_session", lambda: _session_over(link_db))
    monkeypatch.setattr(webhook, "slack_client", lambda: slack)
    monkeypatch.setattr(webhook, "generate_soft_approval", lambda db, **kw: SimpleNamespace(status="no_lead"))
    monkeypatch.setattr(webhook, "share_facts_with_enrichment", lambda *args: shared.append(args))
    webhook._process_submission("call-1", FACTS, "U1", "CDIAL", "1.1")
    assert shared == [("call-1", PHONE, FACTS)] and slack.posts[-1]["thread_ts"] == "1.1"
    shared.clear()
    webhook._process_submission("unknown-call", FACTS, "U1", "CDIAL", "1.1")  # no such call: nothing to share
    assert shared == []
