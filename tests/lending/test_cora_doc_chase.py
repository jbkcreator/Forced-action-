"""P2.4 in Cora: the start_document_chase / get_document_chases tools and the two sweep steps."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from packages.agent_core.governance import SafetyLevel
from packages.agent_core.tools import ToolContext, ToolInputError
from src.lending import cora_agent
from src.lending.closing.doc_chase import open_chase_summaries, start_chase
from src.lending.closing.drafts import Recipient
from src.lending.cora_tools import CoraToolkit, StartChaseInput, _Lead
from tests.lending.test_cora_agent import _sweep_services, _SweepParts

JOSH = ToolContext(user_id="U_JOSH", is_approver=True, channel="C", thread_ts="1.0", tool_use_id="toolu_1")
SAM = _Lead("web_lead:7", "Sam Smith", "+17275550100", "sam@example.com", "ghl-123")
NOW = datetime(2026, 11, 17, 15, 0, tzinfo=timezone.utc)


class _Same:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, *exc):
        return False


@pytest.fixture
def chase_on(monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_doc_chase_enabled", True, raising=False)
    monkeypatch.setattr(get_settings(), "lending_partner_link_base_url", "https://api.example.com", raising=False)


def _toolkit(monkeypatch, db=None):
    toolkit = CoraToolkit(engine=None, memory=None, inbox_session=lambda: _Same(db))  # type: ignore[arg-type]
    monkeypatch.setattr(toolkit, "_lead", lambda ref: SAM)
    return toolkit


def test_tools_registered_with_the_right_safety():
    tools = {tool.name: tool for tool in CoraToolkit(engine=None, memory=None).tools()}  # type: ignore[arg-type]
    assert tools["start_document_chase"].safety is SafetyLevel.INTERNAL_WRITE and tools["start_document_chase"].approver_only
    assert tools["get_document_chases"].safety is SafetyLevel.READ_ONLY


def test_start_document_chase_opens_a_chase_and_hides_links(monkeypatch, closing_db, chase_on):
    result = _toolkit(monkeypatch, closing_db).start_document_chase(StartChaseInput(lead_ref="web_lead:7"), JOSH)
    assert result["reused"] is False and len(result["documents_requested"]) == 6 and "http" not in str(result)
    row = closing_db.execute(text("SELECT first_name, accepted_by, phone FROM lending.doc_chases")).one()
    assert row == ("Sam", "slack:U_JOSH", "+17275550100")


def test_start_document_chase_refused_while_switched_off(monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_doc_chase_enabled", False, raising=False)
    with pytest.raises(ToolInputError):
        _toolkit(monkeypatch).start_document_chase(StartChaseInput(lead_ref="web_lead:7"), JOSH)


def test_quote_id_must_be_a_uuid():
    with pytest.raises(ValueError):
        StartChaseInput(lead_ref="web_lead:7", quote_id="drop table")


def test_open_chase_summaries(closing_db, chase_on):
    chase_id = start_chase(closing_db, Recipient("Mark", "+18135550101", None, None), lead_ref="web_lead:1",
                           accepted_by="slack:U1", now=NOW - timedelta(days=3)).chase_id
    closing_db.execute(text("UPDATE lending.doc_chase_items SET received_at = now() WHERE item_code = 'valuation'"))
    closing_db.execute(text("INSERT INTO lending.doc_chase_touches (chase_id, chase_day) VALUES (:c, 0), (:c, 2)"),
                       {"c": chase_id})
    summary, = open_chase_summaries(closing_db, NOW)
    assert summary == {"chase": chase_id, "borrower": "Mark (…0101)", "days_since_acceptance": 3, "received": 1,
                       "required": 6, "missing": ["Signed purchase agreement", "Scope of work and budget",
                                                  "LLC documents", "Photo ID for each guarantor",
                                                  "Last 30 days of bank statements (down payment plus reserves)"],
                       "last_step": "day 2 reminder", "next_step": "day 5 reminder"}


def test_sweep_runs_closing_steps_before_relay_and_survives_their_errors():
    order: list[str] = []
    parts = _SweepParts()
    original_run = parts.run
    parts.run = lambda: order.append("relay") or original_run()

    def broken():
        order.append("blockers")
        raise RuntimeError("db down")

    services = _sweep_services(parts, None)._replace(closing_blockers=broken,
                                                     doc_chase=lambda: order.append("chase") or 1)
    cora_agent.run_sweep(services)
    assert order == ["blockers", "chase", "relay"]


def test_closing_steps_off_when_flags_false():
    settings = SimpleNamespace(lending_closing_blockers_enabled=False, lending_doc_chase_enabled=False)
    assert cora_agent.build_closing_steps(settings, engine=None, chat=None, gate=None, config=None) == (None, None)
