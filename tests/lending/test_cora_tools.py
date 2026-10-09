"""Cora's lending tools: the read-only SQL guard, the tool set, and drafts that take their recipient
from the database. No database is touched (lead lookups are replaced)."""
from __future__ import annotations

import pytest

from packages.agent_core.governance import SafetyLevel
from packages.agent_core.tools import EgressDraft, ToolContext, ToolInputError
from src.lending import cora_tools
from src.lending.cora_tools import (
    CoraToolkit,
    DraftEmailInput,
    DraftPipelineMoveInput,
    DraftSmsInput,
    _Lead,
    check_read_only_sql,
)

CONTEXT = ToolContext(user_id="U_JOSH", is_approver=True, channel="C", thread_ts="1.0", tool_use_id="toolu_1")


@pytest.mark.parametrize("sql", [
    "SELECT count(*) FROM lending.web_leads",
    "select caller_name, count(*) from lending.call_dispositions group by 1;",
    'SELECT * FROM "lending"."web_leads" w JOIN lending.contacts c ON c.phone = w.phone',
    "WITH recent AS (SELECT * FROM lending.web_leads) SELECT count(*) FROM recent",
    "SELECT date_part('hour', call_ended_at) h, count(*) FROM lending.call_dispositions GROUP BY 1",
    "SELECT count(*) FROM lending.contacts WHERE do_not_contact",
])
def test_read_only_lookups_are_allowed(sql: str) -> None:
    assert check_read_only_sql(sql) == sql.strip().rstrip(";").strip()


@pytest.mark.parametrize("sql,reason", [
    ("DELETE FROM lending.web_leads", "SELECT or WITH"),
    ("SELECT 1; DROP TABLE lending.web_leads", "one statement"),
    ("WITH x AS (DELETE FROM lending.web_leads RETURNING *) SELECT * FROM x", "not allowed"),
    ("SELECT * INTO lending.copy FROM lending.web_leads", "not allowed"),
    ("SELECT email FROM public.subscribers", "only lending"),
    ("SELECT * FROM subscribers", "only lending"),
    ("SELECT pg_read_file('/etc/passwd')", "not allowed"),
    ("SELECT pg_sleep(30)", "not allowed"),
    ("SELECT * FROM lending.web_leads w JOIN properties p ON true", "only lending"),
])
def test_writes_other_schemas_and_server_functions_are_refused(sql: str, reason: str) -> None:
    with pytest.raises(ToolInputError, match=reason):
        check_read_only_sql(sql)


def _toolkit(monkeypatch, lead: _Lead | None) -> CoraToolkit:
    toolkit = CoraToolkit(engine=None, memory=None)  # type: ignore[arg-type]

    def fake_lead(lead_ref: str) -> _Lead:
        if lead is None:
            raise ToolInputError(f"{lead_ref} does not exist; use find_lead first")
        return lead

    monkeypatch.setattr(toolkit, "_lead", fake_lead)
    return toolkit


SAM = _Lead("web_lead:7", "Sam Smith", "+17275550100", "sam@example.com", "ghl-123")


def test_tool_set_and_safety_levels() -> None:
    tools = {tool.name: tool for tool in CoraToolkit(engine=None, memory=None).tools()}  # type: ignore[arg-type]
    assert len(tools) == 13
    egress = {name for name, tool in tools.items() if tool.safety is SafetyLevel.EXTERNAL_EGRESS}
    assert egress == {"draft_sms", "draft_email", "draft_pipeline_move"}
    assert {name for name, tool in tools.items() if tool.approver_only} == {"save_standing_rule",
                                                                          "deactivate_standing_rule"}
    assert tools["query_lending_data"].safety is SafetyLevel.READ_ONLY


def test_sms_draft_takes_the_recipient_from_the_database(monkeypatch) -> None:
    draft = _toolkit(monkeypatch, SAM).draft_sms(DraftSmsInput(lead_ref="web_lead:7", body="Hi Sam"), CONTEXT)
    assert isinstance(draft, EgressDraft)
    assert draft.channel == "ghl_sms"
    assert draft.payload == {"lead_ref": "web_lead:7", "to_phone": "+17275550100", "body": "Hi Sam"}
    assert (draft.recipient_phone, draft.recipient_email, draft.contact_ref) == ("+17275550100", "sam@example.com",
                                                                                 "web_lead:7")
    assert "…0100" in draft.summary and "+1727" not in draft.summary


def test_draft_input_cannot_carry_a_phone_number() -> None:
    with pytest.raises(ValueError):
        DraftSmsInput(lead_ref="web_lead:7", body="x", to_phone="+19995550000")
    with pytest.raises(ValueError):
        DraftSmsInput(lead_ref="+19995550000", body="x")


def test_drafts_need_the_matching_contact_detail(monkeypatch) -> None:
    no_phone = _Lead("contact:3", None, None, "a@b.com", None)
    with pytest.raises(ToolInputError, match="no phone"):
        _toolkit(monkeypatch, no_phone).draft_sms(DraftSmsInput(lead_ref="contact:3", body="x"), CONTEXT)
    no_email = _Lead("contact:4", None, "+17275550111", None, None)
    with pytest.raises(ToolInputError, match="no email"):
        _toolkit(monkeypatch, no_email).draft_email(DraftEmailInput(lead_ref="contact:4", subject="s", body="b"), CONTEXT)
    not_in_ghl = _Lead("web_lead:8", "Al", "+17275550122", None, None)
    with pytest.raises(ToolInputError, match="GoHighLevel"):
        _toolkit(monkeypatch, not_in_ghl).draft_pipeline_move(
            DraftPipelineMoveInput(lead_ref="web_lead:8", stage="Qualified"), CONTEXT)


def test_unknown_lead_is_a_tool_error(monkeypatch) -> None:
    with pytest.raises(ToolInputError, match="does not exist"):
        _toolkit(monkeypatch, None).draft_sms(DraftSmsInput(lead_ref="web_lead:999", body="x"), CONTEXT)


def test_pipeline_move_carries_the_ghl_contact(monkeypatch) -> None:
    draft = _toolkit(monkeypatch, SAM).draft_pipeline_move(
        DraftPipelineMoveInput(lead_ref="web_lead:7", stage="Qualified"), CONTEXT)
    assert draft.channel == "ghl_stage"
    assert draft.payload == {"lead_ref": "web_lead:7", "ghl_contact_id": "ghl-123", "stage": "Qualified"}
    assert draft.recipient_phone == "+17275550100"


def test_lender_fit_is_marked_internal(monkeypatch) -> None:
    from src.lending.contracts import LoanRequest, LoanType

    args = cora_tools.LenderFitInput(loan=LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=400000, state="FL"))
    result = CoraToolkit(engine=None, memory=None).lender_fit(args, CONTEXT)  # type: ignore[arg-type]
    assert "Never quote" in result["internal_only"]
    assert {"fitting", "non_fitting", "missing_fields"} <= set(result)
