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
    relations_outside_schema,
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
    ("SELECT * FROM lending.web_leads, public.subscribers", "only lending"),
    ("SELECT * FROM lending.web_leads w, subscribers s WHERE s.id = w.id", "only lending"),
    ("SELECT * FROM lending.web_leads AS w , lending.contacts c, public.users u", "only lending"),
    ("SELECT count(*) FROM (SELECT 1 FROM lending.contacts) x, public.subscribers", "only lending"),
    ("SELECT consent_text FROM lending.web_leads", "consent wording"),
    ("SELECT RAW_CERTIFICATE AS c FROM lending.lead_consent_certificates", "consent wording"),
    ("SELECT w.id FROM lending.web_leads w WHERE tcpa_disclosure_text IS NOT NULL", "consent wording"),
])
def test_writes_other_schemas_and_server_functions_are_refused(sql: str, reason: str) -> None:
    with pytest.raises(ToolInputError, match=reason):
        check_read_only_sql(sql)


def test_comma_list_of_lending_tables_is_allowed() -> None:
    sql = "SELECT count(*) FROM lending.web_leads w, lending.contacts c WHERE c.phone = w.phone"
    assert check_read_only_sql(sql) == sql


def _scan(schema: str, relation: str) -> dict:
    return {"Node Type": "Seq Scan", "Schema": schema, "Relation Name": relation}


def test_query_plan_check_finds_every_table_outside_lending() -> None:
    plan = [{"Plan": {"Node Type": "Nested Loop", "Plans": [
        _scan("lending", "web_leads"),
        {"Node Type": "Subquery Scan", "Plans": [_scan("public", "subscribers")]},
        {"Node Type": "Hash", "Plans": [_scan("pg_catalog", "pg_authid")]},
    ]}}]
    assert relations_outside_schema(plan) == {"public.subscribers", "pg_catalog.pg_authid"}
    assert relations_outside_schema([{"Plan": _scan("lending", "contacts")}]) == set()


def test_query_tool_refuses_a_plan_that_reads_outside_lending(monkeypatch) -> None:
    plan = [{"Plan": _scan("public", "subscribers")}]
    executed: list[str] = []

    class FakeSession:
        def execute(self, statement, *args):
            executed.append(str(statement))
            return type("R", (), {"scalar": lambda self: plan})()

    toolkit = CoraToolkit(engine=None, memory=None)  # type: ignore[arg-type]
    monkeypatch.setattr(toolkit, "_read_only", lambda: _context_manager(FakeSession()))
    with pytest.raises(ToolInputError, match="public.subscribers"):
        toolkit.query_lending_data(cora_tools.QueryInput(sql="SELECT * FROM lending.web_leads_view"), CONTEXT)
    assert len(executed) == 1 and executed[0].startswith("EXPLAIN")  # the query itself never ran


def test_consent_match_flag_is_not_mistaken_for_consent_text() -> None:
    sql = "SELECT count(*) FROM lending.web_leads WHERE consent_text_matches"
    assert check_read_only_sql(sql) == sql


def test_select_star_blanks_consent_text_but_keeps_other_columns(monkeypatch) -> None:
    class Result:
        def keys(self):
            return ["id", "sms_consent", "consent_text"]

        def all(self):
            return [(1, True, "I agree to receive texts from Next Deal Lending"), (2, False, None)]

    class FakeSession:
        def execute(self, statement, *args):
            if str(statement).startswith("EXPLAIN"):
                return type("R", (), {"scalar": lambda self: [{"Plan": _scan("lending", "web_leads")}]})()
            return Result()

    toolkit = CoraToolkit(engine=None, memory=None)  # type: ignore[arg-type]
    monkeypatch.setattr(toolkit, "_read_only", lambda: _context_manager(FakeSession()))
    result = toolkit.query_lending_data(cora_tools.QueryInput(sql="SELECT * FROM lending.web_leads"), CONTEXT)
    assert result["rows"] == [
        {"id": 1, "sms_consent": True, "consent_text": cora_tools.CONSENT_TEXT_HIDDEN},
        {"id": 2, "sms_consent": False, "consent_text": None},
    ]


class _context_manager:
    def __init__(self, value) -> None:
        self._value = value

    def __enter__(self):
        return self._value

    def __exit__(self, *exc) -> bool:
        return False


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
    assert len(tools) == 16
    egress = {name for name, tool in tools.items() if tool.safety is SafetyLevel.EXTERNAL_EGRESS}
    assert egress == {"draft_sms", "draft_email", "draft_pipeline_move"}
    assert {name for name, tool in tools.items() if tool.approver_only} == {
        "save_standing_rule", "deactivate_standing_rule", "dismiss_inbox_item", "route_inbox_item"}
    assert tools["query_lending_data"].safety is SafetyLevel.READ_ONLY
    assert tools["list_inbox_items"].safety is SafetyLevel.READ_ONLY
    # Inbox routing posts to the team's own channels, never to a borrower: no approval card, the approver's ask is it.
    assert tools["route_inbox_item"].safety is SafetyLevel.INTERNAL_WRITE


def test_sms_draft_takes_the_recipient_from_the_database(monkeypatch) -> None:
    draft = _toolkit(monkeypatch, SAM).draft_sms(DraftSmsInput(lead_ref="web_lead:7", body="Hi Sam"), CONTEXT)
    assert isinstance(draft, EgressDraft)
    assert draft.channel == "ghl_sms"
    assert draft.payload == {"lead_ref": "web_lead:7", "to_phone": "+17275550100", "body": "Hi Sam"}
    assert (draft.recipient_phone, draft.recipient_email, draft.contact_ref) == ("+17275550100", "sam@example.com",
                                                                                 "web_lead:7")
    assert "…0100" in draft.summary and "+1727" not in draft.summary


@pytest.mark.parametrize("text_value", [
    "We can do 100% rehab funding",
    "Rates start at 9.5%",
    "That's about $350,000 total",
    "Around 400k purchase price",
    "2 points at closing",
    "Rate of 11 on this one",
    "LTV up to 75 on purchase",
    "The origination fee is 1,500",
    "Monthly payment around 2,100",
    "A 12-month term works",
    "We usually go 90 percent of cost",
])
def test_borrower_drafts_that_quote_numbers_are_refused(text_value: str) -> None:
    assert cora_tools.borrower_draft_problem({"body": text_value}) is not None


@pytest.mark.parametrize("text_value", [
    "Hi Laira, thanks for your fix and flip inquiry in Tampa. When are you hoping to close?",
    "Do you have a property under contract? Reply STOP to opt out.",
    "You mentioned 1 to 2 completed projects. Grab a time that works for you here.",
    "Call me back at (727) 436-9951 when you have 10 minutes.",
])
def test_ordinary_borrower_drafts_pass(text_value: str) -> None:
    assert cora_tools.borrower_draft_problem({"body": text_value}) is None


def test_quoting_drafts_are_refused_before_any_lookup(monkeypatch) -> None:
    toolkit = _toolkit(monkeypatch, SAM)
    with pytest.raises(ToolInputError, match="never quote numbers"):
        toolkit.draft_sms(DraftSmsInput(lead_ref="web_lead:7", body="Rates from 9.5%"), CONTEXT)
    with pytest.raises(ToolInputError, match="subject"):
        toolkit.draft_email(DraftEmailInput(lead_ref="web_lead:7", subject="Rates from 9.5%", body="Hi"), CONTEXT)


def test_revision_check_covers_messages_but_not_stage_moves() -> None:
    assert cora_tools.draft_content_check("ghl_email", {"subject": "Hi", "body": "Rates from 9.5%"})
    assert cora_tools.draft_content_check("ghl_stage", {"stage": "Funded 2026"}) is None


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
