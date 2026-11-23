"""T3: who is looked up for each new Florida real estate LLC, and what a run spends and stores. No database, no Tracerfy."""
from datetime import date

import pytest

from src.lending.sunbiz_llc_trace import plan, trace_name
from src.scrappers.sunbiz.daily_filings import Filing, Officer
from src.tasks import sunbiz_llc_trace as task


def _officer(name="SMITH JOHN M", type_="P", address="12 OAK ST", zip_code="33601"):
    return Officer(title="MGR", type=type_, name=name, address=address, city="TAMPA", state="FL", zip=zip_code)


def _filing(doc="L26000000001", officers=(), agent_type="C", agent_name="", agent_address="", agent_zip=""):
    return Filing(document_number=doc, name=f"{doc} HOMES LLC", status="A", filing_type="FLAL", file_date=date(2026, 11, 20),
                  address_1="", address_2="", city="", state="", zip="", country="US", mail_address_1="", mail_address_2="",
                  mail_city="", mail_state="", mail_zip="", mail_country="US", agent_name=agent_name, agent_type=agent_type,
                  agent_address=agent_address, agent_city="TAMPA", agent_state="FL", agent_zip=agent_zip,
                  officers=tuple(officers))


@pytest.mark.parametrize("raw, expected", [
    ("SMITH JOHN", "SMITH, JOHN"), ("SMITH JOHN M", "SMITH, JOHN"), ("SMITH, JOHN", "SMITH, JOHN"),
    ("SMITH", ""), ("", ""),
])
def test_the_state_name_order_is_turned_into_last_comma_first(raw, expected):
    assert trace_name(raw) == expected


def test_the_first_person_officer_is_the_subject_and_company_officers_are_ignored():
    result = plan([_filing(officers=[_officer("ACME CAPITAL", "C"), _officer("DOE JANE"), _officer("ROE RICK")])])
    assert [(lead.owner_name, lead.role) for lead in result.leads] == [("DOE, JANE", "officer")]


def test_a_person_agent_is_the_fallback_and_a_company_agent_is_not():
    person = _filing("L1", officers=[_officer(type_="C")], agent_type="P", agent_name="LOPEZ ANA",
                     agent_address="9 PINE RD", agent_zip="33602")
    company = _filing("L2", officers=[], agent_type="C", agent_name="REGISTERED AGENTS INC",
                      agent_address="1 MAIN ST", agent_zip="33602")
    result = plan([person, company])
    assert [(lead.radar_id, lead.role) for lead in result.leads] == [("L1", "agent")]
    assert result.skipped["no_person"] == 1


def test_an_address_shared_by_more_than_two_llcs_is_skipped():
    filings = [_filing(f"L{n}", officers=[_officer(f"PERSON{n} AL", address="1 FORMATION WAY")]) for n in range(3)]
    filings.append(_filing("L9", officers=[_officer("OWNER OLA", address="77 ELM ST")]))
    result = plan(filings)
    assert [lead.radar_id for lead in result.leads] == ["L9"]
    assert result.skipped["shared_address"] == 3


class _Session:
    def __init__(self):
        self.upserts = []

    def execute(self, statement, params=None):
        self.upserts.extend(params or [])

    def commit(self):
        pass


@pytest.fixture
def traced(monkeypatch):
    ledger_rows = []
    monkeypatch.setattr(task, "already_traced", lambda session, source, keys: {})
    monkeypatch.setattr("src.tasks.property_radar_lead_handoff._write_usage_ledger",
                        lambda session, entries: ledger_rows.extend(entries))
    return ledger_rows


def _submit(rows):
    return [{"address": row["address"], "city": row["city"], "email_1": "owner@example.com"} for row in rows]


def _two_llcs():
    return plan([_filing("L1", officers=[_officer("DOE JANE", address="12 OAK ST")]),
                 _filing("L2", officers=[_officer("ROE RICK", address="34 ELM ST")])])


def test_a_hit_is_stored_against_its_llc_and_ledgered(traced):
    session = _Session()
    counts = task.run(session, _two_llcs(), submit=_submit, cap_cents=100, dry_run=False)
    assert counts["looked_up"] == 2 and counts["with_contacts"] == 2
    assert {u["doc"]: (u["role"], u["emails"]) for u in session.upserts} == {
        "L1": ("officer", ["owner@example.com"]), "L2": ("officer", ["owner@example.com"])}
    assert len(traced) == 2 and all(e["cost_cents"] == 2 for e in traced)


def test_the_cost_cap_stops_a_run_before_anything_is_submitted(traced):
    session = _Session()
    counts = task.run(session, _two_llcs(), submit=lambda rows: pytest.fail("spent past the cap"), cap_cents=2,
                      dry_run=False)
    assert counts["looked_up"] == 0 and counts["over_cap"] == 2 and not session.upserts


def test_a_dry_run_counts_and_spends_nothing(traced):
    session = _Session()
    counts = task.run(session, _two_llcs(), submit=lambda rows: pytest.fail("a dry run submitted"), cap_cents=100,
                      dry_run=True)
    assert counts["would_look_up"] == 2 and counts["worst_case_cents"] == 4
    assert not session.upserts and not traced
