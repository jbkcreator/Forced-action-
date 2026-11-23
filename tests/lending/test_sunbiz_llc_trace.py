"""T3: who is looked up for each new Florida real estate LLC, and what a run spends and stores. No database, no Tracerfy."""
from datetime import date

import pytest

from src.lending.sunbiz_llc_trace import plan, trace_name
from src.services.skip_trace_ledger import trace_key
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
    """The shared ledger as the database behaves: a key is 'traced' once a row with that target_address exists."""
    ledger_rows = []
    monkeypatch.setattr(task, "already_traced",
                        lambda session, source, keys: {k: {"normal"} for k in keys if any(
                            e["target_address"] == k for e in ledger_rows)})
    monkeypatch.setattr("src.tasks.property_radar_lead_handoff._write_usage_ledger",
                        lambda session, entries: ledger_rows.extend(entries))
    return ledger_rows


def _submit(rows):
    return [{"address": row["address"], "city": row["city"], "email_1": "owner@example.com"} for row in rows]


def _two_llcs():
    return plan([_filing("L1", officers=[_officer("DOE JANE", address="12 OAK ST")]),
                 _filing("L2", officers=[_officer("ROE RICK", address="34 ELM ST")])])


def _many(n):
    return plan([_filing(f"L{i}", officers=[_officer(f"PERSON{i:03d} AL", address=f"{100 + i} OAK ST")]) for i in range(n)])


def test_a_hit_is_stored_against_its_llc_and_ledgered(traced):
    session = _Session()
    counts = task.run(session, _two_llcs(), submit=_submit, cap_cents=100, dry_run=False)
    assert counts["looked_up"] == 2 and counts["with_contacts"] == 2
    assert {u["doc"]: (u["role"], u["emails"]) for u in session.upserts} == {
        "L1": ("officer", ["owner@example.com"]), "L2": ("officer", ["owner@example.com"])}
    assert len(traced) == 2 and all(e["cost_cents"] == 2 for e in traced)


def test_a_day_over_the_cap_is_looked_up_to_the_cap_not_skipped(traced):
    counts = task.run(_Session(), _many(51), submit=_submit, cap_cents=100, dry_run=False)
    assert counts["looked_up"] == 50 and counts["over_cap"] == 1


def test_a_cap_below_one_lookup_submits_nothing(traced):
    counts = task.run(_Session(), _two_llcs(), submit=lambda rows: pytest.fail("spent past the cap"), cap_cents=1,
                      dry_run=False)
    assert counts["looked_up"] == 0 and counts["over_cap"] == 2


@pytest.mark.parametrize("cap", [0, -5])
def test_a_zero_or_negative_cap_never_calls_the_vendor(traced, cap):
    session = _Session()
    counts = task.run(session, _two_llcs(), submit=lambda rows: pytest.fail("a zero cap spent money"), cap_cents=cap,
                      dry_run=False)
    assert counts["looked_up"] == 0 and counts["over_cap"] == 2 and not session.upserts and not traced


def test_a_dry_run_counts_and_spends_nothing(traced):
    session = _Session()
    counts = task.run(session, _two_llcs(), submit=lambda rows: pytest.fail("a dry run submitted"), cap_cents=100,
                      dry_run=True)
    assert counts["would_look_up"] == 2 and counts["worst_case_cents"] == 4
    assert not session.upserts and not traced


def test_two_people_at_one_address_are_each_looked_up_and_the_plain_address_is_never_ledgered(traced):
    shared = plan([_filing("L1", officers=[_officer("DOE JANE", address="12 OAK ST")]),
                   _filing("L2", officers=[_officer("ROE RICK", address="12 OAK ST")])])
    counts = task.run(_Session(), shared, submit=_submit, cap_cents=100, dry_run=False)
    assert counts["looked_up"] == 2
    plain = trace_key("12 OAK ST", "33601")
    assert plain not in {e["target_address"] for e in traced}
    assert {e["target_address"] for e in traced} == {f"{plain}|DOE, JANE", f"{plain}|ROE, RICK"}


def test_a_propertyradar_lookup_at_the_same_address_does_not_block_a_sunbiz_person(monkeypatch, traced):
    plain = trace_key("12 OAK ST", "33601")  # what PropertyRadar ledgers for its own lead at this address
    monkeypatch.setattr(task, "already_traced",
                        lambda session, source, keys: {k: {"normal"} for k in keys if k == plain})
    counts = task.run(_Session(), plan([_filing("L1", officers=[_officer("DOE JANE", address="12 OAK ST")])]),
                      submit=_submit, cap_cents=100, dry_run=False)
    assert counts["looked_up"] == 1


def test_the_same_person_is_never_looked_up_twice_in_one_run_or_the_next(traced):
    twice = plan([_filing("L1", officers=[_officer("DOE JANE", address="12 OAK ST")]),
                  _filing("L2", officers=[_officer("DOE JANE", address="12 OAK ST")])])
    counts = task.run(_Session(), twice, submit=_submit, cap_cents=100, dry_run=False)
    assert counts["looked_up"] == 1 and counts["already_traced"] == 1
    again = task.run(_Session(), twice, submit=lambda rows: pytest.fail("paid twice"), cap_cents=100, dry_run=False)
    assert again["looked_up"] == 0 and again["already_traced"] == 2


class _Source:
    """A state server whose file for one day shows up later."""

    def __init__(self):
        self.available = {}

    def fetch(self, day):
        return self.available.get(day)


def test_a_state_file_posted_after_the_run_is_looked_up_by_a_later_run(monkeypatch):
    first_day, second_day = date(2026, 11, 20), date(2026, 11, 21)
    filings = {b"day1": [_filing("L1", officers=[_officer("DOE JANE")])]}
    monkeypatch.setattr(task, "parse_file", lambda data: (filings[data], 0))
    monkeypatch.setattr(task, "real_estate_llcs", lambda fs, day: ((f, "HOMES") for f in fs))
    source = _Source()
    assert task.window_plan(source, first_day).leads == []  # the file is not there yet at the scheduled run
    source.available[first_day] = b"day1"  # the state posts it later
    assert [lead.radar_id for lead in task.window_plan(source, second_day).leads] == ["L1"]
    assert task.window_plan(source, date(2026, 11, 25)).leads == []  # outside the catch-up window: no longer retried
