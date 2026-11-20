"""Cash purchases and investor-owned portfolios are pulled, traced and dialed like loans coming due; linking covers
every county Forced Action holds parcels for.

No network, no database: the trace, the session and the PropertyRadar port are fakes.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from config.lending_queues import SOURCE_TAG_QUEUES
from config.property_radar_campaigns import (
    INVESTOR_PORTFOLIO_MAX_PROPERTIES,
    INVESTOR_PORTFOLIO_MIN_PROPERTIES,
    build_campaign_criteria,
)
from config.property_radar_handoff import CAMPAIGN_OPPORTUNITY_TYPES, DIALER_TRACE_CAMPAIGNS
from src.lending import pr_maturity_bridge as bridge
from src.lending.queues import assign_queue
from src.tasks.property_radar_lead_handoff import DialerLead, dialer_trace_eligible


# ── campaigns ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("state", ["FL", "GA"])
def test_investor_portfolios_search_owners_of_3_to_99_properties(state):
    criteria = {c["name"]: c["value"] for c in build_campaign_criteria(state, "investor_portfolio")}
    assert criteria["NumberPropertiesOwned"] == [[INVESTOR_PORTFOLIO_MIN_PROPERTIES, INVESTOR_PORTFOLIO_MAX_PROPERTIES]]
    assert (INVESTOR_PORTFOLIO_MIN_PROPERTIES, INVESTOR_PORTFOLIO_MAX_PROPERTIES) == (3, 99)


def test_a_dialer_only_campaign_never_creates_an_fa_max_lead():
    assert DIALER_TRACE_CAMPAIGNS == {"cash_purchase", "investor_portfolio"}
    assert not DIALER_TRACE_CAMPAIGNS & set(CAMPAIGN_OPPORTUNITY_TYPES)


# ── bridge and queues ───────────────────────────────────────────────────────

@pytest.mark.parametrize("campaign,tag", [("cash_purchase", "list_2"),
                                          ("investor_portfolio", "investor_portfolio_propertyradar")])
def test_cash_and_portfolio_records_are_staged_for_transaction_ready(campaign, tag):
    assert campaign in bridge.DIALED_CAMPAIGNS
    for state in ("FL", "GA"):
        assert bridge._source_tag(campaign, state) == tag
    assert SOURCE_TAG_QUEUES[tag] == "transaction_ready"
    assert assign_queue({"source_tag": tag})["pool"] == "transaction_ready"


def test_loans_coming_due_still_go_to_verified_maturity():
    assert bridge._source_tag("maturity_target_lender", "FL") == "list_1"
    assert bridge._source_tag("maturity_target_lender", "GA") == "list_5"
    assert SOURCE_TAG_QUEUES["list_1"] == SOURCE_TAG_QUEUES["list_5"] == "verified_maturity"


# ── dialer-only trace ───────────────────────────────────────────────────────

def _lead(**kw):
    base = dict(radar_id="R1", status="active", property_state="FL", ownership_type="Individual",
                owner_name="SMITH JOHN", principal_name=None, property_address="1 MAIN ST", city="TAMPA",
                state="FL", zip="33602")
    return DialerLead(**{**base, **kw})


@pytest.mark.parametrize("lead,eligible", [
    (_lead(), True),                                                                  # a Florida person
    (_lead(status="sold"), False),                                                    # sold since it was bought
    (_lead(property_state="GA", owner_name="PEACH HOLDINGS LLC"), True),              # a Georgia company
    (_lead(property_state="GA", owner_name="DOE JANE"), False),                       # a Georgia person: never traced
    (_lead(property_state="GA", owner_name="ACME CORPORATION"), True),
    (_lead(property_state="GA", owner_name="PEACHTREE PARTNERS", ownership_type="Corporate"), True),  # no suffix
    (_lead(property_state="GA", owner_name="DOE JANE", state="FL"), False),           # GA property, FL mailing
    (_lead(property_state="FL", owner_name="DOE JANE", state="GA"), True),            # FL property, GA mailing
])
def test_only_leads_the_dialer_may_call_are_traced(lead, eligible):
    assert dialer_trace_eligible(lead) is eligible


class _Session:
    def commit(self):
        pass

    def rollback(self):
        pass


def _stages(monkeypatch):
    from src.tasks import property_radar_runner as runner

    monkeypatch.setattr("src.services.property_radar_port.get_property_radar_port", lambda: SimpleNamespace())
    return runner.default_stages()


def test_a_dialer_only_campaign_is_traced_and_never_handed_to_fa_max(monkeypatch):
    from src.services.property_radar import lead_handoff
    from src.tasks import property_radar_lead_handoff as handoff_task

    traced = []
    monkeypatch.setattr(handoff_task, "_dialer_trace",
                        lambda session, campaign: traced.append(campaign) or {"R1": object(), "R2": object()})
    monkeypatch.setattr(lead_handoff, "run_handoff", lambda **kw: pytest.fail("handed a dialer-only lead to FA Max"))
    result = _stages(monkeypatch).handoff(_Session(), "investor_portfolio", True, None)
    assert traced == ["investor_portfolio"] and result == {"traced_for_dialer": 2}


def test_a_dry_run_of_a_dialer_only_campaign_spends_nothing(monkeypatch):
    from src.tasks import property_radar_lead_handoff as handoff_task

    monkeypatch.setattr(handoff_task, "_dialer_trace", lambda *a: pytest.fail("traced on a dry run"))
    assert _stages(monkeypatch).handoff(_Session(), "cash_purchase", False, None) == {"dialer_trace_planned": 1}


def test_the_dialer_trace_goes_to_the_owners_mailing_address(monkeypatch):
    from src.services.property_radar import live_trace
    from src.services.skip_trace_ledger import trace_key
    from src.tasks import property_radar_lead_handoff as handoff_task

    row = dict(radar_id=7, status="active", property_state="FL", ownership_type="Company", owner_name="ACME LLC",
               principal_name=None, property_address="9 OAK AVE", city="ATLANTA", state="GA", zip="30303")

    class Session:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "to_regclass" in sql:
                return SimpleNamespace(scalar=lambda: "property_radar_traced_contacts")
            assert "mailing_address" in sql and "NOT EXISTS" in sql and params == {"campaign": "cash_purchase"}
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [row]))

    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "property_radar_enabled", True, raising=False)
    seen = {}

    def already_traced(session, vendor, keys):
        seen["keys"] = keys
        return {}

    def trace(leads, **kw):
        seen["leads"] = leads
        return live_trace.TraceOutcome()

    monkeypatch.setattr(handoff_task, "already_traced", already_traced)
    monkeypatch.setattr(handoff_task, "trace_staged_leads", trace)
    handoff_task._dialer_trace(Session(), "cash_purchase")
    lead = seen["leads"][0]
    assert (lead.radar_id, lead.property_address, lead.city, lead.state) == ("7", "9 OAK AVE", "ATLANTA", "GA")
    assert seen["keys"] == {trace_key("9 OAK AVE", "30303")}  # ledger key = the address sent
    assert live_trace._submit_row(lead)["address"] == "9 OAK AVE"


def test_the_dialer_trace_query_falls_back_to_the_property_address():
    from src.tasks.property_radar_lead_handoff import _DIALER_LEADS_SQL

    sql = " ".join(str(_DIALER_LEADS_SQL).split())
    assert "coalesce(nullif(r.mailing_address, ''), r.property_address) AS property_address" in sql
    assert "r.state AS property_state" in sql and "r.ownership_type" in sql


def test_the_dialer_trace_refuses_while_property_radar_is_off(monkeypatch):
    from config.settings import get_settings
    from src.tasks import property_radar_lead_handoff as handoff_task

    monkeypatch.setattr(get_settings(), "property_radar_enabled", False, raising=False)
    with pytest.raises(RuntimeError):
        handoff_task._dialer_trace(_Session(), "cash_purchase")


# ── linking ─────────────────────────────────────────────────────────────────

class _CountySession:
    def __init__(self, rows=None, fail=False):
        self.rows, self.fail = rows or [], fail

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, statement, params=None):
        from sqlalchemy.exc import OperationalError

        if self.fail:
            raise OperationalError("SELECT", {}, Exception("down"))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: self.rows))


def test_every_county_with_parcels_is_linked_not_just_the_configured_three():
    from src.services.property_radar import linking

    counties = linking.linkable_counties(_CountySession(rows=[{"county_id": "polk", "fips": "105"},
                                                              {"county_id": "fulton", "fips": "13121"}]))
    assert counties["12105"] == "polk" and counties["13121"] == "fulton"
    assert counties["12057"] == "hillsborough"  # the configured counties stay


def test_a_failed_county_lookup_falls_back_to_the_configured_counties():
    from config.property_radar import COUNTY_FIPS_TO_SLUG
    from src.services.property_radar import linking

    assert linking.linkable_counties(_CountySession(fail=True)) == dict(COUNTY_FIPS_TO_SLUG)
