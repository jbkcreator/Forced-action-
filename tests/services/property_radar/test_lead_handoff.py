"""Tests for the PropertyRadar → FA Max lead handoff. No database: a fake store."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from src.services.property_radar.lead_handoff import (
    HandoffOutcome,
    ScreeningFacts,
    StagedLead,
    decide,
    phone_digits,
    run_handoff,
)
from src.services.property_radar.trace_contacts import LeadContacts, load_trace_contacts, parse_contacts

HILLSBOROUGH, PINELLAS, MIAMI_DADE = "12057", "12103", "12025"


def make_lead(radar_id: str = "P0000001", county_fips: str = HILLSBOROUGH, **overrides) -> StagedLead:
    base = StagedLead(
        radar_id=radar_id, campaign="maturity_target_lender", status="active", state="FL",
        county_fips=county_fips, county_name="SAMPLE", owner_name="SAMPLE HOLDINGS LLC",
        principal_name="PAT SAMPLE", property_address="100 SAMPLE ST", city="TAMPA", zip="33602",
        loan_amount=250000, est_maturity_date="2026-11-01",
    )
    return replace(base, **overrides)


CONTACTS = LeadContacts(emails=("pat@sample.test",), phones=("+18135550100",))


def fresh_facts(**overrides) -> ScreeningFacts:
    facts = ScreeningFacts(backflip_feed_fresh=True)
    for key, value in overrides.items():
        setattr(facts, key, value)
    return facts


class FakeStore:
    """Records what the handoff would write; never touches a database."""

    def __init__(self, facts: ScreeningFacts | None = None, fail_for: set[str] | None = None):
        self.facts = facts or fresh_facts()
        self.fail_for = fail_for or set()
        self.created: list[tuple[str, bool]] = []
        self.consent_rows: list[str] = []
        self.recorded = []

    def screening_facts(self, leads, contacts_by_radar):
        return self.facts

    def create_lead(self, lead, contacts, *, contact_rules_enabled):
        if lead.radar_id in self.fail_for:
            raise RuntimeError("simulated insert failure")
        self.created.append((lead.radar_id, contact_rules_enabled))
        if contact_rules_enabled and contacts.emails:
            self.consent_rows.append(lead.radar_id)
        return f"person-{lead.radar_id}", f"opp-{lead.radar_id}"

    def record_decisions(self, decisions):
        self.recorded.extend(decisions)


def run(store, leads, contacts=None, *, thin_path_only=False, contact_rules_enabled=False, apply=True):
    contacts = contacts if contacts is not None else {lead.radar_id: CONTACTS for lead in leads}
    return run_handoff(
        store=store, pages=[leads], contacts_by_radar=contacts, thin_path_only=thin_path_only,
        contact_rules_enabled=contact_rules_enabled, apply=apply,
    )


# -- decide(): one rule per test -------------------------------------------------

def test_clean_active_lead_is_handed_off():
    assert decide(make_lead(), CONTACTS, fresh_facts(), thin_path_only=False) == (
        HandoffOutcome.HANDED_OFF, "new_lead")


@pytest.mark.parametrize("status", ["sold", "refinanced"])
def test_sold_or_refinanced_record_is_skipped(status):
    assert decide(make_lead(status=status), CONTACTS, fresh_facts(), thin_path_only=False) == (
        HandoffOutcome.SKIPPED, f"record_{status}")


def test_unconfigured_campaign_is_never_handed_off():
    outcome, reason = decide(make_lead(campaign="cash_buyers"), CONTACTS, fresh_facts(), thin_path_only=False)
    assert (outcome, reason) == (HandoffOutcome.SKIPPED, "campaign_not_configured")


def test_email_opt_out_is_suppressed():
    facts = fresh_facts(opted_out_emails={"pat@sample.test"})
    assert decide(make_lead(), CONTACTS, facts, thin_path_only=False) == (
        HandoffOutcome.SUPPRESSED, "email_opt_out")


def test_sms_opt_out_matches_any_phone_format():
    facts = fresh_facts(opted_out_phone_digits={phone_digits("(813) 555-0100")})
    assert decide(make_lead(), CONTACTS, facts, thin_path_only=False) == (
        HandoffOutcome.SUPPRESSED, "sms_opt_out")


def test_active_backflip_campaign_is_suppressed():
    facts = fresh_facts(backflip_active_identifiers={"+18135550100"})
    assert decide(make_lead(), CONTACTS, facts, thin_path_only=False) == (
        HandoffOutcome.SUPPRESSED, "backflip_active_campaign")


def test_stale_backflip_feed_skips_rather_than_hands_off():
    facts = fresh_facts(backflip_feed_fresh=False)
    assert decide(make_lead(), CONTACTS, facts, thin_path_only=False) == (
        HandoffOutcome.SKIPPED, "backflip_feed_stale")


def test_existing_fa_max_person_is_skipped():
    facts = fresh_facts(known_emails={"pat@sample.test"})
    assert decide(make_lead(), CONTACTS, facts, thin_path_only=False) == (
        HandoffOutcome.SKIPPED, "existing_fa_max_person")


def test_lead_without_contacts_is_skipped_until_contacts_exist():
    assert decide(make_lead(), LeadContacts(), fresh_facts(), thin_path_only=False) == (
        HandoffOutcome.SKIPPED, "no_contact_data")


def test_already_handed_off_record_is_skipped():
    facts = fresh_facts(handed_off_radar_ids={"P0000001"})
    assert decide(make_lead(), CONTACTS, facts, thin_path_only=False) == (
        HandoffOutcome.SKIPPED, "already_handed_off")


# -- run_handoff(): end-to-end behaviour --------------------------------------------

def test_suppressed_lead_never_reaches_fa_max():
    store = FakeStore(fresh_facts(opted_out_emails={"pat@sample.test"}))
    report = run(store, [make_lead()])
    assert store.created == []
    assert [d.outcome for d in report.decisions] == [HandoffOutcome.SUPPRESSED]


def test_thin_path_hands_off_only_hillsborough_and_pinellas():
    leads = [make_lead("P1", HILLSBOROUGH), make_lead("P2", PINELLAS), make_lead("P3", MIAMI_DADE)]
    distinct = {lead.radar_id: LeadContacts(emails=(f"{lead.radar_id.lower()}@sample.test",)) for lead in leads}
    store = FakeStore()
    report = run(store, leads, distinct, thin_path_only=True)
    assert [radar_id for radar_id, _ in store.created] == ["P1", "P2"]
    assert {d.radar_id: d.reason for d in report.decisions}["P3"] == "outside_thin_path"


def test_contact_rules_off_writes_no_consent():
    store = FakeStore()
    run(store, [make_lead()], contact_rules_enabled=False)
    assert store.created == [("P0000001", False)]
    assert store.consent_rows == []


def test_contact_rules_on_writes_email_consent():
    store = FakeStore()
    run(store, [make_lead()], contact_rules_enabled=True)
    assert store.consent_rows == ["P0000001"]


def test_same_person_twice_in_one_page_creates_one_lead():
    leads = [make_lead("P1"), make_lead("P2")]
    store = FakeStore()
    report = run(store, leads)
    assert [radar_id for radar_id, _ in store.created] == ["P1"]
    assert {d.radar_id: d.reason for d in report.decisions}["P2"] == "existing_fa_max_person"


def test_rerun_does_not_duplicate_a_handed_off_record():
    store = FakeStore(fresh_facts(handed_off_radar_ids={"P0000001"}))
    run(store, [make_lead()])
    assert store.created == []


def test_dry_run_writes_nothing_but_reports_decisions():
    store = FakeStore()
    report = run(store, [make_lead()], apply=False)
    assert store.created == [] and store.recorded == []
    assert report.decisions[0].outcome is HandoffOutcome.HANDED_OFF
    assert "dry run" in report.summary()


def test_one_failed_insert_does_not_stop_the_batch():
    store = FakeStore(fail_for={"P1"})
    report = run(store, [make_lead("P1"), make_lead("P2", principal_name="ALEX OTHER")],
                 contacts={"P1": CONTACTS, "P2": LeadContacts(emails=("alex@other.test",))})
    reasons = {d.radar_id: d.reason for d in report.decisions}
    assert reasons == {"P1": "handoff_error", "P2": "new_lead"}
    assert len(store.recorded) == 2


# -- trace contacts -------------------------------------------------------------------

def test_parse_contacts_normalizes_and_dedupes():
    parsed = parse_contacts(["Pat@Sample.test", "pat@sample.test", "not-an-email"],
                            ["(813) 555-0100", "8135550100", "123"])
    assert parsed.emails == ("pat@sample.test",)
    assert parsed.phones == ("+18135550100",)


def test_load_trace_contacts_reads_results_csv(tmp_path: Path):
    csv_path = tmp_path / "trace_results.csv"
    csv_path.write_text(
        "RadarID,phones,emails\nP1,8135550100;8135550101,a@x.test\nP2,,\n", encoding="utf-8",
    )
    contacts = load_trace_contacts(csv_path)
    assert set(contacts) == {"P1"}
    assert contacts["P1"].phones == ("+18135550100", "+18135550101")
