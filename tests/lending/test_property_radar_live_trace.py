"""Go Live G17: live Tracerfy trace of staged PropertyRadar leads through the skip-trace ledger."""
from __future__ import annotations

from types import SimpleNamespace

from src.services.property_radar.live_trace import trace_staged_leads
from src.services.skip_trace_ledger import RunSpendCap, trace_key


def lead(radar_id, address, zip_code="33602", name="Jane Roe"):
    return SimpleNamespace(radar_id=radar_id, property_address=address, city="Tampa", state="FL",
                           zip=zip_code, principal_name=name, owner_name=name)


class Vendor:
    def __init__(self, rows):
        self.rows, self.submitted = rows, []

    def __call__(self, batch):
        self.submitted.append([r["address"] for r in batch])
        return self.rows


OTHER = {"address": "999 Elm St", "city": "Tampa", "state": "FL"}   # a returned row with no contacts
HIT = {"address": "100 Main St", "city": "Tampa", "state": "FL", "primary_phone": "8135550111",
       "primary_phone_type": "Mobile", "email_1": "Jane@Example.com"}


def run(leads, vendor, ledger=None, cap=None, batch_size=250, events=None):
    written = []
    events = events if events is not None else []
    outcome = trace_staged_leads(
        leads, ledger=ledger or {}, submit=vendor,
        write_ledger=lambda entries: (events.append("ledger"), written.extend(entries)),
        write_contacts=lambda contacts: events.append(("contacts", sorted(contacts))),
        cap=cap or RunSpendCap(None), batch_size=batch_size)
    return outcome, written


def test_traced_contacts_come_back_keyed_by_radar_id():
    outcome, written = run([lead("R1", "100 Main St")], Vendor([HIT]))
    assert outcome.contacts["R1"].phones == ("+18135550111",)
    assert outcome.contacts["R1"].emails == ("jane@example.com",)
    assert outcome.submitted == 1 and written[0]["target_address"] == trace_key("100 Main St", "33602")
    assert written[0]["vendor"] == "tracerfy" and written[0]["success"] is True and written[0]["cost_cents"] == 2


def test_an_address_already_in_the_ledger_is_never_paid_for_twice():
    ledger = {trace_key("100 Main St", "33602"): {"normal"}}
    vendor = Vendor([HIT])
    outcome, written = run([lead("R1", "100 Main St")], vendor, ledger=ledger)
    assert vendor.submitted == [] and written == []
    assert outcome.skipped_already_traced == 1 and outcome.contacts == {}


def test_two_leads_at_one_address_share_a_single_paid_trace():
    vendor = Vendor([HIT])
    outcome, _ = run([lead("R1", "100 Main St"), lead("R2", "100 Main St")], vendor)
    assert vendor.submitted == [["100 Main St"]]
    assert set(outcome.contacts) == {"R1", "R2"}


def test_an_unusable_address_is_never_submitted():
    vendor = Vendor([])
    outcome, _ = run([lead("R1", None), lead("R2", "")], vendor)
    assert vendor.submitted == [] and outcome.skipped_unkeyable == 2


def test_spend_cap_stops_before_the_batch_that_would_exceed_it():
    leads = [lead(f"R{i}", f"{i}00 Oak St") for i in range(1, 5)]
    vendor = Vendor([OTHER])
    outcome, written = run(leads, vendor, cap=RunSpendCap(5), batch_size=2)   # 2 leads = 4c fits, next 4c -> 8c > 5c
    assert len(vendor.submitted) == 1 and outcome.submitted == 2 and outcome.skipped_cap == 2


def test_a_miss_is_logged_free():
    outcome, written = run([lead("R1", "100 Main St")], Vendor([OTHER]))
    assert outcome.contacts == {} and written[0]["success"] is False and written[0]["cost_cents"] == 0


def test_contacts_are_stored_before_their_ledger_rows():
    events = []
    run([lead("R1", "100 Main St")], Vendor([HIT]), events=events)
    assert events == [("contacts", ["R1"]), "ledger"]


def test_an_empty_poll_is_unknown_not_a_miss():
    leads = [lead("R1", "100 Main St"), lead("R2", "200 Oak St")]
    vendor = Vendor([])
    events = []
    outcome, written = run(leads, vendor, batch_size=1, events=events)
    assert written == [] and events == [] and len(vendor.submitted) == 1   # nothing ledgered, run stopped
    assert outcome.skipped_no_result == 2


def test_contacts_go_only_to_the_owner_that_was_traced():
    same_building = [lead("R1", "100 Main St Apt 1", name="Jane Roe"),
                     lead("R2", "100 Main St Apt 2", name="Other Owner"),
                     lead("R3", "100 Main St Apt 3", name="JANE ROE")]
    outcome, written = run(same_building, Vendor([HIT]))
    assert set(outcome.contacts) == {"R1", "R3"} and len(written) == 1 and written[0]["success"] is True


def test_live_trace_without_apply_is_rejected_before_any_spend():
    import pytest
    from src.tasks.property_radar_lead_handoff import run as handoff_run

    with pytest.raises(ValueError):
        handoff_run(live_trace=True, apply=False)
