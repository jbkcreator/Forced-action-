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


def test_county_last_comma_first_names_are_sent_first_name_first_without_the_co_owner():
    from src.services.property_radar.live_trace import _split_name
    assert _split_name("MCGOVERN,STEPHEN & JULIANNE") == ("STEPHEN", "MCGOVERN")
    assert _split_name("MARLER,TOM") == ("TOM", "MARLER")
    assert _split_name("DE LA CRUZ, MARIA E") == ("MARIA E", "DE LA CRUZ")
    assert _split_name("Ann Lee") == ("Ann", "Lee")
    assert _split_name(None) == ("", "")


def test_the_requested_batch_size_reaches_the_tracerfy_request(monkeypatch, tmp_path):
    from src.tasks import lending_pr_calling_list_trace as task
    sizes = []

    def submit(rows):
        sizes.append(len(rows))
        return [{"address": rows[0]["address"], "mobile_1": "8135550100"}]

    leads = [task.Lead(radar_id=f"R{i}", property_address=f"{i} Main St", city="Tampa", state="FL", zip="33602",
                       owner_name="LEE,ANN", principal_name=None) for i in range(600)]
    monkeypatch.setattr(task, "already_traced", lambda db, vendor, keys: {})
    submit.lands_on_disk = True
    task.run_trace(None, leads, folder=tmp_path, trace_limit=600, balance=lambda: 1000, submit=submit,
                   write_ledger=lambda e: None, write_contacts=lambda c: None, batch_size=600)
    assert sizes == [600]


def _adv_lead(rid, owner, address="1 Main St"):
    from types import SimpleNamespace
    return SimpleNamespace(radar_id=rid, property_address=address, city="Tampa", state="FL", zip="33602",
                           owner_name=owner, principal_name=None)


def test_an_advanced_hit_is_kept_only_when_the_last_name_is_the_owners():
    from src.services.property_radar.live_trace import trace_staged_leads
    from src.services.skip_trace_ledger import ADVANCED, NORMAL, RunSpendCap, trace_key
    leads = [_adv_lead("R1", "LEE,ANN", "1 Main St"), _adv_lead("R2", "DIAZ,BO", "2 Elm St")]
    ledger = {trace_key(l.property_address, l.zip): {NORMAL} for l in leads}   # both normal misses
    rows = [{"address": "1 MAIN ST", "last_name": "LEE", "mobile_1": "8135550101"},
            {"address": "2 ELM ST", "last_name": "TENANT", "mobile_1": "8135550102"}]
    written = []
    out = trace_staged_leads(leads, ledger=ledger, submit=lambda r: rows, write_ledger=written.extend,
                             cap=RunSpendCap(1000), mode=ADVANCED)
    assert set(out.contacts) == {"R1"} and out.skipped_name_mismatch == 1
    assert all(e["trace_type"] == ADVANCED for e in written) and len(written) == 2   # both paid hits ledgered


def test_an_address_already_tried_advanced_is_never_sent_again():
    from src.services.property_radar.live_trace import trace_staged_leads
    from src.services.skip_trace_ledger import ADVANCED, NORMAL, RunSpendCap, trace_key
    lead = _adv_lead("R1", "LEE,ANN")
    sent = []
    trace_staged_leads([lead], ledger={trace_key(lead.property_address, lead.zip): {NORMAL, ADVANCED}},
                       submit=lambda r: sent.extend(r) or [], write_ledger=lambda e: None,
                       cap=RunSpendCap(1000), mode=ADVANCED)
    lead2 = _adv_lead("R2", "DIAZ,BO", "9 Oak Ave")
    trace_staged_leads([lead2], ledger={}, submit=lambda r: sent.extend(r) or [], write_ledger=lambda e: None,
                       cap=RunSpendCap(1000), mode=ADVANCED)                        # never had a normal try
    assert sent == []
