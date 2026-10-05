"""Go Live G17: live Tracerfy trace of staged PropertyRadar leads through the skip-trace ledger."""
from __future__ import annotations

from types import SimpleNamespace

from src.services.property_radar.live_trace import trace_staged_leads
from src.services.property_radar.trace_contacts import parse_contacts
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


HIT = {"address": "100 Main St", "city": "Tampa", "state": "FL", "primary_phone": "8135550111",
       "primary_phone_type": "Mobile", "email_1": "Jane@Example.com"}


def run(leads, vendor, ledger=None, cap=None, batch_size=250):
    written = []
    outcome = trace_staged_leads(leads, ledger=ledger or {}, submit=vendor, write_ledger=written.extend,
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
    outcome, _ = run([lead("R1", "100 Main St"), lead("R2", "100 Main St", name="Other Co")], vendor)
    assert vendor.submitted == [["100 Main St"]]
    assert set(outcome.contacts) == {"R1", "R2"}


def test_an_unusable_address_is_never_submitted():
    vendor = Vendor([])
    outcome, _ = run([lead("R1", None), lead("R2", "")], vendor)
    assert vendor.submitted == [] and outcome.skipped_unkeyable == 2


def test_spend_cap_stops_before_the_batch_that_would_exceed_it():
    leads = [lead(f"R{i}", f"{i}00 Oak St") for i in range(1, 5)]
    vendor = Vendor([])
    outcome, written = run(leads, vendor, cap=RunSpendCap(5), batch_size=2)   # 2 leads = 4c fits, next 4c -> 8c > 5c
    assert len(vendor.submitted) == 1 and outcome.submitted == 2 and outcome.skipped_cap == 2


def test_a_miss_is_logged_free():
    outcome, written = run([lead("R1", "100 Main St")], Vendor([]))
    assert outcome.contacts == {} and written[0]["success"] is False and written[0]["cost_cents"] == 0


def test_a_newly_traced_hit_is_persisted_for_a_later_run_to_read_back():
    """Finding #4: a paid-for contact must survive even if this run's handoff can't
    use it (e.g. a stale Backflip feed), so a later run reads it back instead of
    paying Tracerfy again for the same key."""
    written_contacts = {}
    outcome = trace_staged_leads(
        [lead("R1", "100 Main St")], ledger={}, submit=Vendor([HIT]), write_ledger=lambda e: None,
        cap=RunSpendCap(None), write_contacts=lambda cs: written_contacts.update(cs),
    )
    key = trace_key("100 Main St", "33602")
    assert key in written_contacts
    assert written_contacts[key].emails == ("jane@example.com",)


class FlakyVendor:
    """Succeeds on the first batch, raises on every batch after."""

    def __init__(self, first_batch_rows):
        self.first_batch_rows, self.submitted, self._calls = first_batch_rows, [], 0

    def __call__(self, batch):
        self._calls += 1
        self.submitted.append([r["address"] for r in batch])
        if self._calls == 1:
            return self.first_batch_rows
        raise RuntimeError("tracerfy 500")


def test_a_later_batch_failure_still_returns_the_earlier_batchs_contacts():
    """Finding #8's own suggested regression: the second of two batches raises, and
    the first batch's contacts/ledger entries must not be thrown away with it."""
    vendor = FlakyVendor([HIT])
    written = []
    leads = [lead("R1", "100 Main St"), lead("R2", "200 Oak St", name="Other")]
    outcome = trace_staged_leads(
        leads, ledger={}, submit=vendor, write_ledger=written.extend,
        cap=RunSpendCap(None), batch_size=1,
    )
    assert vendor.submitted == [["100 Main St"], ["200 Oak St"]]   # both attempted
    assert outcome.contacts == {"R1": outcome.contacts["R1"]}      # R1 survived; R2 did not raise up
    assert outcome.submitted == 2
    # The failed batch's key is still written to the ledger (as a zero-cost miss), so a
    # later run's already_traced() check won't bill it again.
    failed_entry = next(e for e in written if e["target_address"] == trace_key("200 Oak St", "33602"))
    assert failed_entry["success"] is False and failed_entry["cost_cents"] == 0


def test_an_already_traced_key_reads_its_persisted_contact_back():
    """The ledger already blocks re-billing this key; without read_contacts the lead
    would otherwise be reported with no contacts every run after the first."""
    persisted = {trace_key("100 Main St", "33602"): parse_contacts(["jane@example.com"], [])}
    ledger = {trace_key("100 Main St", "33602"): {"normal"}}
    vendor = Vendor([HIT])
    outcome = trace_staged_leads(
        [lead("R1", "100 Main St")], ledger=ledger, submit=vendor, write_ledger=lambda e: None,
        cap=RunSpendCap(None), read_contacts=lambda keys: {k: persisted[k] for k in keys if k in persisted},
    )
    assert vendor.submitted == []                       # still never re-billed
    assert outcome.contacts["R1"].emails == ("jane@example.com",)
