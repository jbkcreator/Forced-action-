"""Go Live G17: live Tracerfy trace of staged PropertyRadar leads through the skip-trace ledger."""
from __future__ import annotations

from types import SimpleNamespace

from src.services.property_radar.live_trace import CENTS_PER_HIT, TraceBilledError, trace_staged_leads
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
    """Succeeds on the first batch, then raises ``second_error`` on every batch after."""

    def __init__(self, first_batch_rows, second_error):
        self.first_batch_rows, self.second_error = first_batch_rows, second_error
        self.submitted, self._calls = [], 0

    def __call__(self, batch):
        self._calls += 1
        self.submitted.append([r["address"] for r in batch])
        if self._calls == 1:
            return self.first_batch_rows
        raise self.second_error


def _two_batch_run(vendor, cap=None):
    written = []
    leads = [lead("R1", "100 Main St"), lead("R2", "200 Oak St", name="Other"), lead("R3", "300 Elm St", name="Third")]
    cap = cap or RunSpendCap(None)
    outcome = trace_staged_leads(
        leads, ledger={}, submit=vendor, write_ledger=written.extend, cap=cap, batch_size=1,
    )
    return outcome, written, cap


def test_a_failed_submit_ledgers_nothing_and_stops_the_run():
    """Nothing was billed when the submit step itself fails (bad key, no credits, 5xx,
    timeout), so no address may be ledgered as consumed and the run must not keep
    trying later batches against the same broken vendor."""
    vendor = FlakyVendor([HIT], RuntimeError("tracerfy 401"))
    outcome, written, cap = _two_batch_run(vendor)
    assert vendor.submitted == [["100 Main St"], ["200 Oak St"]]       # stopped; R3 never attempted
    assert outcome.aborted is True
    assert set(outcome.contacts) == {"R1"}                              # earlier batch's contacts kept
    assert [e["target_address"] for e in written] == [trace_key("100 Main St", "33602")]
    assert outcome.submitted == 1
    assert cap.projected_cents == CENTS_PER_HIT                         # failed batch not charged to the cap


def test_a_poll_failure_after_billing_ledgers_the_batch_as_consumed_with_its_queue_id():
    vendor = FlakyVendor([HIT], TraceBilledError("94858"))
    outcome, written, _ = _two_batch_run(vendor)
    assert outcome.aborted is False
    assert vendor.submitted == [["100 Main St"], ["200 Oak St"], ["300 Elm St"]]
    consumed = next(e for e in written if e["target_address"] == trace_key("200 Oak St", "33602"))
    assert consumed["success"] is False and consumed["cost_cents"] == 0 and consumed["request_ref"] == "94858"


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


class Tx:
    """Records the order of ledger/contact writes and commits/rollbacks."""

    def __init__(self, fail_contacts=False, fail_ledger_times=0):
        self.events, self.fail_contacts, self.fail_ledger_times = [], fail_contacts, fail_ledger_times

    def write_ledger(self, entries):
        if self.fail_ledger_times:
            self.fail_ledger_times -= 1
            raise RuntimeError("db down")
        self.events.append(("ledger", len(entries)))

    def write_contacts(self, contacts):
        if self.fail_contacts:
            raise RuntimeError("deadlock detected")
        self.events.append(("contacts", len(contacts)))

    def commit(self):
        self.events.append(("commit",))

    def rollback(self):
        self.events.append(("rollback",))


def _persisting_run(tx):
    return trace_staged_leads(
        [lead("R1", "100 Main St")], ledger={}, submit=Vendor([HIT]), write_ledger=tx.write_ledger,
        write_contacts=tx.write_contacts, commit=tx.commit, rollback=tx.rollback, cap=RunSpendCap(None),
    )


def test_a_billed_batchs_ledger_and_contacts_commit_together_once():
    tx = Tx()
    _persisting_run(tx)
    assert tx.events == [("ledger", 1), ("contacts", 1), ("commit",)]


def test_a_contacts_write_failure_keeps_the_paid_contacts_and_still_ledgers_the_batch():
    """Finding 5: the contacts write fails after the ledger write. The paid hits must still
    be returned, and the ledger rows must still be saved so the batch is never re-billed."""
    tx = Tx(fail_contacts=True)
    outcome = _persisting_run(tx)
    assert outcome.contacts["R1"].phones == ("+18135550111",)
    assert outcome.aborted is False
    assert tx.events == [("ledger", 1), ("rollback",), ("ledger", 1), ("commit",)]


def test_the_run_stops_spending_when_even_the_ledger_cannot_be_saved():
    tx = Tx(fail_contacts=True, fail_ledger_times=2)
    leads = [lead("R1", "100 Main St"), lead("R2", "200 Oak St", name="Other")]
    vendor = Vendor([HIT])
    outcome = trace_staged_leads(leads, ledger={}, submit=vendor, write_ledger=tx.write_ledger,
                                 write_contacts=tx.write_contacts, commit=tx.commit, rollback=tx.rollback,
                                 cap=RunSpendCap(None), batch_size=1)
    assert outcome.aborted is True and vendor.submitted == [["100 Main St"]]    # second batch never billed
    assert outcome.contacts["R1"].emails == ("jane@example.com",)               # first batch's hit kept
