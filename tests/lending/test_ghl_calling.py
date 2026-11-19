"""A phone worked from GHL is never also dialed by BatchDialer: skipped at load, deleted from the dialer by the sweep.

No network, no database: the database helpers are faked. The SQL itself is exercised by the Postgres tests in
test_dialer_sweep.py and test_compliance_filter.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config.lending_compliance import GHL_CALLING_LOOKBACK_DAYS, ReasonCode, RemovalReason
from src.lending import compliance, ghl_calling
from src.lending.dialer_port import BatchDialerAdapter

NOW = datetime(2026, 11, 3, 16, 0, tzinfo=timezone.utc)
GHL_PHONE = "+18135557101"
DIALER_PHONE = "+18135557102"


class Rows:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class RecordingDb:
    """Records every statement; answers the GHL-calling lookup with ``ghl_rows``."""

    def __init__(self, ghl_rows=()):
        self.ghl_rows, self.statements = list(ghl_rows), []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params))
        return Rows([(p,) for p in self.ghl_rows])


# The lookup

def test_lookup_returns_the_phones_ghl_owns_and_looks_back_fourteen_days():
    db = RecordingDb(ghl_rows=[GHL_PHONE])
    assert ghl_calling.ghl_calling_phones(db, [GHL_PHONE, DIALER_PHONE], now=NOW) == {GHL_PHONE}
    sql, params = db.statements[0]
    assert params["phones"] == [GHL_PHONE, DIALER_PHONE] and params["source"] == "ghl"
    assert params["since"] == NOW - timedelta(days=GHL_CALLING_LOOKBACK_DAYS)
    assert GHL_CALLING_LOOKBACK_DAYS == 14
    for source in ("lending.lendingflow_leads", "lending.web_leads", "d.call_source = :source"):
        assert source in sql


def test_only_a_call_that_got_through_makes_a_number_ghls():
    """One calling rule for every lead: an unanswered GHL call leaves the number in BatchDialer; a live-conversation
    result, or GHL's answered status with a minute of talk when no result was posted, hands it to GHL."""
    from config.lending_compliance import REACHED_CODES, REACHED_GHL_STATUSES, REACHED_MIN_TALK_SECONDS
    db = RecordingDb()
    ghl_calling.ghl_calling_phones(db, [GHL_PHONE], now=NOW)
    sql, params = db.statements[0]
    assert "d.disposition = ANY(:codes)" in sql and "d.disposition_raw = ANY(:statuses)" in sql
    assert params["codes"] == list(REACHED_CODES) and "NO_ANSWER" not in REACHED_CODES
    assert "BOOKED" in REACHED_CODES and "CONNECTED_NOT_INTERESTED" in REACHED_CODES
    assert params["statuses"] == list(REACHED_GHL_STATUSES) and params["min_talk"] == REACHED_MIN_TALK_SECONDS == 60


def test_the_reverse_lookup_reads_batchdialer_calls_with_the_same_rule():
    db = RecordingDb(ghl_rows=[DIALER_PHONE])
    assert ghl_calling.reached_in_batchdialer_phones(db, [DIALER_PHONE], now=NOW) == {DIALER_PHONE}
    sql, params = db.statements[0]
    assert params["source"] == "batchdialer" and "d.disposition = ANY(:codes)" in sql
    assert ghl_calling.reached_in_batchdialer_phones(db, []) == set()


def test_a_lendingflow_lead_is_left_to_the_lendingflow_dialer_flow():
    """LendingFlow leads are loaded into the LendingFlow campaign and removed once handled in GHL
    (src/lending/lendingflow_dialer.py), so the GHL-ownership rule excludes them outright."""
    db = RecordingDb()
    ghl_calling.ghl_calling_phones(db, [GHL_PHONE], now=NOW)
    sql, _ = db.statements[0]
    assert "NOT EXISTS (SELECT 1 FROM lending.lendingflow_leads" in sql


def test_both_permanent_ghl_reasons_delete_the_dialer_contact():
    from config.lending_compliance import PERMANENT_REMOVAL_REASONS, RemovalReason
    from src.lending import dialer_port
    assert {RemovalReason.OPT_OUT, RemovalReason.GHL_CALLING, RemovalReason.HANDLED_IN_GHL} == PERMANENT_REMOVAL_REASONS
    assert dialer_port._DELETING_REASONS == {r.value for r in PERMANENT_REMOVAL_REASONS}


def test_lookup_with_no_phones_runs_no_query():
    db = RecordingDb()
    assert ghl_calling.ghl_calling_phones(db, []) == set()
    assert db.statements == []


# The load gate (inside filter_loadable, before the paid scrub)

@pytest.fixture
def gate(monkeypatch):
    """filter_loadable with every database helper faked: nothing suppressed, exhausted or pre-scrubbed."""
    scrubbed: list[list[str]] = []
    monkeypatch.setattr(compliance, "_suppressed_phones", lambda db, phones: set())
    monkeypatch.setattr(compliance, "_attempt_history_exhausted_phones", lambda db, phones, now: set())
    monkeypatch.setattr(compliance, "_stored_scrubs", lambda db, phones: {})
    monkeypatch.setattr(compliance, "_stamp_contacts", lambda db, fresh: None)
    monkeypatch.setattr(compliance, "_flag_nurture", lambda db, phones: None)
    monkeypatch.setattr(compliance, "ghl_calling_phones", lambda db, phones, now=None: {GHL_PHONE} & set(phones))

    def fake_scrub(db, phones, scrubber):
        scrubbed.append(list(phones))
        return {p: compliance.ScrubResult(national_dnc=False, state_dnc=False, litigator=False, checked_at=NOW)
                for p in phones}

    monkeypatch.setattr(compliance, "_scrub", fake_scrub)
    return scrubbed


def test_a_phone_worked_from_ghl_is_never_loaded_and_never_scrubbed(gate):
    results = compliance.filter_loadable([{"phone": GHL_PHONE}, {"phone": DIALER_PHONE}], object(),
                                         now=NOW, scrubber=lambda phones: [])
    assert results[0].allowed is False and results[0].reason is ReasonCode.GHL_CALLING
    assert results[1].allowed is True
    assert gate == [[DIALER_PHONE]]


# The sweep

class FakeDialer:
    def __init__(self, fail=()):
        self.removed, self.fail = [], set(fail)

    def remove(self, phone, *, reason):
        if phone in self.fail:
            raise RuntimeError("dialer 500")
        self.removed.append((phone, reason))


def test_sweep_deletes_a_loaded_ghl_phone_and_closes_its_load_row_and_hold(monkeypatch):
    monkeypatch.setattr(compliance, "ghl_calling_phones", lambda db, phones, now=None: {GHL_PHONE} & set(phones))
    db, dialer = RecordingDb(), FakeDialer()
    moved = compliance._move_to_ghl_calling(db, [GHL_PHONE, DIALER_PHONE], NOW, dialer.remove)
    assert moved == {GHL_PHONE}
    assert dialer.removed == [(GHL_PHONE, RemovalReason.GHL_CALLING.value)]
    load_update, hold_update = db.statements
    assert "dialer_load_records SET active = false" in load_update[0]
    assert "dialer_holds SET released_at" in hold_update[0]
    assert load_update[1] == [{"phone": GHL_PHONE, "now": NOW, "why": "ghl_calling"}]


def test_a_failed_delete_keeps_the_row_active_so_the_next_sweep_retries(monkeypatch):
    monkeypatch.setattr(compliance, "ghl_calling_phones", lambda db, phones, now=None: {GHL_PHONE})
    db = RecordingDb()
    assert compliance._move_to_ghl_calling(db, [GHL_PHONE], NOW, FakeDialer(fail={GHL_PHONE}).remove) == set()
    assert db.statements == []


def test_no_ghl_phones_means_no_dialer_call_and_no_write(monkeypatch):
    monkeypatch.setattr(compliance, "ghl_calling_phones", lambda db, phones, now=None: set())
    db, dialer = RecordingDb(), FakeDialer()
    assert compliance._move_to_ghl_calling(db, [DIALER_PHONE], NOW, dialer.remove) == set()
    assert dialer.removed == [] and db.statements == []


# The dialer: a GHL handover deletes the contact (permanent), it never just leaves the campaign

class FakeHttp:
    def __init__(self):
        self.calls = []

    def __call__(self, method, path, *, json=None):
        self.calls.append((method, path))
        return {}


def test_a_ghl_handover_deletes_every_dialer_contact_for_the_phone():
    http = FakeHttp()
    endpoints = {"contact_delete": ("DELETE", "/contact/{id}"), "campaign_remove": ("POST", "/campaign/remove")}
    BatchDialerAdapter(http=http, endpoints=endpoints, contact_ids=lambda phone: ["77"]).remove(
        GHL_PHONE, reason=RemovalReason.GHL_CALLING.value)
    assert http.calls == [("DELETE", "/contact/77")]
