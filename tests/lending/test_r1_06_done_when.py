"""R1 0.6 done-when, with fakes only: LendingFlow silent and a PropertyRadar pull failed -> nothing stale loads and the
sources report offline; a worked-out phone in a new list is skipped; a stale GHL opt-out feed holds the card and
refuses the dialer load; fresh feeds allow everything."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from config import lending_fallbacks as fb
from src.lending import dependency_health as dh
from src.lending import dependency_watch as dw
from src.lending import pool_source

NOW = datetime(2026, 11, 25, 19, 0, tzinfo=timezone.utc)  # Wednesday 14:00 ET


class _SourcesDb:
    """The newest PropertyRadar pull, the LendingFlow health row and the newest LendingFlow lead."""

    def __init__(self, *, pr_finished, lf_last):
        self.pr_finished, self.lf_last = pr_finished, lf_last

    def execute(self, stmt, params=None):
        if stmt is pool_source._NEWEST_PULL:
            return SimpleNamespace(scalar=lambda: self.pr_finished)
        if stmt is dh._READ:
            return iter([SimpleNamespace(dependency=fb.FEED_LENDINGFLOW_INBOUND, last_success_at=self.lf_last,
                                         failing_since=None, detail=None)])
        if stmt is dw._LAST_LENDINGFLOW_LEAD:
            return SimpleNamespace(scalar=lambda: self.lf_last)
        raise AssertionError(stmt)


def test_outage_on_lendingflow_and_propertyradar_reports_both_sources_offline():
    db = _SourcesDb(pr_finished=NOW - timedelta(hours=31), lf_last=NOW - timedelta(hours=25))
    reasons = dw.lead_source_offline_reasons(db, NOW, lendingflow_on=True)
    assert reasons[dw.SOURCE_PR_MATURITY] == "PropertyRadar data 31 h old"
    assert reasons[dw.SOURCE_LENDINGFLOW].startswith("LendingFlow silent: no LendingFlow lead for 9.0 business hours")


def test_healthy_sources_report_nothing_offline():
    db = _SourcesDb(pr_finished=NOW - timedelta(hours=7), lf_last=NOW - timedelta(minutes=30))
    assert dw.lead_source_offline_reasons(db, NOW, lendingflow_on=True) == {}


# ── A stale GHL opt-out feed: the dialer load is refused, before any push ────

@pytest.fixture
def stale_ghl_feed(monkeypatch):
    monkeypatch.setattr(dh, "pause_enabled", lambda: True)
    monkeypatch.setattr(dh, "evaluate", lambda channel, now=None: "GHL opt-out sync: last success 50 min ago")


def test_a_stale_ghl_feed_refuses_a_live_dialer_load(monkeypatch, stale_ghl_feed):
    from src.lending import dialer_load

    monkeypatch.setattr(dialer_load, "is_globally_halted", lambda: False)
    with pytest.raises(dialer_load.LoadRefused, match="opt-outs can't be verified"):
        dialer_load.run_dialer_load([{"phone": "+18135557001"}], db=None, run_id="t", dry_run=False,
                                    scrubber=lambda phones: [], dialer=SimpleNamespace(), backflip_check=False)


def test_a_stale_ghl_feed_stops_the_lendingflow_dialer_sweep(monkeypatch, stale_ghl_feed):
    from src.lending import lendingflow_dialer

    monkeypatch.setattr(lendingflow_dialer, "is_globally_halted", lambda: False)
    source = SimpleNamespace(enabled=lambda: True)
    assert lendingflow_dialer._may_load(object(), object(), object(), source) is False


def test_a_stale_ghl_feed_refuses_ghl_tag_writes(monkeypatch, stale_ghl_feed):
    from src.lending import ghl_crm

    monkeypatch.setattr(ghl_crm, "is_globally_halted", lambda: False)
    writer = ghl_crm.GhlCrmWriter(SimpleNamespace(api_key="k", location_id="l"),
                                  request=lambda *a, **k: pytest.fail("GHL called while paused"))
    with pytest.raises(ghl_crm.GhlCrmError, match="opt-outs can't be verified"):
        writer.move_stage("contact-1", "Booked")


def test_fresh_feeds_allow_every_channel(monkeypatch):
    monkeypatch.setattr(dh, "pause_enabled", lambda: True)
    monkeypatch.setattr(dh, "evaluate", lambda channel, now=None: None)
    assert all(dh.outbound_allowed(c).allowed for c in fb.ALL_CHANNELS)


def test_switched_off_nothing_pauses_even_when_stale(monkeypatch):
    monkeypatch.setattr(dh, "pause_enabled", lambda: False)
    monkeypatch.setattr(dh, "evaluate", lambda channel, now=None: pytest.fail("read while switched off"))
    assert not dh.outbound_paused(fb.CHANNEL_DIALER)


# ── A worked-out phone in a new list is skipped by the list-build gate ───────

def test_a_worked_out_phone_in_a_new_list_is_skipped(monkeypatch):
    from config.lending_compliance import ReasonCode
    from src.lending import compliance

    ana, ben = "+18135557001", "+18135557002"
    monkeypatch.setattr(compliance, "_suppressed_phones", lambda db, phones: set())
    monkeypatch.setattr(compliance, "_attempt_history_exhausted_phones",
                        lambda db, phones, now: {p for p in phones if p == ana})
    monkeypatch.setattr(compliance, "ghl_calling_phones", lambda db, phones, now: set())
    monkeypatch.setattr(compliance, "_flag_nurture", lambda db, phones: None)
    results = compliance.filter_loadable([{"phone": ana}, {"phone": ben}], db=None, now=NOW,
                                         scrubber=lambda phones: [], dnc_scrub=False)
    assert [(r.phone, r.allowed, r.reason) for r in results] == [
        (ana, False, ReasonCode.ATTEMPT_HISTORY_EXCEEDED), (ben, True, None)]

