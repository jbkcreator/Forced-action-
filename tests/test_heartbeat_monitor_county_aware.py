"""County-aware heartbeat monitor — DB-free unit tests.

Covers the multi-county fix: per-(source, county) freshness checks so one
county's success can't mask another county going stale, plus per-county dedup
keying. No DB — the DB-touching helper (_beat_for) and get_db_context are
patched.
"""
from datetime import datetime, timezone
from unittest import mock

import src.tasks.heartbeat_monitor as hm
from src.tasks.heartbeat_monitor import Heartbeat


# ── Heartbeat dataclass ──────────────────────────────────────────────────────

def test_label_and_alert_county_for_county_beat():
    b = Heartbeat("violations", 1500, None, None, True, county_id="pinellas")
    assert b.label() == "violations/pinellas"
    assert b.alert_county() == "pinellas"
    assert "violations/pinellas" in b.alert_subject()


def test_pasco_monitored_now_scraper_active():
    # WP-T2-8 Stage F: Pasco's permit scraper is active on cron with an approved
    # playwright_code (county_sources is_active=TRUE), so its feed IS now
    # SLA-monitored — a stale/failed Pasco pull pages ops like any permit county.
    assert "pasco" in hm.MULTI_COUNTY_SOURCES["permits"]
    assert {"hillsborough", "pinellas"} <= hm.MULTI_COUNTY_SOURCES["permits"]


def test_label_and_alert_county_for_source_wide_beat():
    b = Heartbeat("sunbiz", 1500, None, None, True)  # county_id defaults None
    assert b.label() == "sunbiz"
    assert b.alert_county() == hm._DEFAULT_ALERT_COUNTY  # NOT NULL fallback
    assert "sunbiz" in b.alert_subject() and "/" not in b.alert_subject().split("stale")[0]


# ── Registry invariants (the deploy-safety decision) ─────────────────────────

def test_multi_county_registry_excludes_known_bad_sources():
    # judgments (pinellas stale) and sunbiz (verdict bug) are deliberately NOT
    # per-county yet — they'd false/off-task page on deploy.
    assert "judgments" not in hm.MULTI_COUNTY_SOURCES
    assert "sunbiz" not in hm.MULTI_COUNTY_SOURCES
    # the fix target is enabled
    assert hm.MULTI_COUNTY_SOURCES["violations"] == {"hillsborough", "pinellas"}
    # the retired pseudo-source is gone
    assert "violations_pinellas" not in hm.HEARTBEAT_SLAS
    assert "violations_pinellas" not in hm.SOURCE_OFF_DAYS


# ── compute_heartbeats iteration (DB patched out) ────────────────────────────

def _patch_compute(monkeypatch_calls):
    """Return a fake _beat_for that records (source, county) and yields a beat."""
    def fake_beat_for(session, source_type, sla_minutes, county_id, now):
        monkeypatch_calls.append((source_type, county_id))
        return Heartbeat(source_type, sla_minutes, None, None, True, county_id=county_id)
    return fake_beat_for


def test_multi_county_source_produces_one_beat_per_county():
    calls: list = []
    fixed_now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)  # a Friday, no off-day
    with mock.patch.object(hm, "get_db_context"), \
         mock.patch.object(hm, "_beat_for", _patch_compute(calls)), \
         mock.patch.object(hm, "HEARTBEAT_SLAS", {"violations": 1500}), \
         mock.patch.object(hm, "MULTI_COUNTY_SOURCES", {"violations": {"hillsborough", "pinellas"}}), \
         mock.patch.object(hm, "SOURCE_OFF_DAYS", {}):
        beats = hm.compute_heartbeats(now=fixed_now)
    assert sorted(c for _, c in calls) == ["hillsborough", "pinellas"]
    assert {b.label() for b in beats} == {"violations/hillsborough", "violations/pinellas"}


def test_source_wide_source_produces_single_countyless_beat():
    calls: list = []
    fixed_now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    with mock.patch.object(hm, "get_db_context"), \
         mock.patch.object(hm, "_beat_for", _patch_compute(calls)), \
         mock.patch.object(hm, "HEARTBEAT_SLAS", {"sunbiz": 1500}), \
         mock.patch.object(hm, "MULTI_COUNTY_SOURCES", {}), \
         mock.patch.object(hm, "SOURCE_OFF_DAYS", {}):
        beats = hm.compute_heartbeats(now=fixed_now)
    assert calls == [("sunbiz", None)]
    assert beats[0].county_id is None and beats[0].label() == "sunbiz"


def test_off_day_skips_source_entirely():
    calls: list = []
    sunday = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)  # weekday()==6
    assert sunday.weekday() == 6
    with mock.patch.object(hm, "get_db_context"), \
         mock.patch.object(hm, "_beat_for", _patch_compute(calls)), \
         mock.patch.object(hm, "HEARTBEAT_SLAS", {"violations": 1500}), \
         mock.patch.object(hm, "MULTI_COUNTY_SOURCES", {"violations": {"hillsborough", "pinellas"}}), \
         mock.patch.object(hm, "SOURCE_OFF_DAYS", {"violations": {6}}):
        beats = hm.compute_heartbeats(now=sunday)
    assert calls == [] and beats == []


# ── Dedup keys by (source_type, county_id) ───────────────────────────────────

def test_record_alerted_writes_the_beats_county():
    added = []
    fake_session = mock.MagicMock()
    fake_session.add.side_effect = lambda row: added.append(row)
    ctx = mock.MagicMock()
    ctx.__enter__.return_value = fake_session
    with mock.patch.object(hm, "get_db_context", return_value=ctx):
        hm._record_alerted("violations", "pinellas")
    assert len(added) == 1
    assert added[0].source_type == "violations"
    assert added[0].county_id == "pinellas"      # not the old hardcoded hillsborough
    assert added[0].alert_type == "heartbeat_missed"
