"""Morning BatchDialer load of the staged PropertyRadar leads, plus the calling-list fixes it relies on.

No network, no database: the database, the scrubber and the lock folder are fakes or temp folders.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from src.lending import pr_list_landing as landing
from src.lending.compliance import _georgia_blocked
from src.tasks import lending_pr_dialer_autoload as autoload

DAY = date(2026, 11, 21)


def _record(ref, phone, *, tag="list_1", state="FL", entity_status="LLC", **extra):
    return {"source_record_ref": ref, "phone": phone, "source_tag": tag, "pool": "verified_maturity",
            "state": state, "entity_status": entity_status, **extra}


def _select(records, *, active=(), fresh=(), credits=10):
    return autoload.select_records(records, active_phones=set(active), fresh_phones=set(fresh), credits_left=credits,
                                   georgia_blocked=_georgia_blocked)


# ── choosing what to load ───────────────────────────────────────────────────

def test_a_phone_already_in_batchdialer_is_never_loaded_again():
    chosen, counts = _select([_record("a", "+1A"), _record("b", "+1B")], active={"+1A"}, fresh={"+1A", "+1B"})
    assert [r["source_record_ref"] for r in chosen] == ["b"] and counts["already_loaded"] == 1


def test_a_georgia_owner_who_is_not_a_company_is_left_out_before_any_credit_is_spent():
    chosen, counts = _select([_record("g", "+1G", state="GA", entity_status=None)], credits=5)
    assert chosen == [] and counts["georgia_not_a_company"] == 1 and counts["needs_check"] == 0


def test_phones_already_checked_always_load_and_the_rest_wait_for_credits():
    records = [_record("fresh", "+1F"), _record("new1", "+1N1"), _record("new2", "+1N2")]
    chosen, counts = _select(records, fresh={"+1F"}, credits=1)
    assert sorted(r["source_record_ref"] for r in chosen) == ["fresh", "new1"]
    assert counts["needs_check"] == 1 and counts["waiting_for_credits"] == 1


def test_with_no_credits_only_checked_phones_load():
    chosen, counts = _select([_record("fresh", "+1F"), _record("new", "+1N")], fresh={"+1F"}, credits=0)
    assert [r["source_record_ref"] for r in chosen] == ["fresh"] and counts["waiting_for_credits"] == 1


def test_one_record_per_phone_and_none_without_a_phone():
    chosen, counts = _select([_record("a", "+1A"), _record("b", "+1A"), _record("c", None)], fresh={"+1A"})
    assert len(chosen) == 1 and counts["duplicate_phone"] == 1 and counts["no_phone"] == 1


def test_only_the_bridges_pool_in_a_launch_queue_is_loaded(monkeypatch):
    from src.lending import pool_source

    staged = [{**_record("staging:1", "+1A"), "staging_pool": "pr_maturity"},
              {**_record("staging:2", "+1B"), "staging_pool": "builders_permits"},
              {**_record("staging:3", "+1C", tag="not_a_tag"), "staging_pool": "pr_maturity"}]
    monkeypatch.setattr(pool_source, "staged_pool_records", lambda db: staged)
    records = autoload.staged_pr_records(None)
    assert [r["source_record_ref"] for r in records] == ["staging:1"] and records[0]["pool"] == "verified_maturity"


# ── the day's do-not-call credits ───────────────────────────────────────────

def test_the_scrubber_never_sends_more_than_the_days_credits_across_runs(tmp_path):
    sent = []
    scrub = autoload.capped_scrubber(lambda phones: sent.append(list(phones)) or [], day=DAY, cap=5, root=tmp_path)
    scrub(["p1", "p2", "p3"])
    retried = autoload.capped_scrubber(lambda phones: sent.append(list(phones)) or [], day=DAY, cap=5, root=tmp_path)
    retried(["p4", "p5", "p6"])  # a run.sh retry: a new scrubber, same day
    retried(["p7"])
    assert sent == [["p1", "p2", "p3"], ["p4", "p5"]]
    assert autoload.credits_spent(DAY, tmp_path) == 5


def test_what_was_sent_is_counted_even_when_the_call_fails(tmp_path):
    def failing(phones):
        raise RuntimeError("Tracerfy timed out")

    with pytest.raises(RuntimeError):
        autoload.capped_scrubber(failing, day=DAY, cap=10, root=tmp_path)(["p1", "p2"])
    assert autoload.credits_spent(DAY, tmp_path) == 2


def test_each_day_has_its_own_credits(tmp_path):
    autoload.capped_scrubber(lambda p: [], day=DAY, cap=2, root=tmp_path)(["p1", "p2"])
    assert autoload.credits_spent(date(2026, 11, 22), tmp_path) == 0


# ── when it runs ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 11, 21, 14, 5, tzinfo=timezone.utc), True),   # 09:05 EST
    (datetime(2026, 11, 21, 13, 5, tzinfo=timezone.utc), False),  # 08:05 EST
    (datetime(2026, 7, 1, 13, 5, tzinfo=timezone.utc), True),     # 09:05 EDT
])
def test_it_acts_only_at_9_eastern(now, expected):
    assert autoload.in_load_hour(now) is expected


def test_switched_off_it_does_nothing(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_pr_dialer_autoload_enabled", False, raising=False)
    monkeypatch.setattr(autoload, "_run", lambda *a, **k: pytest.fail("ran while switched off"))
    assert autoload.main(["--apply"]) == autoload.EXIT_OK


def test_outside_9_eastern_it_does_nothing(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_pr_dialer_autoload_enabled", True, raising=False)
    monkeypatch.setattr(autoload, "in_load_hour", lambda now: False)
    monkeypatch.setattr(autoload, "_run", lambda *a, **k: pytest.fail("loaded outside 09:xx ET"))
    assert autoload.main(["--apply"]) == autoload.EXIT_OK


def test_without_batchdialer_the_load_is_refused(monkeypatch):
    from src.lending import dialer_port

    monkeypatch.setattr(dialer_port, "get_dialer", lambda: None)
    summary, code = autoload._load(SimpleNamespace(), [_record("a", "+1A")], {}, now=datetime.now(timezone.utc),
                                   day=DAY, cap=0)
    assert code == autoload.EXIT_REFUSED and "refused" in summary


# ── calling-list do-not-call places ─────────────────────────────────────────

def test_a_georgia_calling_list_record_is_judged_as_georgia():
    from src.tasks.lending_pr_calling_list_trace import record_places

    class PlacesDb:
        def execute(self, statement, params):
            return SimpleNamespace(all=lambda: [SimpleNamespace(radar_id="G1", state="GA", ownership_type="Corporate"),
                                                SimpleNamespace(radar_id="G2", state="GA", ownership_type="Individual")])

    places = record_places(PlacesDb(), ["G1", "G2"])
    assert places == {"G1": {"state": "GA", "entity_status": "LLC"}, "G2": {"state": "GA", "entity_status": None}}
    assert not _georgia_blocked(places["G1"]) and _georgia_blocked(places["G2"])


# ── pull lock ───────────────────────────────────────────────────────────────

def test_a_lock_left_by_a_process_that_is_gone_is_taken_over(tmp_path, monkeypatch):
    (tmp_path / ".pull.lock").write_text("pid 999999", encoding="utf-8")
    monkeypatch.setattr(landing, "_process_running", lambda pid: False)
    path = landing.acquire_pull_lock(root=tmp_path)
    assert path.read_text(encoding="utf-8").startswith("pid ")
    landing.release_pull_lock(path)


def test_a_lock_held_by_a_running_process_is_respected(tmp_path, monkeypatch):
    (tmp_path / ".pull.lock").write_text("pid 4242", encoding="utf-8")
    monkeypatch.setattr(landing, "_process_running", lambda pid: True)
    with pytest.raises(landing.PullLocked):
        landing.acquire_pull_lock(root=tmp_path)
    assert (tmp_path / ".pull.lock").read_text(encoding="utf-8") == "pid 4242"


def test_an_unreadable_lock_is_never_taken_over(tmp_path, monkeypatch):
    (tmp_path / ".pull.lock").write_text("garbage", encoding="utf-8")
    monkeypatch.setattr(landing, "_process_running", lambda pid: False)
    with pytest.raises(landing.PullLocked):
        landing.acquire_pull_lock(root=tmp_path)
