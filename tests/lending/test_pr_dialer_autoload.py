"""Morning BatchDialer load of the staged PropertyRadar leads, plus the calling-list fixes it relies on.

No network, no database: the database, the scrubber and the lock folder are fakes or temp folders.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import os
import subprocess
import sys
import textwrap

import pytest

from src.lending import pr_list_landing as landing
from src.lending.compliance import _georgia_blocked
from src.lending.dialer_load import REASON_SCRUB_FAILED
from src.tasks import lending_pr_dialer_autoload as autoload

DAY = date(2026, 11, 21)


def _record(ref, phone, *, tag="list_1", state="FL", entity_status="LLC", **extra):
    return {"source_record_ref": ref, "phone": phone, "source_tag": tag, "pool": "verified_maturity",
            "state": state, "entity_status": entity_status, **extra}


def _select(records, *, active=(), fresh=(), blocked=None, credits=10):
    """``fresh`` phones pass the free checks; ``blocked`` maps a phone to its block reason; the rest need a check."""
    blocked = blocked or {}
    verdicts = [blocked.get(r["phone"]) or (None if r["phone"] in fresh else REASON_SCRUB_FAILED) for r in records]
    return autoload.select_records(records, verdicts, active_phones=set(active), credits_left=credits)


# ── choosing what to load ───────────────────────────────────────────────────

def test_a_phone_already_in_batchdialer_is_never_loaded_again():
    chosen, counts = _select([_record("a", "+1A"), _record("b", "+1B")], active={"+1A"}, fresh={"+1A", "+1B"})
    assert [r["source_record_ref"] for r in chosen] == ["b"] and counts["already_loaded"] == 1


def test_a_georgia_owner_who_is_not_a_company_is_left_out_before_any_credit_is_spent():
    chosen, counts = _select([_record("g", "+1G", state="GA", entity_status=None)], blocked={"+1G": "GA_NATURAL_PERSON"},
                             credits=5)
    assert chosen == [] and counts["blocked_by_reason"] == {"GA_NATURAL_PERSON": 1} and counts["needs_check"] == 0


def test_a_blocked_number_ahead_in_the_queue_never_takes_the_days_only_credit():
    records = [_record("suppressed", "+1S"), _record("exhausted", "+1X"), _record("eligible", "+1E")]
    chosen, counts = _select(records, blocked={"+1S": "SUPPRESSED", "+1X": "ATTEMPT_HISTORY_EXCEEDED"}, credits=1)
    assert [r["source_record_ref"] for r in chosen] == ["eligible"]
    assert counts["needs_check"] == 1 and counts["blocked_before_check"] == 2 and counts["waiting_for_credits"] == 0


def test_the_free_checks_are_the_dialer_loads_own_gates_without_tracerfy(monkeypatch):
    from src.lending import compliance

    seen = {}

    def fake_filter(records, db, *, scrubber, **kw):
        seen["scrubber"] = scrubber
        return [compliance.GateResult(phone="+1A", allowed=True),
                compliance.GateResult(phone="+1B", allowed=False, reason=compliance.ReasonCode.SUPPRESSED)]

    monkeypatch.setattr(compliance, "filter_loadable", fake_filter)
    from src.lending.dialer_load import no_scrub

    assert autoload.pre_check_verdicts(None, [_record("a", "+1A"), _record("b", "+1B")]) == [None, "SUPPRESSED"]
    assert seen["scrubber"] is no_scrub
    assert _georgia_blocked({"state": "GA", "entity_status": None})  # the GA rule itself lives in filter_loadable


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
    scrub = autoload.CappedScrubber(lambda phones: sent.append(list(phones)) or [], day=DAY, cap=5, root=tmp_path)
    scrub(["p1", "p2", "p3"])
    retried = autoload.CappedScrubber(lambda phones: sent.append(list(phones)) or [], day=DAY, cap=5, root=tmp_path)
    retried(["p4", "p5", "p6"])  # a run.sh retry: a new scrubber, same day
    retried(["p7"])
    assert sent == [["p1", "p2", "p3"], ["p4", "p5"]]
    assert autoload.credits_spent(DAY, tmp_path) == 5


def test_what_was_sent_is_counted_even_when_the_call_fails(tmp_path):
    def failing(phones):
        raise RuntimeError("Tracerfy timed out")

    scrub = autoload.CappedScrubber(failing, day=DAY, cap=10, root=tmp_path)
    with pytest.raises(RuntimeError):
        scrub(["p1", "p2"])
    assert autoload.credits_spent(DAY, tmp_path) == 2 and scrub.failed == 2


def test_phones_tracerfy_did_not_answer_count_as_failed(tmp_path):
    scrub = autoload.CappedScrubber(lambda phones: [{"phone": "+18135550101"}], day=DAY, cap=10, root=tmp_path)
    scrub(["+18135550101", "+18135550102"])
    assert scrub.failed == 1


def _live_load(monkeypatch, tmp_path, scrubber_fn, *, cap=5):
    from src.lending import compliance, dialer_load, dialer_port

    monkeypatch.setattr(autoload, "SPEND_ROOT", tmp_path)
    monkeypatch.setattr(dialer_port, "get_dialer", lambda: SimpleNamespace(missing_for_load=lambda: []))
    monkeypatch.setattr(compliance, "tracerfy_scrub", scrubber_fn)

    def fake_load(records, db, *, scrubber, **kw):
        try:
            scrubber(["+18135550101", "+18135550102"])
        except Exception:
            pass  # the dialer load swallows a scrub error (compliance._scrub)
        return SimpleNamespace(failed=0, as_dict=lambda: {"loaded": 0})

    monkeypatch.setattr(dialer_load, "run_dialer_load", fake_load)
    return autoload._load(SimpleNamespace(commit=lambda: None), [_record("a", "+1A")], {}, now=datetime.now(timezone.utc), day=DAY,
                          cap=cap)


def test_a_do_not_call_timeout_fails_the_run_and_a_retry_keeps_within_the_cap(monkeypatch, tmp_path):
    def timeout(phones):
        raise TimeoutError("Tracerfy timed out")

    summary, code = _live_load(monkeypatch, tmp_path, timeout, cap=3)
    assert code == autoload.EXIT_FAILED and summary["do_not_call_check_failed"] == 2
    assert autoload.credits_spent(DAY, tmp_path) == 2
    sent = []
    summary, code = _live_load(monkeypatch, tmp_path, lambda phones: sent.append(phones) or
                               [{"phone": p} for p in phones], cap=3)
    assert sent == [["+18135550101"]] and autoload.credits_spent(DAY, tmp_path) == 3  # only the 1 credit left
    assert code == autoload.EXIT_OK


def test_a_successful_do_not_call_check_is_not_a_failure(monkeypatch, tmp_path):
    summary, code = _live_load(monkeypatch, tmp_path, lambda phones: [{"phone": p} for p in phones])
    assert code == autoload.EXIT_OK and "do_not_call_check_failed" not in summary


def test_each_day_has_its_own_credits(tmp_path):
    autoload.CappedScrubber(lambda p: [], day=DAY, cap=2, root=tmp_path)(["p1", "p2"])
    assert autoload.credits_spent(date(2026, 11, 22), tmp_path) == 0


# ── when it runs ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 11, 21, 14, 5, tzinfo=timezone.utc), True),   # 09:05 EST
    (datetime(2026, 11, 21, 13, 5, tzinfo=timezone.utc), False),  # 08:05 EST
    (datetime(2026, 7, 1, 13, 5, tzinfo=timezone.utc), True),     # 09:05 EDT
    (datetime(2026, 11, 21, 17, 5, tzinfo=timezone.utc), True),   # 12:05 EST
    (datetime(2026, 11, 21, 16, 5, tzinfo=timezone.utc), False),  # 11:05 EST
    (datetime(2026, 7, 1, 16, 5, tzinfo=timezone.utc), True),     # 12:05 EDT
    (datetime(2026, 7, 1, 17, 5, tzinfo=timezone.utc), False),    # 13:05 EDT
])
def test_it_acts_only_at_9_and_12_eastern(now, expected):
    assert autoload.in_load_hour(now) is expected


@pytest.mark.parametrize("morning,midday", [
    (datetime(2026, 1, 15, 14, 5, tzinfo=timezone.utc), datetime(2026, 1, 15, 17, 5, tzinfo=timezone.utc)),  # EST/MST
    (datetime(2026, 7, 15, 13, 5, tzinfo=timezone.utc), datetime(2026, 7, 15, 16, 5, tzinfo=timezone.utc)),  # EDT/MDT
])
@pytest.mark.parametrize("phone", ["+13035550123", "+14155550123"])  # Denver (Mountain), San Francisco (Pacific)
def test_a_western_number_blocked_at_9_eastern_can_load_at_the_12_eastern_pass(morning, midday, phone):
    from src.lending.compliance import _outside_call_window

    assert autoload.in_load_hour(morning) and autoload.in_load_hour(midday)
    assert _outside_call_window(phone, morning) and not _outside_call_window(phone, midday)


def test_every_cron_line_lands_in_a_load_hour_and_each_pass_has_one():
    from pathlib import Path

    cron = Path(__file__).resolve().parents[2] / "scripts" / "cron" / "crontab.txt"
    line = next(l for l in cron.read_text().splitlines() if "lending_pr_dialer_autoload --apply" in l)
    hours = [int(h) for h in line.split()[1].split(",")]
    for month in (1, 7):
        acting = [h for h in hours if autoload.in_load_hour(datetime(2026, month, 15, h, 5, tzinfo=timezone.utc))]
        assert len(acting) == len(autoload.LOAD_HOURS_ET)


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

def test_a_lock_file_left_by_a_crashed_run_is_taken_over(tmp_path):
    (tmp_path / ".pull.lock").write_text("pid 999999", encoding="utf-8")  # nobody holds a lock on it
    lock = landing.acquire_pull_lock(root=tmp_path)
    assert lock.path.read_text(encoding="utf-8") == f"pid {os.getpid()}"
    landing.release_pull_lock(lock)
    assert not lock.path.exists()


def test_a_lock_held_in_this_process_is_respected(tmp_path):
    lock = landing.acquire_pull_lock(root=tmp_path)
    try:
        with pytest.raises(landing.PullLocked):
            landing.acquire_pull_lock(root=tmp_path)
    finally:
        landing.release_pull_lock(lock)


_CONTENDER = textwrap.dedent("""
    import sys, time
    from pathlib import Path
    from src.lending import pr_list_landing as landing
    root, hold = Path(sys.argv[1]), float(sys.argv[2])
    try:
        lock = landing.acquire_pull_lock(root=root)
    except landing.PullLocked:
        print("locked", flush=True)
        sys.exit(0)
    print("acquired", flush=True)
    time.sleep(hold)
    landing.release_pull_lock(lock)
""")


def _contender(tmp_path, hold):
    env = {**os.environ, "PYTHONPATH": os.getcwd()}
    return subprocess.Popen([sys.executable, "-c", _CONTENDER, str(tmp_path), str(hold)], stdout=subprocess.PIPE,
                            text=True, env=env)


def test_two_processes_after_a_crash_never_both_hold_the_lock(tmp_path):
    (tmp_path / ".pull.lock").write_text("pid 999999", encoding="utf-8")  # a crashed run's file
    contenders = [_contender(tmp_path, hold=2.0) for _ in range(4)]
    outcomes = sorted(p.communicate(timeout=30)[0].strip() for p in contenders)
    assert outcomes.count("acquired") == 1, outcomes


def test_a_holder_that_dies_releases_the_lock(tmp_path):
    holder = _contender(tmp_path, hold=60)
    assert holder.stdout.readline().strip() == "acquired"
    with pytest.raises(landing.PullLocked):
        landing.acquire_pull_lock(root=tmp_path)
    holder.kill()
    holder.wait(timeout=10)
    lock = landing.acquire_pull_lock(root=tmp_path)  # the kernel dropped the dead holder's lock
    landing.release_pull_lock(lock)
