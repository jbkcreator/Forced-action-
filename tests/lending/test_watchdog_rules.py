from datetime import datetime, timedelta, timezone

from src.lending.watchdog.rules import (
    CheckState,
    Observation,
    evaluate,
    is_business_time,
    load_state,
    save_state,
)

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
KEY = "unit:fa-lending-cora"


def bad(k=KEY):
    return Observation(k, False, "inactive")


def good(k=KEY):
    return Observation(k, True, "active")


def run(obs, state, minutes, fail_polls=2, repeat_minutes=120):
    return evaluate(obs, state, NOW + timedelta(minutes=minutes), fail_polls=fail_polls, repeat_minutes=repeat_minutes)


def test_single_bad_poll_does_not_alert():
    alerts, state = evaluate([bad()], {}, NOW, fail_polls=2, repeat_minutes=120)
    assert alerts == [] and state[KEY].failures == 1


def test_second_bad_poll_alerts_down_once():
    _, s = evaluate([bad()], {}, NOW, fail_polls=2, repeat_minutes=120)
    alerts, s = evaluate([bad()], s, NOW + timedelta(minutes=2), fail_polls=2, repeat_minutes=120)
    assert [a.kind for a in alerts] == ["down"]
    assert alerts[0].key == KEY and alerts[0].detail == "inactive"
    assert s[KEY].open_since == s[KEY].last_alert_at == NOW + timedelta(minutes=2)
    alerts, s = evaluate([bad()], s, NOW + timedelta(minutes=4), fail_polls=2, repeat_minutes=120)
    assert alerts == []


def test_still_down_repeats_after_repeat_minutes():
    _, s = run([bad()], {}, 0)
    _, s = run([bad()], s, 2)
    alerts, s = run([bad()], s, 121)
    assert alerts == []
    alerts, s = run([bad()], s, 122)
    assert [a.kind for a in alerts] == ["still_down"]
    assert alerts[0].since == NOW + timedelta(minutes=2)
    assert s[KEY].last_alert_at == NOW + timedelta(minutes=122)
    alerts, s = run([bad()], s, 124)
    assert alerts == []


def test_recovery_alerts_once_and_resets():
    _, s = run([bad()], {}, 0)
    _, s = run([bad()], s, 2)
    opened = s[KEY].open_since
    alerts, s = run([good()], s, 4)
    assert [a.kind for a in alerts] == ["recovered"]
    assert alerts[0].since == opened and alerts[0].detail == "active"
    assert s[KEY] == CheckState()
    alerts, s = run([good()], s, 6)
    assert alerts == []


def test_good_poll_resets_failure_count():
    _, s = run([bad()], {}, 0)
    alerts, s = run([good()], s, 2)
    assert alerts == [] and s[KEY].failures == 0
    alerts, s = run([bad()], s, 4)
    assert alerts == [] and s[KEY].failures == 1


def test_unobserved_key_keeps_state():
    _, s = evaluate([bad("cards")], {}, NOW, fail_polls=1, repeat_minutes=120)
    alerts, s2 = evaluate([], s, NOW + timedelta(minutes=2), fail_polls=1, repeat_minutes=120)
    assert alerts == [] and s2["cards"].open_since is not None


def test_inconclusive_poll_breaks_a_failure_streak_before_an_alert():
    unsure = Observation("ghl_outcomes", None, "inconclusive")
    s = {}
    for minutes, obs in ((0, bad("ghl_outcomes")), (2, unsure), (4, bad("ghl_outcomes"))):
        alerts, s = evaluate([obs], s, NOW + timedelta(minutes=minutes), fail_polls=2, repeat_minutes=120)
        assert alerts == []
    assert s["ghl_outcomes"].failures == 1


def test_inconclusive_poll_keeps_an_open_incident():
    _, s = evaluate([bad("ghl_outcomes")], {}, NOW, fail_polls=1, repeat_minutes=120)
    alerts, s2 = evaluate([Observation("ghl_outcomes", None, "inconclusive")], s, NOW + timedelta(minutes=2),
                          fail_polls=1, repeat_minutes=120)
    assert alerts == [] and s2["ghl_outcomes"] == s["ghl_outcomes"]


def test_keys_are_independent():
    alerts, s = run([bad("cards"), good("replies")], {}, 0, fail_polls=1)
    assert [(a.key, a.kind) for a in alerts] == [("cards", "down")]
    assert s["replies"].failures == 0


def test_evaluate_does_not_mutate_input_state():
    s = {KEY: CheckState(failures=1)}
    run([bad()], s, 0)
    assert s[KEY].failures == 1


def test_is_business_time():
    assert is_business_time(datetime(2026, 11, 24, 14, 0, tzinfo=timezone.utc))
    assert not is_business_time(datetime(2026, 11, 24, 13, 59, tzinfo=timezone.utc))
    assert is_business_time(datetime(2026, 11, 25, 0, 14, tzinfo=timezone.utc))
    assert not is_business_time(datetime(2026, 11, 25, 0, 15, tzinfo=timezone.utc))
    assert not is_business_time(datetime(2026, 11, 28, 16, 0, tzinfo=timezone.utc))


def test_state_round_trip(tmp_path):
    p = tmp_path / "s.json"
    original = {
        KEY: CheckState(failures=3, open_since=NOW, last_alert_at=NOW + timedelta(minutes=5)),
        "cards": CheckState(),
    }
    save_state(p, original)
    assert load_state(p) == original
    assert [f.name for f in tmp_path.iterdir()] == ["s.json"]


def test_missing_or_corrupt_state_starts_fresh(tmp_path):
    p = tmp_path / "s.json"
    assert load_state(p) == {}
    p.write_text("{not json")
    assert load_state(p) == {}


def test_bad_field_state_starts_fresh(tmp_path):
    p = tmp_path / "s.json"
    p.write_text('{"cards": {"failures": "x", "open_since": "not-a-date", "last_alert_at": null}}')
    assert load_state(p) == {}
    p.write_text('["a"]')
    assert load_state(p) == {}


def test_naive_timestamp_state_starts_fresh(tmp_path):
    p = tmp_path / "s.json"
    p.write_text('{"cards": {"failures": 1, "open_since": "2026-11-24T15:00:00", "last_alert_at": null}}')
    assert load_state(p) == {}
