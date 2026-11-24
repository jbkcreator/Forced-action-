import subprocess

import pytest
import requests

import src.lending.watchdog.collectors as collectors
from config.lending_watchdog import DOORS
from src.lending.watchdog.collectors import door_observations, health_observation, unit_observations


def _completed(stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=3, stdout=stdout, stderr="")


class _Resp:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def test_units_parse_each_line():
    obs = unit_observations(["a", "b", "c"], run=lambda cmd: _completed("active\nfailed\nactivating\n"))
    assert [o.ok for o in obs] == [True, False, False]
    assert [o.key for o in obs] == ["unit:a", "unit:b", "unit:c"]


def test_units_make_one_systemctl_call():
    seen = []
    unit_observations(["a", "b"], run=lambda cmd: seen.append(cmd) or _completed("active\nactive\n"))
    assert seen == [["systemctl", "is-active", "a", "b"]]


def test_units_systemctl_error_marks_all_down():
    def boom(cmd):
        raise FileNotFoundError("nope")

    obs = unit_observations(["a", "b"], run=boom)
    assert [o.ok for o in obs] == [False, False]
    assert all(o.detail == "systemctl unavailable: FileNotFoundError" for o in obs)


def test_units_missing_output_line_is_down():
    obs = unit_observations(["a", "b"], run=lambda cmd: _completed("active\n"))
    assert [o.ok for o in obs] == [True, False]


def test_health_ok():
    obs = health_observation("http://x/health", get=lambda *a, **k: _Resp(200, {"status": "ok"}))
    assert obs.key == "health:lending-api" and obs.ok


def test_health_non_200_is_down():
    obs = health_observation("http://x/health", get=lambda *a, **k: _Resp(502))
    assert not obs.ok and obs.detail == "HTTP 502"


def test_health_wrong_body_is_down():
    assert not health_observation("http://x/health", get=lambda *a, **k: _Resp(200, {"status": "degraded"})).ok
    assert not health_observation("http://x/health", get=lambda *a, **k: _Resp(200)).ok


def test_health_timeout_is_down():
    def boom(*a, **k):
        raise requests.exceptions.Timeout()

    obs = health_observation("http://x/health", get=boom)
    assert not obs.ok and obs.detail == "Timeout"


def _doors_with(status_by_name):
    def request(method, url, **kwargs):
        name = next(d.name for d in DOORS if url.endswith(d.path))
        return _Resp(status_by_name[name])

    return request


def test_door_refused_is_healthy():
    healthy = {d.name: next(iter(d.healthy)) for d in DOORS}
    obs = door_observations("https://pub", request=_doors_with(healthy))
    assert all(o.ok for o in obs)
    assert {o.key for o in obs} == {f"door:{d.name}" for d in DOORS}
    assert next(d for d in DOORS if d.name == "website").healthy == frozenset({405})


@pytest.mark.parametrize("code", [404, 502, 503])
def test_door_404_502_503_are_broken(code):
    obs = door_observations("https://pub", request=_doors_with({d.name: code for d in DOORS}))
    assert all(not o.ok and o.detail == f"HTTP {code}" for o in obs)


def test_door_timeout_is_broken():
    def boom(*a, **k):
        raise requests.exceptions.ConnectTimeout()

    obs = door_observations("https://pub", request=boom)
    assert all(not o.ok and o.detail == "ConnectTimeout" for o in obs)


def test_door_sends_no_secret_header():
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return _Resp(401)

    door_observations("https://pub/", request=request)
    assert len(calls) == len(DOORS)
    for method, url, kwargs in calls:
        headers = {k.lower() for k in (kwargs.get("headers") or {})}
        assert "x-webhook-secret" not in headers and "x-instantly-webhook-secret" not in headers
        assert kwargs["timeout"] == 10 and kwargs["allow_redirects"] is False
        assert "//" not in url.split("://", 1)[1]


def test_doors_off_when_base_url_unset(monkeypatch):
    monkeypatch.setattr(collectors, "_warned_doors_off", False)
    assert door_observations("", request=lambda *a, **k: pytest.fail("no call")) == []
    assert door_observations("") == []
