"""The recording status getter never follows redirects. Database-free."""
from types import SimpleNamespace

from src.tasks import lending_recording_check as t


def _fake_get(code, seen):
    class Resp(SimpleNamespace):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def get(url, **kw):
        seen.update(kw)
        return Resp(status_code=code)

    return get


def test_redirect_is_not_reported_as_readable_and_is_not_followed(monkeypatch):
    seen = {}
    monkeypatch.setattr(t.requests, "get", _fake_get(302, seen))
    assert t.make_status_getter("k")("https://app.batchdialer.com/api/callrecording/1") not in (200, 401, 403, 404)
    assert seen["allow_redirects"] is False and seen["stream"] is True


def test_plain_statuses_pass_through(monkeypatch):
    monkeypatch.setattr(t.requests, "get", _fake_get(403, {}))
    assert t.make_status_getter("k")("https://app.batchdialer.com/api/callrecording/1") == 403


def test_key_is_never_sent_to_a_non_dialer_host(monkeypatch):
    calls = []
    monkeypatch.setattr(t.requests, "get", lambda *a, **k: calls.append(a) or None)
    assert t.make_status_getter("secret")("https://evil.example/rec") == 0
    assert calls == []
    seen = {}
    monkeypatch.setattr(t.requests, "get", _fake_get(200, seen))
    assert t.make_status_getter("secret")("https://app.batchdialer.com/api/callrecording/1") == 200
    assert seen["headers"] == {"X-ApiKey": "secret"}
