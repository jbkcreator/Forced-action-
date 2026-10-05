"""The hourly reminder for unresolved ambiguous dialer creates."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from src.tasks import lending_unconfirmed_create_alert as alert


class _Slack:
    def __init__(self):
        self.messages = []

    def chat_postMessage(self, **kwargs):
        self.messages.append(kwargs)


def _session_with(rows):
    @contextmanager
    def session():
        yield SimpleNamespace(execute=lambda *a, **k: SimpleNamespace(all=lambda: rows))
    return session


def test_open_rows_are_posted_once_with_a_count(monkeypatch):
    rows = [_Row("ab" * 32, "run-1", datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc))]
    monkeypatch.setattr(alert, "lending_session", _session_with(rows))
    slack = _Slack()
    assert alert.run(slack_client=slack) == 1
    assert len(slack.messages) == 1 and "1 dialer create(s)" in slack.messages[0]["text"]


def test_nothing_open_posts_nothing(monkeypatch):
    monkeypatch.setattr(alert, "lending_session", _session_with([]))
    slack = _Slack()
    assert alert.run(slack_client=slack) == 0
    assert slack.messages == []


class _Row(tuple):
    def __new__(cls, phone_hash, run_id, attempted_at):
        row = super().__new__(cls, (phone_hash, run_id, attempted_at))
        row.attempted_at = attempted_at
        return row
