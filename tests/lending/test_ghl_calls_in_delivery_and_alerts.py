"""G3: GHL calls go through the same Sheet/Slack retry and missing-disposition alert as BatchDialer calls; a GHL
outcome that never finds its call is posted once. Pure: sessions and Slack are faked, SQL is inspected."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class _Db:
    def __init__(self, answers):
        self.answers, self.sql = answers, []

    def execute(self, sql, params=None):
        s = " ".join(str(sql).split())
        self.sql.append((s, params or {}))
        for marker, rows in self.answers:
            if marker in s:
                if isinstance(rows, Exception):
                    raise rows
                return _Result(rows)
        return _Result([])

    def begin_nested(self):
        @contextmanager
        def ctx():
            yield
        return ctx()

    def commit(self):
        pass

    def rollback(self):
        pass


def _session(db):
    @contextmanager
    def factory():
        yield db
    return factory


class _Slack:
    def __init__(self):
        self.posts = []

    def chat_postMessage(self, **kw):
        self.posts.append(kw["text"])


# ---- delivery retry ------------------------------------------------------------------------------------------------

def test_delivery_retry_includes_ghl_calls(monkeypatch):
    from src.tasks import lending_disposition_delivery_retry as task

    db = _Db([])
    monkeypatch.setattr(task, "lending_session", _session(db))
    task.behind_row_ids()
    sql = db.sql[0][0]
    assert "call_source IN ('batchdialer', 'ghl')" in sql and "call_source = 'batchdialer'" not in sql


# ---- missing disposition -------------------------------------------------------------------------------------------

@pytest.fixture
def alert(monkeypatch):
    from src.tasks import lending_missing_disposition_alert as task

    monkeypatch.setattr(task, "names_by_phone", lambda db, phones: {})
    monkeypatch.setattr(task, "hidden_phones", lambda db, phones: set())
    return task


LIVE = datetime(2026, 11, 5, 13, 0, tzinfo=timezone.utc)


def test_missing_alert_covers_connected_outbound_ghl_calls_after_their_own_grace(alert, monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_ghl_outcomes_live_since", LIVE, raising=False)
    db = _Db([])
    monkeypatch.setattr(alert, "lending_session", _session(db))
    alert.run(slack_client=_Slack(), minutes=10)
    sql, params = next((s, p) for s, p in db.sql if "disposition_missing_alerted_at = now()" in s)
    # BatchDialer rule unchanged
    assert "call_source = 'batchdialer' AND disposition IS NULL AND disposition_raw IS NULL" in sql
    # GHL: outbound only, disposition_raw is always "ghl:<status>" so it is not required to be NULL
    assert "call_source = 'ghl' AND direction = 'outbound' AND disposition IS NULL" in sql
    assert "call_ended_at >= :ghl_since" in sql
    assert params["m"] == 10 and params["ghl_m"] == 30 and params["ghl_since"] == LIVE


def test_ghl_calls_are_not_flagged_until_the_outcome_workflow_is_live(alert, monkeypatch):
    """Unset LENDING_GHL_OUTCOMES_LIVE_SINCE: calls from before the workflow existed would all be reported."""
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_ghl_outcomes_live_since", None, raising=False)
    db = _Db([])
    monkeypatch.setattr(alert, "lending_session", _session(db))
    alert.run(slack_client=_Slack(), minutes=10)
    sql, params = next((s, p) for s, p in db.sql if "disposition_missing_alerted_at = now()" in s)
    assert "call_source = 'ghl'" not in sql and params == {"m": 10}


def test_unmatched_ghl_outcome_is_posted_once(alert, monkeypatch):
    claimed = [(3, "+18135550142", "BOOKED", "Josh")]
    db = _Db([("UPDATE lending.ghl_disposition_events SET alerted_at", claimed)])
    monkeypatch.setattr(alert, "lending_session", _session(db))
    slack = _Slack()
    alert.run(slack_client=slack, minutes=10)
    (sql, params), = [(s, p) for s, p in db.sql if "ghl_disposition_events" in s]
    assert "matched_call_id IS NULL AND alerted_at IS NULL" in sql and params["h"] == 2
    assert len(slack.posts) == 1 and "BOOKED" in slack.posts[0] and "0142" in slack.posts[0]
    assert "+18135550142" not in slack.posts[0]  # phone masked to the last 4


def test_unmatched_alert_fails_soft_when_the_table_is_missing(alert, monkeypatch):
    db = _Db([("ghl_disposition_events", RuntimeError("relation does not exist"))])
    monkeypatch.setattr(alert, "lending_session", _session(db))
    slack = _Slack()
    assert alert.run(slack_client=slack, minutes=10) == 0
    assert slack.posts == []


def test_unmatched_alert_is_unclaimed_when_slack_fails(alert, monkeypatch):
    db = _Db([("UPDATE lending.ghl_disposition_events SET alerted_at", [(4, "+18135550142", "NURTURE", None)])])
    monkeypatch.setattr(alert, "lending_session", _session(db))

    class _Down:
        def chat_postMessage(self, **kw):
            raise RuntimeError("slack down")
    alert.run(slack_client=_Down(), minutes=10)
    assert any("SET alerted_at = NULL" in s and p == {"ids": [4]} for s, p in db.sql)
