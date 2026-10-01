"""The disposition list, cause tags and aliases stay consistent, and the missing-disposition alert."""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from config import lending_dispositions as cfg


def test_shipped_config_is_consistent():
    cfg.validate_dispositions_config()


@pytest.mark.parametrize("mutate", [
    lambda: setattr(cfg, "UNANSWERED_CODES", frozenset({"NOPE"})),
    lambda: setattr(cfg, "DEFAULT_CAUSE", {"BAD_NUMBER": "made_up"}),
    lambda: setattr(cfg, "SYSTEM_DISPOSITION_ALIASES", {"X": "NOPE"}),
    lambda: setattr(cfg, "DISPOSITIONS", ("BOOKED", "BOOKED")),
])
def test_inconsistent_config_is_refused(monkeypatch, mutate):
    for name in ("UNANSWERED_CODES", "DEFAULT_CAUSE", "SYSTEM_DISPOSITION_ALIASES", "DISPOSITIONS"):
        monkeypatch.setattr(cfg, name, getattr(cfg, name))
    mutate()
    with pytest.raises(ValueError):
        cfg.validate_dispositions_config()


pytestmark_db = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")


@pytest.fixture
def alert_env(monkeypatch, lending_db):
    from src.tasks import lending_missing_disposition_alert as task

    @contextmanager
    def session():
        yield lending_db

    monkeypatch.setattr(task, "lending_session", session)
    monkeypatch.setattr(task, "get_settings", lambda: SimpleNamespace(
        lending_disposition_missing_alert_minutes=10, lending_dial_tasks_channel="C1"))
    return task


def _call(db, call_id, ended_min_ago, duration=45, disposition=None, raw=None):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, caller_seat, caller_name, talk_duration_sec, "
        "call_ended_at, disposition, disposition_raw, raw_event) VALUES (:c, '7', 'Sam', :d, :e, :disp, :raw, '{}')"),
        {"c": call_id, "d": duration, "e": datetime.now(timezone.utc) - timedelta(minutes=ended_min_ago),
         "disp": disposition, "raw": raw})


@pytestmark_db
def test_connected_call_without_a_disposition_alerts_once(alert_env, lending_db):
    _call(lending_db, "late", 30)
    _call(lending_db, "fresh", 2)
    _call(lending_db, "done", 30, disposition="BOOKED", raw="BOOKED")
    _call(lending_db, "unknown-code", 30, raw="Hot Lead")
    _call(lending_db, "ringout", 30, duration=0)
    slack = MagicMock()
    assert alert_env.run(slack_client=slack) == 1
    assert "Sam" in slack.chat_postMessage.call_args.kwargs["text"] and "late" in slack.chat_postMessage.call_args.kwargs["text"]
    assert alert_env.run(slack_client=slack) == 0  # already alerted
    assert slack.chat_postMessage.call_count == 1


def test_approved_thirteen_codes_are_pinned_and_three_stay_separate():
    from config.lending_dispositions import DISPOSITIONS

    assert DISPOSITIONS == (
        "NO_ANSWER", "LEFT_VOICEMAIL", "BAD_NUMBER", "CALL_FAILED", "WRONG_PERSON",
        "NOT_DECISION_MAKER", "REFERRED", "DNC_REQUEST", "CONNECTED_NOT_INTERESTED",
        "CALLBACK_REQUESTED", "DATA_NURTURE_ONLY", "GATE_FAILED_NURTURE", "BOOKED",
    )
    assert len({"WRONG_PERSON", "NOT_DECISION_MAKER", "REFERRED"} & set(DISPOSITIONS)) == 3
