from __future__ import annotations

import os
import sys
import types
from datetime import date

os.environ.setdefault("DEBUG", "false")

sys.modules.setdefault(
    "jinja2",
    types.SimpleNamespace(Environment=object, FileSystemLoader=object),
)

from src.tasks.daily_dashboard import _fetch_cora_autonomy, _fetch_cora_decision_stats


class _Result:
    def __init__(self, *, fetchone=None, fetchall=None):
        self._fetchone = fetchone
        self._fetchall = fetchall or []

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall


class _Session:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append({"sql": str(statement), "params": params})
        return self.result


def test_fetch_cora_autonomy_uses_platform_denominator():
    session = _Session(_Result(fetchone=(100, 28)))

    assert _fetch_cora_autonomy(session, date(2026, 6, 4)) == "28.0%"

    sql = session.calls[0]["sql"]
    assert "FROM agent_decisions" in sql
    assert "learning_cards" not in sql
    assert "terminal_status IS NOT NULL" in sql
    assert "overridden_at IS NULL" in sql
    assert session.calls[0]["params"] == {
        "start_date": "2026-05-29",
        "end_date": "2026-06-04",
    }


def test_fetch_cora_autonomy_returns_na_without_finalized_decisions():
    session = _Session(_Result(fetchone=(0, 0)))

    assert _fetch_cora_autonomy(session, date(2026, 6, 4)) == "N/A"


def test_fetch_cora_decision_stats_uses_same_autonomy_definition():
    session = _Session(_Result(fetchall=[
        ("fomo", 25, 12, 8, 5, 7, 1234, 0.0123),
    ]))

    rows = _fetch_cora_decision_stats(session, date(2026, 6, 4))

    assert rows[0]["autonomous_pct"] == "28.0%"
    sql = session.calls[0]["sql"]
    assert "was_autonomous" not in sql
    assert "terminal_status IS NOT NULL" in sql
    assert "approved_at IS NULL" in sql
    assert "overridden_at IS NULL" in sql
