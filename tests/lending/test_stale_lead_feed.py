"""R1 0.6: a failed PropertyRadar pull never turns into an old list loaded as new. No database: the newest-pull read
runs against a fake session."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.lending import pool_source
from src.tasks import lending_pr_dialer_autoload as autoload
from src.tasks import lending_pr_maturity_bridge as bridge_task

NOW = datetime(2026, 11, 26, 14, 5, tzinfo=timezone.utc)  # Wednesday 09:05 ET


class _Db:
    def __init__(self, finished):
        self.finished = finished
        self.rolled_back = False

    def execute(self, stmt, params=None):
        assert stmt is pool_source._NEWEST_PULL
        return SimpleNamespace(scalar=lambda: self.finished)

    def rollback(self):
        self.rolled_back = True

    def commit(self):
        pass


def test_a_pull_from_last_night_is_fresh():
    assert pool_source.propertyradar_staleness(_Db(NOW - timedelta(hours=7)), NOW) is None


def test_a_pull_older_than_26_hours_is_refused_with_its_age():
    stale = pool_source.propertyradar_staleness(_Db(NOW - timedelta(hours=31, minutes=40)), NOW)
    assert stale.age_hours == 31 and stale.message() == "PropertyRadar data is 31 h old — nothing loaded"


def test_no_successful_pull_at_all_is_refused():
    stale = pool_source.propertyradar_staleness(_Db(None), NOW)
    assert stale.age_hours is None and "nothing loaded" in stale.message()


def _patch_db(monkeypatch, db):
    @contextmanager
    def ctx():
        yield db

    import src.core.database as database

    monkeypatch.setattr(database, "get_db_context", ctx)


def test_the_morning_load_loads_nothing_from_a_stale_pull(monkeypatch):
    """Tuesday night's pull failed: at 09:05 Wednesday the newest pull is 31 h old."""
    db = _Db(NOW - timedelta(hours=31))
    _patch_db(monkeypatch, db)
    monkeypatch.setattr(autoload, "plan_load", lambda *a, **k: pytest.fail("planned a load from stale data"))
    summary, code = autoload._run(NOW, 10, apply=True)
    assert code == autoload.EXIT_REFUSED and db.rolled_back
    assert summary == {"stale_data": "PropertyRadar data is 31 h old — nothing loaded", "propertyradar_age_hours": 31}


def test_the_morning_load_posts_the_plain_alert(monkeypatch):
    from config.settings import get_settings

    posted = []
    monkeypatch.setattr(get_settings(), "lending_pr_dialer_autoload_enabled", True, raising=False)
    monkeypatch.setattr(autoload, "in_load_hour", lambda now: True)
    monkeypatch.setattr(autoload, "_run", lambda *a, **k: ({"stale_data": "PropertyRadar data is 31 h old — "
                                                                          "nothing loaded"}, autoload.EXIT_REFUSED))
    monkeypatch.setattr(autoload, "_notify", lambda message, *, failed: posted.append((message, failed)))
    assert autoload.main(["--apply"]) == autoload.EXIT_REFUSED
    assert posted == [("PropertyRadar data is 31 h old — nothing loaded", True)]


def test_a_fresh_pull_plans_the_load(monkeypatch):
    _patch_db(monkeypatch, _Db(NOW - timedelta(hours=7)))
    monkeypatch.setattr(autoload, "plan_load", lambda db, **k: ([], {"to_load": 0}))
    summary, code = autoload._run(NOW, 10, apply=False)
    assert code == autoload.EXIT_OK and summary == {"to_load": 0}


def test_the_bridge_never_restages_old_rows(monkeypatch):
    posted = []
    monkeypatch.setattr(bridge_task, "get_db_context", lambda: _ctx(_Db(NOW - timedelta(days=3))))
    monkeypatch.setattr(bridge_task, "extract_pr_maturity_pool", lambda *a, **k: pytest.fail("re-staged old rows"))
    monkeypatch.setattr("src.lending.ops_alerts.post_ops_alert", lambda message: posted.append(message))
    assert bridge_task.main(["--apply"]) == 3
    assert posted and "nothing loaded" in posted[0]


@contextmanager
def _ctx(db):
    yield db
