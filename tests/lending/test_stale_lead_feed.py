"""R1 0.6: a failed PropertyRadar pull never turns into an old list loaded as new. Freshness is per (state, campaign)
segment: a fresh segment never masks a stale one. No database: the pull-run read runs against a fake session."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.lending import pool_source
from src.lending.pr_maturity_bridge import segment_of
from src.tasks import lending_pr_dialer_autoload as autoload
from src.tasks import lending_pr_maturity_bridge as bridge_task

NOW = datetime(2026, 11, 26, 14, 5, tzinfo=timezone.utc)  # Wednesday 09:05 ET
GA_MATURITY = ("GA", "maturity_target_lender")
FL_CASH = ("FL", "cash_purchase")


class _Db:
    """``segments`` = (state, campaign) -> newest successful pull of a nightly-pulled segment (None = none)."""

    def __init__(self, segments):
        self.segments = segments
        self.rolled_back = False

    def execute(self, stmt, params=None):
        if stmt is autoload._ACTIVE_PHONES_SQL:
            return iter([])
        assert stmt is pool_source._SCHEDULED_SEGMENTS
        return iter([(state, campaign, newest) for (state, campaign), newest in self.segments.items()])

    def rollback(self):
        self.rolled_back = True

    def commit(self):
        pass


def test_the_segment_query_judges_nightly_pulled_segments_only():
    sql = str(pool_source._SCHEDULED_SEGMENTS)
    assert "GROUP BY state, campaign" in sql and "bool_or(run_type = 'daily')" in sql and "status = 'done'" in sql


def test_a_pull_from_last_night_is_fresh():
    assert pool_source.stale_segments(_Db({FL_CASH: NOW - timedelta(hours=7)}), NOW) == {}


def test_a_failed_ga_maturity_pull_is_not_masked_by_a_fresh_fl_cash_purchase_pull():
    stale = pool_source.stale_segments(_Db({FL_CASH: NOW - timedelta(hours=7),
                                            GA_MATURITY: NOW - timedelta(hours=31, minutes=40)}), NOW)
    assert list(stale) == [GA_MATURITY] and stale[GA_MATURITY].age_hours == 31
    assert pool_source.stale_message(stale) == "PropertyRadar GA maturity_target_lender data is 31 h old — not loaded"


def test_a_nightly_segment_that_never_succeeded_is_stale():
    stale = pool_source.stale_segments(_Db({GA_MATURITY: None}), NOW)
    assert stale[GA_MATURITY].age_hours is None and "no successful pull" in stale[GA_MATURITY].message()


@pytest.mark.parametrize("row,segment", [
    ({"state": "GA", "source_tag": "list_5"}, GA_MATURITY),
    ({"state": "FL", "source_tag": "list_1"}, ("FL", "maturity_target_lender")),
    ({"state": "fl", "source_tag": "list_2"}, FL_CASH),
    ({"state": "GA", "source_tag": "investor_portfolio_propertyradar"}, ("GA", "investor_portfolio")),
])
def test_a_staged_row_maps_back_to_its_segment(row, segment):
    assert segment_of(row) == segment


def _patch_db(monkeypatch, db):
    @contextmanager
    def ctx():
        yield db

    import src.core.database as database

    monkeypatch.setattr(database, "get_db_context", ctx)


def test_the_morning_load_leaves_out_the_stale_segment_and_loads_the_fresh_one(monkeypatch):
    """Tuesday night's GA maturity pull failed, FL cash purchases came in: only the FL rows are planned."""
    _patch_db(monkeypatch, _Db({FL_CASH: NOW - timedelta(hours=7), GA_MATURITY: NOW - timedelta(hours=31)}))
    staged = [{"state": "GA", "source_tag": "list_5", "phone": "+14045550101"},
              {"state": "FL", "source_tag": "list_2", "phone": "+18135550102"}]
    monkeypatch.setattr(autoload, "staged_pr_records", lambda db: staged)
    monkeypatch.setattr(autoload, "credits_spent", lambda day, root=None: 0)
    monkeypatch.setattr(autoload, "pre_check_verdicts", lambda db, records: [None] * len(records))
    planned = {}

    def select(records, verdicts, *, active_phones, credits_left):
        planned["phones"] = [r["phone"] for r in records]
        return [], {"to_load": 0}

    monkeypatch.setattr(autoload, "select_records", select)
    summary, code = autoload._run(NOW, 10, apply=False)
    assert planned["phones"] == ["+18135550102"]
    assert code == autoload.EXIT_OK and "GA maturity_target_lender data is 31 h old" in summary["stale_data"]


def test_the_morning_load_posts_the_plain_alert(monkeypatch):
    from config.settings import get_settings

    posted = []
    monkeypatch.setattr(get_settings(), "lending_pr_dialer_autoload_enabled", True, raising=False)
    monkeypatch.setattr(autoload, "in_load_hour", lambda now: True)
    message = "PropertyRadar GA maturity_target_lender data is 31 h old — not loaded"
    monkeypatch.setattr(autoload, "_run", lambda *a, **k: ({"stale_data": message, "to_load": 0}, autoload.EXIT_OK))
    monkeypatch.setattr(autoload, "_notify", lambda text, *, failed: posted.append((text, failed)))
    assert autoload.main(["--apply"]) == autoload.EXIT_OK
    assert posted == [(message, True)]


def test_all_fresh_plans_the_load(monkeypatch):
    _patch_db(monkeypatch, _Db({FL_CASH: NOW - timedelta(hours=7)}))
    monkeypatch.setattr(autoload, "plan_load", lambda db, **k: ([], {"to_load": 0}))
    summary, code = autoload._run(NOW, 10, apply=False)
    assert code == autoload.EXIT_OK and summary == {"to_load": 0}


def test_the_bridge_stages_the_fresh_segments_and_never_restages_the_stale_one(monkeypatch):
    posted, extracted = [], {}
    db = _Db({FL_CASH: datetime.now(timezone.utc) - timedelta(hours=7),
              GA_MATURITY: datetime.now(timezone.utc) - timedelta(days=3)})
    monkeypatch.setattr(bridge_task, "get_db_context", lambda: _ctx(db))
    monkeypatch.setattr(bridge_task, "extract_pr_maturity_pool",
                        lambda session, **k: extracted.update(k) or {"rows_written": 1})
    monkeypatch.setattr("src.lending.ops_alerts.post_ops_alert", lambda message: posted.append(message))
    assert bridge_task.main(["--apply"]) == 0
    assert list(extracted["exclude"]) == [GA_MATURITY]
    assert posted and "GA maturity_target_lender" in posted[0] and "FL" not in posted[0]


def test_the_bridge_query_leaves_out_excluded_segments():
    from src.lending import pr_maturity_bridge

    seen = []

    class _Pages:
        def execute(self, stmt, params):
            seen.append(params)
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))

    assert list(pr_maturity_bridge._iter_record_pages(_Pages(), state=None, exclude=[GA_MATURITY])) == []
    assert seen[0]["stale"] == ["GA|maturity_target_lender"]
    assert "NOT (state || '|' || campaign = ANY(:stale))" in pr_maturity_bridge._RECORDS_SQL


@contextmanager
def _ctx(db):
    yield db
