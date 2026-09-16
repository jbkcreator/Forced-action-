"""Regression tests for PR #268 finding 2:

portfolio_expansion must fire on CROSSING a milestone (>=), deduped on the
highest milestone already alerted (stored monitor_value) — not exact equality
against a point-in-time batch counter that can jump past a milestone (2->4).
"""
from datetime import date

from src.services import repeat_maturity_engine as eng
from src.services.repeat_maturity_engine import (
    MonitorAlert,
    _check_portfolio_expansion,
    _record_fired,
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Session:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, clause, params=None):
        self.executed.append((str(clause), params))
        return _FakeResult(self._rows)


def _row(**kw):
    base = dict(
        event_id=1, buyer_entity_id=7, canonical_name="WHALE LLC",
        event_date=date(2026, 9, 15), property_id=100, property_address="1 Main St",
        total_purchase_count=4, total_cash_volume=1_000_000, buyer_type="flipper",
        financing_signal=None, milestone=3,
    )
    base.update(kw)
    return base


def test_query_uses_ge_crossing_and_monitor_value_dedup():
    sess = _Session([_row()])
    _check_portfolio_expansion(sess, date(2026, 9, 16))
    sql = sess.executed[0][0]
    # Must NOT regress to exact-equality on the batch counter.
    assert "= ANY(:milestones)" not in sql
    assert ">= :min_milestone" in sql
    assert "bml.monitor_value >=" in sql


def test_crossed_milestone_fires_when_count_jumped_past_it():
    # count jumped 2->4, never equalling 3; highest crossed milestone is 3.
    sess = _Session([_row(total_purchase_count=4, milestone=3)])
    alerts = _check_portfolio_expansion(sess, date(2026, 9, 16))
    assert len(alerts) == 1
    assert alerts[0].extra["milestone"] == 3
    assert alerts[0].total_purchase_count == 4


def test_record_fired_persists_the_milestone_as_monitor_value():
    captured = {}

    class _Rec:
        def execute(self, clause, params=None):
            captured["sql"] = str(clause)
            captured["params"] = params

    alert = MonitorAlert(
        monitor_type="portfolio_expansion", buyer_entity_id=7,
        canonical_name="WHALE LLC", source_event_id=1, property_address=None,
        event_date=date(2026, 9, 15), days_since=None, total_purchase_count=4,
        total_cash_volume=1_000_000, buyer_type="flipper", financing_signal=None,
        extra={"milestone": 3},
    )
    _record_fired(_Rec(), alert, slack_ts="123.45")
    assert "monitor_value" in captured["sql"]
    assert captured["params"]["monitor_value"] == 3
