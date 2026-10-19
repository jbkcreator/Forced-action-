"""Runbook P1 + §4.3: partners first, cold maturity as filler; highest score first per queue."""
from __future__ import annotations

import pytest

from config import lending_queues as q
from src.lending.dial_order import order_for_dialing, queue_to_dial_now, record_score


def _rec(ref, queue, **extra):
    return {"source_record_ref": ref, "pool": queue, **extra}


def test_dial_order_puts_partners_first_and_maturity_last():
    assert q.DIAL_ORDER[0] == q.PARTNERS
    assert q.DIAL_ORDER[-1] == q.VERIFIED_MATURITY
    assert set(q.DIAL_ORDER) == q.LAUNCH_QUEUES


def test_records_dial_in_queue_order_then_highest_score_first():
    records = [
        _rec("m-hi", q.VERIFIED_MATURITY, lead_score=10),
        _rec("b-lo", q.BUILDERS, lead_score=2),
        _rec("p-lo", q.PARTNERS, lead_score=3),
        _rec("t-mid", q.TRANSACTION_READY, lead_score=5),
        _rec("p-hi", q.PARTNERS, lead_score=9),
        _rec("b-hi", q.BUILDERS, lead_score=8),
    ]
    refs = [r["source_record_ref"] for r in order_for_dialing(records)]
    assert refs == ["p-hi", "p-lo", "t-mid", "b-hi", "b-lo", "m-hi"]


def test_unscored_records_go_last_in_their_own_queue_not_below_lower_queues():
    records = [
        _rec("m-scored", q.VERIFIED_MATURITY, lead_score=10),
        _rec("p-none", q.PARTNERS),
        _rec("p-scored", q.PARTNERS, lead_score=1),
    ]
    refs = [r["source_record_ref"] for r in order_for_dialing(records)]
    assert refs == ["p-scored", "p-none", "m-scored"]


def test_ties_and_unscored_keep_input_order():
    records = [_rec(f"p{i}", q.PARTNERS, lead_score=4) for i in range(3)] + [_rec("n1", q.PARTNERS), _rec("n2", q.PARTNERS)]
    assert [r["source_record_ref"] for r in order_for_dialing(records)] == ["p0", "p1", "p2", "n1", "n2"]


def test_records_with_no_launch_queue_are_dropped():
    records = [_rec("x", None), _rec("n", q.NURTURE), _rec("p", q.PARTNERS)]
    assert [r["source_record_ref"] for r in order_for_dialing(records)] == ["p"]


def test_record_score_prefers_t16_lead_score_then_falls_back_to_rank():
    assert record_score({"lead_score": 0, "rank": 9}) == 0
    assert record_score({"rank": 7}) == 7
    assert record_score({}) is None


def test_score_source_is_swappable():
    records = [_rec("a", q.PARTNERS, x=1), _rec("b", q.PARTNERS, x=5)]
    out = order_for_dialing(records, score=lambda r: r["x"])
    assert [r["source_record_ref"] for r in out] == ["b", "a"]


@pytest.mark.parametrize("remaining,expected", [
    ({q.PARTNERS: 4, q.VERIFIED_MATURITY: 500}, q.PARTNERS),
    ({q.PARTNERS: 0, q.BUILDERS: 2, q.VERIFIED_MATURITY: 500}, q.BUILDERS),
    ({q.PARTNERS: 0, q.TRANSACTION_READY: 0, q.BUILDERS: 0, q.VERIFIED_MATURITY: 525}, q.VERIFIED_MATURITY),
    ({}, None),
])
def test_queue_to_dial_now_is_first_non_empty_queue(remaining, expected):
    assert queue_to_dial_now(remaining) == expected


def test_line_rules_three_everywhere_five_only_on_cold_maturity():
    """Runbook P4: partner/wholesaler/landline max 3 lines; 5 reserved for cold maturity."""
    assert q.MAX_LINES == {q.PARTNERS: 3, q.TRANSACTION_READY: 3, q.BUILDERS: 3, q.VERIFIED_MATURITY: 5}


def test_validate_rejects_five_lines_outside_cold_maturity(monkeypatch):
    monkeypatch.setitem(q.MAX_LINES, q.PARTNERS, 5)
    with pytest.raises(ValueError):
        q.validate_queue_config()


def test_validate_rejects_dial_order_missing_a_queue(monkeypatch):
    monkeypatch.setattr(q, "DIAL_ORDER", (q.PARTNERS, q.BUILDERS))
    with pytest.raises(ValueError):
        q.validate_queue_config()


@pytest.mark.parametrize("tag", ["wholesaler_sunbiz", "wholesaler_hunter"])
def test_wholesaler_tags_land_in_partners(tag):
    assert q.SOURCE_TAG_QUEUES[tag] == q.PARTNERS


def test_sort_for_load_keeps_records_outside_launch_queues_at_the_end():
    """The load refuses unmapped pools itself; sorting must not hide them by dropping them."""
    from src.lending.dial_order import sort_for_load
    records = [_rec("x", "unknown_pool"), _rec("m", q.VERIFIED_MATURITY), _rec("p", q.PARTNERS)]
    assert [r["source_record_ref"] for r in sort_for_load(records)] == ["p", "m", "x"]


def test_dialer_load_task_loads_in_dial_order(monkeypatch, tmp_path):
    import json
    from contextlib import contextmanager
    from src.tasks import lending_dialer_load as task

    seen = {}

    def fake_load(records, session, **kw):
        seen["refs"] = [r["source_record_ref"] for r in records]
        from src.lending.dialer_load import LoadReport
        return LoadReport(run_id=kw["run_id"], dry_run=True)

    @contextmanager
    def fake_db():
        class S:
            def rollback(self): pass
        yield S()

    monkeypatch.setattr(task, "run_dialer_load", fake_load)
    monkeypatch.setattr(task, "get_db_context", fake_db)
    path = tmp_path / "pools.json"
    path.write_text(json.dumps([_rec("m", q.VERIFIED_MATURITY, lead_score=9), _rec("p", q.PARTNERS)]))
    assert task.main(["--input", str(path)]) == 0
    assert seen["refs"] == ["p", "m"]
