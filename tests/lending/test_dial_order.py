"""Runbook P1 + §4.3: partners first, cold maturity as filler; highest score first per queue."""
from __future__ import annotations

import pytest

from config import lending_queues as q
from src.lending.dial_order import order_for_dialing, queue_to_dial_now, score_field


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


def test_score_field_prefers_t16_lead_score_then_rank():
    assert score_field([{"rank": 9}, {"lead_score": 0}]) == "lead_score"
    assert score_field([{"rank": 7}]) == "rank"
    assert score_field([{}]) is None


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
    assert q.MAX_LINES == {q.PARTNERS: 3, q.WHOLESALERS: 3, q.TRANSACTION_READY: 3, q.BUILDERS: 3, q.VERIFIED_MATURITY: 5}


def test_validate_rejects_five_lines_outside_cold_maturity(monkeypatch):
    monkeypatch.setitem(q.MAX_LINES, q.PARTNERS, 5)
    with pytest.raises(ValueError):
        q.validate_queue_config()


def test_validate_rejects_dial_order_missing_a_queue(monkeypatch):
    monkeypatch.setattr(q, "DIAL_ORDER", (q.PARTNERS, q.BUILDERS))
    with pytest.raises(ValueError):
        q.validate_queue_config()


@pytest.mark.parametrize("tag", ["wholesaler_sunbiz", "wholesaler_hunter"])
def test_wholesaler_tags_land_in_their_own_wholesalers_queue(tag):
    assert q.SOURCE_TAG_QUEUES[tag] == q.WHOLESALERS and q.SOURCE_TAG_QUEUES["list_4"] == q.PARTNERS


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


def test_one_score_scale_per_queue_t16_score_wins_over_old_rank():
    """T-16's 0-10 lead_score and the old 1-10 rank are different scales: once any record in a
    queue has a lead_score, rank-only records in that queue count as unscored."""
    records = [
        _rec("rank-only", q.PARTNERS, rank=10),
        _rec("t16-low", q.PARTNERS, lead_score=2),
        _rec("t16-high", q.PARTNERS, lead_score=8),
    ]
    assert [r["source_record_ref"] for r in order_for_dialing(records)] == ["t16-high", "t16-low", "rank-only"]


def test_rank_is_used_when_no_record_in_the_queue_has_a_t16_score():
    records = [_rec("lo", q.VERIFIED_MATURITY, rank=3), _rec("hi", q.VERIFIED_MATURITY, rank=9)]
    assert [r["source_record_ref"] for r in order_for_dialing(records)] == ["hi", "lo"]
