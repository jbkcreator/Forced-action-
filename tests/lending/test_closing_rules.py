from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.lending.closing.rules import blame, build_checklist, due_chase_day, is_stuck

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)


def test_exactly_three_days_is_not_stuck():
    assert not is_stuck(NOW - timedelta(days=3), NOW)
    assert is_stuck(NOW - timedelta(days=3, minutes=1), NOW)


@pytest.mark.parametrize("stage,missing,partner,owner", [
    ("submitted to lender", 0, False, "lender"), ("submitted to lender", 2, False, "borrower"),
    ("submitted to lender", 2, True, "partner"), ("ready to fund", 0, False, "title"),
    ("application", 0, False, "borrower")])
def test_blame(stage, missing, partner, owner):
    assert blame(stage, missing_items=missing, from_partner=partner) == owner


def test_due_chase_day_cadence():
    accepted = datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)
    assert due_chase_day(accepted, accepted, frozenset()) == 0
    assert due_chase_day(accepted, accepted + timedelta(days=1), frozenset({0})) is None
    assert due_chase_day(accepted, accepted + timedelta(days=2), frozenset({0})) == 2
    assert due_chase_day(accepted, accepted + timedelta(days=6), frozenset({0})) == 5  # missed day 2: latest only
    assert due_chase_day(accepted, accepted + timedelta(days=12), frozenset({0, 2, 5, 8})) is None


def test_chase_days_are_et_calendar_days():
    accepted = datetime(2026, 11, 11, 3, 30, tzinfo=timezone.utc)  # 10:30 PM ET on Nov 10
    assert due_chase_day(accepted, datetime(2026, 11, 12, 13, 0, tzinfo=timezone.utc), frozenset({0})) == 2


def test_interest_reserve_condition_adds_item():
    codes = [item.code for item in build_checklist("central_lending", ["INTEREST_RESERVE_FULL_TERM", "UNKNOWN"])]
    assert len(codes) == 7 and codes[-1] == "interest_reserve_funds"


def test_unknown_lender_refused():
    with pytest.raises(ValueError):
        build_checklist("backflip", [])
