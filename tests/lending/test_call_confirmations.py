"""Borrower-confirmed facts: stored per property and read back as known scoring inputs."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.lending.call_confirmations import record_call_confirmation
from src.lending.lead_facts import _signals_from_row
from src.lending.lead_scoring import Provenance, score_lead

CONFIRMED_AT = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)
ROW = {
    "property_id": 7, "property_address": "1 Test St", "entity_status": "ACTIVE",
    "sunbiz_status": "matched", "sunbiz_doc_number": "L12000000001", "equity_pct": Decimal("42"),
    "lender_name": "Test Lender", "loan_amount": 350000, "est_maturity_date": "2026-12-01",
    "property_count": 1, "recent_permit_count": 0, "latest_permit": None,
    "confirmed_maturity_date": None, "decision_maker_on_call": None,
}


def _record(session, **overrides):
    args = dict(property_id=7, confirmed_at=CONFIRMED_AT, caller_seat="seat-a",
                source_call_ref="cdr-1", maturity_date=date(2026, 11, 15))
    args.update(overrides)
    record_call_confirmation(session, **args)
    return session.execute.call_args.args


def test_confirmation_upserts_and_keeps_unconfirmed_facts():
    sql, params = _record(MagicMock())
    assert "ON CONFLICT (property_id) DO UPDATE" in str(sql)
    assert "COALESCE(EXCLUDED.decision_maker_on_call" in str(sql)
    assert params["maturity_date"] == date(2026, 11, 15)
    assert params["decision_maker_on_call"] is None


def test_decision_maker_alone_can_be_confirmed():
    _, params = _record(MagicMock(), maturity_date=None, decision_maker_on_call=True)
    assert (params["maturity_date"], params["decision_maker_on_call"]) == (None, True)


def test_nothing_confirmed_is_refused():
    with pytest.raises(ValueError):
        _record(MagicMock(), maturity_date=None, decision_maker_on_call=None)


def test_naive_time_is_refused():
    with pytest.raises(ValueError):
        _record(MagicMock(), confirmed_at=datetime(2026, 10, 6, 15, 0))


def test_without_confirmation_maturity_is_estimated_and_decision_maker_missing():
    signals = _signals_from_row(ROW)
    assert signals.maturity_date.provenance is Provenance.ESTIMATED
    assert signals.decision_maker_confirmed.provenance is Provenance.MISSING


def test_confirmed_maturity_overrides_the_estimate_and_is_known():
    signals = _signals_from_row({**ROW, "confirmed_maturity_date": date(2026, 11, 15)})
    assert signals.maturity_date.value == date(2026, 11, 15)
    assert signals.maturity_date.provenance is Provenance.KNOWN


def test_decision_maker_not_on_the_call_is_known_false():
    signals = _signals_from_row({**ROW, "decision_maker_on_call": False})
    assert signals.decision_maker_confirmed.is_known and signals.decision_maker_confirmed.value is False


def test_confirmations_raise_the_rank_and_replace_the_question():
    """Rank 10 also needs known equity; PropertyRadar equity is an estimate, so this stops below 10."""
    before = score_lead(_signals_from_row(ROW), today=date(2026, 10, 6))
    confirmed = _signals_from_row({**ROW, "confirmed_maturity_date": date(2026, 11, 15),
                                   "decision_maker_on_call": True})
    after = score_lead(confirmed, today=date(2026, 10, 6))
    assert before.caller_questions and not after.caller_questions
    assert set(after.signals_met) == {"maturity_within_window", "entity_in_good_standing",
                                      "decision_maker_confirmed"}
    assert after.rank > before.rank
