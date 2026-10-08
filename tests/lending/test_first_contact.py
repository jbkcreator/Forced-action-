"""Write-once first-contact snapshot."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.lending.first_contact import record_first_contact, signals_payload
from src.lending.lead_scoring import LeadSignals, Signal, score_lead

SIGNALS = LeadSignals(maturity_date=Signal.estimated(date(2026, 11, 20)), loan_amount=Signal.known(Decimal("350000")))
SCORE = score_lead(SIGNALS, today=date(2026, 10, 5))
CONTACTED = datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc)


def _record(session, **overrides):
    args = dict(phone="(813) 555-0142", contacted_at=CONTACTED, caller_seat="seat-a",
                script_version="v1", source_tag="list_1", queue="verified_maturity",
                warm=False, signals=SIGNALS, score=SCORE)
    args.update(overrides)
    return record_first_contact(session, **args)


def test_first_contact_writes_a_row():
    session = MagicMock()
    session.execute.return_value.rowcount = 1
    assert _record(session) is True
    sql, params = session.execute.call_args.args
    assert "ON CONFLICT (phone) DO NOTHING" in str(sql)
    assert params["phone"] == "+18135550142"
    assert params["rank"] == SCORE.rank
    assert json.loads(params["signals"])["maturity_date"] == {"value": "2026-11-20", "provenance": "estimated"}


def test_later_contact_writes_nothing():
    session = MagicMock()
    session.execute.return_value.rowcount = 0
    assert _record(session) is False


def test_invalid_phone_is_refused():
    with pytest.raises(ValueError):
        _record(MagicMock(), phone="not-a-phone")


def test_naive_contact_time_is_refused():
    with pytest.raises(ValueError):
        _record(MagicMock(), contacted_at=datetime(2026, 10, 5, 14, 0))


def test_payload_serializes_every_input():
    payload = signals_payload(SIGNALS)
    assert payload["loan_amount"] == {"value": "350000", "provenance": "known"}
    assert payload["equity_pct"] == {"value": None, "provenance": "missing"}
    json.dumps(payload)
