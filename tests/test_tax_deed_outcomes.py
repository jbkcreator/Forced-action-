"""
Unit tests (pure): classify_status.
Integration tests (fresh_db): real Postgres, seeded TaxDeedAuction rows ->
staged OutcomeCandidate rows.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from src.connectors.outcomes import (
    EVENT_TYPE_TAX_DEED_CANCELLED,
    EVENT_TYPE_TAX_DEED_REDEEMED,
    EVENT_TYPE_TAX_DEED_SOLD,
)
from src.connectors.tax_deed_outcomes import classify_status, stage_outcomes
from src.core.models import Property, TaxDeedAuction


def _mk_property(session, parcel: str, *, county_id: str = "pinellas") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_auction(session, property_id, case_number, **overrides) -> TaxDeedAuction:
    defaults = dict(
        property_id=property_id,
        county_id="pinellas",
        parcel_id="X",
        auction_date=date(2026, 1, 21),
        case_number=case_number,
        status=None,
        sold_amount=None,
        sold_to=None,
    )
    defaults.update(overrides)
    a = TaxDeedAuction(**defaults)
    session.add(a)
    session.flush()
    return a


# ---------------------------------------------------------------------------
# Unit tests — classify_status, no DB
# ---------------------------------------------------------------------------

class TestClassifyStatus:
    def test_sold_amount_and_sold_to_wins_regardless_of_status_text(self):
        # Real live-data case: a genuinely sold auction's status field held a
        # resolution timestamp, not the word "Sold" — sold_amount/sold_to must
        # still classify it correctly.
        assert classify_status("01/21/2026 11:26 AM ET", Decimal("120100.00"), "3rd Party Bidder") == EVENT_TYPE_TAX_DEED_SOLD

    def test_redeemed(self):
        assert classify_status("Redeemed", None, None) == EVENT_TYPE_TAX_DEED_REDEEMED

    def test_redeemed_after_sale(self):
        assert classify_status("Redeemed After Sale", None, None) == EVENT_TYPE_TAX_DEED_REDEEMED

    def test_cancelled(self):
        assert classify_status("Canceled per Bankruptcy", None, None) == EVENT_TYPE_TAX_DEED_CANCELLED

    def test_unrecognized_status_with_no_sale_signal_is_unclassifiable(self):
        assert classify_status("01/21/2026 11:26 AM ET", None, None) is None

    def test_empty_status_no_sale_signal_is_unclassifiable(self):
        assert classify_status(None, None, None) is None

    def test_sold_amount_without_sold_to_falls_through_to_status(self):
        # amount alone isn't enough — needs a buyer too, otherwise fall back
        # to status text (which here doesn't match anything -> unclassifiable).
        assert classify_status("some other message", Decimal("100.00"), None) is None


# ---------------------------------------------------------------------------
# Integration tests — real Postgres (fresh_db)
# ---------------------------------------------------------------------------

class TestStageOutcomesPG:
    def test_full_run_classifies_and_skips_correctly(self, fresh_db):
        # NOTE: this runs against the real shared DB (fresh_db is a rolled-back
        # nested transaction, but it sees pre-existing committed rows too) —
        # the live 'pinellas' tax_deed_auctions table already has ~29 real
        # rows, so aggregate ConnectorRunResult counts aren't asserted here;
        # only outcome_candidates rows scoped to this test's own property.
        prop = _mk_property(fresh_db, "TDO-001")
        prop_no_match = None  # simulate an unmatched auction row

        _mk_auction(fresh_db, prop.id, "TDO-CASE-SOLD", status="01/21/2026 11:26 AM ET",
                    sold_amount=Decimal("120100.00"), sold_to="3rd Party Bidder")
        _mk_auction(fresh_db, prop.id, "TDO-CASE-REDEEMED", status="Redeemed")
        _mk_auction(fresh_db, prop.id, "TDO-CASE-UNCLASSIFIED", status="weird message")
        _mk_auction(fresh_db, prop_no_match, "TDO-CASE-NOPROP", status="Redeemed")

        result = stage_outcomes(fresh_db, "pinellas")
        assert result.errors == 0

        rows = fresh_db.execute(
            text("SELECT source_id, event_type, amount, counterparty FROM outcome_candidates "
                 "WHERE source_type = 'tax_deed_outcomes' AND property_id = :pid ORDER BY source_id"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 2
        event_types = {r.event_type for r in rows}
        assert event_types == {EVENT_TYPE_TAX_DEED_SOLD, EVENT_TYPE_TAX_DEED_REDEEMED}

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "TDO-002")
        _mk_auction(fresh_db, prop.id, "TDO-CASE-IDEMPOTENT", status="Redeemed")

        stage_outcomes(fresh_db, "pinellas")
        stage_outcomes(fresh_db, "pinellas")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates WHERE source_type = 'tax_deed_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
