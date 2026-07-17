"""
Integration tests (fresh_db): real Postgres, seeded Foreclosure + Deed rows ->
staged OutcomeCandidate rows for the lis-pendens pre-auction-sale arc.

CDE-05's scope is strictly the complement of foreclosure_outcomes.py: rows
with lis_pendens_date set and auction_date NULL. Rows that reach auction are
that connector's territory and must never be touched here.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import text

from src.connectors.lis_pendens_outcomes import stage_outcomes
from src.connectors.outcomes import EVENT_TYPE_LP_SOLD_PRE_AUCTION
from src.core.models import Deed, Foreclosure, Property


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_lp_foreclosure(session, property_id, case_number, **overrides) -> Foreclosure:
    defaults = dict(
        property_id=property_id,
        county_id="hillsborough",
        case_number=case_number,
        lis_pendens_date=date(2025, 1, 10),
        auction_date=None,
    )
    defaults.update(overrides)
    f = Foreclosure(**defaults)
    session.add(f)
    session.flush()
    return f


def _mk_deed(session, property_id, instrument_number, **overrides) -> Deed:
    defaults = dict(
        property_id=property_id,
        instrument_number=instrument_number,
        record_date=date(2025, 6, 1),
        sale_price=Decimal("210000.00"),
        deed_type="Warranty Deed",
        grantee="BUYER LLC",
    )
    defaults.update(overrides)
    d = Deed(**defaults)
    session.add(d)
    session.flush()
    return d


class TestStageOutcomesPG:
    def test_lp_with_later_deed_stages_pre_auction_sale(self, fresh_db):
        prop = _mk_property(fresh_db, "LP-001")
        fc = _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-001")
        _mk_deed(fresh_db, prop.id, "INST-001")

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT event_type, amount, raw_payload FROM outcome_candidates "
                 "WHERE source_type = 'lis_pendens_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is not None
        assert row.event_type == EVENT_TYPE_LP_SOLD_PRE_AUCTION
        assert row.amount == Decimal("210000.00")
        assert row.raw_payload["foreclosure_case_number"] == "LP-CASE-001"
        assert row.raw_payload["days_lp_to_sale"] == (date(2025, 6, 1) - date(2025, 1, 10)).days

    def test_lp_with_no_deed_is_unresolved(self, fresh_db):
        prop = _mk_property(fresh_db, "LP-002")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-002")

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_quitclaim_deed_excluded_as_resale(self, fresh_db):
        prop = _mk_property(fresh_db, "LP-003")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-003")
        _mk_deed(fresh_db, prop.id, "INST-QC-003", deed_type="Quit Claim Deed")

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_deed_before_lis_pendens_date_ignored(self, fresh_db):
        prop = _mk_property(fresh_db, "LP-004")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-004", lis_pendens_date=date(2025, 6, 1))
        _mk_deed(fresh_db, prop.id, "INST-EARLY-004", record_date=date(2025, 1, 1))

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_nominal_price_resale_excluded(self, fresh_db):
        prop = _mk_property(fresh_db, "LP-005")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-005")
        _mk_deed(fresh_db, prop.id, "INST-NOM-005", sale_price=Decimal("10.00"))

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_null_price_deed_alone_is_not_a_resale(self, fresh_db):
        # Deed loader persists NULL sale_price whenever the source CSV's
        # SalesPrice is missing (e.g. a non-sale instrument). Must not be
        # mistaken for a confirmed sale.
        prop = _mk_property(fresh_db, "LP-008")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-008")
        _mk_deed(fresh_db, prop.id, "INST-NULLPRICE-008", sale_price=None)

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_null_price_deed_does_not_mask_later_real_sale(self, fresh_db):
        # LIMIT 1 orders by record_date ASC -- a NULL-price instrument earlier
        # in the chain must not win over (or block) the real sale that follows.
        prop = _mk_property(fresh_db, "LP-009")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-009")
        _mk_deed(fresh_db, prop.id, "INST-NULLPRICE-009", sale_price=None, record_date=date(2025, 3, 1))
        _mk_deed(fresh_db, prop.id, "INST-REAL-009", sale_price=Decimal("195000.00"), record_date=date(2025, 6, 1))

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT amount, raw_payload FROM outcome_candidates "
                 "WHERE source_type = 'lis_pendens_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is not None
        assert row.amount == Decimal("195000.00")
        assert row.raw_payload["sale_instrument"] == "INST-REAL-009"

    def test_row_with_auction_date_is_never_touched(self, fresh_db):
        # This is foreclosure_outcomes.py's territory -- CDE-05 must not stage it.
        prop = _mk_property(fresh_db, "LP-006")
        _mk_lp_foreclosure(
            fresh_db, prop.id, "LP-CASE-006",
            auction_date=datetime(2026, 3, 1), lis_pendens_date=date(2025, 1, 1),
        )
        _mk_deed(fresh_db, prop.id, "INST-006")

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "LP-007")
        _mk_lp_foreclosure(fresh_db, prop.id, "LP-CASE-007")
        _mk_deed(fresh_db, prop.id, "INST-007")

        stage_outcomes(fresh_db, "hillsborough")
        stage_outcomes(fresh_db, "hillsborough")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates "
                 "WHERE source_type = 'lis_pendens_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
