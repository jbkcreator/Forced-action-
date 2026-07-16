"""
Unit tests (pure): classify_deed(), find_flip_pairs().
Integration tests (fresh_db): real Postgres, seeded Deed rows ->
staged OutcomeCandidate rows.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import text

from src.connectors.deed_flip_outcomes import classify_deed, stage_outcomes
from src.connectors.outcomes import EVENT_TYPE_DEED_FLIP
from src.core.models import Deed, Property


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_deed(session, property_id, instrument_number, **overrides) -> Deed:
    defaults = dict(
        property_id=property_id,
        county_id="hillsborough",
        instrument_number=instrument_number,
        record_date=date(2026, 1, 1),
        sale_price=Decimal("100000.00"),
        deed_type="Warranty Deed",
        grantor="SELLER",
        grantee="BUYER",
    )
    defaults.update(overrides)
    d = Deed(**defaults)
    session.add(d)
    session.flush()
    return d


# ---------------------------------------------------------------------------
# Unit tests — classify_deed(), no DB
# ---------------------------------------------------------------------------

class TestClassifyDeed:
    def test_certificate_of_title_is_distressed(self):
        assert classify_deed("Certificate of Title") == "distressed"

    def test_tax_deed_is_distressed(self):
        assert classify_deed("(TAXDEED) TAX DEED") == "distressed"

    def test_sheriff_deed_is_distressed(self):
        assert classify_deed("Sheriff's Deed") == "distressed"

    def test_quitclaim_is_excluded(self):
        assert classify_deed("Quit Claim Deed") == "excluded"
        assert classify_deed("QUITCLAIM") == "excluded"

    def test_warranty_deed_is_normal(self):
        assert classify_deed("Warranty Deed") == "normal"

    def test_none_and_empty_are_normal(self):
        assert classify_deed(None) == "normal"
        assert classify_deed("") == "normal"


# ---------------------------------------------------------------------------
# Integration tests — real Postgres (fresh_db)
# ---------------------------------------------------------------------------

class TestStageOutcomesPG:
    def test_ct_to_warranty_flip_within_window(self, fresh_db):
        prop = _mk_property(fresh_db, "DFO-001")
        acq = _mk_deed(
            fresh_db, prop.id, "DFO-001-ACQ",
            deed_type="Certificate of Title", sale_price=Decimal("150000.00"),
            record_date=date(2026, 1, 1),
        )
        _mk_deed(
            fresh_db, prop.id, "DFO-001-RESALE",
            deed_type="Warranty Deed", sale_price=Decimal("220000.00"),
            record_date=date(2026, 9, 1),
        )

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        rows = fresh_db.execute(
            text("SELECT event_type, amount, source_id, event_date FROM outcome_candidates "
                 "WHERE source_type = 'deed_flip_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].event_type == EVENT_TYPE_DEED_FLIP
        assert rows[0].source_id == acq.id
        assert rows[0].amount == Decimal("220000.00")
        assert rows[0].event_date == date(2026, 9, 1)

    def test_quitclaim_resale_emits_nothing(self, fresh_db):
        prop = _mk_property(fresh_db, "DFO-002")
        _mk_deed(
            fresh_db, prop.id, "DFO-002-ACQ",
            deed_type="Tax Deed", sale_price=Decimal("50000.00"),
            record_date=date(2026, 1, 1),
        )
        _mk_deed(
            fresh_db, prop.id, "DFO-002-RESALE",
            deed_type="Quit Claim Deed", sale_price=Decimal("60000.00"),
            record_date=date(2026, 3, 1),
        )

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_resale_beyond_24_months_emits_nothing(self, fresh_db):
        prop = _mk_property(fresh_db, "DFO-003")
        _mk_deed(
            fresh_db, prop.id, "DFO-003-ACQ",
            deed_type="Certificate of Title", sale_price=Decimal("50000.00"),
            record_date=date(2026, 1, 1),
        )
        _mk_deed(
            fresh_db, prop.id, "DFO-003-RESALE",
            deed_type="Warranty Deed", sale_price=Decimal("90000.00"),
            record_date=date(2026, 1, 1) + timedelta(days=800),
        )

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_nominal_price_acquisition_excluded_by_sql_floor(self, fresh_db):
        prop = _mk_property(fresh_db, "DFO-004")
        _mk_deed(
            fresh_db, prop.id, "DFO-004-ACQ",
            deed_type="Certificate of Title", sale_price=Decimal("10.00"),
            record_date=date(2026, 1, 1),
        )
        _mk_deed(
            fresh_db, prop.id, "DFO-004-RESALE",
            deed_type="Warranty Deed", sale_price=Decimal("90000.00"),
            record_date=date(2026, 3, 1),
        )

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "DFO-005")
        _mk_deed(
            fresh_db, prop.id, "DFO-005-ACQ",
            deed_type="Sheriff's Deed", sale_price=Decimal("75000.00"),
            record_date=date(2026, 1, 1),
        )
        _mk_deed(
            fresh_db, prop.id, "DFO-005-RESALE",
            deed_type="Warranty Deed", sale_price=Decimal("110000.00"),
            record_date=date(2026, 4, 1),
        )

        stage_outcomes(fresh_db, "hillsborough")
        stage_outcomes(fresh_db, "hillsborough")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates "
                 "WHERE source_type = 'deed_flip_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
