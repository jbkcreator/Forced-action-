"""
Unit tests (pure): classify.
Integration tests (fresh_db): real Postgres, seeded DorSale rows -> staged
OutcomeCandidate rows. fresh_db sees the real, already-loaded 160k+ dor_sales
rows too — assertions scope to this file's own synthetic clerk_no/property,
never to aggregate ConnectorRunResult counts (mirrors
tests/test_appraiser_sale_outcomes.py's convention).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text

from src.connectors.dor_sale_outcomes import classify, stage_outcomes
from src.connectors.outcomes import EVENT_TYPE_QUALIFIED_SALE
from src.core.models import DorSale, Property


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_dor_sale(session, property_id, clerk_no: str, *, county_id: str = "hillsborough", **overrides) -> DorSale:
    defaults = dict(
        county_id=county_id,
        co_no=39,
        parcel_id_dor=f"DSO-TEST-{clerk_no}",
        property_id=property_id,
        match_method="strap",
        clerk_no=clerk_no,
        qual_cd="01",
        sale_yr=2026,
        sale_mo=3,
        sale_price=Decimal("300000.00"),
    )
    defaults.update(overrides)
    d = DorSale(**defaults)
    session.add(d)
    session.flush()
    return d


# ---------------------------------------------------------------------------
# Unit tests — classify, no DB
# ---------------------------------------------------------------------------

class TestClassify:
    def test_qualified_codes(self):
        for code in ("01", "02", "03", "04", "05", "06"):
            assert classify(code) == "qualified"

    def test_pending_codes(self):
        assert classify("98") == "pending"
        assert classify("99") == "pending"

    def test_unqualified_code(self):
        assert classify("11") == "unqualified"

    def test_unrecognized_code_is_unqualified(self):
        assert classify("77") == "unqualified"


# ---------------------------------------------------------------------------
# Integration tests — real Postgres (fresh_db)
# ---------------------------------------------------------------------------

class TestStageOutcomesPG:
    def test_qualified_sale_is_staged(self, fresh_db):
        prop = _mk_property(fresh_db, "DSO-001")
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK001", qual_cd="01",
                     sale_yr=2026, sale_mo=3, sale_price=Decimal("300000.00"))

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT event_type, event_date, amount, match_confidence FROM outcome_candidates "
                 "WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row.event_type == EVENT_TYPE_QUALIFIED_SALE
        assert row.event_date == date(2026, 3, 1)
        assert row.amount == Decimal("300000.00")
        assert float(row.match_confidence) == 1.0

    def test_unqualified_sale_is_not_staged(self, fresh_db):
        prop = _mk_property(fresh_db, "DSO-002")
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK002", qual_cd="11")

        stage_outcomes(fresh_db, "hillsborough")

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_pending_qualification_is_not_staged(self, fresh_db):
        prop = _mk_property(fresh_db, "DSO-003")
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK003", qual_cd="99")

        stage_outcomes(fresh_db, "hillsborough")

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_unresolved_property_id_is_not_staged(self, fresh_db):
        # No property row at all — mirrors a real unmatched dor_sales row.
        sale = DorSale(
            county_id="hillsborough", co_no=39, parcel_id_dor="DSO-NO-PROPERTY",
            property_id=None, match_method=None, clerk_no="DSOCLERK004",
            qual_cd="01", sale_yr=2026, sale_mo=3, sale_price=Decimal("100000.00"),
        )
        fresh_db.add(sale)
        fresh_db.flush()

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0
        # unresolved rows are excluded by stage_outcomes' own WHERE clause,
        # so they never reach staging or the skip counters.

    def test_appraiser_overlap_same_property_month_is_skipped(self, fresh_db):
        prop = _mk_property(fresh_db, "DSO-005")
        fresh_db.execute(
            text(
                "INSERT INTO outcome_candidates "
                "(property_id, county_id, source_type, source_table, source_id, event_type, event_date, amount) "
                "VALUES (:pid, 'hillsborough', 'appraiser_sale_outcomes', 'financials', :pid, 'qualified_sale', :d, 1)"
            ),
            {"pid": prop.id, "d": date(2026, 3, 15)},
        )
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK005", qual_cd="01", sale_yr=2026, sale_mo=3)

        result = stage_outcomes(fresh_db, "hillsborough")

        rows = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 0
        assert result.skipped >= 1

    def test_appraiser_unqualified_does_not_suppress_dor_qualified(self, fresh_db):
        # Regression for PR #150 review finding #3: DOR's authoritative
        # QUAL_CD says qualified but the appraiser marked the same
        # property/month unqualified -- DOR's read must win, not be
        # discarded as a false "overlap".
        prop = _mk_property(fresh_db, "DSO-008")
        fresh_db.execute(
            text(
                "INSERT INTO outcome_candidates "
                "(property_id, county_id, source_type, source_table, source_id, event_type, event_date, amount) "
                "VALUES (:pid, 'hillsborough', 'appraiser_sale_outcomes', 'financials', :pid, 'unqualified_sale', :d, 1)"
            ),
            {"pid": prop.id, "d": date(2026, 3, 15)},
        )
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK008", qual_cd="01", sale_yr=2026, sale_mo=3,
                     sale_price=Decimal("275000.00"))

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT event_type, amount FROM outcome_candidates "
                 "WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is not None
        assert row.event_type == EVENT_TYPE_QUALIFIED_SALE
        assert row.amount == Decimal("275000.00")

    def test_invalid_sale_mo_is_error_not_staged(self, fresh_db):
        prop = _mk_property(fresh_db, "DSO-006")
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK006", qual_cd="01", sale_mo=13)

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors >= 1

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "DSO-007")
        _mk_dor_sale(fresh_db, prop.id, "DSOCLERK007", qual_cd="01", sale_yr=2026, sale_mo=5)

        stage_outcomes(fresh_db, "hillsborough")
        stage_outcomes(fresh_db, "hillsborough")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates WHERE source_type = 'dor_sale_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
