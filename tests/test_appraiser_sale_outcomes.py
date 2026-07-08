"""
Integration tests (fresh_db, real Postgres) for the appraiser sale-outcome
connector. The resale test is the direct regression test for the
outcome_candidates unique-constraint widening (source_type, source_table,
source_id, event_date) — without event_date in the key, a second sale on the
same property (same Financial.id, overwritten in place by the appraiser
loader) would silently clobber the first sale's staged outcome.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text

from src.connectors.appraiser_sale_outcomes import stage_outcomes
from src.connectors.outcomes import EVENT_TYPE_QUALIFIED_SALE, EVENT_TYPE_UNQUALIFIED_SALE
from src.core.models import Financial, Property


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_financial(session, property_id, **overrides) -> Financial:
    defaults = dict(property_id=property_id, county_id="hillsborough")
    defaults.update(overrides)
    f = Financial(**defaults)
    session.add(f)
    session.flush()
    return f


class TestStageOutcomes:
    # NOTE: fresh_db runs against the real shared DB (nested transaction that
    # rolls back, but it sees pre-existing committed rows too) — real
    # last_sale_qualified data now exists in 'hillsborough' from live
    # end-to-end testing, so aggregate ConnectorRunResult counts aren't
    # asserted here; only outcome_candidates rows scoped to this test's own
    # property are.

    def test_qualified_sale_staged(self, fresh_db):
        prop = _mk_property(fresh_db, "ASO-001")
        _mk_financial(fresh_db, prop.id, last_sale_qualified=True,
                      last_sale_date=date(2026, 1, 10), last_sale_price=Decimal("350000.00"))

        result = stage_outcomes(fresh_db, "hillsborough")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT event_type, amount FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row.event_type == EVENT_TYPE_QUALIFIED_SALE
        assert row.amount == Decimal("350000.00")

    def test_unqualified_sale_staged(self, fresh_db):
        prop = _mk_property(fresh_db, "ASO-002")
        _mk_financial(fresh_db, prop.id, last_sale_qualified=False,
                      last_sale_date=date(2026, 2, 1), last_sale_price=Decimal("1.00"))

        stage_outcomes(fresh_db, "hillsborough")

        row = fresh_db.execute(
            text("SELECT event_type FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row.event_type == EVENT_TYPE_UNQUALIFIED_SALE

    def test_unpersisted_qualified_flag_is_skipped(self, fresh_db):
        prop = _mk_property(fresh_db, "ASO-003")
        _mk_financial(fresh_db, prop.id, last_sale_qualified=None,
                      last_sale_date=date(2026, 2, 1), last_sale_price=Decimal("500000.00"))

        stage_outcomes(fresh_db, "hillsborough")

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_resale_creates_second_row_not_overwrite(self, fresh_db):
        """
        Regression test for the widened unique constraint. Financial is 1:1
        with Property — a second sale overwrites last_sale_date/price on the
        SAME Financial.id. Without event_date in outcome_candidates' unique
        key, staging the second sale would silently replace the first sale's
        outcome instead of adding a new one.
        """
        prop = _mk_property(fresh_db, "ASO-004")
        fin = _mk_financial(fresh_db, prop.id, last_sale_qualified=True,
                             last_sale_date=date(2020, 1, 1), last_sale_price=Decimal("200000.00"))

        stage_outcomes(fresh_db, "hillsborough")

        # Simulate the appraiser loader overwriting Financial in place on a later resale.
        fresh_db.execute(
            text("UPDATE financials SET last_sale_date = :d, last_sale_price = :p WHERE id = :id"),
            {"d": date(2026, 3, 1), "p": Decimal("450000.00"), "id": fin.id},
        )
        fresh_db.flush()

        stage_outcomes(fresh_db, "hillsborough")

        rows = fresh_db.execute(
            text("SELECT event_date, amount FROM outcome_candidates WHERE property_id = :pid ORDER BY event_date"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 2
        assert rows[0].event_date == date(2020, 1, 1)
        assert rows[0].amount == Decimal("200000.00")
        assert rows[1].event_date == date(2026, 3, 1)
        assert rows[1].amount == Decimal("450000.00")

    def test_rerun_same_sale_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "ASO-005")
        _mk_financial(fresh_db, prop.id, last_sale_qualified=True,
                      last_sale_date=date(2026, 1, 1), last_sale_price=Decimal("100000.00"))

        stage_outcomes(fresh_db, "hillsborough")
        stage_outcomes(fresh_db, "hillsborough")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
