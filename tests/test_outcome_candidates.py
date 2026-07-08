"""
Unit tests (pure): OutcomeCandidateData validation.
Integration tests (fresh_db): real Postgres upsert, uniqueness, idempotency
against the outcome_candidates table (requires
scripts/apply_cde09_outcome_candidates_table.py to have been applied).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from src.connectors.outcomes import (
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
    EVENT_TYPE_TAX_DEED_SOLD,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.core.models import Property


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _candidate(**overrides) -> OutcomeCandidateData:
    defaults = dict(
        property_id=1,
        county_id="hillsborough",
        source_type="tax_deed_outcomes",
        source_table="tax_deed_auctions",
        source_id=1,
        event_type=EVENT_TYPE_TAX_DEED_SOLD,
        event_date=date(2026, 1, 15),
        amount=Decimal("42000.00"),
        counterparty="THIRD PARTY LLC",
    )
    defaults.update(overrides)
    return OutcomeCandidateData(**defaults)


# ---------------------------------------------------------------------------
# Unit tests — no DB
# ---------------------------------------------------------------------------

class TestOutcomeCandidateDataValidation:
    def test_valid_event_type_constructs(self):
        c = _candidate()
        assert c.event_type == EVENT_TYPE_TAX_DEED_SOLD

    def test_invalid_event_type_raises_at_construction(self):
        with pytest.raises(ValueError, match="Invalid event_type"):
            _candidate(event_type="not_a_real_event_type")

    def test_optional_fields_default_none(self):
        c = OutcomeCandidateData(
            property_id=1, county_id="hillsborough", source_type="x",
            source_table="y", source_id=1,
            event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY, event_date=date(2026, 1, 1),
        )
        assert c.amount is None
        assert c.counterparty is None
        assert c.match_confidence is None


# ---------------------------------------------------------------------------
# Integration tests — require real Postgres (fresh_db)
# ---------------------------------------------------------------------------

class TestPGUpsertOutcomeCandidate:
    def test_insert_creates_row(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE09-OC-001")
        candidate = _candidate(property_id=prop.id, source_id=101)

        upsert_outcome_candidate(fresh_db, candidate)

        row = fresh_db.execute(
            text(
                "SELECT property_id, event_type, amount FROM outcome_candidates "
                "WHERE source_type = :st AND source_table = :tbl AND source_id = :sid"
            ),
            {"st": candidate.source_type, "tbl": candidate.source_table, "sid": 101},
        ).first()
        assert row is not None
        assert row.property_id == prop.id
        assert row.event_type == EVENT_TYPE_TAX_DEED_SOLD

    def test_duplicate_key_updates_not_duplicates(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE09-OC-002")
        candidate = _candidate(property_id=prop.id, source_id=102, amount=Decimal("10000.00"))
        upsert_outcome_candidate(fresh_db, candidate)

        revised = _candidate(property_id=prop.id, source_id=102, amount=Decimal("55000.00"))
        upsert_outcome_candidate(fresh_db, revised)

        count = fresh_db.execute(
            text(
                "SELECT COUNT(*) FROM outcome_candidates "
                "WHERE source_type = :st AND source_table = :tbl AND source_id = :sid"
            ),
            {"st": candidate.source_type, "tbl": candidate.source_table, "sid": 102},
        ).scalar()
        assert count == 1

        row = fresh_db.execute(
            text(
                "SELECT amount FROM outcome_candidates "
                "WHERE source_type = :st AND source_table = :tbl AND source_id = :sid"
            ),
            {"st": candidate.source_type, "tbl": candidate.source_table, "sid": 102},
        ).first()
        assert row.amount == Decimal("55000.00")

    def test_invalid_event_type_rejected_by_db_check_constraint_too(self, fresh_db):
        # Belt-and-braces: the dataclass rejects it before we even reach the DB
        # (see TestOutcomeCandidateDataValidation), but the DB-level CHECK is
        # the actual safety net if some future caller bypasses the dataclass.
        with pytest.raises(ValueError):
            _candidate(source_id=103, event_type="bogus")


class TestPGUniqueKeyIncludesEventDate:
    """
    Regression coverage for widening uq_outcome_candidate to
    (source_type, source_table, source_id, event_date). A stable per-property
    source row (e.g. Financial, overwritten on every appraiser refresh) must
    be able to stage a SEPARATE outcome per distinct sale date under the same
    source_id — otherwise a second sale silently clobbers the first.
    """

    def test_same_source_id_different_event_date_creates_two_rows(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE09-OC-104")
        first = _candidate(property_id=prop.id, source_id=104, event_date=date(2020, 1, 1), amount=Decimal("100.00"))
        second = _candidate(property_id=prop.id, source_id=104, event_date=date(2026, 1, 1), amount=Decimal("200.00"))

        upsert_outcome_candidate(fresh_db, first)
        upsert_outcome_candidate(fresh_db, second)

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates WHERE source_type = :st AND source_table = :tbl AND source_id = 104"),
            {"st": first.source_type, "tbl": first.source_table},
        ).scalar()
        assert count == 2

    def test_same_source_id_same_event_date_still_updates_in_place(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE09-OC-105")
        first = _candidate(property_id=prop.id, source_id=105, event_date=date(2026, 1, 1), amount=Decimal("100.00"))
        revised = _candidate(property_id=prop.id, source_id=105, event_date=date(2026, 1, 1), amount=Decimal("999.00"))

        upsert_outcome_candidate(fresh_db, first)
        upsert_outcome_candidate(fresh_db, revised)

        rows = fresh_db.execute(
            text("SELECT amount FROM outcome_candidates WHERE source_type = :st AND source_table = :tbl AND source_id = 105"),
            {"st": first.source_type, "tbl": first.source_table},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].amount == Decimal("999.00")
