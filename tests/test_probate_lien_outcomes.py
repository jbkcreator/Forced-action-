"""
Integration tests (fresh_db): real Postgres, seeded legal_proceedings /
legal_and_liens / code_violations + deeds -> staged OutcomeCandidate rows.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text

from src.connectors.outcomes import EVENT_TYPE_LIEN_SALE, EVENT_TYPE_PROBATE_SALE
from src.connectors.probate_lien_outcomes import stage_outcomes
from src.core.models import CodeViolation, Deed, LegalAndLien, LegalProceeding, Property


def _mk_property(session, parcel: str, *, county_id: str = "test-plo") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _mk_deed(session, property_id, instrument_number, **overrides) -> Deed:
    defaults = dict(
        property_id=property_id,
        instrument_number=instrument_number,
        record_date=date(2026, 6, 1),
        sale_price=Decimal("200000.00"),
        deed_type="Warranty Deed",
    )
    defaults.update(overrides)
    d = Deed(**defaults)
    session.add(d)
    session.flush()
    return d


def _mk_probate(session, property_id, case_number, **overrides) -> LegalProceeding:
    defaults = dict(
        property_id=property_id,
        county_id="test-plo",
        record_type="Probate",
        case_number=case_number,
        filing_date=date(2025, 1, 1),
    )
    defaults.update(overrides)
    lp = LegalProceeding(**defaults)
    session.add(lp)
    session.flush()
    return lp


def _mk_lien(session, property_id, instrument_number, **overrides) -> LegalAndLien:
    defaults = dict(
        property_id=property_id,
        county_id="test-plo",
        record_type="Lien",
        document_type="TCL",
        instrument_number=instrument_number,
        filing_date=date(2025, 1, 1),
    )
    defaults.update(overrides)
    lien = LegalAndLien(**defaults)
    session.add(lien)
    session.flush()
    return lien


def _mk_violation(session, property_id, record_number, **overrides) -> CodeViolation:
    defaults = dict(
        property_id=property_id,
        county_id="test-plo",
        record_number=record_number,
        opened_date=date(2025, 1, 1),
        is_lien=True,
    )
    defaults.update(overrides)
    v = CodeViolation(**defaults)
    session.add(v)
    session.flush()
    return v


class TestStageOutcomesPG:
    def test_probate_resolved_by_later_deed(self, fresh_db):
        prop = _mk_property(fresh_db, "PL-001")
        _mk_probate(fresh_db, prop.id, "PROB-CASE-001", filing_date=date(2025, 1, 1))
        _mk_deed(fresh_db, prop.id, "PROB-DEED-001", record_date=date(2026, 3, 1), sale_price=Decimal("150000.00"))

        result = stage_outcomes(fresh_db, "test-plo")
        assert result.errors == 0

        rows = fresh_db.execute(
            text("SELECT event_type, amount, counterparty, raw_payload FROM outcome_candidates "
                 "WHERE source_type = 'probate_lien_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].event_type == EVENT_TYPE_PROBATE_SALE
        assert rows[0].counterparty == "PROB-CASE-001"
        assert Decimal(rows[0].amount) == Decimal("150000.00")

        payload = rows[0].raw_payload
        assert payload["source_ref"] == "probate:PROB-CASE-001:PROB-DEED-001"
        assert payload["case_number_or_instrument"] == "PROB-CASE-001"
        assert payload["filing_date"] == "2025-01-01"
        assert payload["sale_instrument"] == "PROB-DEED-001"
        assert payload["sale_date"] == "2026-03-01"
        assert payload["days_filing_to_sale"] == (date(2026, 3, 1) - date(2025, 1, 1)).days
        assert payload["source_table"] == "legal_proceedings"

    def test_probate_no_later_deed_emits_nothing(self, fresh_db):
        prop = _mk_property(fresh_db, "PL-002")
        _mk_probate(fresh_db, prop.id, "PROB-CASE-002")

        result = stage_outcomes(fresh_db, "test-plo")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_tcl_lien_resolved_by_later_sale(self, fresh_db):
        prop = _mk_property(fresh_db, "PL-003")
        _mk_lien(fresh_db, prop.id, "LIEN-003", document_type="TCL", filing_date=date(2025, 2, 1))
        _mk_deed(fresh_db, prop.id, "PROB-DEED-003", record_date=date(2025, 8, 1))

        result = stage_outcomes(fresh_db, "test-plo")
        assert result.errors == 0

        rows = fresh_db.execute(
            text("SELECT event_type FROM outcome_candidates "
                 "WHERE source_type = 'probate_lien_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].event_type == EVENT_TYPE_LIEN_SALE

    def test_lien_flagged_violation_with_same_property_tcl_dedupes(self, fresh_db):
        prop = _mk_property(fresh_db, "PL-004")
        _mk_lien(fresh_db, prop.id, "LIEN-004", document_type="TCL", filing_date=date(2025, 2, 1))
        _mk_violation(fresh_db, prop.id, "VIOL-004", opened_date=date(2025, 1, 1), is_lien=True)
        _mk_deed(fresh_db, prop.id, "PROB-DEED-004", record_date=date(2025, 8, 1))

        result = stage_outcomes(fresh_db, "test-plo")
        assert result.errors == 0

        rows = fresh_db.execute(
            text("SELECT id FROM outcome_candidates "
                 "WHERE source_type = 'probate_lien_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).fetchall()
        assert len(rows) == 1  # TCL row wins, violation row skipped

    def test_deed_before_filing_date_emits_nothing(self, fresh_db):
        prop = _mk_property(fresh_db, "PL-005")
        _mk_deed(fresh_db, prop.id, "PROB-DEED-005", record_date=date(2024, 1, 1))
        _mk_probate(fresh_db, prop.id, "PROB-CASE-005", filing_date=date(2025, 1, 1))

        result = stage_outcomes(fresh_db, "test-plo")
        assert result.errors == 0

        row = fresh_db.execute(
            text("SELECT id FROM outcome_candidates WHERE property_id = :pid"),
            {"pid": prop.id},
        ).first()
        assert row is None

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "PL-006")
        _mk_probate(fresh_db, prop.id, "PROB-CASE-006")
        _mk_deed(fresh_db, prop.id, "PROB-DEED-006", record_date=date(2026, 3, 1))

        stage_outcomes(fresh_db, "test-plo")
        stage_outcomes(fresh_db, "test-plo")

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM outcome_candidates "
                 "WHERE source_type = 'probate_lien_outcomes' AND property_id = :pid"),
            {"pid": prop.id},
        ).scalar()
        assert count == 1
