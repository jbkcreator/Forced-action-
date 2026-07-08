"""
Integration tests for src/connectors/resolve.py — require real Postgres
(fresh_db fixture from tests/conftest.py; skips automatically if
DATABASE_URL is not configured).

The pending_review test is the direct regression test for the
check_unmatched_match_method CheckConstraint fix: BaseLoader.find_property_cascade
returns 'normalized_address' (among other cascade-stage values) as match_method,
but the constraint used to only allow ('address','owner_name','legal_desc',
'parcel_id') — every stage-2-through-5 or LLM-verified quarantine attempt was
silently dropped instead of landing in the review queue. This test proves the
widened constraint accepts it.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import text

from src.connectors.resolve import resolve_or_quarantine
from src.core.models import Property, UnmatchedRecord


def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


class TestResolveExactParcelMatch:
    def test_matched_by_parcel_id_returns_property_no_quarantine_row(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE09-PARCEL-001")

        result = resolve_or_quarantine(
            fresh_db, "hillsborough", "dor_sale_outcomes",
            raw_row={"parcel_id": "CDE09-PARCEL-001"},
            parcel_id="CDE09-PARCEL-001",
        )

        assert result.status == "matched"
        assert result.property_id == prop.id
        assert result.match_confidence == 1.0

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM unmatched_records WHERE source_type = :st"),
            {"st": "dor_sale_outcomes"},
        ).scalar()
        assert count == 0


class TestResolveNoMatch:
    def test_no_candidate_at_all_quarantines_as_unmatched(self, fresh_db):
        result = resolve_or_quarantine(
            fresh_db, "hillsborough", "dor_sale_outcomes",
            raw_row={"owner_name": "NO SUCH OWNER XYZ 999"},
            owner_name="NO SUCH OWNER XYZ 999",
        )

        assert result.status == "unmatched"
        assert result.property_id is None

        row = fresh_db.execute(
            text(
                "SELECT match_status, match_method FROM unmatched_records "
                "WHERE source_type = :st ORDER BY id DESC LIMIT 1"
            ),
            {"st": "dor_sale_outcomes"},
        ).first()
        assert row is not None
        assert row.match_status == "unmatched"


class TestResolvePendingReviewRegression:
    def test_stage2_normalized_address_pending_review_lands_in_queue(self, fresh_db):
        """
        Regression test for the check_unmatched_match_method CheckConstraint bug:
        a cascade match_method of 'normalized_address' with a score in the
        pending_review band (hillsborough: [0.75, 0.92)) must successfully write
        to unmatched_records instead of raising a CheckViolation that gets
        silently swallowed.
        """
        candidate = _mk_property(fresh_db, "CDE09-CANDIDATE-002")

        with patch(
            "src.connectors.resolve._GenericMatcher.find_property_cascade",
            return_value=(candidate, "normalized_address", 80),
        ):
            result = resolve_or_quarantine(
                fresh_db, "hillsborough", "dor_sale_outcomes",
                raw_row={"address": "123 SOMEWHAT SIMILAR ST"},
                address="123 SOMEWHAT SIMILAR ST",
            )

        assert result.status == "pending_review"
        assert result.property_id is None
        assert result.match_method == "normalized_address"

        row = fresh_db.execute(
            text(
                "SELECT match_status, match_method, candidate_property_id FROM unmatched_records "
                "WHERE source_type = :st ORDER BY id DESC LIMIT 1"
            ),
            {"st": "dor_sale_outcomes"},
        ).first()
        assert row is not None
        assert row.match_status == "pending_review"
        assert row.match_method == "normalized_address"
        assert row.candidate_property_id == candidate.id
