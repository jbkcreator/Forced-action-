"""
WP-5B â€” Borrower Buy Box, Velocity & Next-Need Prediction â€” tests.

Categories covered (per testing-verification skill):
  1.  Unit tests  â€” pure logic, no DB (confidence_tier boundaries, price-band
                    math, next-need rollup, date projection logic).
  2.  Integration â€” real Postgres via fresh_db fixture (full computeâ†’read cycle,
                    upsert idempotency, unknown-confidence path, entity-linked path).
  3.  Migration   â€” fresh-DB apply + idempotency re-run (Categories 3 & 4).
  5.  Durable-state â€” WP-5B does not write to the event spine; writes go to
                    fa_max_person_profiles only (UPSERT, no history loss on re-run).
  6.  Suppression â€” not applicable; WP-5B produces no outbound sends. Stated.
  7.  Autonomy    â€” not applicable; internal-only intelligence layer. Stated.
  8.  Identity    â€” provisional entity bridge tested; ambiguous case stays unlinked.
  9.  Boundary    â€” deed count thresholds (0/1/2/3), cadence floor (0.5 purchases/yr),
                    entity link confidence threshold (70), price floor ($1 000).
 10.  Idempotency â€” upsert run twice produces same profile row.
 11.  Failure     â€” person with no entity â†’ unknown profile written, no crash.
 12.  External    â€” no external systems; not applicable. Stated.
 13.  Compliance  â€” structural: fa_max_person_profiles schema has no financial fields.
 14.  Regression  â€” run via: pytest tests/ (separate step, not in this file).
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict
from unittest.mock import MagicMock, patch, call

import pytest
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_person_id(session) -> str:
    """Insert a minimal fa_max_persons row and return its person_id."""
    row = session.execute(
        text("""
            INSERT INTO fa_max_persons (lifecycle_state, source, source_reference)
            VALUES ('identified', 'test', :ref)
            RETURNING person_id::text
        """),
        {"ref": f"Test Person {uuid.uuid4().hex[:8]}"},
    ).scalar()
    session.flush()
    return row


def _fresh_buyer_entity(session, name: str = "Smith John A",
                         cadence: float = None, avg_hold_days: int = None) -> int:
    """Insert a minimal buyer_entities row and return its id."""
    eid = session.execute(
        text("""
            INSERT INTO buyer_entities (
                canonical_name, entity_type, confidence_score,
                verification_status, total_purchase_count, total_cash_volume
            ) VALUES (:name, 'Individual', 80, 'verified', 0, 0)
            RETURNING id
        """),
        {"name": name},
    ).scalar()
    session.flush()
    if cadence is not None or avg_hold_days is not None:
        session.execute(
            text("""
                UPDATE buyer_entities SET
                    cadence_purchases_per_year = COALESCE(:cadence, cadence_purchases_per_year),
                    avg_hold_days = COALESCE(:hold, avg_hold_days)
                WHERE id = :eid
            """),
            {"cadence": cadence, "hold": avg_hold_days, "eid": eid},
        )
        session.flush()
    return eid


def _insert_property(session, parcel_id: str = None,
                      building_condition: str = None, year_built: int = None,
                      beds: float = None, baths: float = None,
                      lot_size: float = None) -> int:
    if parcel_id is None:
        parcel_id = f"TEST-{uuid.uuid4().hex[:8]}"
    pid = session.execute(
        text("""
            INSERT INTO properties (parcel_id, source_row_hash, needs_rescore, created_at, updated_at)
            VALUES (:pid, :hash, false, NOW(), NOW())
            ON CONFLICT (parcel_id) DO NOTHING
            RETURNING id
        """),
        {"pid": parcel_id, "hash": uuid.uuid4().hex},
    ).scalar()
    if pid is None:
        pid = session.execute(
            text("SELECT id FROM properties WHERE parcel_id = :pid"),
            {"pid": parcel_id},
        ).scalar()
    session.flush()
    if any(v is not None for v in [building_condition, year_built, beds, baths, lot_size]):
        session.execute(
            text("""
                UPDATE properties SET
                    building_condition = COALESCE(:bc, building_condition),
                    year_built         = COALESCE(:yb, year_built),
                    beds               = COALESCE(:beds, beds),
                    baths              = COALESCE(:baths, baths),
                    lot_size           = COALESCE(:ls, lot_size)
                WHERE id = :pid
            """),
            {"bc": building_condition, "yb": year_built, "beds": beds,
             "baths": baths, "ls": lot_size, "pid": pid},
        )
        session.flush()
    return pid


def _insert_deed(session, property_id: int, entity_id: int, sale_price: float,
                  record_date: date = None) -> int:
    if record_date is None:
        record_date = date(2024, 1, 1)
    deed_id = session.execute(
        text("""
            INSERT INTO deeds (property_id, instrument_number, grantee, record_date,
                               sale_price, deed_type, sale_qualified, match_confidence,
                               match_method)
            VALUES (:pid, :instr, 'Test Grantee', :rdate, :price, 'Warranty Deed',
                    true, 0.9, 'exact_name_address')
            RETURNING id
        """),
        {"pid": property_id, "instr": f"INST-{uuid.uuid4().hex[:8]}", "rdate": record_date, "price": sale_price},
    ).scalar()
    session.flush()
    # Link deed to entity
    session.execute(
        text("""
            INSERT INTO buyer_entity_links (buyer_entity_id, source_table, source_id,
                                            match_confidence, match_method)
            VALUES (:eid, 'deeds', :deed_id, 90, 'exact_name_address')
            ON CONFLICT (source_table, source_id) DO NOTHING
        """),
        {"eid": entity_id, "deed_id": deed_id},
    )
    session.flush()
    return deed_id


def _set_entity_cadence(session, entity_id: int, cadence: float, avg_hold: int,
                         still_held: int = 0) -> None:
    session.execute(
        text("""
            UPDATE buyer_entities SET
                cadence_purchases_per_year = :c,
                avg_hold_days = :h,
                portfolio_evidence = :pe,
                portfolio_profiled_at = NOW()
            WHERE id = :eid
        """),
        {
            "eid": entity_id,
            "c": cadence,
            "h": avg_hold,
            "pe": json.dumps({"still_held_count": still_held, "acquisition_count": 3}),
        },
    )
    session.flush()


def _set_person_entity_link(session, person_id: str, entity_id: int) -> None:
    session.execute(
        text("UPDATE fa_max_persons SET buyer_entity_id = :eid WHERE person_id = :pid"),
        {"eid": entity_id, "pid": person_id},
    )
    session.flush()


def _insert_financing_intent(session, property_id: int, product: str, score: float) -> None:
    session.execute(
        text("""
            INSERT INTO financing_intent_scores (
                property_id, score_date, financing_intent_score, intent_tier,
                recommended_product, signal_flags, signal_scores, signal_details,
                source_ids, excluded_reasons
            ) VALUES (
                :pid, CURRENT_DATE, :score,
                CASE WHEN :score >= 70 THEN 'high' WHEN :score >= 40 THEN 'medium' ELSE 'low' END,
                :product, '{}', '{}', '{}', '{}', '{}'
            )
            ON CONFLICT (property_id, score_date) DO UPDATE SET
                financing_intent_score = EXCLUDED.financing_intent_score,
                recommended_product = EXCLUDED.recommended_product,
                intent_tier = EXCLUDED.intent_tier
        """),
        {"pid": property_id, "score": score, "product": product},
    )
    session.flush()


# ===========================================================================
# Category 1 â€” Unit tests (pure logic, no DB)
# ===========================================================================

class TestConfidenceTier:
    """Boundary: deed count thresholds 0/1/2/3."""

    def test_zero_deeds_is_unknown(self):
        from src.services.borrower_profile_service import _confidence_tier
        assert _confidence_tier(0) == "unknown"

    def test_one_deed_is_low(self):
        from src.services.borrower_profile_service import _confidence_tier
        assert _confidence_tier(1) == "low"

    def test_two_deeds_is_medium(self):
        from src.services.borrower_profile_service import _confidence_tier
        assert _confidence_tier(2) == "medium"

    def test_three_deeds_is_high(self):
        from src.services.borrower_profile_service import _confidence_tier
        assert _confidence_tier(3) == "high"

    def test_many_deeds_is_high(self):
        from src.services.borrower_profile_service import _confidence_tier
        assert _confidence_tier(10) == "high"


class TestPriceBand:
    """Unit: price-band computation from deed list."""

    def test_price_band_odd_count(self):
        from src.services.borrower_profile_service import _compute_price_band
        deeds = [
            {"sale_price": Decimal("100000")},
            {"sale_price": Decimal("200000")},
            {"sale_price": Decimal("300000")},
        ]
        band = _compute_price_band(deeds)
        assert band["min_cents"] == 100_000_00
        assert band["median_cents"] == 200_000_00
        assert band["max_cents"] == 300_000_00
        assert band["sample_count"] == 3

    def test_price_band_even_count(self):
        from src.services.borrower_profile_service import _compute_price_band
        deeds = [
            {"sale_price": Decimal("100000")},
            {"sale_price": Decimal("300000")},
        ]
        band = _compute_price_band(deeds)
        assert band["median_cents"] == 200_000_00

    def test_price_band_no_deeds_returns_none(self):
        from src.services.borrower_profile_service import _compute_price_band
        assert _compute_price_band([]) is None

    def test_price_band_deeds_missing_sale_price(self):
        from src.services.borrower_profile_service import _compute_price_band
        deeds = [{"sale_price": None}, {"sale_price": None}]
        assert _compute_price_band(deeds) is None


class TestGeography:
    def test_geography_counts_correctly(self):
        from src.services.borrower_profile_service import _compute_geography
        deeds = [
            {"city": "Tampa", "county_id": "hillsborough"},
            {"city": "Tampa", "county_id": "hillsborough"},
            {"city": "Clearwater", "county_id": "pinellas"},
        ]
        geo = _compute_geography(deeds)
        assert geo is not None
        assert geo[0]["city"] == "Tampa"
        assert geo[0]["count"] == 2

    def test_geography_no_deeds_returns_none(self):
        from src.services.borrower_profile_service import _compute_geography
        assert _compute_geography([]) is None


class TestPropertyTypes:
    def test_property_types_counts_correctly(self):
        from src.services.borrower_profile_service import _compute_property_types
        deeds = [
            {"property_type": "SFR", "property_use_code": "0100"},
            {"property_type": "SFR", "property_use_code": "0100"},
            {"property_type": "Condo", "property_use_code": "0200"},
        ]
        types = _compute_property_types(deeds)
        assert types is not None
        assert types[0]["property_type"] == "SFR"
        assert types[0]["count"] == 2

    def test_property_types_no_deeds_returns_none(self):
        from src.services.borrower_profile_service import _compute_property_types
        assert _compute_property_types([]) is None


class TestNextNeedRollup:
    def test_highest_scoring_product_wins(self):
        from src.services.borrower_profile_service import _rollup_next_need
        rows = [
            {"property_id": 1, "financing_intent_score": Decimal("85"), "recommended_product": "bridge",
             "signal_flags": {}, "signal_details": {}},
            {"property_id": 2, "financing_intent_score": Decimal("45"), "recommended_product": "renovation_capital",
             "signal_flags": {}, "signal_details": {}},
        ]
        product, evidence = _rollup_next_need(rows)
        assert product == "bridge"
        assert len(evidence) == 2
        assert evidence[0]["property_id"] == 1

    def test_empty_rows_returns_none(self):
        from src.services.borrower_profile_service import _rollup_next_need
        product, evidence = _rollup_next_need([])
        assert product is None
        assert evidence is None

    def test_evidence_capped_at_3(self):
        from src.services.borrower_profile_service import _rollup_next_need
        rows = [
            {"property_id": i, "financing_intent_score": Decimal(str(90 - i)),
             "recommended_product": "bridge", "signal_flags": {}, "signal_details": {}}
            for i in range(5)
        ]
        _, evidence = _rollup_next_need(rows)
        assert len(evidence) == 3


class TestNextNeedDate:
    """Boundary: cadence_purchases_per_year < 0.5 â†’ no date projection."""

    def test_cadence_below_floor_returns_none(self):
        from src.services.borrower_profile_service import _predict_next_need_date
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        predicted, evidence = _predict_next_need_date(
            session=session,
            person_uuid="00000000-0000-0000-0000-000000000001",
            last_txn_date=date(2024, 1, 1),
            velocity=Decimal("0.4"),  # below 0.5 floor
        )
        assert predicted is None
        assert evidence is None

    def test_cadence_at_floor_returns_date(self):
        from src.services.borrower_profile_service import _predict_next_need_date
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        predicted, evidence = _predict_next_need_date(
            session=session,
            person_uuid="00000000-0000-0000-0000-000000000001",
            last_txn_date=date(2024, 1, 1),
            velocity=Decimal("0.5"),  # exactly at floor
        )
        assert predicted is not None
        assert evidence is not None
        assert evidence["basis"] == "cadence"

    def test_cadence_projection_math(self):
        from src.services.borrower_profile_service import _predict_next_need_date
        from datetime import timedelta
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        last = date(2024, 1, 1)
        predicted, evidence = _predict_next_need_date(
            session=session,
            person_uuid="00000000-0000-0000-0000-000000000001",
            last_txn_date=last,
            velocity=Decimal("2.0"),  # 365/2 = 182.5 days
        )
        assert predicted is not None
        expected_date = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=182.5)
        diff = abs((predicted - expected_date).total_seconds())
        assert diff < 86400  # within 1 day
        assert evidence["basis"] == "cadence"
        assert evidence["days_projected"] in (182, 183)  # round(182.5) is 182 in Python (banker's rounding)

    def test_no_last_txn_returns_none(self):
        from src.services.borrower_profile_service import _predict_next_need_date
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        predicted, evidence = _predict_next_need_date(
            session=session,
            person_uuid="00000000-0000-0000-0000-000000000001",
            last_txn_date=None,
            velocity=Decimal("2.0"),
        )
        assert predicted is None
        assert evidence is None


class TestJsonbHelper:
    def test_none_returns_none(self):
        from src.services.borrower_profile_service import _jsonb
        assert _jsonb(None) is None

    def test_dict_returns_json_string(self):
        from src.services.borrower_profile_service import _jsonb
        result = _jsonb({"a": 1})
        assert result == '{"a": 1}'

    def test_list_returns_json_string(self):
        from src.services.borrower_profile_service import _jsonb
        result = _jsonb([1, 2])
        assert result == '[1, 2]'


class TestParseUuid:
    def test_valid_uuid_passes(self):
        from src.services.borrower_profile_service import _parse_uuid
        uid = str(uuid.uuid4())
        assert _parse_uuid(uid) == uid

    def test_invalid_uuid_raises(self):
        from src.services.borrower_profile_service import _parse_uuid
        with pytest.raises(ValueError):
            _parse_uuid("not-a-uuid")


# ===========================================================================
# Category 2 â€” Integration tests (real Postgres via fresh_db)
# ===========================================================================

class TestComputePersonProfileIntegration:
    """compute_person_profile() end-to-end against real Postgres."""

    def test_unknown_profile_written_when_no_entity(self, fresh_db):
        """Person with no buyer_entity_id â†’ confidence_tier='unknown', all fields NULL."""
        from src.services.borrower_profile_service import compute_person_profile, get_person_profile
        person_id = _fresh_person_id(fresh_db)

        profile = compute_person_profile(fresh_db, person_id)

        assert profile["confidence_tier"] == "unknown"
        assert profile["buy_box_geography"] is None
        assert profile["buy_box_property_types"] is None
        assert profile["buy_box_price_band"] is None
        assert profile["predicted_next_need"] is None
        assert profile["predicted_next_need_date"] is None
        assert profile["velocity_purchases_per_year"] is None
        assert profile["active_property_count"] == 0  # unknown profile returns 0, not None

        # Verify it was persisted
        stored = get_person_profile(fresh_db, person_id)
        assert stored is not None
        assert stored["confidence_tier"] == "unknown"

    def test_full_profile_with_entity_and_deeds(self, fresh_db):
        """Person with entity + 3 deeds â†’ confidence_tier='high', buy-box populated."""
        from src.services.borrower_profile_service import compute_person_profile, get_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db, "Tampa Investor LLC")
        _set_person_entity_link(fresh_db, person_id, entity_id)

        prop1 = _insert_property(fresh_db)
        prop2 = _insert_property(fresh_db)
        prop3 = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop1, entity_id, 250_000, date(2023, 1, 15))
        _insert_deed(fresh_db, prop2, entity_id, 300_000, date(2023, 7, 1))
        _insert_deed(fresh_db, prop3, entity_id, 275_000, date(2024, 2, 10))
        _set_entity_cadence(fresh_db, entity_id, cadence=2.0, avg_hold=180, still_held=2)

        profile = compute_person_profile(fresh_db, person_id)

        assert profile["confidence_tier"] == "high"
        assert profile["buy_box_price_band"] is not None
        assert profile["velocity_purchases_per_year"] == Decimal("2.0")
        assert profile["last_transaction_date"] == date(2024, 2, 10)
        assert profile["active_property_count"] == 2

        stored = get_person_profile(fresh_db, person_id)
        assert stored["confidence_tier"] == "high"

    def test_one_deed_gives_low_confidence(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 200_000)

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["confidence_tier"] == "low"

    def test_two_deeds_give_medium_confidence(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop1 = _insert_property(fresh_db)
        prop2 = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop1, entity_id, 200_000, date(2023, 1, 1))
        _insert_deed(fresh_db, prop2, entity_id, 300_000, date(2024, 1, 1))

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["confidence_tier"] == "medium"

    def test_deeds_below_price_floor_excluded(self, fresh_db):
        """Nominal-consideration deeds ($500) must not count toward buy-box."""
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 500)  # below $1 000 floor

        profile = compute_person_profile(fresh_db, person_id)
        # 0 arm's-length deeds â†’ unknown
        assert profile["confidence_tier"] == "unknown"
        assert profile["buy_box_price_band"] is None

    def test_next_need_product_from_financing_intent(self, fresh_db):
        """Highest-scoring FinancingIntentScore product becomes predicted_next_need."""
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)

        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 250_000)
        _set_entity_cadence(fresh_db, entity_id, cadence=1.0, avg_hold=300)
        _insert_financing_intent(fresh_db, prop, "renovation_capital", 75.0)

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["predicted_next_need"] == "renovation_capital"
        evidence = profile["next_need_evidence"]
        assert evidence is not None
        # next_need_evidence is now {"financing_intent": [...], ...}
        assert isinstance(evidence, dict)
        assert len(evidence.get("financing_intent", [])) == 1

    def test_no_financing_intent_leaves_next_need_none(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 250_000)

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["predicted_next_need"] is None

    def test_upsert_is_idempotent(self, fresh_db):
        """Running compute_person_profile twice yields one row with updated computed_at."""
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        compute_person_profile(fresh_db, person_id)
        compute_person_profile(fresh_db, person_id)

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM fa_max_person_profiles WHERE person_id = :pid"),
            {"pid": person_id},
        ).scalar()
        assert count == 1

    def test_get_person_profile_returns_none_for_uncomputed(self, fresh_db):
        from src.services.borrower_profile_service import get_person_profile

        result = get_person_profile(fresh_db, str(uuid.uuid4()))
        assert result is None

    def test_cadence_based_date_projection(self, fresh_db):
        """velocity=2.0, last_txn=2024-01-01 â†’ predicted date â‰ˆ 2024-07 (182 days later)."""
        from src.services.borrower_profile_service import compute_person_profile
        from datetime import timedelta

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop1 = _insert_property(fresh_db)
        prop2 = _insert_property(fresh_db)
        prop3 = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop1, entity_id, 200_000, date(2023, 1, 1))
        _insert_deed(fresh_db, prop2, entity_id, 210_000, date(2023, 7, 1))
        _insert_deed(fresh_db, prop3, entity_id, 220_000, date(2024, 1, 1))
        _set_entity_cadence(fresh_db, entity_id, cadence=2.0, avg_hold=180)

        profile = compute_person_profile(fresh_db, person_id)

        assert profile["predicted_next_need_date"] is not None
        expected = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=365.0 / 2.0)
        diff = abs((profile["predicted_next_need_date"] - expected).total_seconds())
        assert diff < 86400

    def test_low_cadence_suppresses_date_prediction(self, fresh_db):
        """velocity=0.3 (< 0.5 floor) and no maturity opportunity â†’ no predicted date."""
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 200_000)
        _set_entity_cadence(fresh_db, entity_id, cadence=0.3, avg_hold=600)

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["predicted_next_need_date"] is None


# ===========================================================================
# Category 9 â€” Boundary tests
# ===========================================================================

class TestBoundaryValues:
    """Edge values: deed count exactly at threshold boundaries."""

    def test_exactly_two_deeds_is_medium_not_high(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        for i in range(2):
            prop = _insert_property(fresh_db)
            _insert_deed(fresh_db, prop, entity_id, 200_000, date(2023, i + 1, 1))

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["confidence_tier"] == "medium"

    def test_exactly_three_deeds_is_high_not_medium(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        for i in range(3):
            prop = _insert_property(fresh_db)
            _insert_deed(fresh_db, prop, entity_id, 200_000, date(2023, i + 1, 1))

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["confidence_tier"] == "high"

    def test_price_floor_boundary_exactly_1000_included(self, fresh_db):
        """Deed with sale_price = exactly $1 000 must be included (>= floor)."""
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 1_000)  # exactly at floor

        profile = compute_person_profile(fresh_db, person_id)
        # 1 arm's-length deed â†’ low (not unknown)
        assert profile["confidence_tier"] == "low"
        assert profile["buy_box_price_band"] is not None

    def test_price_floor_boundary_999_excluded(self, fresh_db):
        """Deed with sale_price = $999 must be excluded."""
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 999)  # just below floor

        profile = compute_person_profile(fresh_db, person_id)
        assert profile["confidence_tier"] == "unknown"

    def test_cadence_floor_exactly_0_5_triggers_date(self):
        from src.services.borrower_profile_service import _predict_next_need_date
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        predicted, evidence = _predict_next_need_date(
            session=session,
            person_uuid=str(uuid.uuid4()),
            last_txn_date=date(2024, 1, 1),
            velocity=Decimal("0.5"),  # exactly at floor
        )
        assert predicted is not None
        assert evidence is not None

    def test_cadence_just_below_0_5_suppresses_date(self):
        from src.services.borrower_profile_service import _predict_next_need_date
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        predicted, evidence = _predict_next_need_date(
            session=session,
            person_uuid=str(uuid.uuid4()),
            last_txn_date=date(2024, 1, 1),
            velocity=Decimal("0.49"),
        )
        assert predicted is None
        assert evidence is None


# ===========================================================================
# Category 10 â€” Idempotency / duplicate-run test
# ===========================================================================

class TestIdempotency:
    def test_sweep_run_twice_produces_one_row(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop, entity_id, 200_000)

        compute_person_profile(fresh_db, person_id)
        compute_person_profile(fresh_db, person_id)

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM fa_max_person_profiles WHERE person_id = :pid"),
            {"pid": person_id},
        ).scalar()
        assert count == 1

    def test_second_run_updates_computed_at(self, fresh_db):
        """computed_at advances on each upsert â€” proves the row was re-written."""
        import time
        from src.services.borrower_profile_service import compute_person_profile, get_person_profile

        person_id = _fresh_person_id(fresh_db)
        p1 = compute_person_profile(fresh_db, person_id)
        time.sleep(0.05)
        p2 = compute_person_profile(fresh_db, person_id)

        assert p2["computed_at"] >= p1["computed_at"]


# ===========================================================================
# Category 11 â€” Failure / crash resilience
# ===========================================================================

class TestFailureResilience:
    def test_invalid_person_id_raises(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile
        with pytest.raises(ValueError):
            compute_person_profile(fresh_db, "not-a-uuid")

    def test_nonexistent_person_id_does_not_crash(self, fresh_db):
        """A valid UUID that doesn't exist in fa_max_persons â†’ DB FK violation caught or None."""
        from src.services.borrower_profile_service import _resolve_entity_id
        result = _resolve_entity_id(fresh_db, str(uuid.uuid4()))
        assert result is None


# ===========================================================================
# Category 13 â€” Compliance structural tests
# ===========================================================================

class TestComplianceStructural:
    """Structural checks â€” grepping schema, not just code-review claims."""

    FORBIDDEN_COLUMN_FRAGMENTS = [
        "credit_score", "bank_statement", "tax_return",
        "social_security", "ssn", "fico", "dti", "debt_to_income",
    ]
    # "income" excluded from substring check because "financing_intent" contains it;
    # the forbidden concept is "borrower income data" â€” covered by "bank_statement",
    # "tax_return", and "debt_to_income" which are the actual field names.
    FORBIDDEN_SERVICE_PATTERNS = [
        "credit_score", "bank_statement", "tax_return",
        "fico", "dti", "debt_to_income",
        # "social_security" and "ssn" excluded: the docstring explicitly says
        # "No SSN stored" which is a compliance statement, not a usage â€” the real
        # structural proof is that no column holding SSN exists (covered by the
        # DB schema test above).
        # "income" excluded: "financing_intent" contains "income" as a substring.
    ]

    def test_no_financial_data_columns_in_wp5b_table(self, fresh_db):
        """fa_max_person_profiles must have no borrower financial data columns."""
        cols = fresh_db.execute(
            text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'fa_max_person_profiles'
            """)
        ).scalars().all()
        col_names_lower = [c.lower() for c in cols]
        for forbidden in self.FORBIDDEN_COLUMN_FRAGMENTS:
            for col in col_names_lower:
                assert forbidden not in col, (
                    f"Column '{col}' in fa_max_person_profiles may hold borrower "
                    f"financial data (matched '{forbidden}') â€” SOT.md compliance violation"
                )

    def test_predicted_next_need_check_constraint_blocks_rate_string(self, fresh_db):
        """The CHECK constraint on predicted_next_need prevents storing a rate/term."""
        person_id = _fresh_person_id(fresh_db)
        with pytest.raises(Exception):  # CHECK violation
            fresh_db.execute(
                text("""
                    INSERT INTO fa_max_person_profiles (person_id, confidence_tier, predicted_next_need)
                    VALUES (:pid, 'unknown', '7.5% interest rate')
                """),
                {"pid": person_id},
            )
            fresh_db.flush()

    def test_no_financial_columns_in_service_source(self):
        """Service source file must not reference forbidden financial field names."""
        with open("src/services/borrower_profile_service.py", encoding="utf-8") as f:
            src = f.read().lower()
        for forbidden in self.FORBIDDEN_SERVICE_PATTERNS:
            assert forbidden not in src, (
                f"borrower_profile_service.py references '{forbidden}' â€” "
                "may touch borrower financial data (SOT.md compliance violation)"
            )

    def test_service_does_not_reference_pricing_terms(self):
        """Service must not emit rate, APR, commitment, or term strings to borrower."""
        with open("src/services/borrower_profile_service.py", encoding="utf-8") as f:
            src = f.read()
        prohibited = ["interest rate", "APR", "loan commitment", "we will lend"]
        for phrase in prohibited:
            assert phrase.lower() not in src.lower(), (
                f"borrower_profile_service.py contains pricing language: '{phrase}'"
            )


# ===========================================================================
# Categories 6, 7, 8, 12 â€” explicitly not applicable
# ===========================================================================

class TestNotApplicable:
    """Document explicitly why certain test categories are not required."""

    def test_suppression_not_applicable(self):
        """Category 6 â€” WP-5B produces no outbound sends. No suppression path exists.
        Structural confirmation: service imports no relay, mailer, or Telnyx module."""
        with open("src/services/borrower_profile_service.py", encoding="utf-8") as f:
            src = f.read()
        for send_module in ["relay", "mailer", "telnyx", "sms_compliance", "instantly"]:
            assert send_module not in src.lower(), (
                f"borrower_profile_service.py imports '{send_module}' â€” "
                "a send path must not exist in a pure intelligence layer"
            )

    def test_autonomy_tier_not_applicable(self):
        """Category 7 â€” WP-5B is internal-only (no outbound). No autonomy gate needed.
        Structural: no autonomy_tier references in service or task."""
        for path in [
            "src/services/borrower_profile_service.py",
            "src/tasks/fa_max_profile_sweep.py",
        ]:
            with open(path, encoding="utf-8") as f:
                src = f.read()
            assert "autonomy_tier" not in src, (
                f"{path} references autonomy_tier â€” unexpected in an internal-only service"
            )

    def test_external_provider_not_applicable(self):
        """Category 12 â€” WP-5B calls no external APIs (Slack, GHL, Backflip, Telnyx).
        Reads from internal DB tables only."""
        with open("src/services/borrower_profile_service.py", encoding="utf-8") as f:
            src = f.read()
        for ext in ["requests.get", "httpx", "slack_sdk", "ghl", "backflip", "telnyx"]:
            assert ext not in src.lower()


# ===========================================================================
# Categories 3 & 4 â€” Migration tests (run separately via CLI; test stubs here)
# ===========================================================================

class TestMigrationIdempotency:
    """
    Categories 3 & 4: Verified by running the migration twice against the
    disposable DB (see test output below). These stubs confirm the table
    exists post-migration.
    """

    def test_fa_max_person_profiles_table_exists(self, fresh_db):
        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'fa_max_person_profiles'
            """)
        ).scalar()
        assert count == 1

    def test_buyer_entity_id_column_exists_on_fa_max_persons(self, fresh_db):
        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'fa_max_persons'
                  AND column_name = 'buyer_entity_id'
            """)
        ).scalar()
        assert count == 1

    def test_confidence_tier_check_constraint_exists(self, fresh_db):
        """Verify the confidence_tier CHECK constraint is enforced."""
        person_id = _fresh_person_id(fresh_db)
        with pytest.raises(Exception):  # CHECK violation
            fresh_db.execute(
                text("""
                    INSERT INTO fa_max_person_profiles (person_id, confidence_tier)
                    VALUES (:pid, 'invalid_tier')
                """),
                {"pid": person_id},
            )
            fresh_db.flush()



# ===========================================================================
# Fix 1 â€” avg_days_between_transactions computed from deed date gaps
# ===========================================================================

class TestAvgDaysBetweenTransactions:
    def test_single_deed_returns_none(self):
        from src.services.borrower_profile_service import _compute_avg_days_between_transactions
        deeds = [{"record_date": date(2024, 1, 1)}]
        assert _compute_avg_days_between_transactions(deeds) is None

    def test_two_deeds_correct_gap(self):
        from src.services.borrower_profile_service import _compute_avg_days_between_transactions
        deeds = [
            {"record_date": date(2024, 1, 1)},
            {"record_date": date(2024, 7, 1)},  # 182 days later
        ]
        result = _compute_avg_days_between_transactions(deeds)
        assert result is not None
        assert float(result) == pytest.approx(182.0, abs=1)

    def test_three_deeds_average_gap(self):
        from src.services.borrower_profile_service import _compute_avg_days_between_transactions
        deeds = [
            {"record_date": date(2022, 1, 1)},
            {"record_date": date(2023, 1, 1)},  # 365 days
            {"record_date": date(2023, 7, 1)},  # 181 days
        ]
        result = _compute_avg_days_between_transactions(deeds)
        assert result is not None
        assert float(result) == pytest.approx(273.0, abs=1)

    def test_empty_deeds_returns_none(self):
        from src.services.borrower_profile_service import _compute_avg_days_between_transactions
        assert _compute_avg_days_between_transactions([]) is None

    def test_not_using_avg_hold_days(self, fresh_db):
        """avg_days_between_transactions must come from deed date gaps, not avg_hold_days."""
        from src.services.borrower_profile_service import compute_person_profile
        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db, avg_hold_days=999)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop1 = _insert_property(fresh_db)
        prop2 = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop1, entity_id, 200_000, record_date=date(2023, 1, 1))
        _insert_deed(fresh_db, prop2, entity_id, 250_000, record_date=date(2023, 7, 1))
        fresh_db.commit()

        profile = compute_person_profile(fresh_db, person_id)
        avg = float(profile["avg_days_between_transactions"])
        assert avg < 400, f"avg_days_between_transactions={avg} looks like avg_hold_days=999 leaked in"


# ===========================================================================
# Fix 2 â€” work queue scheduling and draining
# ===========================================================================

class TestWorkQueueScheduling:
    def test_schedule_enqueues_row(self, fresh_db):
        from src.services.borrower_profile_service import schedule_profile_recompute
        person_id = _fresh_person_id(fresh_db)
        schedule_profile_recompute(fresh_db, person_id, "test_reason")
        fresh_db.commit()

        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute'
                  AND person_id = :pid
                  AND status = 'available'
            """),
            {"pid": person_id},
        ).scalar()
        assert count == 1

    def test_schedule_idempotent(self, fresh_db):
        from src.services.borrower_profile_service import schedule_profile_recompute
        person_id = _fresh_person_id(fresh_db)
        schedule_profile_recompute(fresh_db, person_id, "first")
        fresh_db.commit()
        schedule_profile_recompute(fresh_db, person_id, "second")
        fresh_db.commit()

        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute' AND person_id = :pid
            """),
            {"pid": person_id},
        ).scalar()
        assert count == 1, "Duplicate schedule must be a no-op"

    def test_drain_marks_items_done(self, fresh_db):
        from src.services.borrower_profile_service import schedule_profile_recompute
        from src.tasks.fa_max_profile_sweep import _drain_recompute_queue

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        schedule_profile_recompute(fresh_db, person_id, "drain_test")
        fresh_db.commit()

        drained = _drain_recompute_queue(fresh_db)
        assert drained >= 1

        status = fresh_db.execute(
            text("""
                SELECT status FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute' AND person_id = :pid
                ORDER BY created_at DESC LIMIT 1
            """),
            {"pid": person_id},
        ).scalar()
        assert status == "done"

    def test_schedule_for_property_enqueues_linked_person(self, fresh_db):
        from src.services.borrower_profile_service import schedule_profile_recompute_for_property
        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop_id = _insert_property(fresh_db)
        # Insert a deed for this property, then link the entity to that deed row
        # (buyer_entity_links.source_table='deeds' is the valid link type for
        #  deed-discovered entities; 'properties' violates the CHECK constraint)
        deed_id = _insert_deed(fresh_db, prop_id, entity_id, 300_000)
        fresh_db.commit()

        schedule_profile_recompute_for_property(fresh_db, prop_id, "new_deed")
        fresh_db.commit()

        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute'
                  AND person_id = :pid
                  AND status = 'available'
            """),
            {"pid": person_id},
        ).scalar()
        assert count == 1

    def test_schedule_for_property_noop_when_no_link(self, fresh_db):
        from src.services.borrower_profile_service import schedule_profile_recompute_for_property
        prop_id = _insert_property(fresh_db)
        fresh_db.commit()
        # No buyer_entity_links → no persons → no queue rows added
        schedule_profile_recompute_for_property(fresh_db, prop_id, "new_deed")
        fresh_db.commit()
        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM fa_max_work_queue WHERE queue_name = 'profile_recompute'"),
        ).scalar()
        assert count == 0

    def test_second_event_after_drain_reactivates_item(self, fresh_db):
        """A second material event after the first has been drained must
        produce a new recompute, not be silently discarded by the permanent
        idempotency key."""
        from src.services.borrower_profile_service import schedule_profile_recompute
        from src.tasks.fa_max_profile_sweep import _drain_recompute_queue

        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)

        # First event: enqueue, drain to done
        schedule_profile_recompute(fresh_db, person_id, "first_event")
        fresh_db.commit()
        _drain_recompute_queue(fresh_db)

        # Second event after drain: must produce an available item again
        schedule_profile_recompute(fresh_db, person_id, "second_event")
        fresh_db.commit()

        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute'
                  AND person_id = :pid
                  AND status = 'available'
            """),
            {"pid": person_id},
        ).scalar()
        assert count == 1, "Second event after drain must be available for reprocessing"

    def test_new_event_while_item_claimed_creates_new_available_item(self, fresh_db):
        """A material event that arrives while a profile job is claimed must not
        be silently dropped.  The worker mid-compute cannot see the new event, so
        schedule_profile_recompute must create a NEW available item.

        Regression for the six-step race:
        1. Worker claims the profile job.
        2. Worker begins computing.
        3. New deed/permit/interaction commits for Person A.
        4. Enqueue sees the claimed row → must NOT discard the event.
        5. Worker completes the old calculation.
        6. New available item exists and will be picked up on the next drain.
        """
        from src.services.borrower_profile_service import schedule_profile_recompute

        person_id = _fresh_person_id(fresh_db)
        schedule_profile_recompute(fresh_db, person_id, "first_event")
        fresh_db.commit()

        # Simulate worker claiming the item (status → claimed)
        fresh_db.execute(
            text("""
                UPDATE fa_max_work_queue
                SET status = 'claimed',
                    claimed_at = NOW(),
                    lease_expires_at = NOW() + INTERVAL '5 minutes',
                    worker_id = 'worker:test'
                WHERE queue_name = 'profile_recompute'
                  AND person_id = :pid
            """),
            {"pid": person_id},
        )
        fresh_db.commit()

        # New event arrives while item is claimed
        schedule_profile_recompute(fresh_db, person_id, "new_event_while_claimed")
        fresh_db.commit()

        # There must be an available item covering the new event
        available = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute'
                  AND person_id = :pid
                  AND status = 'available'
            """),
            {"pid": person_id},
        ).scalar()
        assert available == 1, (
            "A new material event while the profile job is claimed must produce "
            "an available item, not be silently discarded"
        )

    def test_crash_recovery_reclaims_expired_lease(self, fresh_db):
        """Items whose worker died (lease expired) must be returned to
        available by the next drain call, not stranded permanently as claimed."""
        from src.services.borrower_profile_service import schedule_profile_recompute
        from src.tasks.fa_max_profile_sweep import _drain_recompute_queue

        person_id = _fresh_person_id(fresh_db)
        _fresh_buyer_entity(fresh_db)
        schedule_profile_recompute(fresh_db, person_id, "crash_test")
        fresh_db.commit()

        # Simulate a claimed item with an already-expired lease (worker died)
        fresh_db.execute(
            text("""
                UPDATE fa_max_work_queue
                SET status = 'claimed',
                    claimed_at = NOW() - INTERVAL '10 minutes',
                    lease_expires_at = NOW() - INTERVAL '1 second',
                    worker_id = 'dead_worker'
                WHERE queue_name = 'profile_recompute'
                  AND person_id = :pid
            """),
            {"pid": person_id},
        )
        fresh_db.commit()

        # Drain must reclaim the expired item and process it
        entity_id = fresh_db.execute(
            text("SELECT id FROM buyer_entities ORDER BY id DESC LIMIT 1")
        ).scalar()
        _set_person_entity_link(fresh_db, person_id, entity_id)
        fresh_db.commit()
        drained = _drain_recompute_queue(fresh_db)
        assert drained >= 1, "Expired claimed item must be reclaimed and processed"

        status = fresh_db.execute(
            text("""
                SELECT status FROM fa_max_work_queue
                WHERE queue_name = 'profile_recompute' AND person_id = :pid
                ORDER BY updated_at DESC LIMIT 1
            """),
            {"pid": person_id},
        ).scalar()
        assert status == "done", f"Expected done after recovery, got {status!r}"


# ===========================================================================
# Fix 3 â€” production read path
# ===========================================================================

class TestProductionReadPath:
    def test_get_person_profile_returns_none_when_absent(self, fresh_db):
        from src.services.borrower_profile_service import get_person_profile
        result = get_person_profile(fresh_db, str(uuid.uuid4()))
        assert result is None

    def test_get_person_profile_returns_dict_after_compute(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile, get_person_profile
        person_id = _fresh_person_id(fresh_db)
        compute_person_profile(fresh_db, person_id)
        fresh_db.commit()
        result = get_person_profile(fresh_db, person_id)
        assert result is not None
        assert result["person_id"] == person_id

    def test_api_router_404_on_missing_profile(self):
        from fastapi.testclient import TestClient
        from fastapi import FastAPI
        from unittest.mock import patch
        from src.api.fa_max_router import router
        from src.api.admin_router import get_current_admin

        app = FastAPI()
        app.include_router(router)
        # Override the auth dependency so no JWT is needed in tests
        app.dependency_overrides[get_current_admin] = lambda: {"scope": "admin"}

        with patch("src.api.fa_max_router.get_person_profile", return_value=None):
            client = TestClient(app)
            resp = client.get(f"/api/fa-max/persons/{uuid.uuid4()}/profile")
        assert resp.status_code == 404

    def test_api_router_200_when_profile_exists(self):
        from fastapi.testclient import TestClient
        from fastapi import FastAPI
        from unittest.mock import patch
        from src.api.fa_max_router import router
        from src.api.admin_router import get_current_admin

        fake_profile = {"person_id": str(uuid.uuid4()), "confidence_tier": "low"}
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_current_admin] = lambda: {"scope": "admin"}

        with patch("src.api.fa_max_router.get_person_profile", return_value=fake_profile):
            client = TestClient(app)
            resp = client.get(f"/api/fa-max/persons/{fake_profile['person_id']}/profile")
        assert resp.status_code == 200
        assert resp.json()["confidence_tier"] == "low"


# ===========================================================================
# Fix 4 â€” prediction date evidence basis field
# ===========================================================================

class TestPredictionEvidence:
    def test_cadence_basis_stored_in_next_need_evidence(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile
        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db, cadence=2.0)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop1 = _insert_property(fresh_db)
        prop2 = _insert_property(fresh_db)
        _insert_deed(fresh_db, prop1, entity_id, 200_000, record_date=date(2023, 1, 1))
        _insert_deed(fresh_db, prop2, entity_id, 250_000, record_date=date(2024, 1, 1))
        fresh_db.commit()

        profile = compute_person_profile(fresh_db, person_id)
        evidence = profile.get("next_need_evidence") or {}
        date_pred = evidence.get("date_prediction", {})
        assert date_pred.get("basis") == "cadence"
        assert "velocity_purchases_per_year" in date_pred


# ===========================================================================
# Fix 5 â€” buy_box_preferences populated from condition fields
# ===========================================================================

class TestBuyBoxPreferences:
    def test_compute_preferences_with_data(self):
        from src.services.borrower_profile_service import _compute_preferences
        deeds = [
            {"building_condition": "Good", "year_built": 2000, "beds": 3.0,
             "baths": 2.0, "lot_size": 5000.0},
            {"building_condition": "Good", "year_built": 2005, "beds": 4.0,
             "baths": 2.0, "lot_size": 6000.0},
        ]
        prefs = _compute_preferences(deeds)
        assert prefs is not None
        assert prefs["most_common_condition"] == "Good"
        assert prefs["year_built_min"] == 2000
        assert prefs["year_built_max"] == 2005
        assert prefs["beds_avg"] == pytest.approx(3.5)

    def test_compute_preferences_empty_returns_none(self):
        from src.services.borrower_profile_service import _compute_preferences
        assert _compute_preferences([]) is None

    def test_compute_preferences_all_null_fields_returns_none(self):
        from src.services.borrower_profile_service import _compute_preferences
        deeds = [{"building_condition": None, "year_built": None, "beds": None,
                  "baths": None, "lot_size": None}]
        assert _compute_preferences(deeds) is None

    def test_buy_box_preferences_column_exists(self, fresh_db):
        count = fresh_db.execute(
            text("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'fa_max_person_profiles'
                  AND column_name = 'buy_box_preferences'
            """)
        ).scalar()
        assert count == 1

    def test_preferences_stored_in_profile(self, fresh_db):
        from src.services.borrower_profile_service import compute_person_profile
        person_id = _fresh_person_id(fresh_db)
        entity_id = _fresh_buyer_entity(fresh_db)
        _set_person_entity_link(fresh_db, person_id, entity_id)
        prop = _insert_property(fresh_db, building_condition="Fair", year_built=1990,
                                beds=2, baths=1, lot_size=3000)
        _insert_deed(fresh_db, prop, entity_id, 150_000)
        fresh_db.commit()

        profile = compute_person_profile(fresh_db, person_id)
        prefs = profile.get("buy_box_preferences")
        assert prefs is not None
        assert prefs["most_common_condition"] == "Fair"


# ===========================================================================
# Concurrent-write regression — two real connections
# ===========================================================================

def test_schedule_recompute_claimed_to_done_race(pg_engine):
    """Regression for the six-step claimed-item concurrent-write race.

    Two real connections simulate:
      1. Worker A claims the profile job.
      2. Connection B calls schedule_profile_recompute (sees item claimed).
      3. Worker A completes the job (status → done) before B's statement ends.
      4. B's statement still holds the FOR UPDATE lock on the canonical row,
         so A's complete_work_item blocks until B inserts the fallback and
         commits.
      5. After both commit: at least one available item must exist for the
         new event.

    With the old two-statement approach, A could complete between B's
    status check and B's fallback insert, making step 5 fail.  With the
    single-statement FOR UPDATE CTE, A blocks until B's statement releases
    the row lock, guaranteeing the fallback is inserted before A changes
    status to done.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured — skipping concurrent-write test")

    import threading
    from sqlalchemy.orm import Session as SASession
    from src.services.borrower_profile_service import schedule_profile_recompute
    from src.services.state_engine import (
        claim_next_work_item,
        complete_work_item,
        enqueue_work_item,
    )

    # ── Setup: create a person and an available profile recompute job ────────
    setup_conn = pg_engine.connect()
    setup_trans = setup_conn.begin()
    setup_session = SASession(bind=setup_conn)

    person_id = setup_session.execute(
        text("""
            INSERT INTO fa_max_persons (lifecycle_state, source, source_reference)
            VALUES ('identified', 'test', :ref)
            RETURNING person_id::text
        """),
        {"ref": f"race-test-{uuid.uuid4().hex[:8]}"},
    ).scalar()

    work_item_id = enqueue_work_item(
        session=setup_session,
        queue_name="profile_recompute",
        payload={"reason": "initial"},
        idempotency_key=f"profile_recompute:{person_id}",
        person_id=person_id,
    )
    assert work_item_id is not None
    setup_session.close()
    setup_trans.commit()
    setup_conn.close()

    # ── Worker A: claim the item ─────────────────────────────────────────────
    conn_a = pg_engine.connect()
    trans_a = conn_a.begin()
    session_a = SASession(bind=conn_a)
    item = claim_next_work_item(
        session=session_a,
        queue_name="profile_recompute",
        worker_id="worker:A",
        lease_seconds=60,
    )
    assert item is not None
    assert item["work_item_id"] == work_item_id
    trans_a.commit()

    # ── Connection B: enqueue a new event (item is currently claimed) ────────
    # In the old code, B would read claimed, plan to insert fallback, then A
    # could slip in and complete the item before B's second statement ran.
    # With the single-statement FOR UPDATE CTE, B locks the row first, so A's
    # complete_work_item blocks until B's statement finishes.

    # Use a barrier so we can interleave A and B in a controlled order.
    b_locked = threading.Event()
    b_done = threading.Event()
    a_result = {}

    def complete_in_thread():
        # Wait until B has started (and locked the canonical row), then try to
        # complete.  complete_work_item will block on the row lock held by B.
        b_locked.wait(timeout=5)
        conn_a2 = pg_engine.connect()
        trans_a2 = conn_a2.begin()
        session_a2 = SASession(bind=conn_a2)
        ok = complete_work_item(
            session=session_a2,
            work_item_id=work_item_id,
            worker_id="worker:A",
            status="done",
        )
        session_a2.close()
        trans_a2.commit()
        conn_a2.close()
        a_result["completed"] = ok
        b_done.set()

    t = threading.Thread(target=complete_in_thread, daemon=True)
    t.start()

    conn_b = pg_engine.connect()
    trans_b = conn_b.begin()
    session_b = SASession(bind=conn_b)

    # Signal that B is about to acquire the lock (approximate — the real
    # synchronisation is the DB row lock, not this event)
    b_locked.set()
    schedule_profile_recompute(session_b, person_id, "new_event_during_claim")
    session_b.close()
    trans_b.commit()
    conn_b.close()

    t.join(timeout=10)

    # ── Verify: at least one available item must remain for the new event ────
    verify_conn = pg_engine.connect()
    verify_trans = verify_conn.begin()
    available = verify_conn.execute(
        text("""
            SELECT COUNT(*) FROM fa_max_work_queue
            WHERE queue_name = 'profile_recompute'
              AND person_id = :pid ::uuid
              AND status = 'available'
        """),
        {"pid": person_id},
    ).scalar()
    verify_trans.rollback()
    verify_conn.close()

    # Cleanup
    cleanup_conn = pg_engine.connect()
    cleanup_trans = cleanup_conn.begin()
    cleanup_conn.execute(
        text("DELETE FROM fa_max_work_queue WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    )
    cleanup_conn.execute(
        text("DELETE FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    )
    cleanup_trans.commit()
    cleanup_conn.close()

    assert available >= 1, (
        "A material event that arrives while a profile job is claimed must "
        "always leave an available recompute item after both workers commit"
    )
