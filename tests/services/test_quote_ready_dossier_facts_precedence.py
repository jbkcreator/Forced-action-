"""
WP-8A/8B <-> WP-T3-7 integration: the Scenario Builder's auto-trigger
(src/services/quote_ready/dossier.py::maybe_trigger_quote_ready_review) must
use T3-7's client-confirmed facts (fa_max_opportunity_facts) over its own
raw financials read, wherever T3-7 has a confirmed value (code-review
finding, eighth round, 2026-09).

Real Postgres integration tests -- the whole point is proving these two
independently-developed pieces actually agree on "the current facts" when
run against the real schema, not a mocked one.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text


pytestmark = pytest.mark.usefixtures("fresh_db")


def _make_property(session) -> int:
    row = session.execute(
        text(
            "INSERT INTO properties (parcel_id, address, city, state, zip, county_id, "
            "beds, baths, sq_ft, year_built, created_at, updated_at) "
            "VALUES (:parcel, '123 Dossier Test St', 'Tampa', 'FL', '33602', "
            "'hillsborough', 3, 2, 1400, 1985, now(), now()) RETURNING id"
        ),
        {"parcel": f"dossier-test-{uuid.uuid4().hex[:8]}"},
    ).scalar_one()
    return row


def _make_financials(session, property_id: int, *, est_repair_cost, assessed_value_mkt=None) -> None:
    session.execute(
        text(
            "INSERT INTO financials (property_id, assessed_value_mkt, est_repair_cost) "
            "VALUES (:pid, :assessed, :repair)"
        ),
        {"pid": property_id, "assessed": assessed_value_mkt, "repair": est_repair_cost},
    )


def _make_opportunity_with_property(session, property_id: int, opportunity_type: str = "rehab") -> str:
    person_id = str(uuid.uuid4())
    session.execute(
        text(
            "INSERT INTO fa_max_persons (person_id, lifecycle_state, source)"
            " VALUES (:pid ::uuid, 'identified', 'test')"
        ),
        {"pid": person_id},
    )
    opp_id = session.execute(
        text(
            "INSERT INTO fa_max_opportunities"
            " (person_id, opportunity_type, current_stage, source)"
            " VALUES (:pid ::uuid, :otype, 'qualifying', 'test')"
            " RETURNING opportunity_id::text"
        ),
        {"pid": person_id, "otype": opportunity_type},
    ).scalar_one()
    session.execute(
        text(
            "INSERT INTO fa_max_opportunity_properties (opportunity_id, property_id, role)"
            " VALUES (:oid ::uuid, :pid, 'subject')"
        ),
        {"oid": opp_id, "pid": property_id},
    )
    return opp_id


class TestClientConfirmedFactOverridesFinancials:
    def test_50k_client_rehab_overrides_20k_financials_estimate(self, fresh_db):
        """The exact scenario from the integration review: financials says
        $20,000, the client confirms $50,000 through T3-7 -- the dossier
        must use $50,000."""
        from src.services.fa_max_qualification import set_facts
        from src.services.quote_ready.dossier import maybe_trigger_quote_ready_review

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 50000}, source="client", set_by="admin:josh",
        )
        fresh_db.flush()

        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()

        row = fresh_db.execute(
            text(
                "SELECT inputs, computed_by FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid ORDER BY computed_at DESC LIMIT 1"
            ),
            {"oid": opp_id},
        ).mappings().first()
        assert row is not None
        assert float(row["inputs"]["rehab_estimate"]) == 50000
        assert "facts_rev=1" in row["computed_by"]

    def test_no_client_fact_falls_back_to_financials(self, fresh_db):
        """An opportunity T3-7 never touched must still use the financials
        value -- the fallback path must keep working exactly as before."""
        from src.services.quote_ready.dossier import maybe_trigger_quote_ready_review

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()

        row = fresh_db.execute(
            text(
                "SELECT inputs FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid ORDER BY computed_at DESC LIMIT 1"
            ),
            {"oid": opp_id},
        ).mappings().first()
        assert row is not None
        assert float(row["inputs"]["rehab_estimate"]) == 20000

    def test_missing_client_field_falls_back_field_by_field(self, fresh_db):
        """Client confirms rehab_estimate only -- estimated_value must still
        come from financials (fallback is per-field, not all-or-nothing)."""
        from src.services.fa_max_qualification import set_facts
        from src.services.quote_ready.dossier import maybe_trigger_quote_ready_review

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 50000}, source="client", set_by="admin:josh",
        )
        fresh_db.flush()

        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()

        row = fresh_db.execute(
            text(
                "SELECT inputs FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid ORDER BY computed_at DESC LIMIT 1"
            ),
            {"oid": opp_id},
        ).mappings().first()
        assert float(row["inputs"]["rehab_estimate"]) == 50000
        assert float(row["inputs"]["estimated_value"]) == 300000  # untouched, from financials


class TestUpdateDuringProcessingAndDuplicatePrevention:
    def test_repeat_trigger_with_unchanged_facts_does_not_duplicate(self, fresh_db):
        """Point 3/4 of the integration plan: duplicate prevention. Calling
        the trigger twice with nothing changed must not create a second row."""
        from src.services.quote_ready.dossier import maybe_trigger_quote_ready_review

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()
        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()

        count = fresh_db.execute(
            text(
                "SELECT COUNT(*) FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid"
            ),
            {"oid": opp_id},
        ).scalar()
        assert count == 1

    def test_updated_fact_after_first_dossier_produces_a_new_result(self, fresh_db):
        """A client correction arriving AFTER a dossier was already posted
        must produce a fresh (superseding) result on the next trigger --
        proves the connection isn't a one-shot, frozen-at-scoping snapshot."""
        from src.services.fa_max_qualification import set_facts
        from src.services.quote_ready.dossier import maybe_trigger_quote_ready_review

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 50000}, source="client", set_by="admin:josh",
        )
        fresh_db.flush()

        maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opp_id)
        fresh_db.flush()

        rows = fresh_db.execute(
            text(
                "SELECT inputs, status FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid ORDER BY computed_at ASC"
            ),
            {"oid": opp_id},
        ).mappings().all()
        assert len(rows) == 2
        assert float(rows[0]["inputs"]["rehab_estimate"]) == 20000
        assert float(rows[1]["inputs"]["rehab_estimate"]) == 50000


class TestResolveQuoteReadyFactsUnit:
    """Pure precedence-resolution tests, isolated from the dossier's own
    financials/ARV assembly logic."""

    def test_client_value_wins_over_fallback(self, fresh_db):
        from src.services.fa_max_qualification import set_facts, resolve_quote_ready_facts

        property_id = _make_property(fresh_db)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)
        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 50000}, source="client", set_by="test",
        )
        fresh_db.flush()

        resolved = resolve_quote_ready_facts(
            session=fresh_db, opportunity_id=opp_id,
            fallback={"rehab_estimate": 20000, "estimated_value": 300000},
        )
        assert resolved["rehab_estimate"] == 50000
        assert resolved["estimated_value"] == 300000  # no client value -> fallback kept
        assert resolved["facts_revision"] == 1

    def test_no_facts_row_returns_fallback_unchanged(self, fresh_db):
        from src.services.fa_max_qualification import resolve_quote_ready_facts

        resolved = resolve_quote_ready_facts(
            session=fresh_db, opportunity_id=str(uuid.uuid4()),
            fallback={"rehab_estimate": 20000},
        )
        assert resolved["rehab_estimate"] == 20000
        assert resolved["facts_revision"] == 0
