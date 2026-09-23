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


class TestCorrectionAfterScopingRebuildsScenario:
    """code-review finding, ninth round, 2026-09: the state_engine transition
    hook only fires on the qualifying->scoping STAGE CHANGE. A later
    correction that keeps the opportunity in 'scoping' produced no new
    trigger at all -- _handle_sufficient must now rebuild the scenario on
    EVERY sufficient evaluation, not just the first."""

    def test_second_sufficient_evaluation_from_scoping_produces_new_result(self, fresh_db):
        from src.services.fa_max_qualification import set_facts, SufficiencyResult
        from src.services.state_engine import get_opportunity_state
        from src.agents.fa_max.qualification_worker import _handle_sufficient

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 200000, "rehab_estimate": 50000, "arv": 320000},
            source="client", set_by="admin:test",
        )
        fresh_db.commit()

        opp = get_opportunity_state(session=fresh_db, opportunity_id=opp_id)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "src.agents.fa_max.qualification_worker.get_db_context",
                lambda: _SessionCtx(fresh_db),
            )
            _handle_sufficient(
                opportunity_id=opp_id, person_id=opp["person_id"],
                current_stage=opp["current_stage"], state_version=opp["state_version"],
                facts_revision=1,
                result=SufficiencyResult(verdict="sufficient", gaps=[], opportunity_id=opp_id, facts_revision=1),
            )
        fresh_db.expire_all()

        # Now already in 'scoping' -- a correction must still rebuild.
        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 75000},
            source="client", set_by="admin:test",
        )
        fresh_db.commit()
        opp2 = get_opportunity_state(session=fresh_db, opportunity_id=opp_id)
        assert opp2["current_stage"] == "scoping"  # confirms this is the correction case, not first-sufficiency

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "src.agents.fa_max.qualification_worker.get_db_context",
                lambda: _SessionCtx(fresh_db),
            )
            _handle_sufficient(
                opportunity_id=opp_id, person_id=opp2["person_id"],
                current_stage=opp2["current_stage"], state_version=opp2["state_version"],
                facts_revision=2,
                result=SufficiencyResult(verdict="sufficient", gaps=[], opportunity_id=opp_id, facts_revision=2),
            )
        fresh_db.expire_all()

        rows = fresh_db.execute(
            text(
                "SELECT inputs FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid ORDER BY computed_at ASC"
            ),
            {"oid": opp_id},
        ).mappings().all()
        assert len(rows) == 2
        assert float(rows[0]["inputs"]["rehab_estimate"]) == 50000
        assert float(rows[1]["inputs"]["rehab_estimate"]) == 75000


class _SessionCtx:
    """Minimal context-manager wrapper so a test can substitute get_db_context()
    with an already-open fresh_db session (matching the pattern used
    elsewhere in this test suite's TestQualificationWorkerTransition test)."""
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, *exc):
        return False


class TestDeliveryNeverHappensBeforeCommit:
    """code-review finding, ninth round, 2026-09: Slack delivery previously
    happened INSIDE the same transaction/savepoint as the persist -- a
    later failure in that same transaction could leave a posted card with
    no committed row behind it. Prove delivery is now strictly post-commit:
    if something in the SAME transaction fails after persist, Slack must
    NEVER have been called at all (not "called then the card lacks a row" --
    zero calls)."""

    def test_downstream_failure_after_persist_results_in_zero_slack_calls(self):
        from unittest.mock import patch
        from src.services.quote_ready.dossier import compute_and_persist_quote_ready
        from src.core.database import get_db_context
        from src.services.fa_max_qualification import set_facts

        with get_db_context() as setup_session:
            property_id = _make_property(setup_session)
            _make_financials(setup_session, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
            opp_id = _make_opportunity_with_property(setup_session, property_id)
            setup_session.commit()
        try:
            with get_db_context() as session:
                set_facts(
                    session=session, opportunity_id=opp_id,
                    updates={"purchase_price": 200000, "rehab_estimate": 50000, "arv": 320000},
                    source="client", set_by="admin:test",
                )
                session.commit()

            with patch(
                "src.services.relay.slack_post.post_exceptions_alert"
            ), patch(
                "slack_sdk.WebClient.chat_postMessage"
            ) as mock_slack_post:
                with pytest.raises(RuntimeError):
                    with get_db_context() as session:
                        result_id = compute_and_persist_quote_ready(session, opportunity_id=opp_id)
                        assert result_id is not None
                        # Simulate a downstream failure in the SAME
                        # transaction, after persist but before commit.
                        raise RuntimeError("simulated downstream failure")

            mock_slack_post.assert_not_called()

            with get_db_context() as session:
                # The persisted row must also be gone -- the whole
                # transaction rolled back, exactly as intended.
                count = session.execute(
                    text(
                        "SELECT COUNT(*) FROM fa_max_quote_ready_results"
                        " WHERE opportunity_id = :oid ::uuid"
                    ),
                    {"oid": opp_id},
                ).scalar()
                assert count == 0
        finally:
            with get_db_context() as cleanup:
                cleanup.execute(
                    text("DELETE FROM fa_max_opportunity_facts WHERE opportunity_id = :oid ::uuid"),
                    {"oid": opp_id},
                )
                cleanup.execute(
                    text("DELETE FROM fa_max_opportunity_properties WHERE opportunity_id = :oid ::uuid"),
                    {"oid": opp_id},
                )
                cleanup.execute(
                    text("DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"),
                    {"oid": opp_id},
                )
                cleanup.commit()


class TestMissingFinancialsRowStillProducesScenario:
    def test_complete_t37_facts_with_no_financials_row_computes(self, fresh_db):
        """code-review finding, ninth round, 2026-09: the financials-row
        early return fired BEFORE T3-7 facts were ever read -- a rehab
        opportunity with a complete client-confirmed purchase price, rehab
        estimate, and ARV produced NO scenario if its property simply had
        no financials row yet (a newly discovered property, by
        construction, not an error state)."""
        from src.services.fa_max_qualification import set_facts
        from src.services.quote_ready.dossier import compute_and_persist_quote_ready

        property_id = _make_property(fresh_db)
        # Deliberately NO _make_financials() call -- no row at all.
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 200000, "rehab_estimate": 50000, "arv": 320000},
            source="client", set_by="admin:test",
        )
        fresh_db.flush()

        result_id = compute_and_persist_quote_ready(fresh_db, opportunity_id=opp_id)
        assert result_id is not None

        row = fresh_db.execute(
            text(
                "SELECT status, inputs FROM fa_max_quote_ready_results"
                " WHERE result_id = :rid ::uuid"
            ),
            {"rid": result_id},
        ).mappings().first()
        assert row["status"] == "computed"
        assert float(row["inputs"]["purchase_price"]) == 200000
        assert float(row["inputs"]["rehab_estimate"]) == 50000
        assert float(row["inputs"]["arv"]) == 320000


class TestRevertedValueBecomesCurrentAgain:
    def test_50k_to_60k_to_50k_ends_with_50k_current(self, fresh_db):
        """code-review finding, ninth round, 2026-09: reverting rehab_estimate
        from $50k to $60k and back to $50k must leave the $50k result
        'computed' (current, reviewable) and the $60k result 'superseded' --
        not the reverse, which was the bug (the exact-match lookup returned
        the historical $50k row's id without reviving its status, while the
        genuinely stale $60k row stayed marked 'computed')."""
        from src.services.fa_max_qualification import set_facts
        from src.services.quote_ready.dossier import compute_and_persist_quote_ready

        property_id = _make_property(fresh_db)
        _make_financials(fresh_db, property_id, est_repair_cost=20000, assessed_value_mkt=300000)
        opp_id = _make_opportunity_with_property(fresh_db, property_id)

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 200000, "rehab_estimate": 50000, "arv": 320000},
            source="client", set_by="admin:test",
        )
        fresh_db.flush()
        result_50k_first = compute_and_persist_quote_ready(fresh_db, opportunity_id=opp_id)

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 60000}, source="client", set_by="admin:test",
        )
        fresh_db.flush()
        result_60k = compute_and_persist_quote_ready(fresh_db, opportunity_id=opp_id)
        assert result_60k != result_50k_first

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"rehab_estimate": 50000}, source="client", set_by="admin:test",
        )
        fresh_db.flush()
        result_50k_again = compute_and_persist_quote_ready(fresh_db, opportunity_id=opp_id)
        assert result_50k_again == result_50k_first  # revived, not a fresh row

        rows = {
            r["result_id"]: r["status"]
            for r in fresh_db.execute(
                text(
                    "SELECT result_id::text AS result_id, status"
                    " FROM fa_max_quote_ready_results WHERE opportunity_id = :oid ::uuid"
                ),
                {"oid": opp_id},
            ).mappings().all()
        }
        assert rows[result_50k_first] == "computed"
        assert rows[result_60k] == "superseded"

        current = fresh_db.execute(
            text(
                "SELECT inputs FROM fa_max_quote_ready_results"
                " WHERE opportunity_id = :oid ::uuid AND status = 'computed'"
            ),
            {"oid": opp_id},
        ).mappings().first()
        assert float(current["inputs"]["rehab_estimate"]) == 50000
