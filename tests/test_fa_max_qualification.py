"""
WP-T3-7 tests — FA Max Qualification Agent.

Coverage map
============
Category 1  — Unit: pure checklist evaluation logic (no DB)
Category 2  — Unit: set_facts() precedence rules (mocked session)
Category 3  — Unit: SufficiencyResult gap-content-hash determinism
Category 4  — Unit: alternative-group satisfaction (e.g. refinance value basis)
Category 5  — Integration: set_facts → enqueue_qualification_recheck flow (fresh_db)
Category 6  — Integration: evaluate_sufficiency writes decision row (fresh_db)
Category 7  — Integration: sufficient verdict transitions qualifying→scoping (fresh_db)
Category 8  — Integration: stale-handoff abort — revision moved between claim and
               enqueue_quote_ready (fresh_db)
Category 9  — Integration: cancellation of superseded gap-hash alerts (fresh_db)
Category 10 — Compliance: no borrower financial data in FaMaxOpportunityFactsRequest
Category 11 — Unit: cancel_pending_alert() marks pending unclaimed rows cancelled
Category 12 — Unit: enrichment fact cannot overwrite a client-owned fact

Tests that require real Postgres use `fresh_db` and skip when DATABASE_URL absent.
"""

from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import hashlib
import json
import uuid
from decimal import Decimal
from typing import Any, Dict
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_opportunity(session, opportunity_type: str = "rehab") -> str:
    """Insert a minimal person + opportunity. Returns opportunity_id."""
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
    return opp_id


# ===========================================================================
# Category 1 — Pure checklist evaluation (no DB)
# ===========================================================================

class TestChecklistConfig:
    def test_all_opportunity_types_have_checklist(self):
        from config.fa_max_qualification import CHECKLIST
        expected_types = {
            "acquisition", "rehab", "construction", "extension",
            "refinance", "dscr_takeout", "repeat",
        }
        assert set(CHECKLIST.keys()) == expected_types

    def test_get_checklist_raises_for_unknown_type(self):
        from config.fa_max_qualification import get_checklist
        with pytest.raises(KeyError, match="unknown_invalid"):
            get_checklist("unknown_invalid")

    def test_rehab_requires_three_facts(self):
        from config.fa_max_qualification import get_checklist
        specs = get_checklist("rehab")
        keys = [s.fact_key for s in specs]
        assert "purchase_price" in keys
        assert "rehab_estimate" in keys
        assert "arv" in keys

    def test_rehab_purchase_price_is_client_gap(self):
        from config.fa_max_qualification import get_checklist
        specs = get_checklist("rehab")
        pp = next(s for s in specs if s.fact_key == "purchase_price")
        assert pp.gap_type == "client_gap"

    def test_rehab_arv_is_enrichment_sourceable(self):
        from config.fa_max_qualification import get_checklist
        specs = get_checklist("rehab")
        arv = next(s for s in specs if s.fact_key == "arv")
        assert arv.gap_type == "pending_enrichment"

    def test_refinance_uses_alternative_group(self):
        from config.fa_max_qualification import get_checklist
        specs = get_checklist("refinance")
        groups = {s.alternative_group for s in specs if s.alternative_group}
        assert "value_basis" in groups

    def test_checklist_version_is_semver(self):
        from config.fa_max_qualification import CHECKLIST_VERSION
        parts = CHECKLIST_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)


# ===========================================================================
# Category 3 — Gap content hash
# ===========================================================================

class TestSufficiencyResultHash:
    def test_no_gaps_returns_none_hash(self):
        from src.services.fa_max_qualification import SufficiencyResult
        r = SufficiencyResult(verdict="sufficient", gaps=[], opportunity_id="x", facts_revision=0)
        assert r.gap_content_hash is None

    def test_hash_is_deterministic(self):
        from src.services.fa_max_qualification import Gap, SufficiencyResult
        gaps = [
            Gap(fact_key="purchase_price", display_name="PP", reason="missing", gap_type="client_gap"),
            Gap(fact_key="arv", display_name="ARV", reason="missing", gap_type="pending_enrichment"),
        ]
        r1 = SufficiencyResult(verdict="insufficient", gaps=gaps, opportunity_id="o", facts_revision=1)
        r2 = SufficiencyResult(verdict="insufficient", gaps=list(reversed(gaps)), opportunity_id="o", facts_revision=1)
        # Sorted (key, reason) → same hash regardless of order
        assert r1.gap_content_hash == r2.gap_content_hash

    def test_different_gaps_different_hash(self):
        from src.services.fa_max_qualification import Gap, SufficiencyResult
        g1 = Gap(fact_key="purchase_price", display_name="PP", reason="r1", gap_type="client_gap")
        g2 = Gap(fact_key="arv", display_name="ARV", reason="r2", gap_type="pending_enrichment")
        r1 = SufficiencyResult(verdict="insufficient", gaps=[g1], opportunity_id="o", facts_revision=1)
        r2 = SufficiencyResult(verdict="insufficient", gaps=[g2], opportunity_id="o", facts_revision=1)
        assert r1.gap_content_hash != r2.gap_content_hash

    def test_changed_reason_changes_hash(self):
        from src.services.fa_max_qualification import Gap, SufficiencyResult
        gap_a = Gap(fact_key="purchase_price", display_name="PP", reason="reason A", gap_type="client_gap")
        gap_b = Gap(fact_key="purchase_price", display_name="PP", reason="reason B", gap_type="client_gap")
        r1 = SufficiencyResult(verdict="insufficient", gaps=[gap_a], opportunity_id="o", facts_revision=1)
        r2 = SufficiencyResult(verdict="insufficient", gaps=[gap_b], opportunity_id="o", facts_revision=1)
        assert r1.gap_content_hash != r2.gap_content_hash


# ===========================================================================
# Category 4 — Alternative-group logic
# ===========================================================================

class TestAlternativeGroup:
    def _eval(self, opportunity_type: str, facts: Dict[str, Any]):
        """Call evaluate_sufficiency with a mocked session (no DB)."""
        from src.services.fa_max_qualification import evaluate_sufficiency
        mock_session = MagicMock()
        mock_session.execute.return_value = MagicMock(scalar_one=MagicMock(return_value=None))
        # Patch _write_decision so it doesn't hit the DB
        with patch("src.services.fa_max_qualification._write_decision"):
            return evaluate_sufficiency(
                session=mock_session,
                opportunity_id="opp-test",
                opportunity_type=opportunity_type,
                facts=facts,
                facts_revision=0,
            )

    def test_refinance_sufficient_with_estimated_value(self):
        """refinance is in PENDING_CONTRACT_APPROVAL_TYPES (code-review
        finding, fourth round, 2026-09) — a would-be-sufficient checklist
        result is downgraded to 'sufficient_pending_contract', not an
        authoritative 'sufficient' handoff, until Dev 4 confirms the
        checklist. Zero gaps either way — the checklist itself is unchanged."""
        result = self._eval("refinance", {"estimated_value": 500000})
        assert result.verdict == "sufficient_pending_contract"
        assert result.gaps == []

    def test_refinance_sufficient_with_assessed_value_only(self):
        result = self._eval("refinance", {"assessed_value_mkt": 480000})
        assert result.verdict == "sufficient_pending_contract"
        assert result.gaps == []

    def test_refinance_insufficient_with_no_value(self):
        result = self._eval("refinance", {})
        assert result.verdict == "pending_enrichment"
        # Both alt-group members should be in gaps
        gap_keys = {g.fact_key for g in result.gaps}
        assert "estimated_value" in gap_keys or "assessed_value_mkt" in gap_keys

    def test_dscr_takeout_requires_exit_strategy(self):
        result = self._eval("dscr_takeout", {"estimated_value": 500000})
        gap_keys = {g.fact_key for g in result.gaps}
        assert "expected_exit_strategy" in gap_keys

    def test_dscr_takeout_sufficient_with_both(self):
        """dscr_takeout is also pending contract approval — see
        test_refinance_sufficient_with_estimated_value's docstring."""
        result = self._eval(
            "dscr_takeout",
            {"estimated_value": 500000, "expected_exit_strategy": "dscr"},
        )
        assert result.verdict == "sufficient_pending_contract"


# ===========================================================================
# Category 10 — Compliance: no borrower financial data in the API request
# ===========================================================================

class TestComplianceBoundary:
    def test_request_model_has_no_financial_fields(self):
        """The Pydantic model must not define any borrower-financial-data field."""
        # Importing this triggers the model's field registration — no DB needed.
        # Import is delayed so missing env doesn't crash collection.
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        forbidden = {"credit_score", "income", "bank_statement", "tax_return", "ssn", "fico", "dti"}
        for field_name in forbidden:
            assert field_name not in cls.model_fields, (
                f"FaMaxOpportunityFactsRequest must not define field {field_name!r}"
            )

    def test_set_by_is_not_a_request_field(self):
        """set_by is no longer client-settable — it's derived server-side
        from the authenticated admin's JWT subject (code-review finding,
        2026-09: a caller-supplied audit identity let any admin caller
        attribute a write to an arbitrary label). Confirm the field is gone
        and that a caller attempting to pass it is rejected by extra='forbid'."""
        import importlib
        import pytest
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        assert "set_by" not in cls.model_fields
        with pytest.raises(Exception):
            cls(set_by="credit_score_analyst", purchase_price=100000.0)

    def test_unknown_field_rejected(self):
        """extra='forbid': an unknown field (e.g. a typo, or an attempted
        forbidden key not already declared) is rejected outright rather than
        silently dropped (code-review finding, 2026-09)."""
        import importlib
        import pytest
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        with pytest.raises(Exception):
            cls(purchase_price=100000.0, income=50000.0)

    def test_null_fact_fields_accepted(self):
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        req = cls()  # all optional, all None
        assert req.purchase_price is None
        assert req.arv is None

    def test_valid_request_accepted(self):
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        req = cls(
            purchase_price=350000.0,
            rehab_estimate=45000.0,
            rehab_source="job_estimator",
            source="client",
        )
        assert req.purchase_price == 350000.0
        assert req.rehab_source == "job_estimator"


# ===========================================================================
# Category 12 — Enrichment cannot overwrite client-owned fact (unit, mock session)
# ===========================================================================

class TestClientOverridePrecedence:
    def _build_mock_session(self, existing_facts: Dict[str, Any]) -> MagicMock:
        mock = MagicMock()
        from unittest.mock import MagicMock as MM

        def execute_side_effect(stmt, params=None):
            result = MM()
            result.mappings.return_value.first.return_value = existing_facts
            result.scalar.return_value = existing_facts.get("facts_revision", 0)
            result.scalar_one.return_value = existing_facts.get("facts_revision", 0)
            return result

        mock.execute.side_effect = execute_side_effect
        mock.begin_nested.return_value.__enter__ = lambda s: s
        mock.begin_nested.return_value.__exit__ = MagicMock(return_value=False)
        return mock

    def test_enrichment_cannot_overwrite_client_purchase_price(self):
        from src.services.fa_max_qualification import set_facts

        existing = {
            "facts_revision": 1,
            "facts_provenance": json.dumps({
                "purchase_price": {"source": "client", "set_by": "admin:josh", "set_at": "2026-01-01"}
            }),
            "purchase_price": Decimal("350000"),
            "estimated_value": None,
            "assessed_value_mkt": None,
            "last_sale_price": None,
            "rehab_estimate": None,
            "rehab_source": None,
            "rehab_confidence": None,
            "arv": None,
            "arv_source": None,
            "arv_confidence": None,
            "expected_exit_strategy": None,
            "current_use": None,
            "existing_sqft": None,
            "property_id": None,
        }
        session = self._build_mock_session(existing)

        # Enrichment tries to update purchase_price — should be skipped
        result_revision = set_facts(
            session=session,
            opportunity_id="opp-uuid-test",
            updates={"purchase_price": 999999},
            source="enrichment",
            set_by="agent:fundability",
        )
        # No effective change → revision stays at 1
        assert result_revision == 1

    def test_client_can_overwrite_enrichment_fact(self):
        from src.services.fa_max_qualification import set_facts

        existing = {
            "facts_revision": 2,
            "facts_provenance": json.dumps({
                "arv": {"source": "enrichment", "set_by": "agent:fundability", "set_at": "2026-01-01"}
            }),
            "purchase_price": Decimal("350000"),
            "estimated_value": None,
            "assessed_value_mkt": None,
            "last_sale_price": None,
            "rehab_estimate": None,
            "rehab_source": None,
            "rehab_confidence": None,
            "arv": Decimal("450000"),
            "arv_source": "comp_engine",
            "arv_confidence": "medium",
            "expected_exit_strategy": None,
            "current_use": None,
            "existing_sqft": None,
            "property_id": None,
        }
        session = self._build_mock_session(existing)

        # Mock the UPDATE RETURNING to return revision 3
        update_result = MagicMock()
        update_result.scalar_one.return_value = 3

        call_count = [0]
        original_side_effect = session.execute.side_effect

        def patched_execute(stmt, params=None):
            call_count[0] += 1
            stmt_str = str(stmt)
            if "UPDATE fa_max_opportunity_facts" in stmt_str:
                return update_result
            return original_side_effect(stmt, params)

        session.execute.side_effect = patched_execute

        new_rev = set_facts(
            session=session,
            opportunity_id="opp-uuid-test",
            updates={"arv": 500000},
            source="client",
            set_by="admin:josh",
        )
        # Client override → revision should bump
        assert new_rev == 3

    def test_same_value_no_revision_bump(self):
        from src.services.fa_max_qualification import set_facts

        existing = {
            "facts_revision": 1,
            "facts_provenance": json.dumps({
                "purchase_price": {"source": "client", "set_by": "admin", "set_at": "2026-01-01"}
            }),
            "purchase_price": Decimal("350000"),
            "estimated_value": None,
            "assessed_value_mkt": None,
            "last_sale_price": None,
            "rehab_estimate": None,
            "rehab_source": None,
            "rehab_confidence": None,
            "arv": None,
            "arv_source": None,
            "arv_confidence": None,
            "expected_exit_strategy": None,
            "current_use": None,
            "existing_sqft": None,
            "property_id": None,
        }
        session = self._build_mock_session(existing)

        # Writing the exact same value → no effective change
        result_revision = set_facts(
            session=session,
            opportunity_id="opp-uuid-test",
            updates={"purchase_price": 350000},
            source="client",
            set_by="admin",
        )
        assert result_revision == 1

    def test_unknown_fact_key_raises(self):
        from src.services.fa_max_qualification import set_facts
        session = MagicMock()
        with pytest.raises(ValueError, match="Unknown fact keys"):
            set_facts(
                session=session,
                opportunity_id="opp",
                updates={"credit_score": 720},  # FORBIDDEN
                source="client",
                set_by="admin",
            )


# ===========================================================================
# Category 11 — cancel_pending_alert unit test (mocked DB context)
# ===========================================================================

class TestCancelPendingAlert:
    def test_cancel_marks_pending_unclaimed_row_cancelled(self):
        from src.services.relay.exceptions_alert_queue import cancel_pending_alert

        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = 42

        with patch(
            "src.services.relay.exceptions_alert_queue.get_db_context"
        ) as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = mock_session
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = cancel_pending_alert(
                venture_key="fa_max_lending",
                rule="qualification_gap:opp-123:abc123",
            )

        assert result is True
        mock_session.commit.assert_called_once()

    def test_cancel_returns_false_when_no_pending_row(self):
        from src.services.relay.exceptions_alert_queue import cancel_pending_alert

        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = None

        with patch(
            "src.services.relay.exceptions_alert_queue.get_db_context"
        ) as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = mock_session
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = cancel_pending_alert(
                venture_key="fa_max_lending",
                rule="qualification_gap:opp-123:abc123",
            )

        assert result is False


# ===========================================================================
# Category 5 — DB integration: set_facts + enqueue flow
# ===========================================================================

@pytest.mark.usefixtures("fresh_db")
class TestSetFactsIntegration:
    def test_first_write_creates_facts_row(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import set_facts, get_opportunity_facts

        rev = set_facts(
            session=fresh_db,
            opportunity_id=opp_id,
            updates={"purchase_price": 300000},
            source="client",
            set_by="admin:test",
        )
        fresh_db.flush()

        facts = get_opportunity_facts(session=fresh_db, opportunity_id=opp_id)
        assert facts is not None
        assert rev == 1
        assert facts["purchase_price"] == Decimal("300000")

    def test_effective_update_bumps_revision(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import set_facts

        set_facts(session=fresh_db, opportunity_id=opp_id,
                  updates={"purchase_price": 300000}, source="client", set_by="admin")
        fresh_db.flush()

        rev2 = set_facts(session=fresh_db, opportunity_id=opp_id,
                         updates={"purchase_price": 310000}, source="client", set_by="admin")
        fresh_db.flush()
        assert rev2 == 2

    def test_noop_write_does_not_bump_revision(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import set_facts

        set_facts(session=fresh_db, opportunity_id=opp_id,
                  updates={"purchase_price": 300000}, source="client", set_by="admin")
        fresh_db.flush()

        rev2 = set_facts(session=fresh_db, opportunity_id=opp_id,
                         updates={"purchase_price": 300000}, source="client", set_by="admin")
        fresh_db.flush()
        assert rev2 == 1  # no effective change

    def test_enqueue_qualification_recheck(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        person_id = fresh_db.execute(
            text("SELECT person_id::text FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"),
            {"oid": opp_id},
        ).scalar()

        from src.services.fa_max_qualification import set_facts, enqueue_qualification_recheck

        rev = set_facts(session=fresh_db, opportunity_id=opp_id,
                        updates={"purchase_price": 300000}, source="client", set_by="admin")
        fresh_db.flush()

        wid = enqueue_qualification_recheck(
            session=fresh_db, opportunity_id=opp_id, facts_revision=rev, person_id=person_id
        )
        fresh_db.flush()
        assert wid is not None

        # Second enqueue with same args is a no-op
        wid2 = enqueue_qualification_recheck(
            session=fresh_db, opportunity_id=opp_id, facts_revision=rev, person_id=person_id
        )
        fresh_db.flush()
        assert wid2 is None  # idempotent skip


# ===========================================================================
# Category 6 — DB integration: evaluate_sufficiency writes decision row
# ===========================================================================

@pytest.mark.usefixtures("fresh_db")
class TestEvaluateSufficiency:
    def test_insufficient_verdict_writes_decision(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")

        from src.services.fa_max_qualification import evaluate_sufficiency

        result = evaluate_sufficiency(
            session=fresh_db,
            opportunity_id=opp_id,
            opportunity_type="rehab",
            facts={},
            facts_revision=0,
        )
        fresh_db.flush()

        assert result.verdict == "insufficient"
        # purchase_price is a client_gap → verdict must be 'insufficient'
        assert any(g.gap_type == "client_gap" for g in result.gaps)

        # Decision row written
        decision = fresh_db.execute(
            text(
                "SELECT verdict, gaps FROM fa_max_qualification_decisions"
                " WHERE opportunity_id = :oid ::uuid ORDER BY decided_at DESC LIMIT 1"
            ),
            {"oid": opp_id},
        ).mappings().first()
        assert decision is not None
        assert decision["verdict"] == "insufficient"

    def test_sufficient_verdict_all_facts_present(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import evaluate_sufficiency, set_facts

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 300000, "rehab_estimate": 50000, "arv": 420000},
            source="client", set_by="admin:test",
        )
        fresh_db.flush()

        facts = {"purchase_price": 300000, "rehab_estimate": 50000, "arv": 420000}
        result = evaluate_sufficiency(
            session=fresh_db,
            opportunity_id=opp_id,
            opportunity_type="rehab",
            facts=facts,
            facts_revision=1,
        )
        fresh_db.flush()

        assert result.verdict == "sufficient"
        assert result.gaps == []

    def test_pending_enrichment_when_only_enrichment_gaps(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "refinance")
        from src.services.fa_max_qualification import evaluate_sufficiency

        # refinance only needs a value basis (enrichment-sourceable); no client facts needed
        result = evaluate_sufficiency(
            session=fresh_db,
            opportunity_id=opp_id,
            opportunity_type="refinance",
            facts={},  # nothing present
            facts_revision=0,
        )
        fresh_db.flush()

        assert result.verdict == "pending_enrichment"
        assert all(g.gap_type == "pending_enrichment" for g in result.gaps)


# ===========================================================================
# Category 7 — DB integration: sufficient → qualifying→scoping transition
# ===========================================================================

@pytest.mark.usefixtures("fresh_db")
class TestQualificationWorkerTransition:
    def test_sufficient_transitions_qualifying_to_scoping(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import set_facts
        from src.agents.fa_max.qualification_worker import _handle_sufficient
        from src.services.state_engine import get_opportunity_state

        set_facts(
            session=fresh_db,
            opportunity_id=opp_id,
            updates={"purchase_price": 300000, "rehab_estimate": 50000, "arv": 420000},
            source="client", set_by="admin:test",
        )
        fresh_db.commit()

        opp = get_opportunity_state(session=fresh_db, opportunity_id=opp_id)

        from src.services.fa_max_qualification import SufficiencyResult
        result = SufficiencyResult(
            verdict="sufficient", gaps=[],
            opportunity_id=opp_id, facts_revision=1,
        )

        with patch(
            "src.agents.fa_max.qualification_worker.get_db_context"
        ) as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = fresh_db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            _handle_sufficient(
                opportunity_id=opp_id,
                person_id=opp["person_id"],
                current_stage=opp["current_stage"],
                state_version=opp["state_version"],
                facts_revision=1,
                result=result,
            )

        # Verify stage advanced
        fresh_db.expire_all()
        updated_opp = get_opportunity_state(session=fresh_db, opportunity_id=opp_id)
        assert updated_opp is not None
        assert updated_opp["current_stage"] in ("scoping", "qualifying")  # may be scoping


# ===========================================================================
# Category 8 — Stale-handoff: revision moved between evaluation and enqueue
# ===========================================================================

class TestStaleHandoffAbort:
    def test_stale_revision_skips_builder_enqueue(self):
        """If db_revision != facts_revision at publication time, no builder work
        is enqueued (a newer recheck is already in the queue)."""
        from src.agents.fa_max.qualification_worker import _handle_sufficient
        from src.services.fa_max_qualification import SufficiencyResult

        result = SufficiencyResult(
            verdict="sufficient", gaps=[],
            opportunity_id="opp-stale-test", facts_revision=1,
        )

        mock_session = MagicMock()
        # Simulate transition succeeding
        from src.services.state_engine import TransitionResult, TransitionOutcome
        with patch("src.agents.fa_max.qualification_worker.transition") as mock_trans, \
             patch("src.agents.fa_max.qualification_worker.ensure_entity_registry") as mock_reg, \
             patch("src.agents.fa_max.qualification_worker.enqueue_quote_ready_work") as mock_enqueue, \
             patch("src.agents.fa_max.qualification_worker.get_db_context") as mock_ctx:

            mock_ctx.return_value.__enter__.return_value = mock_session
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            mock_reg.return_value = "entity-uuid"
            mock_trans.return_value = TransitionResult(
                outcome=TransitionOutcome.idempotent_skip, current_state="scoping"
            )

            # DB revision is 2, but we evaluated revision 1 → stale
            mock_session.execute.return_value.scalar.return_value = 2

            _handle_sufficient(
                opportunity_id="opp-stale-test",
                person_id="person-xyz",
                current_stage="qualifying",
                state_version=0,
                facts_revision=1,
                result=result,
            )

        mock_enqueue.assert_not_called()
        # code-review fix, 2026-09: the CAS check must gate the stage
        # transition too, not only the builder enqueue — a stale revision
        # must not advance qualifying->scoping either.
        mock_trans.assert_not_called()


# ===========================================================================
# Category 13 — code-review fix regression tests (2026-09)
# ===========================================================================

class TestNumericGapValidity:
    """A numeric fact of 0 (or negative) is not usable and must still gap."""

    def test_purchase_price_and_arv_zero_are_insufficient(self):
        """purchase_price=0 and arv=0 gap (matches compute.py's own
        `value > _ZERO` / `arv > _ZERO` checks) — but rehab_estimate=0 must
        NOT gap (matches compute.py's `rehab_estimate >= _ZERO`); a second
        code-review round (2026-09) caught the first fix applying `> 0` to
        all three, which disagreed with the actual Scenario Builder."""
        from src.services.fa_max_qualification import evaluate_sufficiency

        class _FakeSession:
            def execute(self, *a, **k):
                class _R:
                    def scalar_one(self):
                        return None
                return _R()

        facts = {"purchase_price": 0, "rehab_estimate": 0, "arv": 0}
        result = evaluate_sufficiency(
            session=_FakeSession(),
            opportunity_id="opp-zero-test",
            opportunity_type="rehab",
            facts=facts,
            facts_revision=1,
        )
        assert result.verdict == "insufficient"
        gapped_keys = {g.fact_key for g in result.gaps}
        assert gapped_keys == {"purchase_price", "arv"}
        assert "rehab_estimate" not in gapped_keys

    def test_rehab_estimate_zero_is_usable(self):
        """A no-rehab deal (rehab_estimate=0) with a positive purchase price
        and ARV must be sufficient — 0 is a genuinely valid rehab budget."""
        from src.services.fa_max_qualification import evaluate_sufficiency

        class _FakeSession:
            def execute(self, *a, **k):
                class _R:
                    def scalar_one(self):
                        return None
                return _R()

        facts = {"purchase_price": 300000.0, "rehab_estimate": 0, "arv": 320000.0}
        result = evaluate_sufficiency(
            session=_FakeSession(),
            opportunity_id="opp-zero-rehab-test",
            opportunity_type="rehab",
            facts=facts,
            facts_revision=1,
        )
        assert result.verdict == "sufficient"

    def test_positive_numeric_values_pass(self):
        from src.services.fa_max_qualification import evaluate_sufficiency

        class _FakeSession:
            def execute(self, *a, **k):
                class _R:
                    def scalar_one(self):
                        return None
                return _R()

        facts = {"purchase_price": 1.0, "rehab_estimate": 1.0, "arv": 1.0}
        result = evaluate_sufficiency(
            session=_FakeSession(),
            opportunity_id="opp-positive-test",
            opportunity_type="rehab",
            facts=facts,
            facts_revision=1,
        )
        assert result.verdict == "sufficient"

    def test_acquisition_without_rehab_estimate_is_insufficient(self):
        """code-review finding, 2026-09: acquisition's checklist never asked
        for rehab_estimate at all, so purchase_price+arv alone were marked
        'sufficient' even though compute_quote_ready() always needs
        rehab_estimate to be non-None (0 is fine) to compute project_cost —
        the Scenario Builder disagreed with T3-7's own sufficiency verdict."""
        from src.services.fa_max_qualification import evaluate_sufficiency

        class _FakeSession:
            def execute(self, *a, **k):
                class _R:
                    def scalar_one(self):
                        return None
                return _R()

        facts = {"purchase_price": 300000.0, "arv": 320000.0}  # no rehab_estimate
        result = evaluate_sufficiency(
            session=_FakeSession(),
            opportunity_id="opp-acq-no-rehab-test",
            opportunity_type="acquisition",
            facts=facts,
            facts_revision=1,
        )
        assert result.verdict == "insufficient"
        assert "rehab_estimate" in {g.fact_key for g in result.gaps}

    def test_acquisition_with_zero_rehab_estimate_is_sufficient(self):
        from src.services.fa_max_qualification import evaluate_sufficiency

        class _FakeSession:
            def execute(self, *a, **k):
                class _R:
                    def scalar_one(self):
                        return None
                return _R()

        facts = {"purchase_price": 300000.0, "rehab_estimate": 0, "arv": 320000.0}
        result = evaluate_sufficiency(
            session=_FakeSession(),
            opportunity_id="opp-acq-zero-rehab-test",
            opportunity_type="acquisition",
            facts=facts,
            facts_revision=1,
        )
        assert result.verdict == "sufficient"


def _seed_open_opportunity(opportunity_type: str = "rehab") -> str:
    """Real commit (not fresh_db's rolled-back transaction) — needed because
    _handle_insufficient/_handle_sufficient open their OWN get_db_context()
    sessions internally (multiple locked queries across two tables), which
    a fresh_db-scoped transaction is invisible to. Also ensures the facts
    row exists (facts_revision=0) — _lock_and_check_eligibility treats a
    missing facts row as an automatic staleness mismatch. Caller must clean
    up via _cleanup_opportunity()."""
    from src.core.database import get_db_context
    from src.services.fa_max_qualification import _ensure_facts_row
    with get_db_context() as session:
        opp_id = _make_opportunity(session, opportunity_type)
        _ensure_facts_row(session, opp_id)
        session.commit()
    return opp_id


def _cleanup_opportunity(opp_id: str) -> None:
    from src.core.database import get_db_context
    with get_db_context() as session:
        session.execute(text(
            "DELETE FROM fa_max_exceptions_alert_queue WHERE rule LIKE :prefix"
        ), {"prefix": f"qualification_gap:{opp_id}:%"})
        session.execute(text(
            "DELETE FROM fa_max_work_queue WHERE payload->>'opportunity_id' = :oid"
        ), {"oid": opp_id})
        session.execute(text(
            "DELETE FROM fa_max_qualification_decisions WHERE opportunity_id = :oid ::uuid"
        ), {"oid": opp_id})
        session.execute(text(
            "DELETE FROM fa_max_opportunity_facts WHERE opportunity_id = :oid ::uuid"
        ), {"oid": opp_id})
        session.execute(text(
            "DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"
        ), {"oid": opp_id})
        session.commit()


class TestPendingEnrichmentNotRoutedToExceptions:
    """A pure pending_enrichment verdict (no client_gap) must not page Josh.

    Real-DB tests (code-review finding, sixth round, 2026-09): the locked,
    atomic redesign of _handle_insufficient makes multiple sequential
    session.execute() calls across two tables inside its own
    get_db_context() — mocking that reliably is fragile; a real seeded
    opportunity plus mocking only the true I/O boundary
    (post_exceptions_alert) is more robust and exercises the real locking."""

    def test_pure_pending_enrichment_skips_exceptions_alert(self):
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap

        opp_id = _seed_open_opportunity("refinance")
        try:
            result = SufficiencyResult(
                verdict="pending_enrichment",
                gaps=[Gap(
                    fact_key="estimated_value", display_name="Estimated Value",
                    reason="pending enrichment", gap_type="pending_enrichment",
                )],
                opportunity_id=opp_id, facts_revision=0,
            )
            with patch(
                "src.services.relay.exceptions_alert_queue.post_exceptions_alert"
            ) as mock_slack:
                _handle_insufficient(
                    opportunity_id=opp_id,
                    opportunity_type="refinance",
                    current_stage="qualifying",
                    result=result,
                )
            mock_slack.assert_not_called()
        finally:
            _cleanup_opportunity(opp_id)

    def test_mixed_gaps_route_only_client_gap_to_exceptions(self):
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap

        opp_id = _seed_open_opportunity("rehab")
        try:
            result = SufficiencyResult(
                verdict="insufficient",
                gaps=[
                    Gap(fact_key="purchase_price", display_name="Purchase Price",
                        reason="client must supply", gap_type="client_gap"),
                    Gap(fact_key="arv", display_name="ARV",
                        reason="pending enrichment", gap_type="pending_enrichment"),
                ],
                opportunity_id=opp_id, facts_revision=0,
            )
            with patch(
                "src.services.relay.exceptions_alert_queue.post_exceptions_alert",
                return_value=True,
            ) as mock_slack:
                _handle_insufficient(
                    opportunity_id=opp_id,
                    opportunity_type="rehab",
                    current_stage="qualifying",
                    result=result,
                )
            mock_slack.assert_called_once()
            _, kwargs = mock_slack.call_args
            assert "Purchase Price" in kwargs["message"]
            assert "arv" not in kwargs["rule"]  # rule is hashed from client_gap only
        finally:
            _cleanup_opportunity(opp_id)


class TestSuccessCancelsGapAlerts:
    def test_sufficient_cancels_pending_gap_alerts(self, fresh_db):
        """Real-DB test (code-review finding, sixth round, 2026-09): seeds a
        real pending EXCEPTIONS row, then confirms _handle_sufficient
        cancels it (status -> 'cancelled') as part of its own locked
        transaction — using fresh_db mapped as get_db_context, matching
        TestQualificationWorkerTransition's established pattern (a single
        get_db_context() block, so one mock target covers it)."""
        from src.services.fa_max_qualification import set_facts
        from src.agents.fa_max.qualification_worker import _handle_sufficient
        from src.services.fa_max_qualification import SufficiencyResult
        from src.services.state_engine import get_opportunity_state

        opp_id = _make_opportunity(fresh_db, "rehab")
        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 300000, "rehab_estimate": 50000, "arv": 420000},
            source="client", set_by="admin:test",
        )
        fresh_db.commit()

        # Seed a pending EXCEPTIONS alert this opportunity is presumed to
        # already have, from an earlier insufficient evaluation.
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_exceptions_alert_queue"
                " (venture_key, rule, message, status)"
                " VALUES ('fa_max_lending', :rule, 'test', 'pending')"
            ),
            {"rule": f"qualification_gap:{opp_id}:deadbeef"},
        )
        fresh_db.commit()

        opp = get_opportunity_state(session=fresh_db, opportunity_id=opp_id)
        result = SufficiencyResult(
            verdict="sufficient", gaps=[], opportunity_id=opp_id, facts_revision=1,
        )

        with patch(
            "src.agents.fa_max.qualification_worker.get_db_context"
        ) as mock_ctx:
            mock_ctx.return_value.__enter__.return_value = fresh_db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            _handle_sufficient(
                opportunity_id=opp_id,
                person_id=opp["person_id"],
                current_stage=opp["current_stage"],
                state_version=opp["state_version"],
                facts_revision=1,
                result=result,
            )

        fresh_db.expire_all()
        status = fresh_db.execute(
            text(
                "SELECT status FROM fa_max_exceptions_alert_queue"
                " WHERE rule = :rule"
            ),
            {"rule": f"qualification_gap:{opp_id}:deadbeef"},
        ).scalar()
        assert status == "cancelled"


class TestClientConfirmationOwnershipTransfer:
    def test_client_confirming_enrichment_value_takes_ownership(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import set_facts, get_opportunity_facts

        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 250000.0},
            source="enrichment", set_by="agent:fundability",
        )
        fresh_db.flush()

        # Client confirms the SAME value — must still take ownership even
        # though the raw value didn't change (code-review fix, 2026-09).
        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 250000.0},
            source="client", set_by="admin:josh",
        )
        fresh_db.flush()

        facts = get_opportunity_facts(session=fresh_db, opportunity_id=opp_id)
        assert facts["facts_provenance"]["purchase_price"]["source"] == "client"

        # A later enrichment write must now be rejected (client-owned).
        set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"purchase_price": 999999.0},
            source="enrichment", set_by="agent:fundability",
        )
        fresh_db.flush()
        facts2 = get_opportunity_facts(session=fresh_db, opportunity_id=opp_id)
        assert facts2["purchase_price"] == Decimal("250000.0")


class TestClientGapResolvedToPendingEnrichmentCancelsAlert:
    """code-review finding, second round, 2026-09: when the client_gap set
    resolves to empty but pending_enrichment gaps remain, the early return
    in _handle_insufficient must still cancel any prior client-gap alert —
    the earlier fix's early return skipped cancellation entirely, so a
    resolved client gap kept paging Josh forever."""

    def test_resolved_to_pure_pending_enrichment_cancels_prior_alert(self):
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap

        opp_id = _seed_open_opportunity("acquisition")
        try:
            from src.core.database import get_db_context
            with get_db_context() as session:
                session.execute(
                    text(
                        "INSERT INTO fa_max_exceptions_alert_queue"
                        " (venture_key, rule, message, status)"
                        " VALUES ('fa_max_lending', :rule, 'test', 'pending')"
                    ),
                    {"rule": f"qualification_gap:{opp_id}:deadbeef"},
                )
                session.commit()

            result = SufficiencyResult(
                verdict="pending_enrichment",
                gaps=[Gap(
                    fact_key="arv", display_name="ARV",
                    reason="pending enrichment", gap_type="pending_enrichment",
                )],
                opportunity_id=opp_id, facts_revision=0,
            )
            _handle_insufficient(
                opportunity_id=opp_id,
                opportunity_type="acquisition",
                current_stage="qualifying",
                result=result,
            )

            with get_db_context() as session:
                status = session.execute(
                    text(
                        "SELECT status FROM fa_max_exceptions_alert_queue"
                        " WHERE rule = :rule"
                    ),
                    {"rule": f"qualification_gap:{opp_id}:deadbeef"},
                ).scalar()
            assert status == "cancelled"
        finally:
            _cleanup_opportunity(opp_id)


class TestCurrentUseVocabulary:
    def test_valid_value_accepted(self):
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        req = cls(current_use="single_family")
        assert req.current_use == "single_family"

    def test_out_of_vocabulary_value_rejected(self):
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        with pytest.raises(Exception):
            cls(current_use="borrower has excellent credit score 780")

    def test_db_check_constraint_rejects_out_of_vocabulary(self, fresh_db):
        from src.services.fa_max_qualification import _ensure_facts_row

        opp_id = _make_opportunity(fresh_db, "rehab")
        _ensure_facts_row(fresh_db, opp_id)
        fresh_db.flush()
        with pytest.raises(Exception):
            fresh_db.execute(
                text(
                    "UPDATE fa_max_opportunity_facts SET current_use = :cu"
                    " WHERE opportunity_id = :oid ::uuid"
                ),
                {"cu": "not_a_real_category", "oid": opp_id},
            )
            fresh_db.flush()


class TestArvSourceStructuredIdentifier:
    def test_valid_identifier_accepted(self):
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        req = cls(arv_source="legacy_financial.arv")
        assert req.arv_source == "legacy_financial.arv"

    def test_free_prose_rejected(self):
        import importlib
        router_mod = importlib.import_module("src.api.admin_router")
        cls = router_mod.FaMaxOpportunityFactsRequest
        with pytest.raises(Exception):
            cls(arv_source="Borrower's income is $150,000/yr, SSN 123-45-6789")

    def test_db_check_constraint_rejects_free_prose(self, fresh_db):
        from src.services.fa_max_qualification import _ensure_facts_row

        opp_id = _make_opportunity(fresh_db, "rehab")
        _ensure_facts_row(fresh_db, opp_id)
        fresh_db.flush()
        with pytest.raises(Exception):
            fresh_db.execute(
                text(
                    "UPDATE fa_max_opportunity_facts SET arv_source = :s"
                    " WHERE opportunity_id = :oid ::uuid"
                ),
                {"s": "not a valid identifier, has spaces", "oid": opp_id},
            )
            fresh_db.flush()


class TestExtensionChecklistPurchasePriceMeaning:
    def test_extension_display_name_does_not_reference_outstanding_principal(self):
        """code-review finding, third round, 2026-09: 'outstanding principal'
        is a loan-balance concept with no corresponding field anywhere
        downstream (QuoteReadyInput has no such field) — the checklist must
        not imply it's an acceptable answer for purchase_price."""
        from config.fa_max_qualification import get_checklist
        specs = get_checklist("extension")
        purchase_spec = next(s for s in specs if s.fact_key == "purchase_price")
        assert "outstanding" not in purchase_spec.display_name.lower()
        assert "outstanding" not in purchase_spec.gap_reason.lower()


class TestExceptionsAlertPersistenceFailureRetries:
    """code-review finding, sixth round, 2026-09: the redesign replaced the
    old 'does a row exist afterward?' ambiguity-resolution hack with
    enqueue_pending() running inside the caller's own locked transaction —
    a genuine INSERT failure now propagates as an ordinary exception with
    no special-casing needed, and enqueue_pending()'s return value (row id
    vs None) unambiguously distinguishes 'durably recorded' from 'already
    covered by dedup', with no separate existence check required."""

    def test_persistence_failure_propagates_for_retry(self):
        """A genuine DB error durably inserting the new alert row must
        propagate out of _handle_insufficient (into the worker's
        bounded-retry path), not be silently swallowed."""
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap

        opp_id = _seed_open_opportunity("rehab")
        try:
            result = SufficiencyResult(
                verdict="insufficient",
                gaps=[Gap(
                    fact_key="purchase_price", display_name="Purchase Price",
                    reason="client must supply", gap_type="client_gap",
                )],
                opportunity_id=opp_id, facts_revision=0,
            )
            with patch(
                "src.services.relay.exceptions_alert_queue.enqueue_pending",
                side_effect=RuntimeError("simulated DB failure"),
            ):
                with pytest.raises(RuntimeError, match="simulated DB failure"):
                    _handle_insufficient(
                        opportunity_id=opp_id,
                        opportunity_type="rehab",
                        current_stage="qualifying",
                        result=result,
                    )
        finally:
            _cleanup_opportunity(opp_id)

    def test_legitimate_dedup_skip_does_not_raise_or_deliver(self):
        """A pre-existing pending row for the identical rule means
        enqueue_pending() returns None (dedup) — must not raise, and must
        not attempt delivery (nothing new was durably recorded)."""
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap

        opp_id = _seed_open_opportunity("rehab")
        try:
            result = SufficiencyResult(
                verdict="insufficient",
                gaps=[Gap(
                    fact_key="purchase_price", display_name="Purchase Price",
                    reason="client must supply", gap_type="client_gap",
                )],
                opportunity_id=opp_id, facts_revision=0,
            )
            # Seed a pending row for the EXACT rule this result will hash to.
            client_hash = result.gap_content_hash
            from src.core.database import get_db_context
            with get_db_context() as session:
                session.execute(
                    text(
                        "INSERT INTO fa_max_exceptions_alert_queue"
                        " (venture_key, rule, message, status)"
                        " VALUES ('fa_max_lending', :rule, 'test', 'pending')"
                    ),
                    {"rule": f"qualification_gap:{opp_id}:{client_hash}"},
                )
                session.commit()

            with patch(
                "src.services.relay.exceptions_alert_queue.attempt_delivery"
            ) as mock_deliver:
                _handle_insufficient(
                    opportunity_id=opp_id,
                    opportunity_type="rehab",
                    current_stage="qualifying",
                    result=result,
                )  # must not raise
            mock_deliver.assert_not_called()
        finally:
            _cleanup_opportunity(opp_id)


class TestBackstopSweepCoverage:
    """run_checklist_version_backstop_sweep() always opens its own
    get_db_context() session (it commits per-batch across pagination), so
    it cannot see data inserted through the fresh_db fixture's single
    outer transaction (never truly committed, only rolled back at test
    teardown — see conftest.py). These tests use real commits via
    get_db_context() directly, with explicit cleanup, matching how this
    was manually verified during implementation."""

    def test_opportunity_with_no_decision_is_swept(self):
        """LEFT JOIN (not INNER JOIN) fix: an opportunity with a facts row
        but NO decision row at all must still be caught by the sweep."""
        from src.core.database import get_db_context
        from src.services.fa_max_qualification import _ensure_facts_row
        from src.agents.fa_max.qualification_worker import run_checklist_version_backstop_sweep

        with get_db_context() as session:
            opp_id = _make_opportunity(session, "rehab")
            _ensure_facts_row(session, opp_id)
            session.commit()
        try:
            n = run_checklist_version_backstop_sweep()
            assert n >= 1
        finally:
            with get_db_context() as session:
                session.execute(text(
                    "DELETE FROM fa_max_work_queue WHERE payload->>'opportunity_id' = :oid"
                ), {"oid": opp_id})
                session.execute(text(
                    "DELETE FROM fa_max_opportunity_facts WHERE opportunity_id = :oid ::uuid"
                ), {"oid": opp_id})
                session.execute(text(
                    "DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"
                ), {"oid": opp_id})
                session.commit()

    def test_stale_facts_revision_ahead_of_decision_is_swept(self):
        """An opportunity whose facts_revision has moved past its latest
        decision's facts_revision (a lost recheck) must be caught even when
        checklist_version already matches current."""
        from config.fa_max_qualification import CHECKLIST_VERSION
        from src.core.database import get_db_context
        from src.agents.fa_max.qualification_worker import run_checklist_version_backstop_sweep

        with get_db_context() as session:
            opp_id = _make_opportunity(session, "rehab")
            session.execute(
                text(
                    "INSERT INTO fa_max_opportunity_facts (opportunity_id, facts_revision)"
                    " VALUES (:oid ::uuid, 3)"
                ),
                {"oid": opp_id},
            )
            session.execute(
                text(
                    "INSERT INTO fa_max_qualification_decisions"
                    " (opportunity_id, facts_revision, checklist_version, verdict, decided_by)"
                    " VALUES (:oid ::uuid, 1, :cv, 'insufficient', 'test')"
                ),
                {"oid": opp_id, "cv": CHECKLIST_VERSION},
            )
            session.commit()
        try:
            n = run_checklist_version_backstop_sweep()
            assert n >= 1
        finally:
            with get_db_context() as session:
                session.execute(text(
                    "DELETE FROM fa_max_work_queue WHERE payload->>'opportunity_id' = :oid"
                ), {"oid": opp_id})
                session.execute(text(
                    "DELETE FROM fa_max_qualification_decisions WHERE opportunity_id = :oid ::uuid"
                ), {"oid": opp_id})
                session.execute(text(
                    "DELETE FROM fa_max_opportunity_facts WHERE opportunity_id = :oid ::uuid"
                ), {"oid": opp_id})
                session.execute(text(
                    "DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"
                ), {"oid": opp_id})
                session.commit()

    def test_keyset_pagination_visits_every_opportunity_once(self):
        """code-review finding, fourth round, 2026-09: without a keyset
        cursor, a batch_size=1 sweep re-selected the SAME single opportunity
        every iteration (enqueuing a recheck doesn't itself change the
        decisions table), never reaching a second one."""
        from src.core.database import get_db_context
        from src.services.fa_max_qualification import _ensure_facts_row
        from src.agents.fa_max.qualification_worker import run_checklist_version_backstop_sweep

        opp_ids = []
        with get_db_context() as session:
            for _ in range(3):
                oid = _make_opportunity(session, "rehab")
                _ensure_facts_row(session, oid)
                opp_ids.append(oid)
            session.commit()
        try:
            n = run_checklist_version_backstop_sweep(batch_size=1, max_total=3)
            assert n == 3

            with get_db_context() as session:
                enqueued_for = session.execute(
                    text(
                        "SELECT DISTINCT payload->>'opportunity_id' AS oid"
                        " FROM fa_max_work_queue"
                        " WHERE payload->>'opportunity_id' = ANY(:oids)"
                    ),
                    {"oids": opp_ids},
                ).scalars().all()
            assert set(enqueued_for) == set(opp_ids)
        finally:
            with get_db_context() as session:
                for oid in opp_ids:
                    session.execute(text(
                        "DELETE FROM fa_max_work_queue WHERE payload->>'opportunity_id' = :oid"
                    ), {"oid": oid})
                    session.execute(text(
                        "DELETE FROM fa_max_opportunity_facts WHERE opportunity_id = :oid ::uuid"
                    ), {"oid": oid})
                    session.execute(text(
                        "DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"
                    ), {"oid": oid})
                session.commit()



class TestReactivateFailedWorkItems:
    def test_failed_item_is_reactivated_and_attempt_count_reset(self, fresh_db):
        from src.services.state_engine import (
            enqueue_work_item, claim_next_work_item, complete_work_item,
            reactivate_failed_work_items,
        )

        work_item_id = enqueue_work_item(
            session=fresh_db, queue_name="fa_max_qualification",
            payload={"opportunity_id": "opp-reactivate-test", "facts_revision": 1},
        )
        fresh_db.flush()
        claimed = claim_next_work_item(
            session=fresh_db, queue_name="fa_max_qualification", worker_id="test-worker",
        )
        assert claimed["work_item_id"] == work_item_id
        complete_work_item(
            session=fresh_db, work_item_id=work_item_id, worker_id="test-worker", status="failed",
        )
        fresh_db.flush()

        n = reactivate_failed_work_items(session=fresh_db, queue_name="fa_max_qualification")
        fresh_db.flush()
        assert n == 1

        row = fresh_db.execute(
            text(
                "SELECT status, attempt_count FROM fa_max_work_queue"
                " WHERE work_item_id = :id ::uuid"
            ),
            {"id": work_item_id},
        ).mappings().first()
        assert row["status"] == "available"
        assert row["attempt_count"] == 0

    def test_reenqueue_with_same_idempotency_key_is_a_noop_without_reactivation(self, fresh_db):
        """Proves the underlying problem this fix solves: without
        reactivation, a second enqueue attempt for the SAME idempotency_key
        silently does nothing once the original item exists."""
        from src.services.state_engine import enqueue_work_item

        key = "qual:opp-idempotency-test:1:1.0.0"
        first = enqueue_work_item(
            session=fresh_db, queue_name="fa_max_qualification",
            payload={"opportunity_id": "opp-idempotency-test", "facts_revision": 1},
            idempotency_key=key,
        )
        fresh_db.flush()
        assert first is not None

        second = enqueue_work_item(
            session=fresh_db, queue_name="fa_max_qualification",
            payload={"opportunity_id": "opp-idempotency-test", "facts_revision": 1},
            idempotency_key=key,
        )
        fresh_db.flush()
        assert second is None  # confirms the recovery gap this fix addresses


class TestSufficientPendingContractDispatch:
    def test_no_transition_no_enqueue_no_alert(self):
        """code-review finding, fourth round, 2026-09: a 'sufficient_pending_
        contract' verdict must produce NO handoff at all — no stage
        transition, no builder enqueue, no EXCEPTIONS alert — until Dev 4
        confirms the checklist for refinance/dscr_takeout/repeat."""
        from src.services.fa_max_qualification import evaluate_sufficiency

        class _FakeSession:
            def execute(self, *a, **k):
                class _R:
                    def scalar_one(self):
                        return None
                return _R()

        result = evaluate_sufficiency(
            session=_FakeSession(),
            opportunity_id="opp-contract-test",
            opportunity_type="refinance",
            facts={"estimated_value": 500000.0},
            facts_revision=1,
        )
        assert result.verdict == "sufficient_pending_contract"

        with patch(
            "src.agents.fa_max.qualification_worker.transition"
        ) as mock_trans, patch(
            "src.agents.fa_max.qualification_worker.enqueue_quote_ready_work"
        ) as mock_enqueue, patch(
            "src.services.relay.exceptions_alert_queue.enqueue_and_attempt"
        ) as mock_alert:
            from src.agents.fa_max.qualification_worker import _process_qualification_item
            with patch(
                "src.agents.fa_max.qualification_worker.get_opportunity_facts",
                return_value={"facts_revision": 1, "estimated_value": 500000.0},
            ), patch(
                "src.agents.fa_max.qualification_worker.get_opportunity_state",
                return_value={
                    "opportunity_type": "refinance", "current_stage": "qualifying",
                    "state_version": 0, "person_id": "person-xyz",
                },
            ), patch(
                "src.agents.fa_max.qualification_worker.get_db_context"
            ) as mock_ctx:
                mock_ctx.return_value.__enter__.return_value = MagicMock()
                mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
                _process_qualification_item(
                    opportunity_id="opp-contract-test", claimed_revision=1,
                    claimed_checklist="1.2.1", worker_id="test-worker",
                )
        mock_trans.assert_not_called()
        mock_enqueue.assert_not_called()
        mock_alert.assert_not_called()


class TestArvSourceServiceLayerRejection:
    """code-review finding, fifth round, 2026-09: the forbidden-term check
    previously existed only in the API request validator — set_facts()
    itself (the authoritative write path) had none, so a direct caller
    could bypass it entirely."""

    def test_set_facts_rejects_forbidden_term_in_arv_source(self):
        from src.services.fa_max_qualification import set_facts
        with pytest.raises(ValueError, match="borrower financial data"):
            set_facts(
                session=MagicMock(), opportunity_id="opp-test",
                updates={"arv_source": "borrower_income:123"},
                source="client", set_by="test",
            )

    def test_set_facts_accepts_legitimate_identifier(self, fresh_db):
        opp_id = _make_opportunity(fresh_db, "rehab")
        from src.services.fa_max_qualification import set_facts
        rev = set_facts(
            session=fresh_db, opportunity_id=opp_id,
            updates={"arv_source": "legacy_financial.arv"},
            source="client", set_by="test",
        )
        assert rev == 1

    def test_db_check_rejects_forbidden_term_via_direct_write(self, fresh_db):
        from src.services.fa_max_qualification import _ensure_facts_row
        opp_id = _make_opportunity(fresh_db, "rehab")
        _ensure_facts_row(fresh_db, opp_id)
        fresh_db.flush()
        with pytest.raises(Exception):
            fresh_db.execute(
                text(
                    "UPDATE fa_max_opportunity_facts SET arv_source = :s"
                    " WHERE opportunity_id = :oid ::uuid"
                ),
                {"s": "borrower_income:123", "oid": opp_id},
            )
            fresh_db.flush()


class TestTerminalOpportunityNoHandoff:
    def test_closed_opportunity_gets_no_builder_enqueue(self):
        """code-review finding, fifth round, 2026-09: _handle_sufficient
        never re-checked outcome before enqueueing builder work — an
        opportunity that closed between claim and this handler still got
        a fa_max_quote_ready work item."""
        from src.agents.fa_max.qualification_worker import _handle_sufficient
        from src.services.fa_max_qualification import SufficiencyResult

        result = SufficiencyResult(
            verdict="sufficient", gaps=[],
            opportunity_id="opp-closed-test", facts_revision=1,
        )
        mock_session = MagicMock()
        with patch("src.agents.fa_max.qualification_worker.transition") as mock_trans, \
             patch("src.agents.fa_max.qualification_worker.ensure_entity_registry"), \
             patch("src.agents.fa_max.qualification_worker.enqueue_quote_ready_work") as mock_enqueue, \
             patch("src.agents.fa_max.qualification_worker.get_db_context") as mock_ctx, \
             patch("src.agents.fa_max.qualification_worker._cancel_all_gap_alerts_for_opportunity"):

            mock_ctx.return_value.__enter__.return_value = mock_session
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            # First execute() call = revision check (matches), second = outcome check (closed).
            revision_result = MagicMock()
            revision_result.scalar.return_value = 1
            outcome_result = MagicMock()
            outcome_result.scalar.return_value = "dead"
            mock_session.execute.side_effect = [revision_result, outcome_result]

            _handle_sufficient(
                opportunity_id="opp-closed-test",
                person_id="person-xyz",
                current_stage="qualifying",
                state_version=0,
                facts_revision=1,
                result=result,
            )

        mock_trans.assert_not_called()
        mock_enqueue.assert_not_called()


class TestStaleInsufficientResultSkipsExceptions:
    def test_stale_result_does_not_alert(self):
        """code-review finding, fifth/sixth round, 2026-09: an out-of-order
        stale result (evaluated against a revision the DB has since moved
        past) must not touch EXCEPTIONS at all — the newer evaluation's own
        outcome is authoritative. Seeds a real opportunity at facts_revision
        1, then evaluates against the stale claimed revision 0."""
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap, set_facts
        from src.core.database import get_db_context

        opp_id = _seed_open_opportunity("rehab")
        try:
            with get_db_context() as session:
                set_facts(
                    session=session, opportunity_id=opp_id,
                    updates={"purchase_price": 300000}, source="client", set_by="test",
                )
                session.commit()  # facts_revision now 1

            result = SufficiencyResult(
                verdict="insufficient",
                gaps=[Gap(
                    fact_key="rehab_estimate", display_name="Rehab Estimate",
                    reason="client must supply", gap_type="client_gap",
                )],
                opportunity_id=opp_id, facts_revision=0,  # stale — DB is now 1
            )
            with patch(
                "src.services.relay.exceptions_alert_queue.post_exceptions_alert"
            ) as mock_slack:
                _handle_insufficient(
                    opportunity_id=opp_id,
                    opportunity_type="rehab",
                    current_stage="qualifying",
                    result=result,
                )
            mock_slack.assert_not_called()
        finally:
            _cleanup_opportunity(opp_id)

    def test_current_result_alerts_normally(self):
        from src.agents.fa_max.qualification_worker import _handle_insufficient
        from src.services.fa_max_qualification import SufficiencyResult, Gap

        opp_id = _seed_open_opportunity("rehab")
        try:
            result = SufficiencyResult(
                verdict="insufficient",
                gaps=[Gap(
                    fact_key="purchase_price", display_name="Purchase Price",
                    reason="client must supply", gap_type="client_gap",
                )],
                opportunity_id=opp_id, facts_revision=0,  # matches the seeded facts row
            )
            with patch(
                "src.services.relay.exceptions_alert_queue.post_exceptions_alert",
                return_value=True,
            ) as mock_slack:
                _handle_insufficient(
                    opportunity_id=opp_id,
                    opportunity_type="rehab",
                    current_stage="qualifying",
                    result=result,
                )
            mock_slack.assert_called_once()
        finally:
            _cleanup_opportunity(opp_id)


class TestSufficientPendingContractCancelsResolvedAlert:
    def test_cancels_when_revision_current(self):
        """code-review finding, fourth round flagged the missing call; sixth
        round replaced the unlocked freshness pre-check with the real
        locked check, tested here against a real seeded opportunity and a
        real pending alert row."""
        from src.agents.fa_max.qualification_worker import _process_qualification_item
        from src.core.database import get_db_context

        opp_id = _seed_open_opportunity("refinance")
        try:
            with get_db_context() as session:
                session.execute(
                    text(
                        "INSERT INTO fa_max_exceptions_alert_queue"
                        " (venture_key, rule, message, status)"
                        " VALUES ('fa_max_lending', :rule, 'test', 'pending')"
                    ),
                    {"rule": f"qualification_gap:{opp_id}:deadbeef"},
                )
                session.commit()

            with patch(
                "src.agents.fa_max.qualification_worker.get_opportunity_facts",
                return_value={"facts_revision": 0, "estimated_value": 500000.0},
            ), patch(
                "src.agents.fa_max.qualification_worker.get_opportunity_state",
                return_value={
                    "opportunity_type": "refinance", "current_stage": "qualifying",
                    "state_version": 0, "person_id": "person-xyz",
                },
            ):
                _process_qualification_item(
                    opportunity_id=opp_id, claimed_revision=0,
                    claimed_checklist="1.2.1", worker_id="test-worker",
                )

            with get_db_context() as session:
                status = session.execute(
                    text(
                        "SELECT status FROM fa_max_exceptions_alert_queue"
                        " WHERE rule = :rule"
                    ),
                    {"rule": f"qualification_gap:{opp_id}:deadbeef"},
                ).scalar()
            assert status == "cancelled"
        finally:
            _cleanup_opportunity(opp_id)


class TestEnqueuePendingPreservesCallerTransaction:
    """code-review finding, seventh round, 2026-09: enqueue_pending()
    caught IntegrityError on a unique-index collision but never rolled
    back or used a savepoint -- Postgres marks the whole transaction
    aborted (InFailedSqlTransaction) regardless of the Python-level catch,
    so every later statement in the SAME transaction, including the
    caller's own commit, failed. Fixed with INSERT ... ON CONFLICT ...
    DO NOTHING, which never raises for the collision it's declared
    against, so no rollback/savepoint machinery is needed at all."""

    def test_collision_does_not_abort_caller_transaction(self):
        from src.services.relay.exceptions_alert_queue import enqueue_pending
        from src.core.database import get_db_context

        rule = "qualification_gap:opp-conflict-test:deadbeef"
        try:
            with get_db_context() as session:
                # First insert succeeds.
                first_id = enqueue_pending(
                    session=session, venture_key="fa_max_lending",
                    rule=rule, message="first",
                )
                assert first_id is not None

                # Second insert for the SAME (venture_key, rule) collides
                # with the partial unique index while still 'pending'.
                second_id = enqueue_pending(
                    session=session, venture_key="fa_max_lending",
                    rule=rule, message="second",
                )
                assert second_id is None

                # The transaction must still be usable -- an earlier write
                # AND a subsequent statement must both still commit.
                session.execute(
                    text(
                        "UPDATE fa_max_exceptions_alert_queue"
                        " SET message = 'still alive' WHERE rule = :rule"
                    ),
                    {"rule": rule},
                )
                session.commit()

            with get_db_context() as session:
                row = session.execute(
                    text(
                        "SELECT message FROM fa_max_exceptions_alert_queue WHERE rule = :rule"
                    ),
                    {"rule": rule},
                ).first()
            assert row is not None
            assert row[0] == "still alive"
        finally:
            with get_db_context() as cleanup:
                cleanup.execute(
                    text("DELETE FROM fa_max_exceptions_alert_queue WHERE rule = :rule"),
                    {"rule": rule},
                )
                cleanup.commit()
