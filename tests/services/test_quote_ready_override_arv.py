"""Regression tests for the Override ARV wiring (Low finding on PR #299
review: override_arv_result() was fully built end-to-end -- models,
migration, validation, audit fields -- but had no button, endpoint, or CLI
that ever invoked it. This wires it into the dossier card's real Slack
surface via dossier.open_override_arv_modal() / handle_override_arv_submission().

Tests that require real Postgres use the `fresh_db` fixture (per conftest.py)
and are automatically skipped when DATABASE_URL is not configured.
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

from decimal import Decimal

from sqlalchemy import text


def _make_property_with_arv(session) -> tuple[int, str]:
    """Inserts a minimal properties row plus a 'computed' fa_max_arv_results
    row, and returns (property_id, arv_result_id)."""
    property_id = session.execute(
        text(
            "INSERT INTO properties (parcel_id, needs_rescore, created_at, updated_at) "
            "VALUES (:p, false, now(), now()) RETURNING id"
        ),
        {"p": f"test-override-arv-{os.urandom(4).hex()}"},
    ).scalar()

    arv_result_id = session.execute(
        text(
            """
            INSERT INTO fa_max_arv_results
                (property_id, low, high, point, confidence, comp_count, weak_comp,
                 source, arv_unknown, calculation_version, input_hash, status)
            VALUES
                (:property_id, 200000, 260000, 230000, 'medium', 3, false,
                 'test', false, 'v1', 'test-hash', 'computed')
            RETURNING arv_result_id::text
            """
        ),
        {"property_id": property_id},
    ).scalar()
    session.commit()
    return property_id, arv_result_id


def _make_quote_ready_result_for_property(session, property_id: int) -> str:
    session.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = session.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    session.execute(
        text("""
            INSERT INTO fa_max_opportunities
                (person_id, opportunity_type, current_stage, source)
            VALUES (:person_id ::uuid, 'acquisition', 'new', 'test')
        """),
        {"person_id": person_id},
    )
    opportunity_id = session.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities ORDER BY created_at DESC LIMIT 1")
    ).scalar()

    session.execute(
        text("""
            INSERT INTO fa_max_quote_ready_results
                (opportunity_id, property_id, calculation_version, input_hash, status,
                 inputs, outputs, provenance, confidence, computed_by)
            VALUES
                (:opportunity_id ::uuid, :property_id, 'v1', 'test-hash', 'computed',
                 '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'test')
        """),
        {"opportunity_id": opportunity_id, "property_id": property_id},
    )
    result_id = session.execute(
        text(
            "SELECT result_id::text FROM fa_max_quote_ready_results "
            "WHERE opportunity_id = :opportunity_id ::uuid ORDER BY computed_at DESC LIMIT 1"
        ),
        {"opportunity_id": opportunity_id},
    ).scalar()
    session.commit()
    return result_id


def _values(low: str, point: str, high: str, reason: str) -> dict:
    return {
        "override_low_block": {"override_low": {"value": low}},
        "override_point_block": {"override_point": {"value": point}},
        "override_high_block": {"override_high": {"value": high}},
        "reason_block": {"reason": {"value": reason}},
    }


def test_dossier_card_includes_override_button_when_arv_published(fresh_db):
    from src.services.quote_ready.dossier import _build_dossier_blocks, assemble_dossier_row

    property_id, arv_result_id = _make_property_with_arv(fresh_db)
    result_id = _make_quote_ready_result_for_property(fresh_db, property_id)

    row = assemble_dossier_row(fresh_db, result_id)
    blocks = _build_dossier_blocks(row, "text")
    action_ids = [el["action_id"] for el in blocks[1]["elements"]]

    assert "quote_ready_override_arv" in action_ids


def test_dossier_card_omits_override_button_when_no_published_arv(fresh_db):
    from src.services.quote_ready.dossier import _build_dossier_blocks, assemble_dossier_row

    # A quote-ready result with no property_id -> assemble_dossier_row sets arv=None.
    fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
    """))
    person_id = fresh_db.execute(
        text("SELECT person_id::text FROM fa_max_persons ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities (person_id, opportunity_type, current_stage, source)
            VALUES (:person_id ::uuid, 'acquisition', 'new', 'test')
        """),
        {"person_id": person_id},
    )
    opportunity_id = fresh_db.execute(
        text("SELECT opportunity_id::text FROM fa_max_opportunities ORDER BY created_at DESC LIMIT 1")
    ).scalar()
    fresh_db.execute(
        text("""
            INSERT INTO fa_max_quote_ready_results
                (opportunity_id, calculation_version, input_hash, status,
                 inputs, outputs, provenance, confidence, computed_by)
            VALUES
                (:opportunity_id ::uuid, 'v1', 'test-hash', 'computed',
                 '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'test')
        """),
        {"opportunity_id": opportunity_id},
    )
    result_id = fresh_db.execute(
        text("SELECT result_id::text FROM fa_max_quote_ready_results "
             "WHERE opportunity_id = :opportunity_id ::uuid ORDER BY computed_at DESC LIMIT 1"),
        {"opportunity_id": opportunity_id},
    ).scalar()
    fresh_db.commit()

    from src.services.quote_ready.dossier import _build_dossier_blocks, assemble_dossier_row
    row = assemble_dossier_row(fresh_db, result_id)
    blocks = _build_dossier_blocks(row, "text")
    action_ids = [el["action_id"] for el in blocks[1]["elements"]]

    assert "quote_ready_override_arv" not in action_ids


def test_handle_override_arv_submission_applies_and_is_reflected_in_published_arv(fresh_db):
    from src.services.quote_ready.dossier import handle_override_arv_submission
    from src.services.quote_ready.arv_persistence import get_published_arv

    property_id, arv_result_id = _make_property_with_arv(fresh_db)

    outcome = handle_override_arv_submission(
        fresh_db,
        values=_values("210000", "235000", "265000", "Manual comp review — subject undervalued by algo"),
        metadata={"arv_result_id": arv_result_id},
        submitted_by="U1",
    )

    assert outcome == {"ok": True}

    published = get_published_arv(fresh_db, property_id)
    assert published is not None
    assert published.point == Decimal("235000.00")
    assert published.overridden is True


def test_handle_override_arv_submission_requires_reason(fresh_db):
    from src.services.quote_ready.dossier import handle_override_arv_submission

    _, arv_result_id = _make_property_with_arv(fresh_db)

    outcome = handle_override_arv_submission(
        fresh_db,
        values=_values("210000", "235000", "265000", ""),
        metadata={"arv_result_id": arv_result_id},
        submitted_by="U1",
    )

    assert outcome["ok"] is False
    assert "reason_block" in outcome["error"]


def test_handle_override_arv_submission_rejects_inverted_range(fresh_db):
    from src.services.quote_ready.dossier import handle_override_arv_submission

    _, arv_result_id = _make_property_with_arv(fresh_db)

    outcome = handle_override_arv_submission(
        fresh_db,
        values=_values("300000", "235000", "265000", "typo test"),
        metadata={"arv_result_id": arv_result_id},
        submitted_by="U1",
    )

    assert outcome["ok"] is False


def test_second_override_on_same_arv_result_is_rejected_not_overwritten(fresh_db):
    """override_arv_result()'s own WHERE guard (status='computed') is the
    data-layer backstop -- once overridden, a second override attempt on
    the same arv_result_id must fail, same 'first decision wins' pattern as
    decide_quote_ready()."""
    from src.services.quote_ready.dossier import handle_override_arv_submission

    _, arv_result_id = _make_property_with_arv(fresh_db)

    first = handle_override_arv_submission(
        fresh_db,
        values=_values("210000", "235000", "265000", "first override"),
        metadata={"arv_result_id": arv_result_id},
        submitted_by="U1",
    )
    assert first == {"ok": True}

    second = handle_override_arv_submission(
        fresh_db,
        values=_values("100000", "150000", "200000", "second, conflicting override"),
        metadata={"arv_result_id": arv_result_id},
        submitted_by="U2",
    )
    assert second["ok"] is False
