"""
CLONE-v2.2 CL2 — golden CLOSE library writer (src/services/golden_close_writer.py).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from src.services.golden_close_writer import transition_status, upsert_golden_close_chain


def _seed_deal(db) -> int:
    return db.execute(text("""
        INSERT INTO deal_outcomes (deal_size_bucket, pipeline_stage, created_at)
        VALUES ('10_25k', 'closed_won', NOW())
        RETURNING id
    """)).scalar_one()


def _cleanup(db, deal_id: int) -> None:
    db.execute(text("DELETE FROM golden_close_chains WHERE deal_id = :id"), {"id": deal_id})
    db.execute(text("DELETE FROM deal_outcomes WHERE id = :id"), {"id": deal_id})
    db.commit()


def test_new_chain_is_created_with_defaults(fresh_db):
    deal_id = _seed_deal(fresh_db)
    try:
        chain_id = upsert_golden_close_chain(
            fresh_db,
            deal_id=deal_id,
            chain_stages=[{"stage": "first_signal", "occurred_at": "2026-01-01T00:00:00Z",
                           "source_table": "foreclosures", "source_id": 1, "summary": "NOD filed"}],
            authored_by="cl2_test",
        )
        assert chain_id is not None

        row = fresh_db.execute(
            text("SELECT venture, status, schema_version FROM golden_close_chains WHERE id = :id"),
            {"id": chain_id},
        ).one()
        assert row.venture == "hillsborough_distress"
        assert row.status == "draft"
        assert row.schema_version == 1
    finally:
        _cleanup(fresh_db, deal_id)


def test_second_call_same_deal_and_venture_updates_in_place(fresh_db):
    deal_id = _seed_deal(fresh_db)
    try:
        first_id = upsert_golden_close_chain(
            fresh_db,
            deal_id=deal_id,
            chain_stages=[{"stage": "first_signal", "occurred_at": "2026-01-01T00:00:00Z",
                           "source_table": "foreclosures", "source_id": 1, "summary": "NOD filed"}],
            authored_by="cl2_test",
        )
        second_id = upsert_golden_close_chain(
            fresh_db,
            deal_id=deal_id,
            chain_stages=[
                {"stage": "first_signal", "occurred_at": "2026-01-01T00:00:00Z",
                 "source_table": "foreclosures", "source_id": 1, "summary": "NOD filed"},
                {"stage": "payment", "occurred_at": "2026-02-01T00:00:00Z",
                 "source_table": "platform_revenue_ledger", "source_id": 99, "summary": "closed"},
            ],
            authored_by="cl2_test",
            status="verified",
        )

        assert first_id == second_id
        assert fresh_db.execute(
            text("SELECT COUNT(*) FROM golden_close_chains WHERE deal_id = :id"), {"id": deal_id},
        ).scalar_one() == 1

        row = fresh_db.execute(
            text("SELECT status, jsonb_array_length(chain_stages) AS n_stages FROM golden_close_chains WHERE id = :id"),
            {"id": first_id},
        ).one()
        assert row.status == "verified"
        assert row.n_stages == 2
    finally:
        _cleanup(fresh_db, deal_id)


def test_different_venture_same_deal_gets_its_own_row(fresh_db):
    deal_id = _seed_deal(fresh_db)
    try:
        hillsborough_id = upsert_golden_close_chain(
            fresh_db, deal_id=deal_id, chain_stages=[], authored_by="cl2_test",
        )
        second_venture_id = upsert_golden_close_chain(
            fresh_db, deal_id=deal_id, chain_stages=[], authored_by="cl2_test",
            venture="second_venture",
        )
        assert hillsborough_id != second_venture_id
        assert fresh_db.execute(
            text("SELECT COUNT(*) FROM golden_close_chains WHERE deal_id = :id"), {"id": deal_id},
        ).scalar_one() == 2
    finally:
        _cleanup(fresh_db, deal_id)


def test_invalid_status_raises(fresh_db):
    deal_id = _seed_deal(fresh_db)
    try:
        with pytest.raises(ValueError):
            upsert_golden_close_chain(
                fresh_db, deal_id=deal_id, chain_stages=[], authored_by="cl2_test",
                status="not_a_real_status",
            )
    finally:
        _cleanup(fresh_db, deal_id)


def test_transition_status_moves_forward_and_blocks_after_retired(fresh_db):
    deal_id = _seed_deal(fresh_db)
    try:
        chain_id = upsert_golden_close_chain(
            fresh_db, deal_id=deal_id, chain_stages=[], authored_by="cl2_test",
        )
        assert transition_status(fresh_db, chain_id, to_status="verified") is True
        assert transition_status(fresh_db, chain_id, to_status="promoted_to_playbook") is True
        assert transition_status(fresh_db, chain_id, to_status="retired") is True
        assert transition_status(fresh_db, chain_id, to_status="draft") is False

        status = fresh_db.execute(
            text("SELECT status FROM golden_close_chains WHERE id = :id"), {"id": chain_id},
        ).scalar_one()
        assert status == "retired"
    finally:
        _cleanup(fresh_db, deal_id)
