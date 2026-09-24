"""Regression tests for decide_quote_ready()'s data-layer terminality.

Covers the Critical review finding on PR #299 (WP-8B/T2-2 fixes): the WHERE
guard previously used `review_status IS DISTINCT FROM :review_status`, which
let a SECOND, CONFLICTING decision (e.g. Reject after an earlier Approve) on
the same result_id silently overwrite the first, since 'rejected' is distinct
from 'approved'. Fixed by requiring `review_status IS NULL`, so only the
FIRST decision on a result_id ever applies.

Tests that require real Postgres use the `fresh_db` fixture (per conftest.py)
and are automatically skipped when DATABASE_URL is not configured.
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

from sqlalchemy import text


def _make_computed_quote_ready_result(session) -> str:
    """Inserts a minimal person -> opportunity -> quote_ready_result chain
    with status='computed', review_status=NULL, and returns the new
    result_id. Mirrors the insert pattern already used in
    tests/test_fa_max_state_engine.py for opportunity-linked fixtures."""
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
                (opportunity_id, calculation_version, input_hash, status,
                 inputs, outputs, provenance, confidence, computed_by)
            VALUES
                (:opportunity_id ::uuid, 'v1', 'test-hash', 'computed',
                 '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'test')
        """),
        {"opportunity_id": opportunity_id},
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


def test_first_decision_applies(fresh_db):
    from src.services.quote_ready.dossier import decide_quote_ready

    result_id = _make_computed_quote_ready_result(fresh_db)

    applied = decide_quote_ready(fresh_db, result_id=result_id, decision="approved", decided_by="U1")
    fresh_db.commit()

    assert applied is True
    row = fresh_db.execute(
        text("SELECT review_status, reviewed_by FROM fa_max_quote_ready_results WHERE result_id = :rid ::uuid"),
        {"rid": result_id},
    ).mappings().first()
    assert row["review_status"] == "approved"
    assert row["reviewed_by"] == "U1"


def test_conflicting_second_decision_is_rejected_not_overwritten(fresh_db):
    """The Critical fix: Approve then Reject on the SAME result_id must not
    silently flip the decision. Before the fix, this second call returned
    True and left review_status='rejected' -- the exact silent-overwrite
    the review flagged."""
    from src.services.quote_ready.dossier import decide_quote_ready

    result_id = _make_computed_quote_ready_result(fresh_db)

    first = decide_quote_ready(fresh_db, result_id=result_id, decision="approved", decided_by="U1")
    fresh_db.commit()
    assert first is True

    second = decide_quote_ready(fresh_db, result_id=result_id, decision="rejected", decided_by="U2")
    fresh_db.commit()
    assert second is False, "a conflicting second decision must be refused, not silently applied"

    row = fresh_db.execute(
        text("SELECT review_status, reviewed_by FROM fa_max_quote_ready_results WHERE result_id = :rid ::uuid"),
        {"rid": result_id},
    ).mappings().first()
    assert row["review_status"] == "approved", "the original decision must survive the conflicting attempt"
    assert row["reviewed_by"] == "U1"


def test_repeat_same_decision_is_a_noop(fresh_db):
    """A duplicate click of the SAME decision (double-click/retry) is also
    a no-op under the new IS NULL guard -- unchanged end-user behavior from
    before the fix, just via a different WHERE clause."""
    from src.services.quote_ready.dossier import decide_quote_ready

    result_id = _make_computed_quote_ready_result(fresh_db)

    first = decide_quote_ready(fresh_db, result_id=result_id, decision="approved", decided_by="U1")
    fresh_db.commit()
    assert first is True

    repeat = decide_quote_ready(fresh_db, result_id=result_id, decision="approved", decided_by="U1")
    fresh_db.commit()
    assert repeat is False
