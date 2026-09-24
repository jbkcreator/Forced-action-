"""WP-T2-6 abandonment — the sequence stops once the borrower completes the portal.

Spec §Abandonment (item 21): halt when the portal is completed, a reply
arrives, or an opt-out is detected. Decisions: GRILL-DECISIONS.md G5.
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import pytest
from sqlalchemy import text


@pytest.fixture(autouse=True)
def _links(monkeypatch):
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")


def _seed(session, lifecycle_state: str = "portal_started") -> tuple[str, str]:
    person_id = session.execute(
        text("""
            INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
            VALUES (gen_random_uuid(), :state, 'test')
            RETURNING person_id::text
        """),
        {"state": lifecycle_state},
    ).scalar()
    opportunity_id = session.execute(
        text("""
            INSERT INTO fa_max_opportunities (person_id, opportunity_type, current_stage, source)
            VALUES (:pid ::uuid, 'acquisition', 'new', 'test')
            RETURNING opportunity_id::text
        """),
        {"pid": person_id},
    ).scalar()
    return person_id, opportunity_id


def _start_sequence(session, person_id: str, opportunity_id: str) -> None:
    from src.agents.reply_concierge.abandonment_agent import enqueue_sequence

    enqueue_sequence(person_id, "borrower@example.test", session, opportunity_id=opportunity_id)


def _make_all_due(session, person_id: str) -> None:
    session.execute(
        text("UPDATE abandonment_sequences SET due_at = NOW() - interval '1 minute' WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    )


def _pending_count(session, person_id: str) -> int:
    return session.execute(
        text("""
            SELECT COUNT(*) FROM abandonment_sequences
            WHERE person_id = :pid ::uuid AND sent_at IS NULL AND cancelled_at IS NULL
        """),
        {"pid": person_id},
    ).scalar()


def _queued_touches(session, person_id: str) -> int:
    return session.execute(
        text("""
            SELECT COUNT(*) FROM relay_approval_queue
            WHERE person_id = :pid ::uuid AND payload->>'type' = 'abandonment_touch'
        """),
        {"pid": person_id},
    ).scalar()


def test_selfserve_handoff_stops_due_touches(fresh_db):
    from src.agents.reply_concierge.abandonment_agent import fire_due_touches

    person_id, opportunity_id = _seed(fresh_db)
    _start_sequence(fresh_db, person_id, opportunity_id)
    fresh_db.execute(
        text("""
            INSERT INTO selfserve_sessions (token, person_id, status, handed_off_at, prefill_snapshot)
            VALUES (gen_random_uuid(), :pid ::uuid, 'handed_off', NOW(), '{}'::jsonb)
        """),
        {"pid": person_id},
    )
    _make_all_due(fresh_db, person_id)

    fire_due_touches(fresh_db)

    assert _queued_touches(fresh_db, person_id) == 0
    assert _pending_count(fresh_db, person_id) == 0


def test_backflip_file_created_halts_sequence(fresh_db):
    from src.services.fa_max_file_state import ensure_file_state

    person_id, opportunity_id = _seed(fresh_db)
    _start_sequence(fresh_db, person_id, opportunity_id)

    ensure_file_state(fresh_db, opportunity_id=opportunity_id, person_id=person_id,
                      contact_email="borrower@example.test")

    assert _pending_count(fresh_db, person_id) == 0


def test_lifecycle_application_submitted_halts_sequence(fresh_db):
    from src.services.state_engine import ensure_entity_registry, transition

    person_id, opportunity_id = _seed(fresh_db)
    _start_sequence(fresh_db, person_id, opportunity_id)
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    transition(
        session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
        from_state="portal_started", to_state="application_submitted",
        actor="test", source_component="test", idempotency_key=f"t-{person_id}",
        acquire_redis_lock=False,
    )

    assert _pending_count(fresh_db, person_id) == 0


def test_stale_handoff_from_before_the_sequence_does_not_halt(fresh_db):
    from src.agents.reply_concierge.abandonment_agent import fire_due_touches

    person_id, opportunity_id = _seed(fresh_db)
    fresh_db.execute(
        text("""
            INSERT INTO selfserve_sessions (token, person_id, status, handed_off_at, prefill_snapshot)
            VALUES (gen_random_uuid(), :pid ::uuid, 'handed_off', NOW() - interval '30 days', '{}'::jsonb)
        """),
        {"pid": person_id},
    )
    _start_sequence(fresh_db, person_id, opportunity_id)
    _make_all_due(fresh_db, person_id)

    fire_due_touches(fresh_db)

    assert _queued_touches(fresh_db, person_id) == 5


def test_lifecycle_move_beyond_application_submitted_halts_sequence(fresh_db):
    from src.services.state_engine import ensure_entity_registry, transition

    person_id, opportunity_id = _seed(fresh_db, lifecycle_state="application_submitted")
    _start_sequence(fresh_db, person_id, opportunity_id)
    entity_uuid = ensure_entity_registry(session=fresh_db, entity_type="person", native_id=person_id)

    transition(
        session=fresh_db, entity_type="person", entity_uuid=entity_uuid,
        from_state="application_submitted", to_state="term_sheet_issued",
        actor="test", source_component="test", idempotency_key=f"t2-{person_id}",
        acquire_redis_lock=False,
    )

    assert _pending_count(fresh_db, person_id) == 0
