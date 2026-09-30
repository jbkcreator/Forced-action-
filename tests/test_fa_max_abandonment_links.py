"""WP-T2-6 abandonment — fired touches carry real links, never placeholder text.

Decisions: max-task/gap-audit-fixes/GRILL-DECISIONS.md (G2 + G6).
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import re

import pytest
from sqlalchemy import text

_BASE = "https://app.example.test"


@pytest.fixture
def links_base(monkeypatch):
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: _BASE)
    alerts = []
    monkeypatch.setattr(
        links_mod, "alert_link_unresolved",
        lambda **kw: alerts.append(kw),
    )
    return links_mod, alerts


def _seed(session) -> tuple[str, str]:
    person_id = session.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'identified', 'test')
        RETURNING person_id::text
    """)).scalar()
    opportunity_id = session.execute(
        text("""
            INSERT INTO fa_max_opportunities (person_id, opportunity_type, current_stage, source)
            VALUES (:pid ::uuid, 'acquisition', 'new', 'test')
            RETURNING opportunity_id::text
        """),
        {"pid": person_id},
    ).scalar()
    return person_id, opportunity_id


def _make_all_due(session, person_id: str) -> None:
    session.execute(
        text("UPDATE abandonment_sequences SET due_at = NOW() - interval '1 minute' WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    )


def _queued_bodies(session, person_id: str) -> list[str]:
    return list(session.execute(
        text("""
            SELECT payload->>'body' FROM relay_approval_queue
            WHERE person_id = :pid ::uuid AND payload->>'type' = 'abandonment_touch'
            ORDER BY (payload->>'touch_number')::int
        """),
        {"pid": person_id},
    ).scalars())


def test_fired_touches_contain_real_links_and_no_placeholders(fresh_db, links_base):
    from src.agents.reply_concierge.abandonment_agent import enqueue_sequence, fire_due_touches

    person_id, opportunity_id = _seed(fresh_db)
    enqueue_sequence(person_id, "borrower@example.test", fresh_db, opportunity_id=opportunity_id)
    _make_all_due(fresh_db, person_id)

    fire_due_touches(fresh_db)

    bodies = _queued_bodies(fresh_db, person_id)
    assert len(bodies) == 5
    for body in bodies:
        assert not re.search(r"\[[^\]]*link\]", body), body
        assert "{" not in body and "}" not in body, body
        assert f"{_BASE}/book/" in body, body
    assert all(f"{_BASE}/go/" in b for b in bodies[:4])


def test_touch_not_queued_when_links_unresolved(fresh_db, links_base, monkeypatch):
    from src.agents.reply_concierge.abandonment_agent import enqueue_sequence, fire_due_touches

    links_mod, alerts = links_base
    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "")
    person_id, opportunity_id = _seed(fresh_db)
    enqueue_sequence(person_id, "borrower@example.test", fresh_db, opportunity_id=opportunity_id)
    _make_all_due(fresh_db, person_id)

    fire_due_touches(fresh_db)

    assert _queued_bodies(fresh_db, person_id) == []
    assert alerts, "link_unresolved must be surfaced to EXCEPTIONS"
    reasons = set(fresh_db.execute(
        text("SELECT cancel_reason FROM abandonment_sequences WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalars())
    assert reasons == {"link_unresolved"}
