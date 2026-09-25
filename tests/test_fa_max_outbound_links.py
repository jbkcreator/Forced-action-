"""FA Max outbound links — every borrower touch carries a real booking + portal link.

Spec §Slack queues / §Calendar (item 9): "Calendar link on every outbound."
Decisions: max-task/gap-audit-fixes/GRILL-DECISIONS.md (G2 + G6).
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import pytest
from sqlalchemy import text


def _seed_person_opportunity(session) -> tuple[str, str]:
    person_id = session.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'portal_started', 'test')
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


def test_links_point_at_booking_page_and_selfserve_portal(fresh_db, monkeypatch):
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    links = links_mod.resolve_links(fresh_db, person_id=person_id, opportunity_id=opportunity_id)

    assert links.calendar_url.startswith("https://app.example.test/book/")
    assert links.portal_url.startswith("https://app.example.test/go/")


def test_links_are_reused_across_touches_for_the_same_opportunity(fresh_db, monkeypatch):
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    first = links_mod.resolve_links(fresh_db, person_id=person_id, opportunity_id=opportunity_id)
    second = links_mod.resolve_links(fresh_db, person_id=person_id, opportunity_id=opportunity_id)

    assert first == second


def test_links_unresolved_without_public_base_url(fresh_db, monkeypatch):
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "")
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    with pytest.raises(links_mod.LinkUnresolved):
        links_mod.resolve_links(fresh_db, person_id=person_id, opportunity_id=opportunity_id)


def test_stage_monitor_send_carries_booking_link(fresh_db, monkeypatch):
    from src.services import fa_max_file_state
    from src.services import fa_max_outbound_links as links_mod
    from src.services import fa_max_send_governance as governance

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    monkeypatch.setattr(governance, "require_consent", lambda *a, **k: type("C", (), {"allowed": True})())
    monkeypatch.setattr(governance, "suppression_reason", lambda *a, **k: None)
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    sent = fa_max_file_state.send_governed_email(
        fresh_db, opportunity_id=opportunity_id, person_id=person_id,
        contact_email="borrower@example.test", subject="Update", body="Still under review.",
        lane="RELATIONSHIPS", agent_name="stage_monitor", idempotency_key=f"t-{opportunity_id}",
    )

    assert sent is True
    body = fresh_db.execute(
        text("SELECT payload->>'body' FROM relay_approval_queue WHERE idempotency_key = :k"),
        {"k": f"t-{opportunity_id}"},
    ).scalar()
    assert "https://app.example.test/book/" in body


def test_stage_monitor_send_withheld_when_links_unresolved(fresh_db, monkeypatch):
    from src.services import fa_max_file_state
    from src.services import fa_max_outbound_links as links_mod
    from src.services import fa_max_send_governance as governance

    alerts = []
    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "")
    monkeypatch.setattr(links_mod, "alert_link_unresolved", lambda **kw: alerts.append(kw))
    monkeypatch.setattr(governance, "require_consent", lambda *a, **k: type("C", (), {"allowed": True})())
    monkeypatch.setattr(governance, "suppression_reason", lambda *a, **k: None)
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    sent = fa_max_file_state.send_governed_email(
        fresh_db, opportunity_id=opportunity_id, person_id=person_id,
        contact_email="borrower@example.test", subject="Update", body="Still under review.",
        lane="RELATIONSHIPS", agent_name="stage_monitor", idempotency_key=f"t-{opportunity_id}",
    )

    assert sent is False
    assert alerts


def test_concierge_reply_carries_booking_link(fresh_db, monkeypatch):
    from src.agents.reply_concierge import router as concierge
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    queue_id = concierge._queue_reply(
        person_id=person_id, opportunity_id=opportunity_id,
        contact_email="borrower@example.test", reply_text="The portal saves your progress.",
        kb_topic_key="portal_save", channel="email", auto_send=False, db=fresh_db,
    )

    reply = fresh_db.execute(
        text("SELECT payload->>'reply_text' FROM relay_approval_queue WHERE id = :id"), {"id": queue_id},
    ).scalar()
    assert "https://app.example.test/book/" in reply


def test_concierge_reply_withheld_when_links_unresolved(fresh_db, monkeypatch):
    from src.agents.reply_concierge import router as concierge
    from src.services import fa_max_outbound_links as links_mod

    alerts = []
    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "")
    monkeypatch.setattr(links_mod, "alert_link_unresolved", lambda **kw: alerts.append(kw))
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    queue_id = concierge._queue_reply(
        person_id=person_id, opportunity_id=opportunity_id,
        contact_email="borrower@example.test", reply_text="The portal saves your progress.",
        kb_topic_key="portal_save", channel="email", auto_send=False, db=fresh_db,
    )

    assert queue_id is None
    assert alerts


def test_slug_collision_keeps_callers_pending_work(fresh_db, monkeypatch):
    from src.services import fa_max_outbound_links as links_mod
    from src.services import tracked_links

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)
    taken = links_mod.resolve_links(fresh_db, person_id=person_id, opportunity_id=opportunity_id)
    taken_slug = taken.calendar_url.rsplit("/", 1)[-1]
    other_person, other_opp = _seed_person_opportunity(fresh_db)
    slugs = iter([taken_slug, "fresh-slug-for-test"])
    monkeypatch.setattr(tracked_links.secrets, "token_urlsafe", lambda _n: next(slugs))

    links = links_mod.resolve_links(fresh_db, person_id=other_person, opportunity_id=other_opp)

    assert links.calendar_url.endswith("/book/fresh-slug-for-test")
    still_there = fresh_db.execute(
        text("SELECT COUNT(*) FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"),
        {"oid": other_opp},
    ).scalar()
    assert still_there == 1


def test_booking_line_goes_above_compliance_footer():
    from src.services.fa_max_outbound_links import OutboundLinks, add_booking_line

    links = OutboundLinks(calendar_url="https://x/book/s", portal_url="https://x/go/s")
    body = add_booking_line("Hi,\n\nAnswer.\n\n---\nJosh Kantor / Reply STOP to opt out.", links)

    assert body.index("https://x/book/s") < body.index("\n---\n")


def test_concierge_reply_booking_line_sits_above_footer(fresh_db, monkeypatch):
    from src.agents.reply_concierge import router as concierge
    from src.services import fa_max_outbound_links as links_mod

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    queue_id = concierge._queue_reply(
        person_id=person_id, opportunity_id=opportunity_id, contact_email="borrower@example.test",
        reply_text="Hi,\n\nAnswer.\n\n---\nJosh Kantor / Reply STOP to opt out.",
        kb_topic_key="portal_save", channel="email", auto_send=False, db=fresh_db,
    )

    reply = fresh_db.execute(
        text("SELECT payload->>'reply_text' FROM relay_approval_queue WHERE id = :id"), {"id": queue_id},
    ).scalar()
    assert reply.index("https://app.example.test/book/") < reply.index("\n---\n")


def test_agent_tool_send_carries_booking_link(fresh_db, monkeypatch):
    from src.agents.fa_max import tool_registry
    from src.services import fa_max_autonomy
    from src.services import fa_max_outbound_links as links_mod
    from src.services.relay import queue as relay_queue

    monkeypatch.setattr(links_mod, "_public_base_url", lambda: "https://app.example.test")
    monkeypatch.setattr(fa_max_autonomy, "check_tier_gate",
                        lambda *a, **k: type("G", (), {"allowed": False, "outcome": type("O", (), {"value": "gated"})()})())
    captured = {}

    def _fake_enqueue(**kw):
        captured.update(kw)
        return type("I", (), {"id": 1, "status": "approved"})()

    monkeypatch.setattr(relay_queue, "enqueue", _fake_enqueue)
    person_id, opportunity_id = _seed_person_opportunity(fresh_db)

    tool_registry.send(
        idempotency_key="t-tool", channel="email", recipient="borrower@example.test",
        payload={"subject": "Hi", "body": "Following up."}, agent_name="cora", lane="MONEY",
        autonomy_tier_at_send="A", person_id=person_id, opportunity_id=opportunity_id, session=fresh_db,
    )

    assert "https://app.example.test/book/" in captured["payload"]["body"]


def test_booking_line_is_added_only_once():
    from src.services.fa_max_outbound_links import OutboundLinks, add_booking_line

    links = OutboundLinks(calendar_url="https://x/book/s", portal_url="https://x/go/s")
    once = add_booking_line("Following up.\n\n---\nReply STOP to opt out.", links)

    twice = add_booking_line(once, links)

    assert twice == once
    assert twice.count("https://x/book/s") == 1
