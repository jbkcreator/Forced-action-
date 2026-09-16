from __future__ import annotations

from sqlalchemy import text

from src.services.cora_throughput import builder, decisions, standing_order_compiler
from tests.services.cora_throughput.conftest import fake_queue_item, seed_decided_item, seed_draft


def _standing_order_row(db, cell_id):
    return db.execute(
        text("SELECT id, active FROM cora_standing_orders WHERE cell_id = :cell_id"), {"cell_id": cell_id},
    ).mappings().first()


def test_five_clean_approvals_propose_a_standing_order(fresh_db, monkeypatch):
    monkeypatch.setattr(standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: None)
    for i in range(5):
        seed_decided_item(fresh_db, f"DRAFT-STREAK-{i}", "founder_tier_blitz")

    result = standing_order_compiler.compile_standing_orders(fresh_db)

    assert len(result["proposed"]) == 1
    assert result["proposed"][0]["cell_id"] == "founder_tier_blitz"
    row = _standing_order_row(fresh_db, "founder_tier_blitz")
    assert row is not None
    assert row["active"] is False


def test_compile_standing_orders_skips_when_cora_is_killed(fresh_db, monkeypatch):
    """STOP CORA / STOP CORA FOREVER must stop new standing-order proposals
    from being posted to the approvals channel too."""
    monkeypatch.setattr(standing_order_compiler, "cora_halted", lambda: True)
    posted = []
    monkeypatch.setattr(
        standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: posted.append(1) or None,
    )
    for i in range(5):
        seed_decided_item(fresh_db, f"DRAFT-STREAK-KILLED-{i}", "cell_kill_switch_gate_only")

    result = standing_order_compiler.compile_standing_orders(fresh_db)

    assert result == {"proposed": []}
    assert posted == []
    assert _standing_order_row(fresh_db, "cell_kill_switch_gate_only") is None


def test_fewer_than_five_approvals_does_not_propose(fresh_db, monkeypatch):
    monkeypatch.setattr(standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: None)
    for i in range(4):
        seed_decided_item(fresh_db, f"DRAFT-SHORT-{i}", "founder_tier_blitz")

    result = standing_order_compiler.compile_standing_orders(fresh_db)

    assert result["proposed"] == []
    assert _standing_order_row(fresh_db, "founder_tier_blitz") is None


def test_a_rejection_in_the_streak_blocks_proposal(fresh_db, monkeypatch):
    monkeypatch.setattr(standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: None)
    for i in range(4):
        seed_decided_item(fresh_db, f"DRAFT-BROKEN-{i}", "founder_tier_blitz")
    seed_decided_item(fresh_db, "DRAFT-BROKEN-4", "founder_tier_blitz", decision="exception_rejected", batch_status="partial")

    result = standing_order_compiler.compile_standing_orders(fresh_db)

    assert result["proposed"] == []
    assert _standing_order_row(fresh_db, "founder_tier_blitz") is None


def test_existing_proposal_is_never_duplicated(fresh_db, monkeypatch):
    monkeypatch.setattr(standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: None)
    for i in range(5):
        seed_decided_item(fresh_db, f"DRAFT-DUP-{i}", "founder_tier_blitz")

    first = standing_order_compiler.compile_standing_orders(fresh_db)
    assert len(first["proposed"]) == 1

    for i in range(5, 10):
        seed_decided_item(fresh_db, f"DRAFT-DUP-{i}", "founder_tier_blitz")
    second = standing_order_compiler.compile_standing_orders(fresh_db)

    assert second["proposed"] == []
    count = fresh_db.execute(
        text("SELECT count(*) FROM cora_standing_orders WHERE cell_id = 'founder_tier_blitz'"),
    ).scalar()
    assert count == 1


def test_ratifying_a_standing_order_makes_builder_auto_approve_future_matches(fresh_db, monkeypatch):
    monkeypatch.setattr(standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: None)
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: None)
    enqueue_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item())[1])
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    for i in range(5):
        seed_decided_item(fresh_db, f"DRAFT-RATIFY-{i}", "founder_tier_blitz")
    proposal = standing_order_compiler.compile_standing_orders(fresh_db)
    standing_order_id = proposal["proposed"][0]["standing_order_id"]

    result = decisions.record_standing_order_decision(fresh_db, standing_order_id, "ratify_standing_order", decided_by="U123")
    assert result["ok"] is True

    seed_draft(fresh_db, "DRAFT-RATIFY-NEW", cell_id="founder_tier_blitz")
    batch_result = builder.build_batch(fresh_db)

    assert batch_result["created"] is False
    assert batch_result["reason"] == "all_covered_by_standing_orders"
    assert batch_result["auto_approved_count"] == 1
    assert len(enqueue_calls) == 1
    status = fresh_db.execute(
        text("SELECT status FROM outbound_drafts WHERE draft_id = 'DRAFT-RATIFY-NEW'"),
    ).scalar()
    assert status == "approved_pending_send"


def test_declining_a_standing_order_allows_a_future_streak_to_propose_again(fresh_db, monkeypatch):
    monkeypatch.setattr(standing_order_compiler, "_post_standing_order_proposal", lambda *a, **k: None)
    for i in range(5):
        seed_decided_item(fresh_db, f"DRAFT-DECLINE-{i}", "founder_tier_blitz")
    proposal = standing_order_compiler.compile_standing_orders(fresh_db)
    standing_order_id = proposal["proposed"][0]["standing_order_id"]

    decline_result = decisions.record_standing_order_decision(fresh_db, standing_order_id, "decline_standing_order", decided_by="U123")
    assert decline_result["ok"] is True
    assert _standing_order_row(fresh_db, "founder_tier_blitz") is None

    for i in range(5, 10):
        seed_decided_item(fresh_db, f"DRAFT-DECLINE-{i}", "founder_tier_blitz")
    second_proposal = standing_order_compiler.compile_standing_orders(fresh_db)

    assert len(second_proposal["proposed"]) == 1
