from __future__ import annotations

from sqlalchemy import text

from src.services.cora_throughput import builder, decisions
from tests.services.cora_throughput.conftest import fake_queue_item, seed_draft


def _batch_row(db, batch_id):
    return db.execute(
        text("SELECT batch_id, status FROM cora_draft_batches WHERE batch_id = :batch_id"),
        {"batch_id": batch_id},
    ).mappings().first()


def _item_count(db, batch_id):
    return db.execute(
        text("SELECT count(*) FROM cora_batch_items WHERE batch_id = :batch_id"), {"batch_id": batch_id},
    ).scalar()


def test_build_batch_creates_batch_from_eligible_drafts(fresh_db, monkeypatch):
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: None)
    for i in range(3):
        seed_draft(fresh_db, f"DRAFT-BUILD-{i}")

    result = builder.build_batch(fresh_db)

    assert result["created"] is True
    assert result["item_count"] == 3
    row = _batch_row(fresh_db, result["batch_id"])
    assert row["status"] == "pending"
    assert _item_count(fresh_db, result["batch_id"]) == 3


def test_build_batch_caps_at_max_batch_size(fresh_db, monkeypatch):
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: None)
    for i in range(builder.MAX_BATCH_SIZE + 1):
        seed_draft(fresh_db, f"DRAFT-CAP-{i}")

    result = builder.build_batch(fresh_db)

    assert result["created"] is True
    assert result["item_count"] == builder.MAX_BATCH_SIZE
    assert _item_count(fresh_db, result["batch_id"]) == builder.MAX_BATCH_SIZE


def test_build_batch_skips_when_batch_already_pending(fresh_db, monkeypatch):
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: None)
    seed_draft(fresh_db, "DRAFT-PENDING-1")
    first = builder.build_batch(fresh_db)
    assert first["created"] is True

    seed_draft(fresh_db, "DRAFT-PENDING-2")
    second = builder.build_batch(fresh_db)

    assert second["created"] is False
    assert second["reason"] == "batch_already_pending"
    # The second eligible draft never got batched — still status='draft', not yet included anywhere.
    assert _item_count(fresh_db, first["batch_id"]) == 1


def test_build_batch_no_eligible_drafts(fresh_db):
    result = builder.build_batch(fresh_db)
    assert result == {"created": False, "reason": "no_eligible_drafts"}


def test_build_batch_auto_approves_standing_order_covered_drafts(fresh_db, monkeypatch):
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: None)
    enqueue_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item(1))[1])
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    fresh_db.execute(
        text("INSERT INTO cora_standing_orders (cell_id, rule_text, active) VALUES (:cell_id, 'auto', true)"),
        {"cell_id": "founder_tier_blitz"},
    )
    seed_draft(fresh_db, "DRAFT-STANDING-1", cell_id="founder_tier_blitz")
    seed_draft(fresh_db, "DRAFT-NORMAL-1", cell_id="auction_fast_follow")

    result = builder.build_batch(fresh_db)

    assert result["created"] is True
    assert result["item_count"] == 1  # only the non-covered draft reaches the Slack batch
    assert result["auto_approved_count"] == 1
    assert len(enqueue_calls) == 1

    status_row = fresh_db.execute(
        text("SELECT status FROM outbound_drafts WHERE draft_id = 'DRAFT-STANDING-1'"),
    ).first()
    assert status_row[0] == "approved_pending_send"


def test_expire_stale_batches_expires_old_pending_batch(fresh_db):
    fresh_db.execute(
        text(
            "INSERT INTO cora_draft_batches (batch_id, status, created_at) "
            "VALUES ('BATCH-STALE-1', 'pending', now() - interval '25 hours')"
        ),
    )
    fresh_db.execute(
        text("INSERT INTO cora_draft_batches (batch_id, status, created_at) VALUES ('BATCH-FRESH-1', 'pending', now())"),
    )

    expired = builder.expire_stale_batches(fresh_db)

    assert expired == 1
    assert _batch_row(fresh_db, "BATCH-STALE-1")["status"] == "expired"
    assert _batch_row(fresh_db, "BATCH-FRESH-1")["status"] == "pending"
