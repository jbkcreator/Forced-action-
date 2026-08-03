from __future__ import annotations

from types import SimpleNamespace

from src.agents.cora import store


def seed_draft(db, draft_id: str, *, cell_id: str = "founder_tier_blitz", channel: str = "email",
                contact_email: str = "prospect@example.com", contact_phone: str = None,
                opportunity_thread_id: str = None) -> None:
    store.append_draft(db, store.OutboundDraftRecord(
        draft_id=draft_id,
        opportunity_thread_id=opportunity_thread_id or f"OPP-{draft_id[:8]}",
        buyer_entity_id=1, cell_id=cell_id, offer="founder_tier", avenue="flippers",
        angle="scarcity_seat_number", subject="Founding seat", body="Noticed your purchases.",
        facts_used=[], source_refs=[], recommended_channel=channel, confidence_score=90,
        contact_email=contact_email, contact_phone=contact_phone,
    ))


def fake_queue_item(item_id: int = 1) -> SimpleNamespace:
    return SimpleNamespace(id=item_id)


def seed_decided_item(db, draft_id: str, cell_id: str, *, decision: str = "included",
                        batch_status: str = "approved") -> None:
    """
    Seeds one draft + one batch + one batch_item already in a terminal,
    decided state — for standing-order-compiler tests, which only care
    about approval-history shape, not about exercising the live
    approve/reject decision path itself.
    """
    from sqlalchemy import text

    seed_draft(db, draft_id, cell_id=cell_id)
    batch_id = f"BATCH-{draft_id}"
    db.execute(
        text("INSERT INTO cora_draft_batches (batch_id, status) VALUES (:batch_id, :status)"),
        {"batch_id": batch_id, "status": batch_status},
    )
    db.execute(
        text(
            "INSERT INTO cora_batch_items (batch_id, draft_id, decision, decided_at) "
            "VALUES (:batch_id, :draft_id, :decision, now())"
        ),
        {"batch_id": batch_id, "draft_id": draft_id, "decision": decision},
    )
    # A real decided item's draft would already have moved off status='draft'
    # (decisions.record_batch_decision does this) — mirror that here so a
    # historical "already decided" seed draft never looks eligible again to
    # builder.build_batch()'s own status='draft' query.
    final_status = "rejected" if decision == "exception_rejected" else "approved_pending_send"
    db.execute(
        text("UPDATE outbound_drafts SET status = :status WHERE draft_id = :draft_id"),
        {"status": final_status, "draft_id": draft_id},
    )
