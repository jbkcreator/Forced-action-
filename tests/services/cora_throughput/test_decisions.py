from __future__ import annotations

from sqlalchemy import text

from src.agents.contracts.base import HandoffRejected
from src.agents.cora import store
from src.services.cora_throughput import decisions
from tests.services.cora_throughput.conftest import fake_queue_item, seed_draft


def _make_batch_with_items(db, batch_id: str, draft_ids: list) -> None:
    db.execute(text("INSERT INTO cora_draft_batches (batch_id, status) VALUES (:batch_id, 'pending')"), {"batch_id": batch_id})
    for draft_id in draft_ids:
        seed_draft(db, draft_id)
        db.execute(
            text("INSERT INTO cora_batch_items (batch_id, draft_id, decision) VALUES (:batch_id, :draft_id, 'included')"),
            {"batch_id": batch_id, "draft_id": draft_id},
        )


def _draft_status(db, draft_id: str) -> str:
    return db.execute(text("SELECT status FROM outbound_drafts WHERE draft_id = :draft_id"), {"draft_id": draft_id}).scalar()


def _batch_status(db, batch_id: str) -> str:
    return db.execute(text("SELECT status FROM cora_draft_batches WHERE batch_id = :batch_id"), {"batch_id": batch_id}).scalar()


def _item_decision(db, batch_id: str, draft_id: str) -> str:
    return db.execute(
        text("SELECT decision FROM cora_batch_items WHERE batch_id = :batch_id AND draft_id = :draft_id"),
        {"batch_id": batch_id, "draft_id": draft_id},
    ).scalar()


def test_approve_all_enqueues_every_item_and_marks_drafts_approved(fresh_db, monkeypatch):
    enqueue_calls = []
    decide_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item(len(enqueue_calls)))[1])
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda item_id, **kw: decide_calls.append((item_id, kw)))

    _make_batch_with_items(fresh_db, "BATCH-APPROVE-1", ["DRAFT-A1", "DRAFT-A2", "DRAFT-A3"])

    result = decisions.record_batch_decision(fresh_db, "BATCH-APPROVE-1", "approve_all", decided_by="U123")

    assert result == {"ok": True, "action": "approve_all", "batch_id": "BATCH-APPROVE-1", "approved_count": 3, "rejected_count": 0}
    assert len(enqueue_calls) == 3
    assert len(decide_calls) == 3
    for item_id, kw in decide_calls:
        assert kw == {"approved": True, "decided_by": "U123"}
    for draft_id in ("DRAFT-A1", "DRAFT-A2", "DRAFT-A3"):
        assert _draft_status(fresh_db, draft_id) == "approved_pending_send"
    assert _batch_status(fresh_db, "BATCH-APPROVE-1") == "approved"

    # idempotency_key derived from draft_id, thread_id passed through — the
    # actual Cora->Relay handoff contract this whole layer exists to satisfy.
    keys = {c["idempotency_key"] for c in enqueue_calls}
    assert keys == {"cora_draft:DRAFT-A1", "cora_draft:DRAFT-A2", "cora_draft:DRAFT-A3"}


def test_reject_item_marks_single_draft_rejected_batch_stays_pending(fresh_db, monkeypatch):
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: fake_queue_item())
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    _make_batch_with_items(fresh_db, "BATCH-REJECT-1", ["DRAFT-R1", "DRAFT-R2"])

    result = decisions.record_batch_decision(fresh_db, "BATCH-REJECT-1", "reject_item", decided_by="U123", draft_id="DRAFT-R1")

    assert result == {"ok": True, "action": "reject_item", "batch_id": "BATCH-REJECT-1", "draft_id": "DRAFT-R1"}
    assert _draft_status(fresh_db, "DRAFT-R1") == "rejected"
    assert _item_decision(fresh_db, "BATCH-REJECT-1", "DRAFT-R1") == "exception_rejected"
    # The rest of the batch is untouched — still open for the remaining tap.
    assert _draft_status(fresh_db, "DRAFT-R2") == "draft"
    assert _batch_status(fresh_db, "BATCH-REJECT-1") == "pending"


def test_approve_all_after_an_exception_reject_marks_batch_partial(fresh_db, monkeypatch):
    enqueue_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item())[1])
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    _make_batch_with_items(fresh_db, "BATCH-PARTIAL-1", ["DRAFT-P1", "DRAFT-P2"])
    decisions.record_batch_decision(fresh_db, "BATCH-PARTIAL-1", "reject_item", decided_by="U123", draft_id="DRAFT-P1")

    result = decisions.record_batch_decision(fresh_db, "BATCH-PARTIAL-1", "approve_all", decided_by="U123")

    assert result["approved_count"] == 1
    assert result["rejected_count"] == 1
    assert _batch_status(fresh_db, "BATCH-PARTIAL-1") == "partial"
    # The already-rejected draft must never reach Relay.
    assert len(enqueue_calls) == 1
    assert enqueue_calls[0]["idempotency_key"] == "cora_draft:DRAFT-P2"


def test_approve_all_is_idempotent_on_a_double_tap(fresh_db, monkeypatch):
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: fake_queue_item())
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    _make_batch_with_items(fresh_db, "BATCH-DOUBLE-1", ["DRAFT-D1"])
    first = decisions.record_batch_decision(fresh_db, "BATCH-DOUBLE-1", "approve_all", decided_by="U123")
    second = decisions.record_batch_decision(fresh_db, "BATCH-DOUBLE-1", "approve_all", decided_by="U456")

    assert first["ok"] is True
    assert second == {"ok": False, "reason": "batch_already_decided"}


def test_reject_item_is_idempotent_on_the_same_item(fresh_db, monkeypatch):
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: fake_queue_item())
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    _make_batch_with_items(fresh_db, "BATCH-DOUBLE-REJECT-1", ["DRAFT-DR1"])
    first = decisions.record_batch_decision(fresh_db, "BATCH-DOUBLE-REJECT-1", "reject_item", decided_by="U123", draft_id="DRAFT-DR1")
    second = decisions.record_batch_decision(fresh_db, "BATCH-DOUBLE-REJECT-1", "reject_item", decided_by="U123", draft_id="DRAFT-DR1")

    assert first["ok"] is True
    assert second == {"ok": False, "reason": "already_decided_or_not_found"}


def test_auto_approve_draft_skips_when_no_recipient(fresh_db, monkeypatch):
    enqueue_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item())[1])

    seed_draft(fresh_db, "DRAFT-NORECIP-1", contact_email=None, contact_phone=None)
    draft = {
        "draft_id": "DRAFT-NORECIP-1", "opportunity_thread_id": "OPP-NORECIP-1",
        "recommended_channel": "email", "subject": "s", "body": "b",
        "contact_email": None, "contact_phone": None,
    }

    result = decisions.auto_approve_draft(fresh_db, draft)

    assert result is False
    assert enqueue_calls == []
    assert _draft_status(fresh_db, "DRAFT-NORECIP-1") == "draft"  # never touched


def test_auto_approve_draft_handoff_rejection_is_parked_not_raised(fresh_db, monkeypatch):
    """Standing-order auto-approval (builder.py's build_batch loop) hits the
    same cora_to_relay contract as approve_all, but never reaches Slack.
    Without parking, a NULL-thread_id draft would stay at status='draft' and
    be silently re-selected into every future sweep forever."""

    def _fake_enqueue(**kw):
        raise HandoffRejected("cora_to_relay", ["thread_id: does not match OPP-YYYY-##### format"], kw["idempotency_key"])

    monkeypatch.setattr(decisions.relay_queue, "enqueue", _fake_enqueue)

    seed_draft(fresh_db, "DRAFT-BADTHREAD-AUTO-1", contact_email="a@b.com")
    draft = {
        "draft_id": "DRAFT-BADTHREAD-AUTO-1", "opportunity_thread_id": None,
        "recommended_channel": "email", "subject": "s", "body": "b",
        "contact_email": "a@b.com", "contact_phone": None,
    }

    result = decisions.auto_approve_draft(fresh_db, draft)

    assert result is False
    assert _draft_status(fresh_db, "DRAFT-BADTHREAD-AUTO-1") == "rejected"
    # Not left at 'draft' — otherwise build_batch() re-selects it every sweep forever.
    eligible_ids = {d["draft_id"] for d in store.read_drafts(fresh_db, status="draft")}
    assert "DRAFT-BADTHREAD-AUTO-1" not in eligible_ids


def test_approve_all_handoff_rejection_is_parked_not_raised(fresh_db, monkeypatch):
    """A draft with a NULL/malformed opportunity_thread_id fails the
    cora_to_relay handoff contract inside relay_queue.enqueue(). Before this
    fix that HandoffRejected propagated uncaught out of record_batch_decision
    (which runs inside a Slack-triggered FastAPI BackgroundTask with nothing
    to catch it) -- the approval died silently. It must instead be parked as
    a visible rejection and let the rest of the batch proceed."""

    def _fake_enqueue(**kw):
        if kw["idempotency_key"] == "cora_draft:DRAFT-BADTHREAD-1":
            raise HandoffRejected("cora_to_relay", ["thread_id: does not match OPP-YYYY-##### format"], kw["idempotency_key"])
        return fake_queue_item()

    monkeypatch.setattr(decisions.relay_queue, "enqueue", _fake_enqueue)
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    _make_batch_with_items(fresh_db, "BATCH-BADTHREAD-1", ["DRAFT-BADTHREAD-1", "DRAFT-OK-1"])

    result = decisions.record_batch_decision(fresh_db, "BATCH-BADTHREAD-1", "approve_all", decided_by="U123")

    assert result["ok"] is True
    assert result["approved_count"] == 1
    assert result["rejected_count"] == 1
    assert _batch_status(fresh_db, "BATCH-BADTHREAD-1") == "partial"
    assert _draft_status(fresh_db, "DRAFT-BADTHREAD-1") == "rejected"
    assert _item_decision(fresh_db, "BATCH-BADTHREAD-1", "DRAFT-BADTHREAD-1") == "exception_rejected"
    # The good draft in the same batch still goes through.
    assert _draft_status(fresh_db, "DRAFT-OK-1") == "approved_pending_send"


def test_record_batch_decision_batch_not_found(fresh_db):
    result = decisions.record_batch_decision(fresh_db, "BATCH-DOES-NOT-EXIST", "approve_all", decided_by="U123")
    assert result == {"ok": False, "reason": "batch_not_found"}


def test_sms_draft_is_skipped_not_enqueued(fresh_db, monkeypatch):
    """SMS channel has no Relay dispatcher — must be skipped, not enqueued, and
    parked at 'pending_channel_support' so the builder stops re-selecting it."""
    enqueue_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item())[1])
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    # Batch with one email draft (should enqueue) and one SMS draft (should skip)
    from sqlalchemy import text
    fresh_db.execute(text("INSERT INTO cora_draft_batches (batch_id, status) VALUES ('BATCH-SMS-1', 'pending')"))
    seed_draft(fresh_db, "DRAFT-EMAIL-1", channel="email", contact_email="a@b.com")
    seed_draft(fresh_db, "DRAFT-SMS-1", channel="sms", contact_phone="+18135550001", contact_email=None)
    for draft_id in ("DRAFT-EMAIL-1", "DRAFT-SMS-1"):
        fresh_db.execute(
            text("INSERT INTO cora_batch_items (batch_id, draft_id, decision) VALUES ('BATCH-SMS-1', :d, 'included')"),
            {"d": draft_id},
        )

    result = decisions.record_batch_decision(fresh_db, "BATCH-SMS-1", "approve_all", decided_by="U123")

    # Only the email draft reaches Relay
    assert len(enqueue_calls) == 1
    assert enqueue_calls[0]["idempotency_key"] == "cora_draft:DRAFT-EMAIL-1"
    # approved_count reflects only successfully enqueued items
    assert result["approved_count"] == 1
    # SMS draft is parked, NOT left at 'draft' — otherwise build_batch() re-selects
    # it every sweep and the founder approves the same draft forever.
    from sqlalchemy import text as _t
    sms_status = fresh_db.execute(_t("SELECT status FROM outbound_drafts WHERE draft_id='DRAFT-SMS-1'")).scalar()
    assert sms_status == "pending_channel_support"
    # And it must not come back in the builder's eligible pool.
    eligible_ids = {d["draft_id"] for d in store.read_drafts(fresh_db, status="draft")}
    assert "DRAFT-SMS-1" not in eligible_ids


def test_missing_recipient_on_supported_channel_is_not_parked_as_channel_problem(fresh_db, monkeypatch):
    """An email draft with no address is an enrichment gap, not a channel gap.
    It must keep its prior behaviour (left at 'draft'), not be mislabelled
    'pending_channel_support' — which is reserved for channels Relay can't send."""
    from sqlalchemy import text as _t

    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: fake_queue_item())
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    fresh_db.execute(_t("INSERT INTO cora_draft_batches (batch_id, status) VALUES ('BATCH-NORECIP', 'pending')"))
    seed_draft(fresh_db, "DRAFT-NORECIP-2", channel="email", contact_email=None, contact_phone=None)
    fresh_db.execute(
        _t("INSERT INTO cora_batch_items (batch_id, draft_id, decision) "
           "VALUES ('BATCH-NORECIP', 'DRAFT-NORECIP-2', 'included')")
    )

    result = decisions.record_batch_decision(fresh_db, "BATCH-NORECIP", "approve_all", decided_by="U123")

    assert result["approved_count"] == 0
    assert _draft_status(fresh_db, "DRAFT-NORECIP-2") == "draft"


def test_stale_reject_on_expired_batch_cannot_undo_a_later_approval(fresh_db, monkeypatch):
    """Reviewer regression case: batch A expires, the same draft is approved in
    replacement batch B, then batch A's still-live Slack Reject button is tapped.
    That stale action must change nothing."""
    from sqlalchemy import text as _t

    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: fake_queue_item())
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    # Batch A holds the draft, then expires without a decision.
    _make_batch_with_items(fresh_db, "BATCH-STALE-A", ["DRAFT-STALE-1"])
    fresh_db.execute(_t("UPDATE cora_draft_batches SET status = 'expired' WHERE batch_id = 'BATCH-STALE-A'"))

    # Batch B picks the same draft up and is approved.
    fresh_db.execute(_t("INSERT INTO cora_draft_batches (batch_id, status) VALUES ('BATCH-STALE-B', 'pending')"))
    fresh_db.execute(
        _t("INSERT INTO cora_batch_items (batch_id, draft_id, decision) "
           "VALUES ('BATCH-STALE-B', 'DRAFT-STALE-1', 'included')")
    )
    decisions.record_batch_decision(fresh_db, "BATCH-STALE-B", "approve_all", decided_by="U123")
    assert _draft_status(fresh_db, "DRAFT-STALE-1") == "approved_pending_send"

    # Now the stale Reject on expired batch A.
    result = decisions.record_batch_decision(
        fresh_db, "BATCH-STALE-A", "reject_item", decided_by="U999", draft_id="DRAFT-STALE-1",
    )

    assert result == {"ok": False, "reason": "batch_already_decided"}
    # The approval survives — this is the corruption the guard exists to prevent.
    assert _draft_status(fresh_db, "DRAFT-STALE-1") == "approved_pending_send"
    assert _item_decision(fresh_db, "BATCH-STALE-A", "DRAFT-STALE-1") == "included"


def test_auto_approve_draft_skips_sms_channel(fresh_db, monkeypatch):
    """auto_approve_draft must also respect the channel guard."""
    enqueue_calls = []
    monkeypatch.setattr(decisions.relay_queue, "enqueue", lambda **kw: (enqueue_calls.append(kw), fake_queue_item())[1])
    monkeypatch.setattr(decisions.relay_queue, "record_decision", lambda *a, **k: None)

    seed_draft(fresh_db, "DRAFT-AUTO-SMS-1", channel="sms", contact_phone="+18135550002", contact_email=None)
    draft = {
        "draft_id": "DRAFT-AUTO-SMS-1", "opportunity_thread_id": "OPP-AUTO-SMS-1",
        "recommended_channel": "sms", "subject": "s", "body": "b",
        "contact_email": None, "contact_phone": "+18135550002",
        "booking_link": None, "payment_link": None, "cell_id": "founder_tier_blitz",
    }

    result = decisions.auto_approve_draft(fresh_db, draft)

    assert result is False
    assert enqueue_calls == []
