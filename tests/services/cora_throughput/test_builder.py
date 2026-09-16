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


def test_build_batch_skips_when_cora_is_killed(fresh_db, monkeypatch):
    """STOP CORA / STOP CORA FOREVER must stop new batches from being built
    and posted to the approvals channel — not just Cora's own worker loop."""
    monkeypatch.setattr(builder, "cora_halted", lambda: True)
    posted = []
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: posted.append(1) or None)
    seed_draft(fresh_db, "DRAFT-KILLED-1")

    result = builder.build_batch(fresh_db)

    assert result == {"created": False, "reason": "cora_halted"}
    assert posted == []


def test_expire_stale_batches_skips_when_cora_is_killed(fresh_db, monkeypatch):
    monkeypatch.setattr(builder, "cora_halted", lambda: True)
    fresh_db.execute(
        text(
            "INSERT INTO cora_draft_batches (batch_id, status, created_at) "
            "VALUES ('BATCH-STALE-KILLED', 'pending', now() - interval '73 hours')"
        ),
    )

    expired = builder.expire_stale_batches(fresh_db)

    assert expired == 0
    assert _batch_row(fresh_db, "BATCH-STALE-KILLED")["status"] == "pending"


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
            "VALUES ('BATCH-STALE-1', 'pending', now() - interval '73 hours')"
        ),
    )
    fresh_db.execute(
        text("INSERT INTO cora_draft_batches (batch_id, status, created_at) VALUES ('BATCH-FRESH-1', 'pending', now())"),
    )

    expired = builder.expire_stale_batches(fresh_db)

    assert expired == 1
    assert _batch_row(fresh_db, "BATCH-STALE-1")["status"] == "expired"
    assert _batch_row(fresh_db, "BATCH-FRESH-1")["status"] == "pending"


# repost_unposted_batch is exercised against a fake session rather than
# `fresh_db`: that fixture binds to the app's real DATABASE_URL, and this
# function deliberately targets "the oldest pending batch with no Slack
# message" — which in a shared database is whatever real batch happens to be
# waiting, not the one the test seeded.
class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeDB:
    def __init__(self, batch_id, draft_rows):
        self._batch_id = batch_id
        self._draft_rows = draft_rows
        self.updates = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if sql.startswith("SELECT batch_id FROM cora_draft_batches"):
            return _FakeResult([(self._batch_id,)] if self._batch_id else [])
        if "FROM cora_batch_items bi" in sql:
            return _FakeResult(self._draft_rows)
        if sql.startswith("UPDATE cora_draft_batches"):
            self.updates.append(params)
            return _FakeResult([])
        raise AssertionError(f"unexpected SQL: {sql}")


def _draft_row(draft_id="D1"):
    return {
        "draft_id": draft_id, "opportunity_thread_id": "DBPR-1",
        "cell_id": "dbpr_storm_blitz", "recommended_channel": "email", "subject": "Hi",
    }


def _stub_power_block(monkeypatch):
    monkeypatch.setattr(builder.power_block, "assemble_power_block", lambda db: {})
    monkeypatch.setattr(builder.power_block, "render_power_block_blocks", lambda blk: [])


def test_repost_unposted_batch_recovers_a_batch_stranded_by_a_slack_outage(monkeypatch):
    """A batch built while Slack was down must post once Slack recovers, rather
    than sitting un-approvable until CORA_BATCH_EXPIRY_HOURS expires it."""
    _stub_power_block(monkeypatch)
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: "1786.0001")
    db = _FakeDB("BATCH-STRANDED", [_draft_row()])

    assert builder.repost_unposted_batch(db) is True
    assert len(db.updates) == 1
    assert db.updates[0]["ts"] == "1786.0001"
    assert db.updates[0]["batch_id"] == "BATCH-STRANDED"


def test_repost_unposted_batch_noop_when_no_unposted_batch(monkeypatch):
    _stub_power_block(monkeypatch)
    calls = []
    monkeypatch.setattr(
        builder.batch_slack, "post_batch_for_approval", lambda *a, **k: calls.append(1) or "ts",
    )
    db = _FakeDB(None, [])

    assert builder.repost_unposted_batch(db) is False
    assert calls == []
    assert db.updates == []


def test_repost_unposted_batch_leaves_state_untouched_while_slack_still_down(monkeypatch):
    """Slack still failing must not stamp a ts — the batch stays pending and
    retries next sweep, so the call is safe to run every interval."""
    _stub_power_block(monkeypatch)
    monkeypatch.setattr(builder.batch_slack, "post_batch_for_approval", lambda *a, **k: None)
    db = _FakeDB("BATCH-STRANDED", [_draft_row()])

    assert builder.repost_unposted_batch(db) is False
    assert db.updates == []


def test_repost_unposted_batch_skips_batch_with_no_included_items(monkeypatch):
    _stub_power_block(monkeypatch)
    calls = []
    monkeypatch.setattr(
        builder.batch_slack, "post_batch_for_approval", lambda *a, **k: calls.append(1) or "ts",
    )
    db = _FakeDB("BATCH-EMPTY", [])

    assert builder.repost_unposted_batch(db) is False
    assert calls == []
    assert db.updates == []


def test_repost_unposted_batch_skips_when_cora_is_killed(monkeypatch):
    _stub_power_block(monkeypatch)
    monkeypatch.setattr(builder, "cora_halted", lambda: True)
    calls = []
    monkeypatch.setattr(
        builder.batch_slack, "post_batch_for_approval", lambda *a, **k: calls.append(1) or "ts",
    )
    db = _FakeDB("BATCH-STRANDED", [_draft_row()])

    assert builder.repost_unposted_batch(db) is False
    assert calls == []
    assert db.updates == []
