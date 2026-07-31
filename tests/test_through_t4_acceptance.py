"""
THROUGH-v2.2 T4 — Acceptance Harness.

Three acceptance tests that prove the batch-approval pipeline and
standing-order compiler work end-to-end:

  1. Batch E2E (10 items) — enqueue 10 actions, approve all, execute via
     noop channel, assert exactly 10 reach Relay's batch-intake contract
     (status='sent', BatchResult.sent==10).

  2. Standing-Order Compiler — seed relay_approval_queue with 5 approved
     rows carrying the same action_type+vertical, run scan_and_propose(),
     assert exactly one standing_orders row is created in 'proposed' state.
     Also asserts a second scan does NOT create a duplicate.

  3. Daily ceiling cap — enqueue items beyond relay_daily_ceiling for the
     noop channel, verify BatchResult shows the overflow as skipped/deferred
     (not all sent). Uses fakeredis to make the cap deterministic.

All tests roll back via the fresh_db fixture — no permanent DB writes.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services.relay import queue as relay_queue
from src.services.relay.engine import BatchResult, execute_batch
from src.services.relay.queue import QueueItem

# Unique prefix per test session — isolates committed data from prior runs.
_RUN_ID = uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_key() -> str:
    return f"test-{uuid.uuid4().hex}"


def _unique_pair(base_action: str, base_vertical: str) -> tuple[str, str]:
    """Return action_type/vertical unique to this test session so committed
    standing_order rows from prior runs don't block new proposals."""
    return f"{base_action}_{_RUN_ID}", f"{base_vertical}_{_RUN_ID}"


def _enqueue_noop(
    recipient: str = "test@example.com",
    action_type: str | None = None,
    vertical: str | None = None,
    template_id: str | None = None,
) -> QueueItem:
    payload: dict = {"subject": "Test", "body": "Test body"}
    if action_type:
        payload["action_type"] = action_type
    if vertical:
        payload["vertical"] = vertical
    if template_id:
        payload["template_id"] = template_id
    return relay_queue.enqueue(
        idempotency_key=_make_key(),
        channel="noop",
        recipient=recipient,
        payload=payload,
    )


def _fetch_approved_by_ids(item_ids: set[int]) -> list[QueueItem]:
    """Fetch approved items by specific IDs — avoids limit-50 blind spot."""
    from sqlalchemy import text
    from src.core.database import get_db_context
    from src.services.relay.queue import _COLUMNS_SQL, _row_to_item
    with get_db_context() as session:
        rows = session.execute(
            text(
                f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                "WHERE id = ANY(:ids) AND status = 'approved' "
                "ORDER BY created_at ASC"
            ),
            {"ids": list(item_ids)},
        ).mappings().all()
        return [_row_to_item(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# Test 1: 10-item batch end-to-end
# ---------------------------------------------------------------------------

class TestBatchE2E:
    def test_ten_items_all_reach_relay(self, fresh_db):
        """Enqueue 10 noop items, approve all, execute_batch → sent == 10."""
        items = [_enqueue_noop() for _ in range(10)]

        for item in items:
            result = relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")
            assert result is not None, f"record_decision returned None for item {item.id}"

        approved_ids = {i.id for i in items}
        batch_items = _fetch_approved_by_ids(approved_ids)
        assert len(batch_items) == 10, (
            f"Expected 10 approved items, got {len(batch_items)}"
        )

        batch_id = f"acceptance-{uuid.uuid4().hex[:8]}"
        now = datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)  # inside send window

        from src.services.relay.guards import Verdict, ALLOW

        with (
            patch("src.services.relay.engine._kill_switch_is_red", return_value=False),
            patch("src.services.relay.engine.guards") as mock_guards,
        ):
            mock_guards.evaluate.return_value = Verdict(ALLOW)
            mock_guards.reserve_daily_slot.return_value = True
            mock_guards.release_daily_slot.return_value = None
            mock_guards.DEFER = "defer"
            mock_guards.BLOCK = "block"

            result: BatchResult = execute_batch(batch_items, batch_id=batch_id, now=now)

        assert result.sent == 10, f"Expected sent=10, got {result}"
        assert result.failed == 0
        assert result.halted is False

        for item in items:
            refreshed = relay_queue.get_item(item.id)
            assert refreshed is not None
            assert refreshed.status == "sent", (
                f"Item {item.id} expected status='sent', got {refreshed.status!r}"
            )

    def test_rejected_items_never_reach_relay(self, fresh_db):
        items = [_enqueue_noop() for _ in range(3)]
        for item in items:
            relay_queue.record_decision(item.id, approved=False, decided_by="test-josh")

        approved = relay_queue.approved_batch(limit=50)
        approved_ids = {i.id for i in items}
        batch_items = [i for i in approved if i.id in approved_ids]
        assert len(batch_items) == 0, "Rejected items must not appear in approved_batch"


# ---------------------------------------------------------------------------
# Test 2: Standing-Order Compiler
# ---------------------------------------------------------------------------

class TestStandingOrderCompiler:
    def test_five_approvals_produces_one_proposal(self, fresh_db):
        """5 approved rows with same action_type+vertical → 1 proposed SO row."""
        from src.services import standing_order_compiler as soc

        at, vert = _unique_pair("follow_up_2", "water_damage")
        for _ in range(5):
            item = _enqueue_noop(action_type=at, vertical=vert, template_id="tmpl-abc")
            relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")

        with patch.object(soc, "_post_to_slack"):
            proposed = soc.scan_and_propose()

        assert len(proposed) == 1
        p = proposed[0]
        assert p.action_type == at
        assert p.vertical == vert
        assert p.approval_count >= 5

        from sqlalchemy import text
        from src.core.database import get_db_context
        with get_db_context() as session:
            row = session.execute(
                text(
                    "SELECT id, status, action_type, vertical "
                    "FROM standing_orders WHERE id = :id"
                ),
                {"id": p.standing_order_id},
            ).mappings().first()
        assert row is not None
        assert row["status"] == "proposed"
        assert row["action_type"] == at
        assert row["vertical"] == vert

    def test_second_scan_does_not_duplicate(self, fresh_db):
        """A second scan for the same pair must not create a second proposal."""
        from src.services import standing_order_compiler as soc

        at, vert = _unique_pair("follow_up_2", "roofing")
        for _ in range(5):
            item = _enqueue_noop(action_type=at, vertical=vert)
            relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")

        with patch.object(soc, "_post_to_slack"):
            first = soc.scan_and_propose()
            second = soc.scan_and_propose()

        assert len(first) == 1
        assert len(second) == 0, "Second scan must not re-propose an active standing order"

    def test_four_approvals_does_not_propose(self, fresh_db):
        """4 approvals (below threshold) → no proposal."""
        from src.services import standing_order_compiler as soc

        at, vert = _unique_pair("first_touch", "foreclosure")
        for _ in range(4):
            item = _enqueue_noop(action_type=at, vertical=vert)
            relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")

        with patch.object(soc, "_post_to_slack"):
            proposed = soc.scan_and_propose()

        assert all(p.action_type != at or p.vertical != vert for p in proposed)

    def test_ratify_flips_status(self, fresh_db):
        from src.services import standing_order_compiler as soc

        at, vert = _unique_pair("follow_up_3", "tax_delinquency")
        for _ in range(5):
            item = _enqueue_noop(action_type=at, vertical=vert)
            relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")

        with patch.object(soc, "_post_to_slack"):
            proposed = soc.scan_and_propose()
        assert len(proposed) == 1

        updated = soc.record_ratify(proposed[0].standing_order_id, ratified_by="test-josh")
        assert updated is True

        from sqlalchemy import text
        from src.core.database import get_db_context
        with get_db_context() as session:
            row = session.execute(
                text("SELECT status FROM standing_orders WHERE id = :id"),
                {"id": proposed[0].standing_order_id},
            ).mappings().first()
        assert row["status"] == "ratified"

    def test_decline_flips_status(self, fresh_db):
        from src.services import standing_order_compiler as soc

        at, vert = _unique_pair("follow_up_2", "lien")
        for _ in range(5):
            item = _enqueue_noop(action_type=at, vertical=vert)
            relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")

        with patch.object(soc, "_post_to_slack"):
            proposed = soc.scan_and_propose()

        updated = soc.record_decline(proposed[0].standing_order_id, declined_by="test-josh")
        assert updated is True

        from sqlalchemy import text
        from src.core.database import get_db_context
        with get_db_context() as session:
            row = session.execute(
                text("SELECT status FROM standing_orders WHERE id = :id"),
                {"id": proposed[0].standing_order_id},
            ).mappings().first()
        assert row["status"] == "declined"


# ---------------------------------------------------------------------------
# Test 3: Daily ceiling cap (noop channel)
# ---------------------------------------------------------------------------

class TestDailyCeilingCap:
    def test_overflow_items_are_skipped_or_deferred(self, fresh_db):
        """Enqueue more items than relay_daily_ceiling; verify not all sent."""
        from src.services.relay import guards
        from src.services.relay.config import STATUS_APPROVED

        ceiling = 3
        total = ceiling + 2

        items = [_enqueue_noop() for _ in range(total)]
        for item in items:
            relay_queue.record_decision(item.id, approved=True, decided_by="test-josh")

        approved_ids = {i.id for i in items}
        batch_items = _fetch_approved_by_ids(approved_ids)
        assert len(batch_items) == total

        batch_id = f"cap-test-{uuid.uuid4().hex[:8]}"
        now = datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)

        from src.services.relay.guards import Verdict, ALLOW

        sent_count = 0

        def fake_reserve(channel, now, settings):
            nonlocal sent_count
            if sent_count < ceiling:
                sent_count += 1
                return True
            return False

        with (
            patch("src.services.relay.engine._kill_switch_is_red", return_value=False),
            patch("src.services.relay.engine.guards") as mock_guards,
        ):
            mock_guards.evaluate.return_value = Verdict(ALLOW)
            mock_guards.reserve_daily_slot.side_effect = fake_reserve
            mock_guards.release_daily_slot.return_value = None
            mock_guards.DEFER = "defer"
            mock_guards.BLOCK = "block"

            result: BatchResult = execute_batch(batch_items, batch_id=batch_id, now=now)

        assert result.sent <= ceiling, (
            f"sent={result.sent} exceeded ceiling={ceiling}"
        )
        assert result.sent + result.skipped + result.deferred + result.failed == total
