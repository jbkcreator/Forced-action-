"""
Real concurrent-worker tests for the T3-7 <-> Scenario Builder handoff
(code-review finding, ninth/tenth round, 2026-09: the locking logic was
proven correct against a real DB via sequential integration tests, but
never driven through genuinely separate, concurrently-executing DB
connections -- exactly what a real production race between two worker
processes looks like).

Uses real threads with INDEPENDENT SQLAlchemy connections (not fresh_db's
single shared connection) -- DB I/O releases the GIL, so two threads each
holding their own Postgres connection produce a genuine concurrent-
transaction race at the database level, the same way two separate OS
worker processes would. A threading.Barrier synchronizes both threads'
start so the race window is real, not accidental ordering.

All Slack network calls mocked -- see the autouse fixture below.
"""
from __future__ import annotations

import threading
import uuid
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import text

from src.core.database import get_db_context
from src.services.fa_max_qualification import set_facts, SufficiencyResult
from src.services.state_engine import get_opportunity_state, claim_next_work_item
from src.agents.fa_max import qualification_worker as worker
from src.services.quote_ready import workflow

from tests.services.test_quote_ready_dossier_facts_precedence import (
    _make_property, _make_financials, _make_opportunity_with_property,
)


@pytest.fixture(autouse=True)
def _block_real_slack():
    with patch("slack_sdk.WebClient.chat_postMessage") as mock_post, \
         patch("src.services.relay.exceptions_alert_queue.post_exceptions_alert"):
        yield mock_post


def _seed_deal() -> str:
    with get_db_context() as session:
        pid = _make_property(session)
        _make_financials(session, pid, est_repair_cost=20000, assessed_value_mkt=300000)
        oid = _make_opportunity_with_property(session, pid)
        set_facts(
            session=session, opportunity_id=oid,
            updates={"purchase_price": 200000, "rehab_estimate": 50000, "arv": 320000},
            source="client", set_by="test",
        )
        session.commit()
    return oid


def _cleanup(oid: str) -> None:
    with get_db_context() as session:
        session.execute(text("DELETE FROM fa_max_work_queue WHERE payload->>'opportunity_id' = :oid"), {"oid": oid})
        session.execute(text("DELETE FROM fa_max_quote_ready_results WHERE opportunity_id = :oid ::uuid"), {"oid": oid})
        session.execute(text("DELETE FROM fa_max_opportunity_facts WHERE opportunity_id = :oid ::uuid"), {"oid": oid})
        session.execute(text("DELETE FROM fa_max_qualification_decisions WHERE opportunity_id = :oid ::uuid"), {"oid": oid})
        pids = [r[0] for r in session.execute(
            text("SELECT property_id FROM fa_max_opportunity_properties WHERE opportunity_id = :oid ::uuid"), {"oid": oid}
        ).all()]
        session.execute(text("DELETE FROM fa_max_opportunity_properties WHERE opportunity_id = :oid ::uuid"), {"oid": oid})
        session.execute(text("DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"), {"oid": oid})
        for pid in pids:
            session.execute(text("DELETE FROM financials WHERE property_id = :pid"), {"pid": pid})
            session.execute(text("DELETE FROM properties WHERE id = :pid"), {"pid": pid})
        session.commit()


class TestConcurrentQueueClaims:
    """Two real worker threads, each with its own DB connection, racing the
    SAME queue for the SAME opportunity's work item -- proves
    claim_next_work_item's FOR UPDATE SKIP LOCKED produces exactly one
    winner, not a double-claim, under a genuine concurrent race (not just
    sequential test-order luck)."""

    def test_two_threads_claim_disjoint_build_items(self):
        oid = _seed_deal()
        try:
            with get_db_context() as session:
                opp = get_opportunity_state(session=session, opportunity_id=oid)
                worker._handle_sufficient(
                    opportunity_id=oid, person_id=str(opp["person_id"]),
                    current_stage=opp["current_stage"], state_version=opp["state_version"],
                    facts_revision=1,
                    result=SufficiencyResult(verdict="sufficient", gaps=[], opportunity_id=oid, facts_revision=1),
                )

            barrier = threading.Barrier(2)
            results: list = [None, None]
            errors: list = [None, None]

            def _claim(idx: int) -> None:
                try:
                    barrier.wait(timeout=5)
                    with get_db_context() as session:
                        item = claim_next_work_item(
                            session=session, queue_name=workflow.BUILD_QUEUE,
                            worker_id=f"race-worker-{idx}",
                        )
                        results[idx] = item
                        session.commit()
                except Exception as exc:  # pragma: no cover - surfaced via errors list
                    errors[idx] = exc

            threads = [threading.Thread(target=_claim, args=(i,)) for i in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)

            assert errors == [None, None], f"thread errors: {errors}"
            claimed = [r for r in results if r is not None]
            # Exactly one thread must have claimed the single available
            # build item for this opportunity -- SKIP LOCKED must never let
            # both threads see and claim the same row.
            assert len(claimed) == 1
            assert claimed[0]["payload"]["opportunity_id"] == oid
        finally:
            _cleanup(oid)


class TestConcurrentCorrectionDuringEligibilityCheck:
    """A correction (set_facts) racing a worker's eligibility check
    (_eligible's FOR UPDATE lock inside workflow.process_work_item) for the
    SAME opportunity -- proves the lock genuinely serializes two separate
    DB connections, not just two calls on the same connection."""

    def test_set_facts_blocks_until_eligibility_check_transaction_ends(self):
        oid = _seed_deal()
        try:
            with get_db_context() as session:
                opp = get_opportunity_state(session=session, opportunity_id=oid)
                worker._handle_sufficient(
                    opportunity_id=oid, person_id=str(opp["person_id"]),
                    current_stage=opp["current_stage"], state_version=opp["state_version"],
                    facts_revision=1,
                    result=SufficiencyResult(verdict="sufficient", gaps=[], opportunity_id=oid, facts_revision=1),
                )

            with get_db_context() as claim_session:
                item = claim_next_work_item(
                    session=claim_session, queue_name=workflow.BUILD_QUEUE, worker_id="holder",
                )
                assert item is not None
                claim_session.commit()

            lock_held = threading.Event()
            release_lock = threading.Event()
            worker_error: list = [None]

            def _hold_eligibility_lock():
                try:
                    with get_db_context() as session:
                        # Re-implements _eligible()'s exact lock order
                        # (facts row, then opportunity row) to hold it open
                        # for a controlled window, proving a concurrent
                        # set_facts() call genuinely blocks on it.
                        session.execute(
                            text("SELECT facts_revision FROM fa_max_opportunity_facts"
                                 " WHERE opportunity_id = :oid ::uuid FOR UPDATE"),
                            {"oid": oid},
                        )
                        session.execute(
                            text("SELECT outcome FROM fa_max_opportunities"
                                 " WHERE opportunity_id = :oid ::uuid FOR UPDATE"),
                            {"oid": oid},
                        )
                        lock_held.set()
                        release_lock.wait(timeout=10)
                        session.commit()
                except Exception as exc:  # pragma: no cover
                    worker_error[0] = exc
                    lock_held.set()

            holder_thread = threading.Thread(target=_hold_eligibility_lock)
            holder_thread.start()
            assert lock_held.wait(timeout=5), "eligibility lock was never acquired"

            correction_done = threading.Event()
            correction_error: list = [None]
            correction_started_at = []
            correction_finished_at = []

            def _attempt_correction():
                import time
                try:
                    correction_started_at.append(time.monotonic())
                    with get_db_context() as session:
                        set_facts(
                            session=session, opportunity_id=oid,
                            updates={"rehab_estimate": 75000}, source="client", set_by="test",
                        )
                        session.commit()
                    correction_finished_at.append(time.monotonic())
                except Exception as exc:  # pragma: no cover
                    correction_error[0] = exc
                finally:
                    correction_done.set()

            correction_thread = threading.Thread(target=_attempt_correction)
            correction_thread.start()

            import time
            # The correction must NOT complete while the lock is held.
            still_blocked = not correction_done.wait(timeout=1.5)
            assert still_blocked, "set_facts() completed despite a concurrent FOR UPDATE holder"

            release_lock.set()
            holder_thread.join(timeout=5)
            correction_thread.join(timeout=5)

            assert worker_error[0] is None
            assert correction_error[0] is None
            assert correction_done.is_set()

            with get_db_context() as session:
                revision = session.execute(
                    text("SELECT facts_revision FROM fa_max_opportunity_facts"
                         " WHERE opportunity_id = :oid ::uuid"),
                    {"oid": oid},
                ).scalar()
            assert revision == 2  # the correction landed cleanly after the lock released
        finally:
            _cleanup(oid)


class TestConcurrentBuildAndDeliveryWorkers:
    """Two real worker threads processing the SAME opportunity's build and
    delivery queue items concurrently, end to end -- the closest thing to
    two actual OS worker processes this test suite can drive without
    spawning real subprocesses. Confirms the full build->deliver pipeline
    survives real concurrent execution with no duplicate Slack post and no
    duplicate persisted result."""

    def test_two_workers_process_build_and_delivery_without_duplication(self, _block_real_slack):
        _block_real_slack.return_value = {"ts": "123.456"}
        # post_quote_ready_dossier no-ops (correctly) when fa_max_slack_bot_token/
        # fa_max_slack_channel_money aren't configured -- don't depend on
        # real .env state for this test's own pass/fail; force the
        # "configured" path so the mocked chat_postMessage is actually
        # reached, exactly like a real deployment with real credentials.
        settings_patch = patch(
            "src.services.quote_ready.dossier.get_settings",
            return_value=Mock(
                fa_max_slack_bot_token=Mock(get_secret_value=lambda: "xoxb-test"),
                fa_max_slack_channel_money="C_TEST",
            ),
        )
        oid = _seed_deal()
        settings_patch.start()
        try:
            with get_db_context() as session:
                opp = get_opportunity_state(session=session, opportunity_id=oid)
                worker._handle_sufficient(
                    opportunity_id=oid, person_id=str(opp["person_id"]),
                    current_stage=opp["current_stage"], state_version=opp["state_version"],
                    facts_revision=1,
                    result=SufficiencyResult(verdict="sufficient", gaps=[], opportunity_id=oid, facts_revision=1),
                )

            errors: list = [None, None]

            def _run_worker(idx: int, queue: str) -> None:
                try:
                    with get_db_context() as session:
                        item = claim_next_work_item(
                            session=session, queue_name=queue, worker_id=f"concurrent-{idx}",
                        )
                        session.commit()
                    if item is not None:
                        workflow.process_work_item(item, worker_id=f"concurrent-{idx}")
                except Exception as exc:  # pragma: no cover
                    errors[idx] = exc

            # Round 1: both threads race the BUILD queue concurrently (only
            # one item exists -- one thread gets it, the other gets None
            # and no-ops, exactly like two real worker processes polling
            # the same queue).
            t1 = threading.Thread(target=_run_worker, args=(0, workflow.BUILD_QUEUE))
            t2 = threading.Thread(target=_run_worker, args=(1, workflow.BUILD_QUEUE))
            t1.start(); t2.start()
            t1.join(timeout=10); t2.join(timeout=10)
            assert errors == [None, None], f"build round errors: {errors}"

            # Round 2: same race for the delivery queue the build step enqueued.
            errors = [None, None]
            t3 = threading.Thread(target=_run_worker, args=(0, workflow.DELIVERY_QUEUE))
            t4 = threading.Thread(target=_run_worker, args=(1, workflow.DELIVERY_QUEUE))
            t3.start(); t4.start()
            t3.join(timeout=10); t4.join(timeout=10)
            assert errors == [None, None], f"delivery round errors: {errors}"

            with get_db_context() as session:
                result_count = session.execute(
                    text("SELECT COUNT(*) FROM fa_max_quote_ready_results WHERE opportunity_id = :oid ::uuid"),
                    {"oid": oid},
                ).scalar()
            assert result_count == 1  # no duplicate result from the concurrent build race
            assert _block_real_slack.call_count == 1  # no duplicate delivery from the concurrent delivery race
        finally:
            settings_patch.stop()
            _cleanup(oid)
