"""Durable handoff regressions. All Slack transport is mocked."""
from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from sqlalchemy import text

from src.services.quote_ready import workflow, dossier
from src.services.fa_max_qualification import set_facts, SufficiencyResult, enqueue_quote_ready_work
from src.services.state_engine import get_opportunity_state, claim_next_work_item
from src.agents.fa_max import qualification_worker as worker
from tests.services.test_quote_ready_dossier_facts_precedence import (
    _make_property, _make_opportunity_with_property,
)


@pytest.fixture(autouse=True)
def block_slack(monkeypatch):
    post = Mock(return_value="123.456")
    monkeypatch.setattr(dossier, "post_quote_ready_dossier", post)
    monkeypatch.setattr("slack_sdk.WebClient.api_call", Mock(side_effect=AssertionError("Real Slack forbidden")))
    return post


@pytest.fixture
def deal(fresh_db, monkeypatch):
    pid = _make_property(fresh_db)
    oid = _make_opportunity_with_property(fresh_db, pid)
    set_facts(session=fresh_db, opportunity_id=oid,
              updates={"purchase_price": 200000, "rehab_estimate": 50000, "arv": 350000},
              source="client", set_by="test")
    @contextmanager
    def db_context():
        yield fresh_db
    monkeypatch.setattr(worker, "get_db_context", db_context)
    monkeypatch.setattr(workflow, "get_db_context", db_context)
    return fresh_db, oid


def qualify(db, oid, rev=1):
    opp = get_opportunity_state(session=db, opportunity_id=oid)
    worker._handle_sufficient(
        opportunity_id=oid, person_id=str(opp["person_id"]),
        current_stage=opp["current_stage"], state_version=opp["state_version"],
        facts_revision=rev,
        result=SufficiencyResult(verdict="sufficient", opportunity_id=oid, facts_revision=rev),
    )


def claim(db, queue):
    item = claim_next_work_item(session=db, queue_name=queue, worker_id="workflow-test")
    assert item is not None
    return item


def process(db, queue):
    item = claim(db, queue)
    workflow.process_work_item(item, worker_id="workflow-test")
    return item


def test_first_transition_never_posts_and_delivery_is_separate(deal, block_slack):
    db, oid = deal
    qualify(db, oid)
    assert get_opportunity_state(session=db, opportunity_id=oid)["current_stage"] == "scoping"
    block_slack.assert_not_called()
    process(db, workflow.BUILD_QUEUE)
    block_slack.assert_not_called()
    process(db, workflow.DELIVERY_QUEUE)
    block_slack.assert_called_once()


def test_actual_first_transition_then_failure_never_posts(deal, block_slack, monkeypatch):
    db, oid = deal
    monkeypatch.setattr(worker, "enqueue_quote_ready_work", Mock(side_effect=RuntimeError("after transition")))
    with pytest.raises(RuntimeError, match="after transition"):
        qualify(db, oid)
    block_slack.assert_not_called()
    db.rollback()


def test_compute_failure_stays_retryable(deal, monkeypatch, block_slack):
    db, oid = deal
    qualify(db, oid)
    with monkeypatch.context() as mp:
        mp.setattr(dossier, "compute_and_persist_quote_ready", Mock(side_effect=RuntimeError("compute unavailable")))
        item = process(db, workflow.BUILD_QUEUE)
    row = db.execute(text("SELECT status, payload FROM fa_max_work_queue WHERE work_item_id=:id ::uuid"), {"id": item["work_item_id"]}).mappings().one()
    assert row["status"] == "available"
    assert "compute unavailable" in row["payload"]["last_error"]
    block_slack.assert_not_called()
    db.execute(text("UPDATE fa_max_work_queue SET available_at=now() WHERE work_item_id=:id ::uuid"), {"id": item["work_item_id"]})
    process(db, workflow.BUILD_QUEUE)
    process(db, workflow.DELIVERY_QUEUE)
    block_slack.assert_called_once()


def test_delivery_failure_retries_without_recomputing(deal, block_slack):
    db, oid = deal
    qualify(db, oid)
    process(db, workflow.BUILD_QUEUE)
    block_slack.return_value = None
    item = process(db, workflow.DELIVERY_QUEUE)
    db.execute(text("UPDATE fa_max_work_queue SET available_at=now() WHERE work_item_id=:id ::uuid"), {"id": item["work_item_id"]})
    block_slack.return_value = "123.789"
    process(db, workflow.DELIVERY_QUEUE)
    assert block_slack.call_count == 2
    assert block_slack.call_args_list[0].kwargs["delivery_id"] == block_slack.call_args_list[1].kwargs["delivery_id"]
    assert db.execute(text("SELECT count(*) FROM fa_max_quote_ready_results WHERE opportunity_id=:id ::uuid"), {"id": oid}).scalar() == 1


def test_correction_supersedes_pending_delivery(deal, block_slack):
    db, oid = deal
    qualify(db, oid)
    process(db, workflow.BUILD_QUEUE)
    set_facts(session=db, opportunity_id=oid, updates={"rehab_estimate": 60000}, source="client", set_by="test")
    process(db, workflow.DELIVERY_QUEUE)
    block_slack.assert_not_called()
    qualify(db, oid, 2)
    process(db, workflow.BUILD_QUEUE)
    process(db, workflow.DELIVERY_QUEUE)
    block_slack.assert_called_once()


def test_generic_transition_hook_only_enqueues(deal, block_slack):
    from src.services.state_engine import _maybe_trigger_quote_ready_review
    db, oid = deal
    _maybe_trigger_quote_ready_review(session=db, entity_type="opportunity", to_state="scoping", opportunity_id=oid)
    block_slack.assert_not_called()
    process(db, workflow.BUILD_QUEUE)
    block_slack.assert_not_called()


def test_reclaimed_delivery_after_worker_death(deal, block_slack):
    db, oid = deal
    qualify(db, oid)
    process(db, workflow.BUILD_QUEUE)
    dead_claim = claim(db, workflow.DELIVERY_QUEUE)
    db.execute(text("UPDATE fa_max_work_queue SET lease_expires_at=now()-interval '1 second' WHERE work_item_id=:id ::uuid"), {"id": dead_claim["work_item_id"]})
    worker.FaMaxQualificationWorker(worker_id="replacement")._sweep_expired()
    new_claim = process(db, workflow.DELIVERY_QUEUE)
    assert new_claim["work_item_id"] == dead_claim["work_item_id"]
    block_slack.assert_called_once()
