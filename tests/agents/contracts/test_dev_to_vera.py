from datetime import datetime, timezone

from src.agents.contracts.dev_to_vera import check_closure

_VALID_CLOSURE = {
    "commit_hash": "9454ba2",
    "plain_english_diff": "Added 4 Cora task_type entries to _TASK_ROUTING so replies bill at haiku, not sonnet",
    "touches": "src/services/claude_router.py",
    "risk_class": "low",
    "test_evidence": "tests/test_cora_routing_fix.py::test_reply_classify_routes_to_haiku PASSED",
    "rollback_plan": "revert the single commit; _TASK_ROUTING falls back to its existing default",
    "claimed_done_at": datetime(2026, 7, 31, tzinfo=timezone.utc),
    "closes_finding_id": "VERA-FINDING-2026-0042",
}


def test_valid_closure_passes():
    result = check_closure(_VALID_CLOSURE)
    assert result.ok is True
    assert result.model.label == "agent-draft"


def test_missing_rollback_plan_fails():
    incomplete = {k: v for k, v in _VALID_CLOSURE.items() if k != "rollback_plan"}
    result = check_closure(incomplete)
    assert result.ok is False
    assert any("rollback_plan" in f for f in result.missing_fields)


def test_missing_closes_finding_id_fails():
    """A closure that doesn't say which finding it closes is exactly the
    kind of incomplete handoff §9.5 wants auto-rejected -- Vera has no way
    to reconcile it against her open findings otherwise."""
    incomplete = {k: v for k, v in _VALID_CLOSURE.items() if k != "closes_finding_id"}
    result = check_closure(incomplete)
    assert result.ok is False
    assert any("closes_finding_id" in f for f in result.missing_fields)


def test_claimed_done_at_must_be_a_real_datetime():
    bad = {**_VALID_CLOSURE, "claimed_done_at": "not-a-date"}
    result = check_closure(bad)
    assert result.ok is False
