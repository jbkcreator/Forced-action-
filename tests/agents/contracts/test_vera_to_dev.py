from src.agents.contracts.vera_to_dev import check_finding

_VALID_FINDING = {
    "issue": "Cora is billed at Sonnet on every call",
    "evidence": "_TASK_ROUTING (claude_router.py:50-75) has zero cora_* entries",
    "repro": "grep -n 'cora_' src/services/claude_router.py returns nothing",
    "suspected_cause": "Cora's task_types were never added to _TASK_ROUTING when PR 180 merged",
    "proposed_fix": "Add 4 cora_* entries mapping to haiku/sonnet per task_type",
    "effort": "S",
    "risk": "low",
}


def test_valid_finding_passes():
    result = check_finding(_VALID_FINDING)
    assert result.ok is True
    assert result.missing_fields == []
    assert result.model.label == "vera-finding"


def test_missing_required_field_fails_with_field_named():
    incomplete = {k: v for k, v in _VALID_FINDING.items() if k != "repro"}
    result = check_finding(incomplete)
    assert result.ok is False
    assert any("repro" in f for f in result.missing_fields)


def test_empty_string_field_fails():
    bad = {**_VALID_FINDING, "evidence": "   "}
    result = check_finding(bad)
    assert result.ok is False
    assert any("evidence" in f for f in result.missing_fields)


def test_label_is_always_vera_finding():
    result = check_finding(_VALID_FINDING)
    assert result.model.label == "vera-finding"
    # Even if a caller tries to override it, the contract pins it (labels
    # are a fleet-wide taxonomy, not per-finding data, per spec :191).
    overridden = check_finding({**_VALID_FINDING, "label": "something-else"})
    assert overridden.ok is False
