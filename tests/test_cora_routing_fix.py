from src.services.claude_router import _TASK_ROUTING
from src.services.vendor_cost_attribution import GRAPH_TO_PAUSE_TARGET


def test_cora_reply_classify_routes_to_haiku():
    """A 100-token classification call must not be billed at Sonnet (3.75× overcharge)."""
    assert _TASK_ROUTING.get("cora_reply_classify") == "haiku", (
        "cora_reply_classify must be haiku — it is a classification call, "
        "not content Josh or a prospect reads"
    )


def test_cora_outreach_routes_to_sonnet():
    """Outreach drafts are read by Josh and the prospect — Sonnet is correct."""
    assert _TASK_ROUTING.get("cora_outreach_draft") == "sonnet"


def test_cora_pre_call_routes_to_sonnet():
    assert _TASK_ROUTING.get("cora_pre_call_brief") == "sonnet"


def test_cora_reply_compose_routes_to_sonnet():
    assert _TASK_ROUTING.get("cora_reply_compose") == "sonnet"


def test_cora_outreach_in_pause_target():
    """Cora must have pause targets so the cost circuit breaker can auto-pause it."""
    assert "cora_outreach" in GRAPH_TO_PAUSE_TARGET, (
        "cora_outreach missing from GRAPH_TO_PAUSE_TARGET — "
        "vendor_cost_monitor cannot anomaly-detect or auto-pause Cora outreach spend"
    )


def test_cora_pre_call_in_pause_target():
    assert "cora_pre_call" in GRAPH_TO_PAUSE_TARGET


def test_cora_reply_in_pause_target():
    assert "cora_reply" in GRAPH_TO_PAUSE_TARGET
