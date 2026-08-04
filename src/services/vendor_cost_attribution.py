"""
Maps raw spend sources into canonical pause_target values.

Primary path: agent_decisions.graph_name
Fallback path: api_usage_logs.task_type (for non-graph Claude usage)
"""

import logging

logger = logging.getLogger(__name__)

# Maps graph_name -> canonical pause_target
# Any graph not in this map falls back to task_type -> pause_target
GRAPH_TO_PAUSE_TARGET: dict[str, str] = {
    "ap_lite_close": "ap_lite_sweep",
    "accelerated_wallet_push": "accelerated_wallet_push",
    "wallet_to_lock": "wallet_to_lock",
    "bundle_dispatcher": "bundle_dispatcher",
    "retention_event_producer": "retention_event_producer",
    "nws_poll": "nws_poll",
    "synthflow_voice_drop": "synthflow_voice_drop",
    "learning_card": "learning_card",
    "lifecycle_anomaly_check": "lifecycle_anomaly_check",
    "human_close_routing": "human_close_routing",
    "referral_milestone": "referral_milestone",
    # Cora — cold-drafting and reply workflows (QUALITY-v2.2 Q2 fix)
    "cora_outreach": "cora_outreach",
    "cora_pre_call": "cora_pre_call",
    "cora_reply":    "cora_reply",
}

# Maps task_type -> canonical pause_target (fallback for non-graph usage)
TASK_TO_PAUSE_TARGET: dict[str, str] = {
    "sms_copy": "sms_copy",
    "classification": "classification",
    "command_parsing": "command_parsing",
    "batch_summarization": "batch_summarization",
    "address_matching": "address_matching",
    "keyword_extraction": "keyword_extraction",
    "conversational_close": "conversational_close",
    "complex_reasoning": "complex_reasoning",
    "lead_analysis": "lead_analysis",
    "learning_card": "learning_card",
    "retention_copy": "retention_copy",
    "email_copy": "email_copy",
    "referral_milestone_sms": "referral_milestone_sms",
    "referral_milestone_email": "referral_milestone_email",
    "edge_case": "edge_case",
}


def resolve_pause_target(
    graph_name: str | None = None,
    task_type: str | None = None,
) -> str | None:
    """
    Resolve a raw graph_name or task_type into a canonical pause_target.

    Priority:
    1. graph_name lookup (primary)
    2. task_type lookup (fallback)
    3. None if neither is recognised
    """
    if graph_name:
        target = GRAPH_TO_PAUSE_TARGET.get(graph_name)
        if target:
            logger.debug(
                "attribution: graph=%s -> pause_target=%s", graph_name, target
            )
            return target
        logger.debug(
            "attribution: graph=%s not in mapping, falling back", graph_name
        )

    if task_type:
        target = TASK_TO_PAUSE_TARGET.get(task_type)
        if target:
            logger.debug(
                "attribution: (no graph) task=%s -> pause_target=%s",
                task_type,
                target,
            )
            return target

    logger.debug("attribution: no pause_target found for graph=%s task=%s", graph_name, task_type)
    return None


def all_pause_targets() -> set[str]:
    """Return the complete set of known pause_target values."""
    return set(GRAPH_TO_PAUSE_TARGET.values()) | set(TASK_TO_PAUSE_TARGET.values())