"""
Prompt experiment engine for Cora's outreach compose task (T-LEARN-07).

Selects which system prompt variant a given opportunity thread receives.
Reuses AgentLaneExperiment / assign_variant_by_thread from agent_lane_experiment_engine.
No new DB schema — the `audience` field on AgentLaneExperiment is set to "prompt"
to distinguish prompt experiments from price-band experiments.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.orm import Session

from config.prompt_variants import (
    CHAMPION_VARIANT,
    CHALLENGER_VARIANTS,
    PROMPT_EXPERIMENT_MIN_SAMPLE,
    PROMPT_EXPERIMENT_NAME,
)

logger = logging.getLogger(__name__)


def get_prompt_system(
    db: Optional[Session],
    opportunity_thread_id: str,
    *,
    offer: str,
    avenue: str,
    angle: str,
) -> str:
    """Return the system prompt string for an outreach compose call.

    If no golden-set-approved challengers exist, the champion is returned
    immediately with no DB interaction. Otherwise the thread is deterministically
    assigned to an arm via AgentLaneExperiment and the corresponding variant's
    system_template is returned.

    Never raises — on any failure the champion prompt is returned and a warning
    is logged so the compose call is not blocked.
    """
    if not CHALLENGER_VARIANTS:
        return CHAMPION_VARIANT["system_template"]

    if db is None:
        logger.warning(
            "prompt_experiment_engine: db is None with active challengers — returning champion"
        )
        return CHAMPION_VARIANT["system_template"]

    try:
        from src.services.agent_lane_experiment_engine import (
            assign_variant_by_thread,
            get_or_create_experiment,
        )

        challenger = CHALLENGER_VARIANTS[0]

        get_or_create_experiment(
            test_name=PROMPT_EXPERIMENT_NAME,
            variant_a={"prompt": "champion"},
            variant_b={"prompt": challenger["name"]},
            traffic_pct=10,
            db=db,
            min_sample=PROMPT_EXPERIMENT_MIN_SAMPLE,
            success_metric="reply_rate_pct",
            hypothesis="Challenger prompt improves reply rate by ≥2pp",
            audience="prompt",
        )

        arm = assign_variant_by_thread(opportunity_thread_id, PROMPT_EXPERIMENT_NAME, db)

        if arm == "b":
            return challenger["system_template"]
        return CHAMPION_VARIANT["system_template"]

    except Exception:  # noqa: BLE001
        logger.warning(
            "prompt_experiment_engine: assignment failed for thread=%s — returning champion",
            opportunity_thread_id,
            exc_info=True,
        )
        return CHAMPION_VARIANT["system_template"]
