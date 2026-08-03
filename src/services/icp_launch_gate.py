"""
ICP launch gate — enforcement predicate for Expansion ICP Channels.

icp_launch_blocked(db, channel_key) returns a list of human-readable
blocking reasons. An empty list means launch is permitted.

Gates checked:
  1. All 7 Expansion Gates green for the Source County (from Redis).
  2. Global Contractor MRR >= $50K.
  3. Channel exists and is in a launchable status (gated or approved).

This module ships NO cron, Slack, or runner — config-only stage.
The icp_channel_evaluator (future) simply wraps this predicate.
"""
from typing import Optional

from sqlalchemy.orm import Session

from config.lifecycle_guardrails import EXPANSION_GATES
from config.settings import settings
from src.core.models import ExpansionIcpChannel
from src.services.contractor_mrr import MRR_ICP_GATE_THRESHOLD, global_contractor_mrr
from src.tasks.county_launch_evaluator import _build_gate_snapshot, _gate_color

_LAUNCHABLE_STATUSES = frozenset(["gated", "approved"])


def icp_launch_blocked(db: Session, channel_key: str) -> list[str]:
    """
    Return a list of reasons blocking the given channel's launch.
    Empty list = launch permitted.

    Args:
        db: SQLAlchemy session.
        channel_key: the ExpansionIcpChannel.key to evaluate.
    """
    reasons: list[str] = []

    # ── Guard: channel must exist and be launchable ───────────────────────
    channel = db.query(ExpansionIcpChannel).filter_by(key=channel_key).first()
    if channel is None:
        reasons.append(f"icp_channel '{channel_key}' not found")
        return reasons  # can't evaluate gates without a channel
    if channel.status not in _LAUNCHABLE_STATUSES:
        reasons.append(
            f"icp_channel '{channel_key}' has status '{channel.status}' "
            f"(must be one of {sorted(_LAUNCHABLE_STATUSES)})"
        )
        return reasons

    # ── Gate 1–7: all Expansion Gates must be green for Source County ─────
    source_county = settings.county_launch_source_county
    snapshot = _build_gate_snapshot(source_county)
    for feature in EXPANSION_GATES:
        gate_info = snapshot.get(feature, {})
        value = gate_info.get("value")
        color = _gate_color(feature, value)
        if color != "green":
            val_str = f"{value}" if value is not None else "N/A"
            threshold = gate_info.get("threshold")
            reasons.append(
                f"expansion_gate '{feature}' is {color} "
                f"(value={val_str}, threshold={threshold})"
            )

    # ── Gate 8 (ICP-only): global Contractor MRR >= $50K ─────────────────
    mrr = global_contractor_mrr(db)
    if mrr < MRR_ICP_GATE_THRESHOLD:
        reasons.append(
            f"contractor_mrr {mrr:.2f} < {MRR_ICP_GATE_THRESHOLD} "
            f"(global contractor MRR must reach ${MRR_ICP_GATE_THRESHOLD:,} "
            "before any ICP channel launches)"
        )

    return reasons
