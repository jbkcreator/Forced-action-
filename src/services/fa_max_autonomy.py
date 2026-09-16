"""FA Max autonomy-tier gating (WP-2 — send governance).

Autonomy tiers gate autonomous FA Max outbound sends. Internal-only agents
(sourcing, enrichment, dedup, scoring) need no gate. A draft below the
graduation threshold may still be queued for explicit human approval.

Tiers (per SOT.md Part 2 / Requirement Two):
    A — Replies in threads Josh started, follow-ups, own funded-borrower
        outreach. Gate: 25 approved sends.
    B — Partner give-first sends, warm introductions.
        Gate: 100 approved sends AND edit rate < 10 %.
    C — Cold first touch under Josh's name/Backflip brand.
        Gate: 300 approved sends AND 5 funded loans.

"Approved send" = relay_approval_queue row with venture_key='fa_max_lending',
status='sent', agent_name=<this agent>. Approved-but-unsent rows don't count
(counting them would let an agent game the gate without real outreach).

Edit rate = rows where the approved payload.body differs from the original
pending payload.body (draft materially changed before approval), divided by
total approved sends for this agent. Tracked via the 'edited_before_approval'
flag in the payload, set by the Slack approve handler when it detects the body
was changed.

This module reads live counts from the DB — graduation state is never
hard-coded. The Relay execution path currently requires a durable human
approval for every FA Max item; autonomous dispatch is not enabled by WP-2.

All reads use sqlalchemy.text() per CLAUDE.md.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

FA_MAX_VENTURE = "fa_max_lending"

# Tier thresholds (SOT.md Part 2)
_TIER_A_MIN_SENDS = 25
_TIER_B_MIN_SENDS = 100
_TIER_B_MAX_EDIT_RATE = 0.10  # 10 %
_TIER_C_MIN_SENDS = 300
_TIER_C_MIN_FUNDED_LOANS = 5


class TierGateOutcome(str, Enum):
    allowed = "allowed"
    below_send_threshold = "below_send_threshold"
    edit_rate_too_high = "edit_rate_too_high"
    funded_loans_below_threshold = "funded_loans_below_threshold"
    unknown_tier = "unknown_tier"


@dataclass(frozen=True)
class GateResult:
    outcome: TierGateOutcome
    tier: str
    agent_name: str
    approved_sends: int
    edit_rate: Optional[float]  # None when not applicable for this tier
    funded_loans: Optional[int]  # None when not applicable for this tier

    @property
    def allowed(self) -> bool:
        return self.outcome == TierGateOutcome.allowed


def check_tier_gate(agent_name: str, tier: str, session: Session) -> GateResult:
    """Return a GateResult for the given agent and tier.

    Always reads live counts from the DB — never cached, never assumed open.
    Caller must not send if result.allowed is False.
    """
    sends = get_approved_send_count(agent_name, session)

    if tier == "A":
        if sends < _TIER_A_MIN_SENDS:
            logger.info(
                "FA Max tier-A gate: %s has %d approved sends (need %d)",
                agent_name, sends, _TIER_A_MIN_SENDS,
            )
            return GateResult(
                outcome=TierGateOutcome.below_send_threshold,
                tier=tier, agent_name=agent_name,
                approved_sends=sends, edit_rate=None, funded_loans=None,
            )
        return GateResult(
            outcome=TierGateOutcome.allowed,
            tier=tier, agent_name=agent_name,
            approved_sends=sends, edit_rate=None, funded_loans=None,
        )

    if tier == "B":
        if sends < _TIER_B_MIN_SENDS:
            return GateResult(
                outcome=TierGateOutcome.below_send_threshold,
                tier=tier, agent_name=agent_name,
                approved_sends=sends, edit_rate=None, funded_loans=None,
            )
        rate = get_edit_rate(agent_name, session)
        if rate >= _TIER_B_MAX_EDIT_RATE:  # "under 10%" = strictly less than 10%
            logger.info(
                "FA Max tier-B gate: %s edit rate %.1f%% exceeds 10%% ceiling",
                agent_name, rate * 100,
            )
            return GateResult(
                outcome=TierGateOutcome.edit_rate_too_high,
                tier=tier, agent_name=agent_name,
                approved_sends=sends, edit_rate=rate, funded_loans=None,
            )
        return GateResult(
            outcome=TierGateOutcome.allowed,
            tier=tier, agent_name=agent_name,
            approved_sends=sends, edit_rate=rate, funded_loans=None,
        )

    if tier == "C":
        if sends < _TIER_C_MIN_SENDS:
            return GateResult(
                outcome=TierGateOutcome.below_send_threshold,
                tier=tier, agent_name=agent_name,
                approved_sends=sends, edit_rate=None, funded_loans=None,
            )
        loans = get_funded_loan_count(session)
        if loans < _TIER_C_MIN_FUNDED_LOANS:
            logger.info(
                "FA Max tier-C gate: %d funded loans (need %d)",
                loans, _TIER_C_MIN_FUNDED_LOANS,
            )
            return GateResult(
                outcome=TierGateOutcome.funded_loans_below_threshold,
                tier=tier, agent_name=agent_name,
                approved_sends=sends, edit_rate=None, funded_loans=loans,
            )
        return GateResult(
            outcome=TierGateOutcome.allowed,
            tier=tier, agent_name=agent_name,
            approved_sends=sends, edit_rate=None, funded_loans=loans,
        )

    logger.error("FA Max: unknown autonomy tier %r for agent %s", tier, agent_name)
    return GateResult(
        outcome=TierGateOutcome.unknown_tier,
        tier=tier, agent_name=agent_name,
        approved_sends=sends, edit_rate=None, funded_loans=None,
    )


def get_approved_send_count(agent_name: str, session: Session) -> int:
    """Count relay_approval_queue rows with status='sent' for this FA Max agent."""
    row = session.execute(
        text(
            "SELECT COUNT(*) FROM relay_approval_queue "
            "WHERE venture_key = :v AND status = 'sent' AND agent_name = :a"
        ),
        {"v": FA_MAX_VENTURE, "a": agent_name},
    ).scalar()
    return int(row or 0)


def get_edit_rate(agent_name: str, session: Session) -> float:
    """Fraction of approved sends where the draft was materially edited before approval.

    'Edited' = payload->>'edited_before_approval' IS TRUE, set by the Slack
    approve handler when it detects the body was changed by Josh before
    approving. Returns 0.0 when no approved sends exist (avoids division by
    zero and is the most accurate representation of an unproven agent).
    """
    row = session.execute(
        text(
            "SELECT "
            "  COUNT(*) FILTER (WHERE (payload->>'edited_before_approval')::boolean IS TRUE) AS edited, "
            "  COUNT(*) AS total "
            "FROM relay_approval_queue "
            "WHERE venture_key = :v AND status = 'sent' AND agent_name = :a"
        ),
        {"v": FA_MAX_VENTURE, "a": agent_name},
    ).mappings().first()
    if not row or not row["total"]:
        return 0.0
    return row["edited"] / row["total"]


def get_funded_loan_count(session: Session) -> int:
    """Count FA Max opportunities with outcome='funded'.

    Used for tier-C gate only. Reads from fa_max_opportunities.
    Returns 0 if the table is empty or doesn't exist yet (safe during
    early deployment when WP-1 tables are being applied).
    """
    try:
        row = session.execute(
            text(
                "SELECT COUNT(*) FROM fa_max_opportunities WHERE outcome = 'funded'"
            )
        ).scalar()
        return int(row or 0)
    except Exception:
        logger.warning("fa_max_opportunities not queryable yet; tier-C funded-loan count = 0")
        return 0
