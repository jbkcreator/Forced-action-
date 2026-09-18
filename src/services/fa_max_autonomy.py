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

EXPLICIT SCOPE DECISION (WP-T2-2 review round 5): the split doc's prose
places "the autonomy tier table... in the registry configuration, not
scattered across individual agents." The three threshold constants below
(_TIER_A_MIN_SENDS etc.) live here, in this service module, not inside
src.agents.fa_max.tool_registry.FA_MAX_TOOL_REGISTRY. This is a deliberate
choice, not an oversight: the tool registry's entries describe CALLABLE
TOOLS (name, category, idempotency, whether a call requires the send gate)
— the tier thresholds describe GRADUATION POLICY, a different concern with
a different lifecycle (Josh/product may change a threshold without
touching what tools exist). Putting policy constants inside a tool-registry
dataclass would conflate the two. This module IS this WP's "central tier
policy": check_tier_gate() is the one function every send path calls, and
these constants are the one place the numbers are declared. WP-T2-2 is
hereby amended to state this explicitly.

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

    WP-T2-2: every count below is scoped to the (agent_name, tier) pair, not
    agent-only/global — an agent's evidence at tier A does not carry over to
    tier B/C, and one agent's evidence never counts toward another agent's
    gate.
    """
    sends = get_approved_send_count(agent_name, tier, session)

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
        rate = get_edit_rate(agent_name, tier, session)
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
        loans = get_funded_loan_count(agent_name, tier, session)
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


def get_approved_send_count(agent_name: str, tier: str, session: Session) -> int:
    """Count relay_approval_queue rows with status='sent' for this
    (agent_name, tier) pair — WP-T2-2: evidence no longer pools across
    tiers, so an agent's tier-A history never counts toward its tier-B/C
    gate."""
    row = session.execute(
        text(
            "SELECT COUNT(*) FROM relay_approval_queue "
            "WHERE venture_key = :v AND status = 'sent' "
            "AND agent_name = :a AND autonomy_tier_at_send = :tier"
        ),
        {"v": FA_MAX_VENTURE, "a": agent_name, "tier": tier},
    ).scalar()
    return int(row or 0)


def get_edit_rate(agent_name: str, tier: str, session: Session) -> float:
    """Lifetime fraction of approved sends where the draft was materially
    edited before approval, scoped to this (agent_name, tier) pair. Used by
    the graduation gate — see get_weekly_edit_rate() for the Friday report's
    current-ISO-week-scoped sibling; both read the same evidence column,
    different WHERE window.

    'Edited' = relay_approval_queue.material_edit IS TRUE, set by
    src.api.admin_router._handle_relay_revise_submission on a Slack Revise
    submission (a normalized-token-diff against the ORIGINAL draft, sticky
    across further revisions — see that function's docstring). WP-T2-2
    review fix: this previously read payload->>'edited_before_approval',
    a flag nothing in production ever wrote, so every agent's edit rate
    always computed as 0% regardless of how many drafts were actually
    revised before approval — silently letting Tier B's <10%-edit-rate gate
    pass on missing evidence rather than real evidence. Returns 0.0 when no
    approved sends exist (avoids division by zero and is the most accurate
    representation of an unproven agent).
    """
    row = session.execute(
        text(
            "SELECT "
            "  COUNT(*) FILTER (WHERE material_edit IS TRUE) AS edited, "
            "  COUNT(*) AS total "
            "FROM relay_approval_queue "
            "WHERE venture_key = :v AND status = 'sent' "
            "AND agent_name = :a AND autonomy_tier_at_send = :tier"
        ),
        {"v": FA_MAX_VENTURE, "a": agent_name, "tier": tier},
    ).mappings().first()
    if not row or not row["total"]:
        return 0.0
    return row["edited"] / row["total"]


def get_weekly_edit_rate(agent_name: str, tier: str, session: Session) -> float:
    """Edit rate scoped to the CURRENT ISO week (Monday 00:00 America/New_York
    through now), for the Friday weekly edit-rate operations report. Reads
    the same material_edit column as get_edit_rate() — the difference is
    the WHERE window, not the evidence source. Returns 0.0 when no approved
    sends exist this week."""
    from datetime import datetime, timedelta, timezone as _tz
    from zoneinfo import ZoneInfo

    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)
    week_start_eastern = (now_eastern - timedelta(days=now_eastern.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_start_utc = week_start_eastern.astimezone(_tz.utc)
    row = session.execute(
        text(
            "SELECT "
            "  COUNT(*) FILTER (WHERE material_edit IS TRUE) AS edited, "
            "  COUNT(*) AS total "
            "FROM relay_approval_queue "
            "WHERE venture_key = :v AND status = 'sent' "
            "AND agent_name = :a AND autonomy_tier_at_send = :tier "
            "AND dispatched_at >= :week_start"
        ),
        {"v": FA_MAX_VENTURE, "a": agent_name, "tier": tier, "week_start": week_start_utc},
    ).mappings().first()
    if not row or not row["total"]:
        return 0.0
    return row["edited"] / row["total"]


def get_funded_loan_count(agent_name: str, tier: str, session: Session) -> int:
    """Causal (not correlational) count of funded opportunities attributable
    to this (agent_name, tier) pair — used for the tier-C gate only.

    An opportunity counts only when its origin_interaction_id is non-NULL
    AND the joined fa_max_interactions row was authored by this agent at
    this tier (agent_name = :a AND autonomy_tier_at_time = :tier). NULL
    origin_interaction_id (unattributed) counts as zero — see
    FaMaxOpportunity.origin_interaction_id's docstring. Counts DISTINCT
    opportunity_id so a borrower touched by multiple interactions from the
    same agent/tier is never double-counted.

    Returns 0 if the tables aren't queryable yet (safe during early
    deployment when WP-1 tables are being applied).
    """
    try:
        row = session.execute(
            text(
                "SELECT COUNT(DISTINCT o.opportunity_id) "
                "FROM fa_max_opportunities o "
                "JOIN fa_max_interactions i ON i.interaction_id = o.origin_interaction_id "
                "WHERE o.outcome = 'funded' "
                "AND o.origin_interaction_id IS NOT NULL "
                "AND i.agent_name = :a AND i.autonomy_tier_at_time = :tier"
            ),
            {"a": agent_name, "tier": tier},
        ).scalar()
        return int(row or 0)
    except Exception:
        logger.warning("fa_max_opportunities/fa_max_interactions not queryable yet; tier-C funded-loan count = 0")
        return 0
