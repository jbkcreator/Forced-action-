"""
Human close routing service.

Identifies high-intent subscribers who have not converted after multiple
Cora interactions, and routes them to a human closer via Slack.

Candidate criteria:
  - revenue_signal_score >= 85
  - >= 3 agent_decisions sent (not blocked) in last 14 days
    (counted from after the most recent DealOutcome in window, if any)
  - no deal captured in last 14 days
  - no non-rescheduled escalation in human_close_escalations in last 7 days
  - passes value gate: vertical is hard_money_lenders OR target_tier price >= $397
  - subscriber is active

After routing, the escalation row is persisted first; Slack post is attempted
and tracked via posted_at / post_attempts. Failed posts are retried by
src/tasks/human_close_retry.py (nightly, max 3 attempts).

To claim an escalation: POST /api/admin/human-close/{id}/outcome with your name
as closer_assigned when setting the outcome.

Called by: src/tasks/human_close_sweep.py (weekday cron 0 13 * * 1-5)
           src/tasks/human_close_retry.py (nightly cron 30 1 * * *)
NOTE: src/agents/graphs/human_close_route.py graph is wired but no upstream
graph emits 'escalate_to_human_closer' yet — sweep is the only live path (MVP).
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import requests
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from config.revenue_ladder import REVENUE_LADDER
from config.settings import settings
from src.core.models import (
    AgentDecision,
    DealOutcome,
    HumanCloseEscalation,
    MessageOutcome,
    Subscriber,
    UserSegment,
)

logger = logging.getLogger(__name__)

# Thresholds
MIN_SCORE = 85
MIN_INTERACTIONS = 3
INTERACTIONS_WINDOW_DAYS = 14
DEAL_WINDOW_DAYS = 14
DEDUP_WINDOW_DAYS = 7
MAX_CANDIDATES_PER_SWEEP = 10  # enforces top 1-2% intent cap

# Value gate — route if vertical is explicitly high-value OR target tier price >= floor
HIGH_VALUE_VERTICALS = frozenset({"hard_money_lenders"})
HIGH_VALUE_PRICE_FLOOR_CENTS = 39700  # $397

_PRICE_BY_TIER: dict = {
    s["name"]: (
        s["price_cents"] if "price_cents" in s
        else s["price_cents_range"][0] if "price_cents_range" in s
        else 0
    )
    for s in REVENUE_LADDER
}


def _passes_value_gate(vertical: Optional[str], target_tier: str) -> bool:
    if vertical in HIGH_VALUE_VERTICALS:
        return True
    return (_PRICE_BY_TIER.get(target_tier) or 0) >= HIGH_VALUE_PRICE_FLOOR_CENTS


@dataclass
class HumanCloseCandidate:
    subscriber_id: int
    revenue_signal_score: int
    interactions_count: int
    target_tier: str
    last_decision_id: str
    subscriber: Subscriber
    target_tier_price_cents: int = 0
    vertical: Optional[str] = None


def find_candidates(db: Session) -> List[HumanCloseCandidate]:
    now = datetime.now(timezone.utc)
    interactions_cutoff = now - timedelta(days=INTERACTIONS_WINDOW_DAYS)
    deal_cutoff = now - timedelta(days=DEAL_WINDOW_DAYS)
    dedup_cutoff = now - timedelta(days=DEDUP_WINDOW_DAYS)

    segs = db.execute(
        select(UserSegment).where(
            UserSegment.revenue_signal_score >= MIN_SCORE,
        )
    ).scalars().all()

    candidates = []
    for seg in segs:
        sub = db.get(Subscriber, seg.subscriber_id)
        if not sub or sub.status != "active":
            continue

        target_tier = _infer_target_tier(sub)
        tier_price = _PRICE_BY_TIER.get(target_tier, 0)

        # Q1: hybrid value gate — HML vertical OR target tier >= $397
        if not _passes_value_gate(sub.vertical, target_tier):
            continue

        # Q3: time-bucket exclusion for DealOutcome in window
        # DealOutcome has no decision_id; count interactions starting from after
        # the most recent deal in the window (full window if no deal).
        # Note: DEAL_WINDOW_DAYS == INTERACTIONS_WINDOW_DAYS == 14, so the
        # no-recent-deal gate below makes this check always return full window;
        # kept for correctness if window sizes diverge.
        last_deal_ts = db.execute(
            select(DealOutcome.created_at).where(
                DealOutcome.subscriber_id == sub.id,
                DealOutcome.created_at >= interactions_cutoff,
            ).order_by(DealOutcome.created_at.desc()).limit(1)
        ).scalar()
        interactions_start = last_deal_ts if last_deal_ts else interactions_cutoff

        interactions = db.execute(
            select(func.count()).select_from(AgentDecision).where(
                AgentDecision.subscriber_id == sub.id,
                AgentDecision.terminal_status == "completed",
                AgentDecision.started_at >= interactions_start,
            )
        ).scalar() or 0

        if interactions < MIN_INTERACTIONS:
            continue

        # No deal captured recently
        recent_deal = db.execute(
            select(DealOutcome).where(
                DealOutcome.subscriber_id == sub.id,
                DealOutcome.created_at >= deal_cutoff,
            ).limit(1)
        ).scalar_one_or_none()
        if recent_deal:
            continue

        # Q5: dedup — ignore rescheduled rows so they re-enter the queue
        recent_esc = db.execute(
            select(HumanCloseEscalation).where(
                HumanCloseEscalation.subscriber_id == sub.id,
                HumanCloseEscalation.routed_at >= dedup_cutoff,
                or_(
                    HumanCloseEscalation.outcome.is_(None),
                    HumanCloseEscalation.outcome != "rescheduled",
                ),
            ).limit(1)
        ).scalar_one_or_none()
        if recent_esc:
            continue

        # Always generate a fresh decision_id for the escalation row to avoid
        # UniqueConstraint conflicts when a rescheduled sub re-enters.
        # The original last_decision_id is stored in context_json for traceability.
        last_decision = db.execute(
            select(AgentDecision).where(
                AgentDecision.subscriber_id == sub.id,
            ).order_by(AgentDecision.started_at.desc()).limit(1)
        ).scalar_one_or_none()
        original_decision_id = last_decision.decision_id if last_decision else None

        candidates.append(HumanCloseCandidate(
            subscriber_id=sub.id,
            revenue_signal_score=int(seg.revenue_signal_score or 0),
            interactions_count=interactions,
            target_tier=target_tier,
            last_decision_id=str(uuid.uuid4()),
            subscriber=sub,
            target_tier_price_cents=tier_price,
            vertical=sub.vertical,
        ))
        # stash original_decision_id so record_escalation can put it in context_json
        candidates[-1]._original_decision_id = original_decision_id  # type: ignore[attr-defined]

    # Q2: top-N cap — sort by score desc, cap at MAX_CANDIDATES_PER_SWEEP
    candidates.sort(key=lambda c: c.revenue_signal_score, reverse=True)
    pre_cap = len(candidates)
    candidates = candidates[:MAX_CANDIDATES_PER_SWEEP]
    if pre_cap > MAX_CANDIDATES_PER_SWEEP:
        logger.info(
            "human_close: capped candidates from %d to %d (MAX_CANDIDATES_PER_SWEEP=%d)",
            pre_cap, len(candidates), MAX_CANDIDATES_PER_SWEEP,
        )

    return candidates


def _infer_target_tier(sub: Subscriber) -> str:
    """Best next tier above the subscriber's current tier."""
    ladder = [
        "annual_lock", "autopilot_lite", "autopilot_pro", "partner",
    ]
    try:
        idx = ladder.index(sub.tier)
        return ladder[idx + 1] if idx + 1 < len(ladder) else sub.tier
    except ValueError:
        return "annual_lock"


def build_context(db: Session, candidate: HumanCloseCandidate) -> dict:
    sub = candidate.subscriber

    last_messages = db.execute(
        select(MessageOutcome).where(
            MessageOutcome.subscriber_id == sub.id,
        ).order_by(MessageOutcome.created_at.desc()).limit(5)
    ).scalars().all()

    messages_preview = [
        {
            "type": m.message_type,
            "body": (m.message_body or "")[:120],
            "sent_at": m.created_at.isoformat() if m.created_at else None,
        }
        for m in last_messages
    ]

    original_decision_id = getattr(candidate, "_original_decision_id", None)

    return {
        "subscriber_id": sub.id,
        "name": sub.name or "Unknown",
        "email": sub.email or "",
        "vertical": sub.vertical,
        "county_id": sub.county_id,
        "current_tier": sub.tier,
        "target_tier": candidate.target_tier,
        "target_tier_price_cents": candidate.target_tier_price_cents,
        "revenue_signal_score": candidate.revenue_signal_score,
        "interactions_count": candidate.interactions_count,
        "last_5_messages": messages_preview,
        "original_decision_id": original_decision_id,
        "recommended_action": (
            f"Call within 24h. Lead with {candidate.target_tier} offer. "
            f"Score={candidate.revenue_signal_score}/100. "
            f"{candidate.interactions_count} Cora touches, no conversion yet."
        ),
        "dashboard_url": (
            f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
            if sub.event_feed_uuid else ""
        ),
    }


def route_to_slack(candidate: HumanCloseCandidate, context: dict) -> Tuple[bool, Optional[str]]:
    """Send Slack notification. Returns (success, error_msg)."""
    webhook = settings.slack_human_close_webhook
    if not webhook:
        msg = "SLACK_HUMAN_CLOSE_WEBHOOK not set — skipping human close route"
        logger.warning(msg)
        return False, msg

    text = (
        f":rotating_light: *Human close needed* — {context['name']} "
        f"(sub #{context['subscriber_id']}, score {context['revenue_signal_score']})"
    )
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Tier now:* {context['current_tier']}"},
                {"type": "mrkdwn", "text": f"*Target:* {context['target_tier']}"},
                {"type": "mrkdwn", "text": f"*Score:* {context['revenue_signal_score']}/100"},
                {"type": "mrkdwn", "text": f"*Touches:* {context['interactions_count']}"},
                {"type": "mrkdwn", "text": f"*Vertical:* {context['vertical']}"},
                {"type": "mrkdwn", "text": f"*County:* {context['county_id']}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Action:* {context['recommended_action']}\n"
                    f"_Claim: POST closer_assigned to /api/admin/human-close/{{id}}/outcome_"
                ),
            },
        },
    ]
    if context.get("dashboard_url"):
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Open Dashboard"},
                "url": context["dashboard_url"],
            }],
        })

    try:
        resp = requests.post(
            webhook,
            json={"text": text, "blocks": blocks},
            timeout=10,
        )
        resp.raise_for_status()
        return True, None
    except Exception as exc:
        logger.error("human_close_routing Slack POST failed: %s", exc)
        return False, str(exc)


def record_escalation(
    db: Session,
    candidate: HumanCloseCandidate,
    context: dict,
    channel: str = "slack",
) -> HumanCloseEscalation:
    """Persist escalation row BEFORE sending notification."""
    esc = HumanCloseEscalation(
        subscriber_id=candidate.subscriber_id,
        decision_id=candidate.last_decision_id,
        revenue_signal_score=candidate.revenue_signal_score,
        interactions_count=candidate.interactions_count,
        target_tier=candidate.target_tier,
        target_tier_price_cents=candidate.target_tier_price_cents,
        vertical=candidate.vertical,
        channel=channel,
        context_json=context,
        post_attempts=0,
    )
    db.add(esc)
    db.flush()

    sub = db.get(Subscriber, candidate.subscriber_id)
    if sub:
        sub.escalation_routed_at = datetime.now(timezone.utc)
        sub.escalation_channel = channel
    db.flush()

    return esc


def route_candidate(db: Session, candidate: HumanCloseCandidate) -> bool:
    """Full routing pipeline for one candidate. Returns True if Slack post succeeded."""
    context = build_context(db, candidate)
    esc = record_escalation(db, candidate, context)
    success, error_msg = route_to_slack(candidate, context)
    now = datetime.now(timezone.utc)
    esc.post_attempts = (esc.post_attempts or 0) + 1
    if success:
        esc.posted_at = now
        esc.last_post_error = None
    else:
        esc.last_post_error = (error_msg or "")[:200]
        logger.warning(
            "human_close: Slack failed for sub=%s (row id=%s persisted, attempts=%d)",
            candidate.subscriber_id, esc.id, esc.post_attempts,
        )
    db.flush()
    return success


def retry_failed_posts(db: Session) -> dict:
    """Retry Slack posts for escalation rows where posted_at is NULL and attempts < 3."""
    now = datetime.now(timezone.utc)
    retry_cutoff = now - timedelta(days=3)

    rows = db.execute(
        select(HumanCloseEscalation).where(
            HumanCloseEscalation.posted_at.is_(None),
            HumanCloseEscalation.post_attempts < 3,
            HumanCloseEscalation.routed_at >= retry_cutoff,
        )
    ).scalars().all()

    results = {"retried": 0, "succeeded": 0, "failed": 0, "capped": 0}
    for esc in rows:
        context = esc.context_json or {}
        sub = db.get(Subscriber, esc.subscriber_id)
        candidate = HumanCloseCandidate(
            subscriber_id=esc.subscriber_id,
            revenue_signal_score=esc.revenue_signal_score,
            interactions_count=esc.interactions_count,
            target_tier=esc.target_tier,
            last_decision_id=esc.decision_id,
            subscriber=sub,
            target_tier_price_cents=esc.target_tier_price_cents or 0,
            vertical=esc.vertical,
        )
        success, error_msg = route_to_slack(candidate, context)
        esc.post_attempts = (esc.post_attempts or 0) + 1
        if success:
            esc.posted_at = now
            esc.last_post_error = None
            results["succeeded"] += 1
        else:
            esc.last_post_error = (error_msg or "")[:200]
            if esc.post_attempts >= 3:
                logger.error(
                    "human_close_retry: 3 failed Slack posts for escalation id=%s sub=%s — manual action required",
                    esc.id, esc.subscriber_id,
                )
                results["capped"] += 1
            else:
                results["failed"] += 1
        results["retried"] += 1

    return results
