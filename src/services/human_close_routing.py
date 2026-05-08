"""
Human close routing service.

Identifies high-intent subscribers who have not converted after multiple
Cora interactions, and routes them to a human closer via Slack.

Candidate criteria:
  - revenue_signal_score >= 85
  - >= 3 agent_decisions sent (not blocked) in last 14 days
  - no deal captured in last 14 days
  - no escalation in human_close_escalations in last 7 days
  - subscriber is active

Called by: src/tasks/human_close_sweep.py (weekday cron 0 13 * * 1-5)
Also triggered inline when Cora logs terminal_status='escalated'.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

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

HIGH_VALUE_TIERS = frozenset({
    "autopilot_pro", "partner", "annual_lock",
})


@dataclass
class HumanCloseCandidate:
    subscriber_id: int
    revenue_signal_score: int
    interactions_count: int
    target_tier: str
    last_decision_id: str
    subscriber: Subscriber


def find_candidates(db: Session) -> List[HumanCloseCandidate]:
    now = datetime.now(timezone.utc)
    interactions_cutoff = now - timedelta(days=INTERACTIONS_WINDOW_DAYS)
    deal_cutoff = now - timedelta(days=DEAL_WINDOW_DAYS)
    dedup_cutoff = now - timedelta(days=DEDUP_WINDOW_DAYS)

    # Active subscribers with high revenue signal score
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

        # Count sent Cora messages in window
        interactions = db.execute(
            select(func.count()).select_from(AgentDecision).where(
                AgentDecision.subscriber_id == sub.id,
                AgentDecision.terminal_status == "completed",
                AgentDecision.started_at >= interactions_cutoff,
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

        # No recent escalation
        recent_esc = db.execute(
            select(HumanCloseEscalation).where(
                HumanCloseEscalation.subscriber_id == sub.id,
                HumanCloseEscalation.routed_at >= dedup_cutoff,
            ).limit(1)
        ).scalar_one_or_none()
        if recent_esc:
            continue

        # Get last decision_id
        last_decision = db.execute(
            select(AgentDecision).where(
                AgentDecision.subscriber_id == sub.id,
            ).order_by(AgentDecision.started_at.desc()).limit(1)
        ).scalar_one_or_none()

        candidates.append(HumanCloseCandidate(
            subscriber_id=sub.id,
            revenue_signal_score=int(seg.revenue_signal_score or 0),
            interactions_count=interactions,
            target_tier=_infer_target_tier(sub),
            last_decision_id=last_decision.decision_id if last_decision else str(uuid.uuid4()),
            subscriber=sub,
        ))

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

    # Last 5 Cora messages sent
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

    return {
        "subscriber_id": sub.id,
        "name": sub.name or "Unknown",
        "email": sub.email or "",
        "vertical": sub.vertical,
        "county_id": sub.county_id,
        "current_tier": sub.tier,
        "target_tier": candidate.target_tier,
        "revenue_signal_score": candidate.revenue_signal_score,
        "interactions_count": candidate.interactions_count,
        "last_5_messages": messages_preview,
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


def route_to_slack(candidate: HumanCloseCandidate, context: dict) -> bool:
    """Send Slack notification. Returns True on success."""
    webhook = settings.slack_human_close_webhook
    if not webhook:
        logger.warning("SLACK_HUMAN_CLOSE_WEBHOOK not set — skipping human close route")
        return False

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
                "text": f"*Action:* {context['recommended_action']}",
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
        return True
    except Exception as exc:
        logger.error("human_close_routing Slack POST failed: %s", exc)
        return False


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
        channel=channel,
        context_json=context,
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
    """Full routing pipeline for one candidate. Returns True if successfully routed."""
    context = build_context(db, candidate)
    esc = record_escalation(db, candidate, context)
    success = route_to_slack(candidate, context)
    if not success:
        logger.warning(
            "human_close: Slack failed for sub=%s (row id=%s persisted)",
            candidate.subscriber_id, esc.id,
        )
    return success
