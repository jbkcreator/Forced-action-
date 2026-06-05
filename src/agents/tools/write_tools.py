"""
Write tools for Cora graphs — priority-list scope.

Only two write tools are needed to close the four priority-list LangGraph
items (supervisor, FOMO, abandonment, retention):

	send_sms         — emit an SMS through the compliance-gated outbound path
	log_decision     — write one agent_decisions row per graph run

Both tools are idempotent. send_sms short-circuits when a duplicate
(subscriber, campaign, variant) send has occurred inside the 24-hour window.
log_decision uses the decision_id UUID as the primary key so a duplicate
call with the same decision_id merges into the existing row rather than
inserting a second.

Write tools deliberately stay thin — business logic stays in
src/services/*. These are the single entry points agents use to mutate
state so policy enforcement (compliance, audit) can live in one place.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, Optional

from sqlalchemy.orm import Session

from src.agents.override_reasons import normalize_override_reason_code
from src.agents.tools.registry import tool
from src.core.database import db
from src.core.models import AgentDecision, MessageOutcome, Subscriber, SmsOptIn

logger = logging.getLogger(__name__)

# Keys containing PII or large blobs that we strip before storing context_snapshot.
_SNAPSHOT_EXCLUDE_KEYS = {"unlock_link", "cta_url", "subscriber_first_name", "first_name"}


def _safe_snapshot(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a JSONB-safe subset of the render context, stripping PII and None-valued entries."""
    if not ctx:
        return None
    return {
        k: v for k, v in ctx.items()
        if k not in _SNAPSHOT_EXCLUDE_KEYS and v is not None and v != ""
    }


@contextmanager
def _session(provided: Optional[Session]) -> Generator[Session, None, None]:
	if provided is not None:
		yield provided
		return
	with db.session_scope() as s:
		yield s


# ──────────────────────────────────────────────────────────────────────────────
# send_sms
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="write", idempotent=True, requires_compliance=True)
def send_sms(
    subscriber_id: int,
    body: str,
    campaign: str,
    variant_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    message_type: str = "marketing",
    personalization_context: Optional[Dict[str, Any]] = None,
    session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Create a MessageOutcome row for every Cora SMS.

    If the message requires human review, it is stored as pending_review and is
    not sent immediately. If it does not require review, it is sent immediately
    through sms_compliance.send_sms().
    """
    from src.services.cora_suppression import has_active_suppression

    with _session(session) as s:
        if message_type != "transactional" and has_active_suppression(s, subscriber_id):
            logger.info(
                "Cora SMS suppressed by active cora_suppression subscriber=%s campaign=%s",
                subscriber_id,
                campaign,
            )
            return {
                "sent": False,
                "reason": "cora_suppressed",
                "subscriber_id": subscriber_id,
                "campaign": campaign,
                "variant_id": variant_id,
                "message_outcome_id": None,
            }

        opt_in = (
            s.query(SmsOptIn)
            .filter(SmsOptIn.subscriber_id == subscriber_id)
            .order_by(SmsOptIn.opted_in_at.desc())
            .first()
        )

        if opt_in is None:
            return {
                "sent": False,
                "reason": "no_phone",
                "subscriber_id": subscriber_id,
                "campaign": campaign,
                "variant_id": variant_id,
                "message_outcome_id": None,
            }

        phone = opt_in.phone
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)

        dup_q = (
            s.query(MessageOutcome)
            .filter(MessageOutcome.subscriber_id == subscriber_id)
            .filter(MessageOutcome.template_id == campaign)
            .filter(MessageOutcome.created_at >= cutoff)
        )

        if variant_id:
            dup_q = dup_q.filter(MessageOutcome.variant_id == variant_id)

        duplicate = dup_q.first()
        if duplicate is not None:
            return {
                "sent": False,
                "reason": "duplicate",
                "subscriber_id": subscriber_id,
                "campaign": campaign,
                "variant_id": variant_id,
                "message_outcome_id": duplicate.id,
            }

        ctx = personalization_context or {}

        # Hold for human review ONLY when the operator has flipped the
        # human-review switch ON. Default is OFF → messages send immediately
        # (we do not hold every marketing send waiting on approval). When the
        # switch is ON, Cora's outbound marketing — and anything a graph
        # explicitly flags via ctx['requires_review'] — is queued for approve/
        # cancel. Transactional sends (receipts, opt-in prompts) never hold.
        from src.services.cora_review_switch import is_review_enabled

        requires_review = is_review_enabled() and (
            message_type == "marketing" or bool(ctx.get("requires_review"))
        )

        outcome = MessageOutcome(
            subscriber_id=subscriber_id,
            message_type="sms",
            template_id=campaign,
            variant_id=variant_id,
            channel="telnyx",
            decision_id=decision_id,
            send_status="pending_review" if requires_review else "approved",
            requires_review=requires_review,
            review_reason=ctx.get("review_reason") if requires_review else None,
            scheduled_send_at=now,
            sent_at=now if not requires_review else None,
            trade_vertical=ctx.get("vertical") or None,
            county_id=ctx.get("county_id") or None,
            behavioral_segment=ctx.get("behavioral_segment") or None,
            revenue_signal_score=ctx.get("revenue_signal_score"),
            revenue_signal_score_band=ctx.get("revenue_signal_score_band") or None,
            last_action_recency_band=ctx.get("last_action_recency_band") or None,
            prompt_version=ctx.get("prompt_version") or None,
            context_snapshot={
                **(_safe_snapshot(ctx) or {}),
                "body": body,
                "phone": phone,
            },
        )
        s.add(outcome)
        s.flush()

        if requires_review:
            return {
                "sent": False,
                "reason": "pending_review",
                "subscriber_id": subscriber_id,
                "campaign": campaign,
                "variant_id": variant_id,
                "message_outcome_id": outcome.id,
            }

        from src.services import sms_compliance

        ok = sms_compliance.send_sms(
            to=phone,
            body=body,
            db=s,
            message_type=message_type,
            subscriber_id=subscriber_id,
            task_type=campaign,
            campaign=campaign,
            variant_id=variant_id,
            decision_id=decision_id,
        )

        if not ok:
            outcome.send_status = "failed"
            return {
                "sent": False,
                "reason": "opted_out_or_sms_error",
                "subscriber_id": subscriber_id,
                "campaign": campaign,
                "variant_id": variant_id,
                "message_outcome_id": outcome.id,
            }

        outcome.send_status = "sent"
        outcome.sent_at = datetime.now(timezone.utc)

        return {
            "sent": True,
            "reason": "ok",
            "subscriber_id": subscriber_id,
            "campaign": campaign,
            "variant_id": variant_id,
            "message_outcome_id": outcome.id,
        }

# ──────────────────────────────────────────────────────────────────────────────
# log_decision
# ──────────────────────────────────────────────────────────────────────────────

_VALID_AUTONOMY_CLASSES = {
	"autonomous", "approval_required", "approved",
	"rejected", "overridden", "recommendation_only",
}


@tool(category="write", idempotent=True)
def log_decision(
	decision_id: str,
	graph_name: str,
	subscriber_id: Optional[int] = None,
	event_type: Optional[str] = None,
	terminal_status: Optional[str] = None,
	tokens_used: int = 0,
	cost_usd: float = 0.0,
	summary: Optional[Dict[str, Any]] = None,
	variant_id: Optional[str] = None,
	# fa036 — autonomy tracking. Defaults preserve back-compat: every
	# existing caller is implicitly 'autonomous' and was_autonomous=TRUE.
	autonomy_class: Optional[str] = "autonomous",
	requires_approval: bool = False,
	approved_at: Optional[datetime] = None,
	approved_by: Optional[str] = None,
	overridden_at: Optional[datetime] = None,
	overridden_by: Optional[str] = None,
	override_reason_code: Optional[str] = None,
	override_reason: Optional[str] = None,
	playbook_id: Optional[int] = None,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Upsert one row into agent_decisions for a graph run.

	Called twice per decision:
	  - once at graph start (terminal_status=None) to establish the row
	  - once at graph end (terminal_status set) to finalize with totals

	Idempotent by decision_id (the primary key). A second call with the
	same decision_id updates the existing row rather than inserting a new
	one, so graphs can safely re-log on resume after a crash.

	fa036 autonomy fields:
	  - autonomy_class:    classification at decision time. Default 'autonomous'.
	                       Callers escalating to humans pass 'approval_required'.
	  - was_autonomous:    sticky flag, NOT a kwarg. Set TRUE on first
	                       'autonomous' classification, never cleared.
	                       Metric 2 ("% overridden") queries on this.
	  - approved_at / by:  set when a human approves a previously-pending decision.
	  - overridden_at / by / reason_code / reason: set when a human reverses an
	                                      autonomous decision.
	  - playbook_id:       optional link to the cora_playbook that drove this decision.

	Returns the final persisted state of the row.
	"""
	valid_statuses = {"completed", "aborted", "escalated", "failed", None}
	if terminal_status not in valid_statuses:
		raise ValueError(
			f"terminal_status must be one of {valid_statuses}, got {terminal_status!r}"
		)
	if autonomy_class is not None and autonomy_class not in _VALID_AUTONOMY_CLASSES:
		raise ValueError(
			f"autonomy_class must be one of {_VALID_AUTONOMY_CLASSES}, "
			f"got {autonomy_class!r}"
		)
	normalized_override_reason_code = normalize_override_reason_code(override_reason_code)
	if overridden_at is not None and normalized_override_reason_code is None:
		raise ValueError(
			"override_reason_code is required when overridden_at is provided"
		)

	with _session(session) as s:
		row = (
			s.query(AgentDecision)
			.filter(AgentDecision.decision_id == decision_id)
			.first()
		)

		if row is None:
			row = AgentDecision(
				decision_id=decision_id,
				graph_name=graph_name,
				subscriber_id=subscriber_id,
				event_type=event_type,
				started_at=datetime.now(timezone.utc),
				terminal_status=terminal_status,
				tokens_used=tokens_used,
				cost_usd=cost_usd,
				summary=summary,
				variant_id=variant_id,
				autonomy_class=autonomy_class,
				# was_autonomous is sticky — set TRUE on first 'autonomous'
				# classification, never cleared afterward.
				was_autonomous=(autonomy_class == "autonomous"),
				requires_approval=requires_approval,
				approved_at=approved_at,
				approved_by=approved_by,
				overridden_at=overridden_at,
				overridden_by=overridden_by,
				override_reason_code=normalized_override_reason_code,
				override_reason=override_reason,
				playbook_id=playbook_id,
			)
			s.add(row)
		else:
			# Update — but never regress non-null identifying fields.
			if subscriber_id is not None:
				row.subscriber_id = subscriber_id
			if event_type is not None:
				row.event_type = event_type
			if terminal_status is not None:
				row.terminal_status = terminal_status
				row.completed_at = datetime.now(timezone.utc)
			# Always take the larger of the counters (we accept additive updates).
			row.tokens_used = max(row.tokens_used or 0, tokens_used)
			row.cost_usd = max(float(row.cost_usd or 0), float(cost_usd))
			if summary is not None:
				row.summary = summary
			if variant_id is not None and row.variant_id is None:
				row.variant_id = variant_id

			# fa036 — autonomy field updates. autonomy_class is mutable
			# (autonomous → overridden / rejected after the fact). The
			# was_autonomous flag is STICKY: once TRUE it stays TRUE.
			if autonomy_class is not None:
				row.autonomy_class = autonomy_class
				if autonomy_class == "autonomous" and not row.was_autonomous:
					row.was_autonomous = True
			if requires_approval and not row.requires_approval:
				row.requires_approval = True
			if approved_at is not None and row.approved_at is None:
				row.approved_at = approved_at
				row.approved_by = approved_by
			if overridden_at is not None and row.overridden_at is None:
				row.overridden_at = overridden_at
				row.overridden_by = overridden_by
				row.override_reason_code = normalized_override_reason_code
				row.override_reason = override_reason
			if playbook_id is not None and row.playbook_id is None:
				row.playbook_id = playbook_id

		s.flush()

		return {
			"decision_id": row.decision_id,
			"graph_name": row.graph_name,
			"subscriber_id": row.subscriber_id,
			"event_type": row.event_type,
			"terminal_status": row.terminal_status,
			"tokens_used": row.tokens_used,
			"cost_usd": float(row.cost_usd or 0),
			"started_at": row.started_at.isoformat() if row.started_at else None,
			"completed_at": row.completed_at.isoformat() if row.completed_at else None,
			"autonomy_class": row.autonomy_class,
			"was_autonomous": bool(row.was_autonomous),
			"requires_approval": bool(row.requires_approval),
			"approved_at": row.approved_at.isoformat() if row.approved_at else None,
			"overridden_at": row.overridden_at.isoformat() if row.overridden_at else None,
			"override_reason_code": getattr(row, "override_reason_code", None),
			"override_reason": getattr(row, "override_reason", None),
			"playbook_id": row.playbook_id,
		}
