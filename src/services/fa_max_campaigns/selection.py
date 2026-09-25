"""Campaign selection engine — plan Sections 6.2, 6.6, 6.7, 6.9, 6.10.

Two sweeps call into this module:

  - The daily enrollment sweep (src.tasks.fa_max_campaign_enrollment_sweep):
    `run_enrollment_sweep()` re-checks every existing active/paused
    enrollment (pause/resume/cancel), then enrolls new candidates from
    eligibility.py, one campaign at a time by priority, respecting the
    daily cap and the switch-up-only rule.

  - The 15-minute due-step sweep (src.tasks.fa_max_campaign_due_steps):
    `process_due_touches()` hands each due, unblocked touch to the right
    drafting agent's work queue and never writes consent.

`cancel_enrollments()` is also called directly by the opt-out hooks
(plan Section 6.6) — it is idempotent and safe to call for a person with no
enrollment at all (a no-op).

Every function commits its own work (matching the existing codebase
convention — see src/agents/reply_concierge/abandonment_agent.py). A
`dry_run=True` sweep runs the exact same decision logic but performs no
writes, so nothing needs to be rolled back afterward.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config import fa_max_campaigns as cfg
from src.services.fa_max_campaigns import content, eligibility
from src.services.fa_max_campaigns.blocks import (
    channel_readiness,
    enrollment_block_reason,
    has_any_unsuppressed_channel,
    reachable_channels,
)
from src.services.state_engine import enqueue_work_item

# channel_readiness() reasons that mean "no consent recorded" (as opposed to
# a suppression reason such as email_opt_out / sms compliance / DNC). Only
# these ever HOLD a touch (waiting for a consent row this module never
# writes) — every other blocked reason SKIPs the channel and advances, since
# it reflects something about the recipient/channel, not a missing opt-in.
_NO_CONSENT_REASONS = frozenset({"consent_absent", "consent_withdrawn"})

logger = logging.getLogger(__name__)

_ACTOR = "system:fa_max_campaign_selection"


@dataclass
class SweepSummary:
    enrolled: dict[str, int] = field(default_factory=dict)
    switched: int = 0
    paused: int = 0
    resumed: int = 0
    cancelled: dict[str, int] = field(default_factory=dict)
    unresolved: dict[str, int] = field(default_factory=dict)
    rules_disabled: list[str] = field(default_factory=list)


# ── Enrollment row helpers ───────────────────────────────────────────────────

def _get_active_or_paused(session: Session, person_id: str) -> Optional[dict]:
    row = session.execute(
        text(
            "SELECT enrollment_id::text, campaign_key, audience, sequence_version, status, "
            "last_touch_sent_at "
            "FROM fa_max_campaign_enrollments "
            "WHERE person_id = CAST(:pid AS uuid) AND status IN ('active', 'paused')"
        ),
        {"pid": person_id},
    ).mappings().fetchone()
    return dict(row) if row else None


def _latest_sequence_version(session: Session, campaign_key: str) -> int:
    version = session.execute(
        text(
            "SELECT COALESCE(MAX(sequence_version), 0) "
            "FROM fa_max_campaign_sequence_steps WHERE campaign_key = :ck"
        ),
        {"ck": campaign_key},
    ).scalar_one()
    return int(version)


def _log_event(session: Session, enrollment_id: str, event: str, reason: Optional[str]) -> None:
    session.execute(
        text(
            "INSERT INTO fa_max_campaign_enrollment_events (enrollment_id, event, reason, actor) "
            "VALUES (CAST(:eid AS uuid), :event, :reason, :actor)"
        ),
        {"eid": enrollment_id, "event": event, "reason": reason, "actor": _ACTOR},
    )


def _step_one_config(session: Session, campaign_key: str, sequence_version: int) -> Optional[dict]:
    row = session.execute(
        text(
            "SELECT step, channel FROM fa_max_campaign_sequence_steps "
            "WHERE campaign_key = :ck AND sequence_version = :ver AND step = 1"
        ),
        {"ck": campaign_key, "ver": sequence_version},
    ).mappings().fetchone()
    return dict(row) if row else None


def _schedule_touch(
    session: Session, *, enrollment_id: str, step: int, channel: str, due_at: datetime,
) -> None:
    idem = hashlib.sha256(f"campaign_touch:{enrollment_id}:{step}".encode()).hexdigest()[:48]
    session.execute(
        text(
            "INSERT INTO fa_max_campaign_touches "
            "(enrollment_id, step, channel, due_at, idempotency_key) "
            "VALUES (CAST(:eid AS uuid), :step, :channel, :due_at, :idem) "
            "ON CONFLICT (idempotency_key) DO NOTHING"
        ),
        {"eid": enrollment_id, "step": step, "channel": channel, "due_at": due_at, "idem": idem},
    )


def _create_enrollment(
    session: Session, *, candidate: "eligibility.EligibilityCandidate", campaign_key: str,
    status: str, dry_run: bool,
) -> Optional[str]:
    if dry_run:
        return None
    import json as _json

    sequence_version = _latest_sequence_version(session, campaign_key)
    row = session.execute(
        text(
            "INSERT INTO fa_max_campaign_enrollments "
            "(person_id, campaign_key, audience, sequence_version, status, trigger_type, "
            " trigger_reason, source, property_id, county_id, state, trigger_context) "
            "VALUES (CAST(:pid AS uuid), :ck, :audience, :ver, :status, :ttype, :treason, "
            " :source, :property_id, :county_id, :state, CAST(:context AS jsonb)) "
            "RETURNING enrollment_id::text"
        ),
        {
            "pid": candidate.person_id,
            "ck": campaign_key,
            "audience": candidate.audience,
            "ver": sequence_version,
            "status": status,
            "ttype": candidate.trigger_type,
            "treason": candidate.trigger_reason,
            "source": candidate.trigger_type,
            "property_id": candidate.property_id,
            "county_id": candidate.county_id,
            "state": candidate.state,
            "context": _json.dumps(candidate.extra, default=str),
        },
    ).fetchone()
    enrollment_id = row[0]
    _log_event(session, enrollment_id, "enrolled", candidate.trigger_reason)

    if status == "active" and sequence_version > 0:
        step_cfg = _step_one_config(session, campaign_key, sequence_version)
        if step_cfg:
            _schedule_touch(
                session, enrollment_id=enrollment_id, step=1,
                channel=step_cfg["channel"], due_at=datetime.now(timezone.utc),
            )
    session.commit()
    return enrollment_id


def cancel_enrollments(session: Session, *, person_id: str, reason: str) -> int:
    """End any active/paused enrollment for this person and cancel its
    scheduled touches. Idempotent — a person with no enrollment is a no-op.
    Called both by the sweeps and directly by the opt-out hooks."""
    row = session.execute(
        text(
            "UPDATE fa_max_campaign_enrollments "
            "SET status = 'cancelled', ended_at = NOW(), end_reason = :reason, updated_at = NOW() "
            "WHERE person_id = CAST(:pid AS uuid) AND status IN ('active', 'paused') "
            "RETURNING enrollment_id::text"
        ),
        {"pid": person_id, "reason": reason},
    ).fetchone()
    if not row:
        session.commit()
        return 0
    enrollment_id = row[0]
    _log_event(session, enrollment_id, "cancelled", reason)
    session.execute(
        text(
            "UPDATE fa_max_campaign_touches SET status = 'cancelled', status_reason = :reason "
            "WHERE enrollment_id = CAST(:eid AS uuid) AND status IN ('scheduled', 'held')"
        ),
        {"eid": enrollment_id, "reason": reason},
    )
    session.commit()
    logger.info("fa_max_campaigns: cancelled enrollment %s for person_id=%s reason=%s", enrollment_id, person_id, reason)
    return 1


def _switch_enrollment(
    session: Session, *, existing: dict, candidate: "eligibility.EligibilityCandidate",
    new_campaign: str, dry_run: bool,
) -> Optional[str]:
    if dry_run:
        return None
    session.execute(
        text(
            "UPDATE fa_max_campaign_enrollments "
            "SET status = 'preempted', ended_at = NOW(), end_reason = :reason, updated_at = NOW() "
            "WHERE enrollment_id = CAST(:eid AS uuid)"
        ),
        {"eid": existing["enrollment_id"], "reason": f"preempted_by:{new_campaign}"},
    )
    _log_event(session, existing["enrollment_id"], "preempted", f"preempted_by:{new_campaign}")
    session.execute(
        text(
            "UPDATE fa_max_campaign_touches SET status = 'cancelled', status_reason = 'preempted' "
            "WHERE enrollment_id = CAST(:eid AS uuid) AND status IN ('scheduled', 'held')"
        ),
        {"eid": existing["enrollment_id"]},
    )

    import json as _json

    sequence_version = _latest_sequence_version(session, new_campaign)
    row = session.execute(
        text(
            "INSERT INTO fa_max_campaign_enrollments "
            "(person_id, campaign_key, audience, sequence_version, status, trigger_type, "
            " trigger_reason, source, property_id, county_id, state, trigger_context) "
            "VALUES (CAST(:pid AS uuid), :ck, :audience, :ver, 'active', :ttype, :treason, "
            " :source, :property_id, :county_id, :state, CAST(:context AS jsonb)) "
            "RETURNING enrollment_id::text"
        ),
        {
            "pid": candidate.person_id, "ck": new_campaign, "audience": candidate.audience,
            "ver": sequence_version, "ttype": candidate.trigger_type, "treason": candidate.trigger_reason,
            "source": candidate.trigger_type, "property_id": candidate.property_id,
            "county_id": candidate.county_id, "state": candidate.state,
            "context": _json.dumps(candidate.extra, default=str),
        },
    ).fetchone()
    new_id = row[0]
    _log_event(session, new_id, "enrolled", f"switched_from:{existing['campaign_key']}")

    earliest = datetime.now(timezone.utc)
    last_sent = existing.get("last_touch_sent_at")
    if last_sent:
        earliest = max(earliest, last_sent + timedelta(days=cfg.MIN_GAP_DAYS))

    step_cfg = _step_one_config(session, new_campaign, sequence_version)
    if step_cfg:
        _schedule_touch(session, enrollment_id=new_id, step=1, channel=step_cfg["channel"], due_at=earliest)
    session.commit()
    return new_id


# ── Existing-enrollment housekeeping (pause / resume / cancel) ──────────────

def _recheck_existing_enrollments(session: Session, *, dry_run: bool) -> tuple[int, int, dict[str, int]]:
    """Re-run the block check against every currently active/paused
    enrollment. Returns (paused_count, resumed_count, cancelled_by_reason).
    """
    paused = resumed = 0
    cancelled: dict[str, int] = {}

    rows = session.execute(
        text(
            "SELECT enrollment_id::text, person_id::text, campaign_key, status, sequence_version "
            "FROM fa_max_campaign_enrollments WHERE status IN ('active', 'paused')"
        ),
    ).mappings().all()

    for row in rows:
        block = enrollment_block_reason(session, person_id=row["person_id"])

        if block.blocked and not block.pause:
            if not dry_run:
                cancel_enrollments(session, person_id=row["person_id"], reason=block.reason)
            cancelled[block.reason] = cancelled.get(block.reason, 0) + 1
            continue

        if block.blocked and block.pause:
            if row["status"] == "active":
                paused += 1
                if not dry_run:
                    session.execute(
                        text(
                            "UPDATE fa_max_campaign_enrollments SET status = 'paused', updated_at = NOW() "
                            "WHERE enrollment_id = CAST(:eid AS uuid)"
                        ),
                        {"eid": row["enrollment_id"]},
                    )
                    _log_event(session, row["enrollment_id"], "paused", block.reason)
                    session.execute(
                        text(
                            "UPDATE fa_max_campaign_touches SET status = 'cancelled', status_reason = 'paused' "
                            "WHERE enrollment_id = CAST(:eid AS uuid) AND status IN ('scheduled', 'held')"
                        ),
                        {"eid": row["enrollment_id"]},
                    )
                    session.commit()
            continue

        # Not blocked. A paused enrollment whose block cleared resumes.
        if row["status"] == "paused":
            resumed += 1
            if not dry_run:
                session.execute(
                    text(
                        "UPDATE fa_max_campaign_enrollments SET status = 'active', updated_at = NOW() "
                        "WHERE enrollment_id = CAST(:eid AS uuid)"
                    ),
                    {"eid": row["enrollment_id"]},
                )
                _log_event(session, row["enrollment_id"], "resumed", "block_cleared")
                # 'sent' OR 'skipped' — a skipped step already advanced the
                # sequence past it (see _skip_touch); resuming from
                # MAX(step) WHERE status='sent' alone would re-target a step
                # that was already correctly skipped and re-process it.
                last_step = session.execute(
                    text(
                        "SELECT COALESCE(MAX(step), 0) FROM fa_max_campaign_touches "
                        "WHERE enrollment_id = CAST(:eid AS uuid) AND status IN ('sent', 'skipped')"
                    ),
                    {"eid": row["enrollment_id"]},
                ).scalar_one()
                next_step_cfg = session.execute(
                    text(
                        "SELECT step, channel FROM fa_max_campaign_sequence_steps "
                        "WHERE campaign_key = :ck AND sequence_version = :ver AND step = :next_step"
                    ),
                    {"ck": row["campaign_key"], "ver": row["sequence_version"], "next_step": last_step + 1},
                ).mappings().fetchone()
                if next_step_cfg:
                    last_sent = session.execute(
                        text(
                            "SELECT MAX(sent_at) FROM fa_max_campaign_touches "
                            "WHERE enrollment_id = CAST(:eid AS uuid) AND status = 'sent'"
                        ),
                        {"eid": row["enrollment_id"]},
                    ).scalar_one()
                    earliest = datetime.now(timezone.utc)
                    if last_sent:
                        earliest = max(earliest, last_sent + timedelta(days=cfg.MIN_GAP_DAYS))
                    _schedule_touch(
                        session, enrollment_id=row["enrollment_id"], step=next_step_cfg["step"],
                        channel=next_step_cfg["channel"], due_at=earliest,
                    )
                session.commit()

    return paused, resumed, cancelled


# ── New-candidate enrollment ─────────────────────────────────────────────────

_ELIGIBILITY_FUNCS = {
    cfg.CAMPAIGN_EXIT_DESK: eligibility.exit_desk_candidates,
    cfg.CAMPAIGN_CAPITAL_DESK_LOOP: eligibility.capital_desk_loop_candidates,
    cfg.CAMPAIGN_RESCUE_CIRCUIT: eligibility.rescue_circuit_candidates,
}


def _today_enrollment_count(session: Session, campaign_key: str) -> int:
    return session.execute(
        text(
            "SELECT COUNT(*) FROM fa_max_campaign_enrollments "
            "WHERE campaign_key = :ck AND enrolled_at >= date_trunc('day', NOW())"
        ),
        {"ck": campaign_key},
    ).scalar_one()


def _in_cooldown(session: Session, person_id: str, campaign_key: str) -> bool:
    row = session.execute(
        text(
            "SELECT ended_at FROM fa_max_campaign_enrollments "
            "WHERE person_id = CAST(:pid AS uuid) AND campaign_key = :ck AND status = 'completed' "
            "ORDER BY ended_at DESC LIMIT 1"
        ),
        {"pid": person_id, "ck": campaign_key},
    ).fetchone()
    if not row or not row[0]:
        return False
    return datetime.now(timezone.utc) - row[0] < timedelta(days=cfg.REENROLL_COOLDOWN_DAYS)


def run_enrollment_sweep(session: Session, *, dry_run: bool = False) -> SweepSummary:
    """The daily enrollment sweep. Order: housekeeping first (pause/resume/
    cancel existing enrollments), then new candidates by campaign priority
    (plan Section 6.7 — switching up only, so priority order here matters:
    Exit Desk claims a person before Capital Desk Loop gets a chance to)."""
    summary = SweepSummary()

    paused, resumed, cancelled = _recheck_existing_enrollments(session, dry_run=dry_run)
    summary.paused, summary.resumed, summary.cancelled = paused, resumed, cancelled

    candidates_by_campaign: dict[str, list] = {}
    for campaign_key in cfg.CAMPAIGN_PRIORITY:
        if not cfg.CAMPAIGN_ENABLED[campaign_key]:
            summary.rules_disabled.append(campaign_key)
            continue
        result = _ELIGIBILITY_FUNCS[campaign_key](session)
        candidates_by_campaign[campaign_key] = result.candidates
        summary.unresolved[campaign_key] = result.unresolved_count

    # person_id -> winning (campaign_key, candidate), plus every other match
    winners: dict[str, tuple[str, object]] = {}
    also_matched: dict[str, list[str]] = {}
    for campaign_key in cfg.CAMPAIGN_PRIORITY:
        for candidate in candidates_by_campaign.get(campaign_key, []):
            if candidate.person_id not in winners:
                winners[candidate.person_id] = (campaign_key, candidate)
            else:
                also_matched.setdefault(candidate.person_id, []).append(campaign_key)

    enrolled_today: dict[str, int] = {ck: _today_enrollment_count(session, ck) for ck in cfg.CAMPAIGNS}

    for person_id, (campaign_key, candidate) in winners.items():
        block = enrollment_block_reason(session, person_id=person_id)
        if block.blocked and not block.pause:
            continue
        if not has_any_unsuppressed_channel(session, person_id=person_id):
            continue
        if _in_cooldown(session, person_id, campaign_key):
            continue

        existing = _get_active_or_paused(session, person_id)

        if existing and existing["campaign_key"] == campaign_key:
            continue  # already exactly where they belong

        if existing:
            existing_priority = cfg.CAMPAIGN_PRIORITY.index(existing["campaign_key"])
            new_priority = cfg.CAMPAIGN_PRIORITY.index(campaign_key)
            if new_priority >= existing_priority:
                continue  # never switch down, and never re-enroll into a lower/equal slot
            # A switch starts a fresh step-1 send under the new campaign —
            # it counts against that campaign's daily cap exactly like a
            # brand-new enrollment (plan Section 6.9: protect the sending
            # domain during warm-up).
            if not block.pause and enrolled_today.get(campaign_key, 0) < cfg.MAX_NEW_ENROLLMENTS_PER_DAY[campaign_key]:
                enrolled_id = _switch_enrollment(
                    session, existing=existing, candidate=candidate, new_campaign=campaign_key, dry_run=dry_run,
                )
                if enrolled_id or dry_run:
                    summary.switched += 1
                    enrolled_today[campaign_key] = enrolled_today.get(campaign_key, 0) + 1
            continue

        if enrolled_today.get(campaign_key, 0) >= cfg.MAX_NEW_ENROLLMENTS_PER_DAY[campaign_key]:
            continue

        status = "paused" if block.pause else "active"
        enrolled_id = _create_enrollment(
            session, candidate=candidate, campaign_key=campaign_key, status=status, dry_run=dry_run,
        )
        if enrolled_id or dry_run:
            summary.enrolled[campaign_key] = summary.enrolled.get(campaign_key, 0) + 1
            enrolled_today[campaign_key] = enrolled_today.get(campaign_key, 0) + 1
            if also_matched.get(person_id) and enrolled_id and not dry_run:
                session.execute(
                    text(
                        "UPDATE fa_max_campaign_enrollments SET also_matched = CAST(:m AS jsonb) "
                        "WHERE enrollment_id = CAST(:eid AS uuid)"
                    ),
                    {"m": __import__("json").dumps(also_matched[person_id]), "eid": enrolled_id},
                )
                session.commit()

    return summary


# ── Due-step sweep (every 15 minutes) ───────────────────────────────────────

def process_due_touches(session: Session, *, now: Optional[datetime] = None, limit: int = 200) -> dict:
    """Hand every due, unblocked touch to the right agent's work queue.
    Never writes consent (plan Section 6.5) — a channel with no consent row
    holds (email) or skips (sms) rather than bypassing the gate."""
    now = now or datetime.now(timezone.utc)
    summary = {"handed_off": 0, "held": 0, "skipped": 0, "ended_no_channel": 0}

    rows = session.execute(
        text(
            "SELECT t.touch_id, t.enrollment_id::text, t.step, t.channel, "
            "       e.person_id::text, e.campaign_key, e.audience, e.sequence_version, "
            "       e.trigger_type, e.trigger_reason, e.property_id, e.county_id, e.state, "
            "       e.trigger_context "
            "FROM fa_max_campaign_touches t "
            "JOIN fa_max_campaign_enrollments e ON e.enrollment_id = t.enrollment_id "
            "WHERE t.status IN ('scheduled', 'held') AND t.due_at <= :now "
            "  AND e.status = 'active' "
            "ORDER BY t.due_at LIMIT :limit"
        ),
        {"now": now, "limit": limit},
    ).mappings().all()

    for row in rows:
        _process_one_touch(session, dict(row), summary)

    return summary


def _process_one_touch(session: Session, touch: dict, summary: dict) -> None:
    person_id = touch["person_id"]
    channel = touch["channel"]
    enrollment_id = touch["enrollment_id"]

    block = enrollment_block_reason(session, person_id=person_id)
    if block.blocked and not block.pause:
        cancel_enrollments(session, person_id=person_id, reason=block.reason)
        return
    if block.blocked and block.pause:
        return  # housekeeping pass will flip status to paused on the next daily sweep

    person_row = session.execute(
        text("SELECT email, phone FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    recipient = (person_row[0] if channel == "email" else person_row[1]) if person_row else None
    if not recipient:
        # No identifier for THIS channel — channel-specific, not person-wide
        # (plan Section 6.5): skip and advance, the same as any other
        # channel-specific gate below, rather than stalling the enrollment
        # forever with no visible reason (see plan revision notes).
        _skip_touch(session, touch, channel_reason=f"no_{channel}_identifier")
        summary["skipped"] += 1
        _end_if_no_channel_left(session, enrollment_id, person_id)
        return

    gate = channel_readiness(session, person_id=person_id, recipient=recipient, channel=channel)
    if gate.blocked:
        if gate.reason in _NO_CONSENT_REASONS and channel == "email":
            # Email-only: hold rather than skip, so the touch is retried
            # once a consent row lands rather than being lost to the next
            # step (plan Section 6.5 — never write consent, never bypass).
            _hold_touch(session, touch, reason="no_email_consent")
            summary["held"] += 1
            return
        _skip_touch(session, touch, channel_reason=gate.reason)
        summary["skipped"] += 1
        _end_if_no_channel_left(session, enrollment_id, person_id)
        return

    trigger_context = touch.get("trigger_context") or {}
    enrollment_ctx = {
        "person_id": person_id,
        "property_id": touch["property_id"],
        "state": touch["state"],
        **trigger_context,
    }
    values, missing_required = content.resolve_merge_values(
        session, campaign_key=touch["campaign_key"], enrollment=enrollment_ctx,
    )
    if missing_required:
        _hold_touch(session, touch, reason=f"missing_merge:{','.join(missing_required)}")
        summary["held"] += 1
        return

    step_cfg = session.execute(
        text(
            "SELECT subject, body_template FROM fa_max_campaign_sequence_steps "
            "WHERE campaign_key = :ck AND sequence_version = :ver AND step = :step"
        ),
        {"ck": touch["campaign_key"], "ver": touch["sequence_version"], "step": touch["step"]},
    ).mappings().fetchone()
    if not step_cfg:
        _hold_touch(session, touch, reason="sequence_content_missing")
        summary["held"] += 1
        return

    queue_name = cfg.AUDIENCE_HANDOFF_QUEUE[touch["audience"]]
    idem = hashlib.sha256(f"campaign_touch:{touch['touch_id']}".encode()).hexdigest()[:48]
    work_item_id = enqueue_work_item(
        session=session,
        queue_name=queue_name,
        payload={
            "touch_id": touch["touch_id"],
            "enrollment_id": enrollment_id,
            "person_id": person_id,
            "audience": touch["audience"],
            "campaign_key": touch["campaign_key"],
            "sequence_version": touch["sequence_version"],
            "step": touch["step"],
            "channel": channel,
            "subject_template": step_cfg["subject"],
            "body_template": step_cfg["body_template"],
            "merge_values": values,
            "trigger_type": touch["trigger_type"],
            "trigger_reason": touch["trigger_reason"],
            "property_id": touch["property_id"],
            "county_id": touch["county_id"],
            "state": touch["state"],
        },
        idempotency_key=idem,
        person_id=person_id,
    )
    session.execute(
        text(
            "UPDATE fa_max_campaign_touches SET status = 'handed_off', work_item_id = CAST(:wid AS uuid) "
            "WHERE touch_id = :tid"
        ),
        {"wid": work_item_id, "tid": touch["touch_id"]},
    )
    session.commit()


def _hold_touch(session: Session, touch: dict, *, reason: str) -> None:
    session.execute(
        text("UPDATE fa_max_campaign_touches SET status = 'held', status_reason = :r WHERE touch_id = :tid"),
        {"r": reason, "tid": touch["touch_id"]},
    )
    session.commit()


def _skip_touch(session: Session, touch: dict, *, channel_reason: str) -> None:
    """Mark this touch skipped and always advance to the next step — a skip
    reflects something about this channel/recipient (no identifier, opted
    out, DNC), not a reason to stall the whole enrollment. Person-wide
    conditions (open deal, Backflip-active, repeat borrower, do_not_contact)
    never reach here — enrollment_block_reason() catches those first and
    the caller returns before calling this (see plan revision notes)."""
    session.execute(
        text(
            "UPDATE fa_max_campaign_touches SET status = 'skipped', status_reason = :r WHERE touch_id = :tid"
        ),
        {"r": channel_reason, "tid": touch["touch_id"]},
    )
    session.commit()
    _advance_after_skip(session, touch)


def _advance_after_skip(session: Session, touch: dict) -> None:
    """Schedule the next step immediately after a skipped (not held) touch —
    a skip doesn't count as "sent", so it doesn't reset last_touch_sent_at
    or apply the send-to-send gap."""
    next_cfg = session.execute(
        text(
            "SELECT step, channel FROM fa_max_campaign_sequence_steps "
            "WHERE campaign_key = :ck AND sequence_version = :ver AND step = :next_step"
        ),
        {"ck": touch["campaign_key"], "ver": touch["sequence_version"], "next_step": touch["step"] + 1},
    ).mappings().fetchone()
    if next_cfg:
        _schedule_touch(
            session, enrollment_id=touch["enrollment_id"], step=next_cfg["step"],
            channel=next_cfg["channel"], due_at=datetime.now(timezone.utc),
        )
    else:
        session.execute(
            text(
                "UPDATE fa_max_campaign_enrollments SET status = 'completed', ended_at = NOW(), "
                "end_reason = 'sequence_complete', updated_at = NOW() WHERE enrollment_id = CAST(:eid AS uuid)"
            ),
            {"eid": touch["enrollment_id"]},
        )
        _log_event(session, touch["enrollment_id"], "completed", "sequence_complete")
    session.commit()


def _end_if_no_channel_left(session: Session, enrollment_id: str, person_id: str) -> None:
    remaining = session.execute(
        text(
            "SELECT COUNT(*) FROM fa_max_campaign_touches "
            "WHERE enrollment_id = CAST(:eid AS uuid) AND status IN ('scheduled', 'held')"
        ),
        {"eid": enrollment_id},
    ).scalar_one()
    if remaining:
        return
    if not reachable_channels(session, person_id=person_id):
        cancel_enrollments(session, person_id=person_id, reason="no_reachable_channel")


def mark_touch_sent(session: Session, *, touch_id: int, sent_at: datetime, relay_item_id: int) -> None:
    """Callback for the drafting agent (plan Section 6.10): schedules the
    next step, timed from the real send, not the hand-off."""
    row = session.execute(
        text(
            "UPDATE fa_max_campaign_touches SET status = 'sent', sent_at = :sent_at, "
            "relay_item_id = :rid WHERE touch_id = :tid "
            "RETURNING enrollment_id::text, step"
        ),
        {"sent_at": sent_at, "rid": relay_item_id, "tid": touch_id},
    ).fetchone()
    if not row:
        return
    enrollment_id, step = row[0], row[1]
    session.execute(
        text(
            "UPDATE fa_max_campaign_enrollments SET last_touch_sent_at = :sent_at, updated_at = NOW() "
            "WHERE enrollment_id = CAST(:eid AS uuid)"
        ),
        {"sent_at": sent_at, "eid": enrollment_id},
    )

    enrollment = session.execute(
        text(
            "SELECT campaign_key, sequence_version FROM fa_max_campaign_enrollments "
            "WHERE enrollment_id = CAST(:eid AS uuid)"
        ),
        {"eid": enrollment_id},
    ).mappings().fetchone()
    next_cfg = session.execute(
        text(
            "SELECT step, channel, days_after_previous FROM fa_max_campaign_sequence_steps "
            "WHERE campaign_key = :ck AND sequence_version = :ver AND step = :next_step"
        ),
        {"ck": enrollment["campaign_key"], "ver": enrollment["sequence_version"], "next_step": step + 1},
    ).mappings().fetchone()
    if next_cfg:
        due_at = sent_at + timedelta(days=next_cfg["days_after_previous"])
        _schedule_touch(session, enrollment_id=enrollment_id, step=next_cfg["step"], channel=next_cfg["channel"], due_at=due_at)
    else:
        session.execute(
            text(
                "UPDATE fa_max_campaign_enrollments SET status = 'completed', ended_at = NOW(), "
                "end_reason = 'sequence_complete', updated_at = NOW() WHERE enrollment_id = CAST(:eid AS uuid)"
            ),
            {"eid": enrollment_id},
        )
        _log_event(session, enrollment_id, "completed", "sequence_complete")
    session.commit()


def mark_touch_not_sent(session: Session, *, touch_id: int, reason: str) -> None:
    """Callback for the drafting agent (plan Section 6.10)."""
    row = session.execute(
        text(
            "SELECT enrollment_id::text, person_id::text FROM fa_max_campaign_touches t "
            "JOIN fa_max_campaign_enrollments e ON e.enrollment_id = t.enrollment_id "
            "WHERE t.touch_id = :tid"
        ),
        {"tid": touch_id},
    ).fetchone()
    if not row:
        return
    enrollment_id, person_id = row

    if reason == "rejected_by_operator":
        session.execute(
            text(
                "UPDATE fa_max_campaign_enrollments SET status = 'cancelled', ended_at = NOW(), "
                "end_reason = :reason, updated_at = NOW() WHERE enrollment_id = CAST(:eid AS uuid)"
            ),
            {"reason": reason, "eid": enrollment_id},
        )
        _log_event(session, enrollment_id, "cancelled", reason)
        session.execute(
            text(
                "UPDATE fa_max_campaign_touches SET status = 'cancelled', status_reason = :r WHERE touch_id = :tid"
            ),
            {"r": reason, "tid": touch_id},
        )
        session.commit()
        return

    if reason.startswith("suppressed:") or reason in ("consent_absent", "consent_withdrawn"):
        cancel_enrollments(session, person_id=person_id, reason=reason)
        return

    # Anything else: leave the touch scheduled/held for the next sweep pass
    # rather than auto-retrying a failed send (spec failure-behavior: "never
    # silently dropped and never retried without a human").
    session.execute(
        text("UPDATE fa_max_campaign_touches SET status_reason = :r WHERE touch_id = :tid"),
        {"r": reason, "tid": touch_id},
    )
    session.commit()
