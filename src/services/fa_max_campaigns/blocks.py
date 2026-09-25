"""Enrollment/advancement blocks for WP-T3-4 (plan Section 6.4 / 6.5).

Every function here is a pure read — no writes. `enrollment_block_reason`
is the single gate both the enrollment sweep and the due-step sweep call
before ever enrolling, advancing, or handing off a touch, wrapping the
existing governance functions rather than reimplementing them:

  - fa_max_send_governance.suppression_reason() — Backflip campaign touch,
    email opt-out, SMS compliance/DNC.
  - fa_max_send_governance.channel_split_reason() — FA vs Backflip lane.
  - automation_block_reason() (new here) — open deal (pause), funded deal
    (repeat borrower, item 31, never enrolled), and later WP-T3-10's
    "route to Josh" flag (Gap E — one more line here once that flag ships).

`channel_readiness()` wraps require_consent() for the hold/skip decision
(plan Section 6.5) — this module never writes to fa_max_person_consent.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.fa_max_send_governance import (
    channel_split_reason,
    is_backflip_suppressed,
    require_consent,
    suppression_reason,
)

# Every channel a v1 campaign step may use (plan decision #4 — email/SMS only).
CAMPAIGN_CHANNELS = ("email", "sms")


@dataclass(frozen=True)
class BlockResult:
    blocked: bool
    reason: Optional[str]
    pause: bool = False  # True = pause (open deal); False + blocked = never enroll


def _lifecycle_state(session: Session, person_id: str) -> Optional[str]:
    row = session.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    return row[0] if row else None


def automation_block_reason(session: Session, person_id: str) -> Optional[str]:
    """Return a block reason for "never automate this contact", or None.

    Checks (plan Section 6.4 point 4):
      - a funded opportunity -> repeat borrower, item 31: always routes to
        Josh, never an automated sequence. Never enrolled.
      - an open opportunity -> pause, resumed once it clears.

    Gap E: WP-T3-10's "route to Josh" flag is not built yet. Add its check
    here, as one more early-return, once Developer 4 confirms the flag name
    (plan D-2) — every caller of this function picks it up automatically.
    """
    funded = session.execute(
        text(
            "SELECT 1 FROM fa_max_opportunities "
            "WHERE person_id = CAST(:pid AS uuid) AND outcome = 'funded' LIMIT 1"
        ),
        {"pid": person_id},
    ).scalar()
    if funded:
        return "repeat_borrower"

    open_deal = session.execute(
        text(
            "SELECT 1 FROM fa_max_opportunities "
            "WHERE person_id = CAST(:pid AS uuid) AND outcome = 'open' LIMIT 1"
        ),
        {"pid": person_id},
    ).scalar()
    if open_deal:
        return "open_opportunity"

    return None


def backflip_active_touch_reason(session: Session, person_id: str) -> Optional[str]:
    """Client Q5, verbatim: "If a prospect already has an active Backflip
    campaign touch in motion, Forced Action holds off rather than sending a
    competing outreach." Checked at enrollment/housekeeping time (not just
    at send time) so a Backflip-active contact is paused up front rather
    than being enrolled, then having every touch skipped and the whole
    sequence silently burn through to completion via skip-and-advance
    without ever giving them a real touch once Backflip's touch clears.

    Reuses is_backflip_suppressed() (fail-closed on a stale/missing feed,
    per Q6) rather than re-querying fa_max_backflip_campaign_contacts here
    — one gate, one implementation. Checks every channel this person has a
    value for; any one hit is enough."""
    row = session.execute(
        text("SELECT email, phone FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    if not row:
        return None
    email, phone = row[0], row[1]
    for recipient, channel in ((email, "email"), (phone, "sms")):
        if not recipient:
            continue
        suppressed, reason = is_backflip_suppressed(
            session, recipient=recipient, channel=channel, person_id=person_id,
        )
        if suppressed:
            return reason
    return None


def enrollment_block_reason(session: Session, *, person_id: str) -> BlockResult:
    """The one gate every campaign write path must pass. Never enrolls or
    advances a blocked person; the pause flag distinguishes a temporary hold
    (open deal, active Backflip touch) from a permanent exclusion."""
    state = _lifecycle_state(session, person_id)
    if state in ("do_not_contact", "suppressed"):
        return BlockResult(blocked=True, reason=f"lifecycle_state:{state}")

    split_reason = channel_split_reason(session, opportunity_id=None, person_id=person_id)
    if split_reason:
        return BlockResult(blocked=True, reason=split_reason)

    auto_reason = automation_block_reason(session, person_id)
    if auto_reason == "repeat_borrower":
        return BlockResult(blocked=True, reason=auto_reason)
    if auto_reason == "open_opportunity":
        return BlockResult(blocked=True, reason=auto_reason, pause=True)

    backflip_reason = backflip_active_touch_reason(session, person_id)
    if backflip_reason:
        return BlockResult(blocked=True, reason=f"backflip_active:{backflip_reason}", pause=True)

    return BlockResult(blocked=False, reason=None)


def channel_readiness(session: Session, *, person_id: str, recipient: str, channel: str) -> BlockResult:
    """Whether a specific channel step for this person may be handed off now.

    Wraps suppression_reason() (Backflip campaign / email opt-out / SMS
    compliance) and require_consent() (explicit opt-in, plan Section 6.5).
    This module never writes consent — a missing row is reported, not
    assumed or bypassed.
    """
    reason = suppression_reason(session, recipient=recipient, channel=channel, person_id=person_id)
    if reason:
        return BlockResult(blocked=True, reason=reason)

    consent = require_consent(session, person_id=person_id, channel=channel)
    if not consent.allowed:
        return BlockResult(blocked=True, reason=consent.reason)

    return BlockResult(blocked=False, reason=None)


def reachable_channels(session: Session, *, person_id: str) -> list[str]:
    """Channels this person has a populated identifier for (email/phone on
    fa_max_persons) — regardless of suppression or consent. Use
    has_any_unsuppressed_channel() to also account for suppression."""
    row = session.execute(
        text("SELECT email, phone FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    if not row:
        return []
    channels = []
    if row[0]:
        channels.append("email")
    if row[1]:
        channels.append("sms")
    return channels


def has_any_unsuppressed_channel(session: Session, *, person_id: str) -> bool:
    """plan Section 6.4, point 2: block enrollment when suppression_reason()
    returns a reason on EVERY channel the campaign could use. A person whose
    only channel is already suppressed (e.g. a hard-bounced email with no
    phone on file) must never be freshly enrolled — they would just skip
    through the whole sequence with zero real touches (see
    _skip_touch/_advance_after_skip in selection.py) and burn a cooldown
    slot for nothing. A person with at least one clean channel still
    enrolls; the per-touch gate (channel_readiness) correctly skips whatever
    specific channel is suppressed for them."""
    row = session.execute(
        text("SELECT email, phone FROM fa_max_persons WHERE person_id = CAST(:pid AS uuid)"),
        {"pid": person_id},
    ).fetchone()
    if not row:
        return False
    email, phone = row[0], row[1]
    for recipient, channel in ((email, "email"), (phone, "sms")):
        if not recipient:
            continue
        if suppression_reason(session, recipient=recipient, channel=channel, person_id=person_id) is None:
            return True
    return False
