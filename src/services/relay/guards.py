"""
Relay execution-time guards (RELAY-v2.2 sub-task R3, ceiling reworked for
the PR #179 review fix).

evaluate() is the gate every approved item passes immediately before a claim
is attempted. It answers: may this specific item be sent right now, ignoring
today's channel-wide volume? -- send window (pure arithmetic), then
suppression (one DB query). The daily ceiling is deliberately NOT part of
evaluate() anymore: a per-channel volume cap is shared state across every
concurrent worker, and evaluate()'s old approach (an in-memory dict, one
DB snapshot per batch) could not enforce that -- two overlapping sweeps each
started from the same snapshot and could jointly exceed the ceiling, since
nothing reserved a slot atomically. See reserve_daily_slot()/
release_daily_slot() below, called directly by engine.execute_batch() around
the claim+dispatch sequence, using Redis's atomic INCR as the shared counter.

`now` is injected rather than looked up inside evaluate(), so callers (and
tests) control the clock directly -- no mocking, no freezegun, no DB in the
window-check tests.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import logging

from config.settings import get_settings
from config.venture_template import DEFAULT_VENTURE_KEY

logger = logging.getLogger(__name__)

_FA_MAX_VENTURE = "fa_max_lending"
from src.core.database import get_db_context
from src.core.redis_client import rdecr, rincr
from src.services.compliance_gator import validate_outbound
from src.services.email_suppression import is_email_suppressed
from src.services.relay.config import (
    GUARD_ALLOW,
    GUARD_BLOCK,
    GUARD_DEFER,
    REASON_FA_MAX_10DLC_NOT_REGISTERED,
    REASON_FA_MAX_BACKLOG_RELEASE_NOT_CONFIRMED,
    REASON_FA_MAX_SEND_MODE_NOT_LIVE,
    REASON_OUTSIDE_SEND_WINDOW,
    REASON_SUPPRESSED,
)
from src.services.relay.queue import QueueItem

ALLOW = GUARD_ALLOW
DEFER = GUARD_DEFER
BLOCK = GUARD_BLOCK

_DAILY_SLOT_KEY_TTL_SECONDS = 2 * 24 * 3600  # outlives the day it counts, then expires


@dataclass(frozen=True)
class Verdict:
    outcome: str          # ALLOW | DEFER | BLOCK
    reason: str = ""       # e.g. "outside_send_window", "suppressed:email_opt_out"


def _within_send_window(now: datetime, settings) -> bool:
    local = now.astimezone(ZoneInfo(settings.relay_send_window_timezone))
    return settings.relay_send_window_start <= local.hour < settings.relay_send_window_end


def _suppression_reason(item: QueueItem) -> str | None:
    """None = clear to send. A string = blocked, and why."""
    with get_db_context() as db:
        if item.channel == "email":
            if is_email_suppressed(db, item.recipient):
                return "email_opt_out"
            return None
        if item.channel in ("sms", "voice"):
            result = validate_outbound(item.recipient, item.channel, db)
            return None if result.allowed else (result.reason or "compliance_blocked")
    return None   # 'noop' and any future non-contact channel


def _record_backflip_decision(*, item: QueueItem, suppressed: bool, reason: str | None) -> bool:
    """Persist one decision without allowing audit storage to abort a batch.

    A blocked contact remains blocked if the audit store is unavailable. A
    clear contact is deferred by evaluate() until its allow decision can be
    durably recorded. Returning False lets the caller apply those different
    fail-closed outcomes while the Relay loop continues with later items.
    """
    try:
        from src.services.fa_max_send_governance import record_backflip_suppression_decision
        record_backflip_suppression_decision(
            gate="send",
            recipient=item.recipient,
            suppressed=suppressed,
            reason=reason,
            opportunity_id=item.opportunity_id,
        )
    except Exception:
        logger.exception(
            "[Relay] item %s Backflip suppression audit write failed; "
            "the item will remain fail-closed",
            item.id,
        )
        return False
    return True


def _fa_max_compliance_reason(item: QueueItem) -> str | None:
    """FA Max-specific pre-send compliance checks (WP-2).

    Returns a block reason string if the item must not be sent, or None if
    clear. Called before _suppression_reason so 10DLC block is visible in
    the refusal log even when the contact is also suppressed.

    Only applies to items with venture_key='fa_max_lending'.
    """
    if item.venture_key != _FA_MAX_VENTURE:
        return None

    if item.channel not in ("email", "sms"):
        return "unsupported_fa_max_channel"

    from src.services.fa_max_send_governance import (
        GovernanceBlocked,
        is_backflip_suppressed,
        require_consent,
        validate_safe_payload,
    )

    if not item.person_id or not item.agent_name or not item.autonomy_tier_at_send or not item.lane:
        return "missing_governance_fields"
    if not item.decided_by or not item.decision_interaction_id:
        return "human_approval_required"
    try:
        validate_safe_payload(item.payload or {})
    except GovernanceBlocked as exc:
        return exc.reason
    with get_db_context() as db:
        _, campaign_reason = is_backflip_suppressed(
            db, recipient=item.recipient, channel=item.channel, person_id=item.person_id,
        )
        if campaign_reason:
            # WP-T2-3: audit the Backflip suppression decision in its own
            # committed transaction (survives any caller rollback).
            _record_backflip_decision(item=item, suppressed=True, reason=campaign_reason)
            return campaign_reason
        # WP-T2-3: channel-split enforcement at the send gate.
        from src.services.fa_max_send_governance import channel_split_reason, get_channel_split_source
        split_reason = channel_split_reason(db, opportunity_id=item.opportunity_id, person_id=item.person_id)
        if split_reason:
            _record_backflip_decision(
                item=item, suppressed=True, reason=f"channel_split:{split_reason}",
            )
            return f"channel_split:{split_reason}"
        current_source = get_channel_split_source(
            db, opportunity_id=item.opportunity_id, person_id=item.person_id,
        )
        if current_source != item.channel_split_source:
            _record_backflip_decision(
                item=item, suppressed=True, reason="channel_split_source_changed",
            )
            return "channel_split_source_changed"
        consent = require_consent(db, person_id=item.person_id, channel=item.channel)
        if not consent.allowed:
            _record_backflip_decision(item=item, suppressed=True, reason=consent.reason)
            return consent.reason
    return None


def evaluate(item: QueueItem, *, now: datetime, venture=None) -> Verdict:
    """`venture` is the resolved VentureConfig for the batch (CLONE-v2.2 /
    CL3) — its send window governs the check. Omitted, the check falls back
    to config/settings.py, which is where the window lived before CL3."""
    settings = venture if venture is not None else get_settings()

    if not _within_send_window(now, settings):
        return Verdict(DEFER, REASON_OUTSIDE_SEND_WINDOW)

    if item.venture_key == _FA_MAX_VENTURE:
        fa_settings = get_settings()

        if fa_settings.fa_max_relay_send_mode != "live":
            # Code-review finding: channels_email.py/channels_sms.py's
            # fake-mode branch returns normally after recording into
            # FAKE_MAIL/FAKE_SMS, and the engine treats any normal return
            # as a real send -- it calls queue.mark_sent(), which writes
            # dispatched_at, a durable WP-1 outbound interaction, and a
            # Slack "sent" completion receipt Josh reads as real.
            # fa_max_relay_send_mode defaults to "fake", so an approved
            # item reaching a real production sweep today would be
            # permanently recorded as sent with no actual send happening.
            #
            # This MUST be DEFER, not BLOCK (second code-review finding, on
            # the first fix): BLOCK -> queue.mark_skipped() is a terminal
            # status transition approved_batch() never revisits, which
            # would silently discard every FA Max item approved before the
            # lane goes live -- exactly the "permanently skips approved
            # messages" bug the first version of this check introduced.
            # DEFER leaves the row untouched in 'approved', so it's
            # retried every sweep tick and dispatches normally once the
            # flag flips to "live", with no work lost.
            #
            # The fake dispatch branches themselves exist so a developer
            # can call send_email()/send_sms() directly in a test with the
            # mode monkeypatched (see
            # tests/test_fa_max_wp_t2_1_send_infra.py) -- those calls
            # bypass evaluate() entirely and are unaffected by this gate.
            return Verdict(DEFER, REASON_FA_MAX_SEND_MODE_NOT_LIVE)

        if not fa_settings.fa_max_send_backlog_release_confirmed:
            # Code-review finding (third round): flipping send_mode to
            # "live" alone must not auto-release the multi-week backlog of
            # items approved while the lane was still in fake mode -- their
            # content or the underlying business decision behind them may
            # be stale by go-live, and guards.py's own freshness rechecks
            # (consent, suppression, campaign membership below) don't cover
            # content staleness. DEFER for the same reason as above: nothing
            # approved is ever lost, only held until this flag is set.
            #
            # This flag is NOT proof review happened (code-review
            # clarification, fourth round) -- see the full go-live procedure
            # documented on config/settings.py's
            # fa_max_send_backlog_release_confirmed field. An item's earlier
            # per-item Slack approval (decided_by/decided_at) is a different
            # review, from before the lane was ready; it does not satisfy
            # this one. Setting this flag true with no review having
            # actually happened dispatches the entire backlog, oldest
            # first, on the very next sweep tick.
            return Verdict(DEFER, REASON_FA_MAX_BACKLOG_RELEASE_NOT_CONFIRMED)

        if item.channel == "sms" and not fa_settings.fa_max_10dlc_registered:
            # Code-review finding, PR #281: this used to be one of the
            # per-item reasons _fa_max_compliance_reason() returns, which
            # evaluate() wraps in BLOCK -> queue.mark_skipped() -- a
            # terminal status never revisited (see mark_skipped()'s own
            # docstring). fa_max_10dlc_registered is a system-wide,
            # temporary-by-design condition exactly like
            # fa_max_relay_send_mode above (channels_sms.py's own comment
            # groups them together), not a per-item defect -- 10DLC
            # registration realistically takes weeks with carriers, and an
            # SMS item approved during that window must be retried once
            # registration completes, not permanently discarded. DEFER
            # leaves it in 'approved' for the next sweep tick, same as the
            # two checks above.
            return Verdict(DEFER, REASON_FA_MAX_10DLC_NOT_REGISTERED)

    # FA Max compliance check must run before suppression so a per-item
    # governance block appears in the refusal log even when the contact is
    # also suppressed.
    if item.venture_key == _FA_MAX_VENTURE and not item.channel_split_source:
        return Verdict(DEFER, "fa_max_opportunity_link_requires_review")
    fa_cause = _fa_max_compliance_reason(item)
    if fa_cause is not None:
        return Verdict(BLOCK, f"{REASON_SUPPRESSED}:{fa_cause}")

    cause = _suppression_reason(item)
    if cause is not None:
        if item.venture_key == _FA_MAX_VENTURE:
            _record_backflip_decision(item=item, suppressed=True, reason=cause)
        return Verdict(BLOCK, f"{REASON_SUPPRESSED}:{cause}")

    if item.venture_key == _FA_MAX_VENTURE:
        if not _record_backflip_decision(item=item, suppressed=False, reason=None):
            return Verdict(DEFER, "suppression_audit_unavailable")

    return Verdict(ALLOW)


def _daily_slot_key(channel: str, now: datetime, settings) -> str:
    """Redis key holding today's send count for one (venture, channel).

    Scoped by venture (CLONE-v2.2 / CL3) because the ceiling is a per-sender
    reputation limit, not a platform-wide one: two ventures sending from two
    different addresses each get their own cap, and without the venture in
    the key they would silently share one.

    `settings` may be a VentureConfig (what Relay passes) or a plain settings
    object (any pre-CL3 caller), hence the getattr — a settings object has no
    venture_key and belongs to venture #1 by definition.
    """
    venture_key = getattr(settings, "venture_key", DEFAULT_VENTURE_KEY)
    local_date = now.astimezone(ZoneInfo(settings.relay_send_window_timezone)).date()
    return f"relay_daily_sent:{venture_key}:{channel}:{local_date.isoformat()}"


def reserve_daily_slot(channel: str, now: datetime, settings) -> bool:
    """Atomically reserve one of today's per-channel send slots (PR #179
    review finding #2). True = go ahead and dispatch; False = at/over the
    ceiling, OR Redis is unreachable.

    Fails CLOSED on a Redis outage rather than falling back to the old
    unsafe local-count behavior: rincr() returns 0 only when it could not
    reach Redis (a real INCR can never return 0), so that case is
    indistinguishable from "ceiling reached" here on purpose -- deferring a
    send is always safe (retried next sweep); silently exceeding the
    configured ceiling because the shared counter was unavailable is not.

    A DENIED attempt immediately undoes its own increment. Without this, a
    deferred item -- which the caller always retries on a later sweep, i.e.
    never actually uses this reservation -- would permanently ratchet the
    counter upward on every retry, since rincr() always increments even
    when the result turns out to be over the cap. Left unfixed, the true
    ceiling would silently shrink over the course of a day as denied
    attempts piled up.
    """
    key = _daily_slot_key(channel, now, settings)
    n = rincr(key, ttl_seconds=_DAILY_SLOT_KEY_TTL_SECONDS)
    if n == 0:
        return False
    if n > settings.relay_daily_ceiling:
        rdecr(key)
        return False
    return True


def release_daily_slot(channel: str, now: datetime, settings) -> None:
    """Give back a reservation that ultimately went unused -- the row's
    claim was lost to a concurrent worker, or the dispatch failed -- so a
    transient failure doesn't permanently eat into today's cap."""
    key = _daily_slot_key(channel, now, settings)
    rdecr(key)
