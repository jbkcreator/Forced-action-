"""
Relay's SMS send channel (WP-T2-1).

Before this module existed, no "sms" dispatcher was ever registered
(src.services.relay.channels.DISPATCHERS started and ended at "noop" —
see channels.py). An approved FA Max SMS item would pass every guard,
including the fa_max_10dlc_registered check in guards.py, and then fail at
dispatch with "unknown_channel:sms" in engine.execute_batch(). Registering
the channel is therefore not optional plumbing — without it, no FA Max SMS
can ever leave the queue, gated or not.

Dispatches through src.services.sms_compliance.send_sms(), the repo's single
mandated SMS sender (CLAUDE.md: "All sends via src.services.sms_compliance.
send_sms with explicit message_type"), NOT src.services.telnyx_sms directly.
Two consequences of that choice, both deliberate:

  1. message_type="transactional". FA Max consent is tracked independently
     per (person_id, channel) in fa_max_person_consent
     (fa_max_send_governance.require_consent(), already checked by
     relay.guards before this dispatcher ever runs) — it is not the
     Subscriber-opt-in model send_sms's "marketing" path expects, and
     send_sms unconditionally blocks marketing sends that carry neither a
     subscriber_id nor a prospect_id (sms_compliance.py, the
     "marketing_requires_subscriber_id" gate). "transactional" skips that
     subscriber-shaped gate while still running send_sms's general TCPA/CTIA
     compliance_gator check (DNC + quiet hours) — a legitimate reuse of the
     existing gate, not a bypass of it.
  2. Enforcement boundary (production-execution review finding #2): FA Max
     outbound SMS must go through relay.enqueue() -> this dispatcher, not a
     direct call. tests/test_fa_max_wp_t2_1_send_infra.py structurally
     enforces the narrower, syntactically-checkable half of that rule —
     nothing but sms_compliance.py (the wrapper) and this file may call the
     raw vendor function telnyx_sms.send_message() — since sms_compliance.
     send_sms() itself is the repo-wide single SMS sender legitimately
     called directly elsewhere for non-FA-Max (Lifecycle/subscriber)
     traffic, and a call site alone can't distinguish FA Max context from
     that traffic. The full invariant is a code-review/architecture
     convention, not a fully automatable check.

send_sms() returns a bool (True = sent or logged dry-run, False = suppressed
or vendor failure) rather than raising — engine.execute_batch() only knows
"sent" from a dispatcher that returns normally, so a naive `return` on
send_sms()'s True/False would mark every suppressed or dry-run send as a
successful dispatch (production-execution review finding #1). This
dispatcher therefore inspects send_sms()'s dry-run precondition itself
(settings.telnyx_sms_enabled) BEFORE calling it and raises rather than ever
reporting a dry-run as a real send to a live recipient, and raises on a
False return so the queue row is marked 'failed', never 'sent'.
"""
from __future__ import annotations

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.relay.channels import register
from src.services.relay.queue import QueueItem

_FA_MAX_VENTURE = "fa_max_lending"


def send_sms(item: QueueItem) -> None:
    """Dispatch one approved SMS item. Raises on any non-real-send outcome
    so the engine records 'failed', never a false 'sent'."""
    body = (item.payload or {}).get("body", "")
    if not body:
        raise RuntimeError(f"item {item.id}: payload missing 'body' for sms channel")

    if item.venture_key == _FA_MAX_VENTURE and get_settings().fa_max_relay_send_mode == "fake":
        # WP-T2-1: no live Telnyx dispatch until fa_max_relay_send_mode
        # flips to "live" (gated on 10DLC registration being confirmed —
        # see guards.py's fa_max_10dlc_registered check, which runs before
        # this dispatcher regardless of send mode).
        from src.services.relay.fakes import FAKE_SMS

        receipt = FAKE_SMS.send(to=item.recipient, body=body, message_type="transactional")
        if not receipt.accepted:
            raise RuntimeError(f"item {item.id}: fake sms send refused ({receipt.reason})")
        return

    settings = get_settings()
    if not settings.telnyx_sms_enabled:
        # A live-mode approved relay item must never be silently recorded as
        # sent because the environment happens to be in dry-run — that would
        # tell Josh a borrower was texted when nobody was.
        raise RuntimeError(
            f"item {item.id}: TELNYX_SMS_ENABLED is false — refusing to mark a "
            f"live approved send as dispatched while sms_compliance.send_sms() "
            f"would only dry-run it"
        )

    from src.services.sms_compliance import send_sms as compliance_send_sms

    with get_db_context() as db:
        ok = compliance_send_sms(item.recipient, body, db, message_type="transactional")

    if not ok:
        # send_sms() already wrote the SmsSendLog/dead-letter row explaining
        # why (suppressed, quiet hours, vendor failure, misconfiguration).
        # Surface it here too so mark_failed()'s error column isn't opaque.
        raise RuntimeError(f"item {item.id}: sms_compliance.send_sms refused or failed the send")


register("sms", send_sms)
