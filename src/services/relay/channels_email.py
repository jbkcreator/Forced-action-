"""
Relay's real email send channel — Instantly.ai (RELAY-v2.2 sub-task R2).

Sends through ONE long-lived "Relay passthrough" Instantly campaign whose
single sequence step is the literal merge tags {{ra_subject}}/{{ra_body}}
(see RELAY-R2-Implementation-Plan.md and cmd_setup_email_channel() in
src/services/relay/__main__.py for how that campaign is created). This
bypasses the DBPR-specific email_campaigns.py wrapper and its whitelisted-
variable template system entirely — Cora's drafts (Phase 2) are fully
custom per-recipient text, not a handful of DBPR-contact fields.

Instantly's add_leads() SILENTLY SKIPS a lead already a member of the
campaign (docs/adr/0011) rather than erroring. Since each
relay_approval_queue row must be sent at most once and "sent" must mean
genuinely sent (build spec §9.1), this module treats any leads_skipped > 0
as a hard failure (fail loud, confirmed 2026-07-24) rather than a false
'sent'. This does not solve repeat-contact-to-the-same-recipient — a real
fix (campaign rotation or remove-then-readd) is deliberately deferred.
"""
from __future__ import annotations

from config.settings import get_settings
from src.services import instantly_service as instantly
from src.services.relay.channels import register
from src.services.relay.queue import QueueItem

PASSTHROUGH_CAMPAIGN_NAME = "Relay Passthrough (RELAY-v2.2 R2)"


def send_email(item: QueueItem) -> None:
    """Dispatch one approved item through the Relay passthrough campaign.

    Raises on any failure (including a duplicate-contact skip) so the
    engine records the item as 'failed' — never a false 'sent'.
    """
    settings = get_settings()
    campaign_id = settings.relay_instantly_campaign_id
    if not campaign_id:
        raise RuntimeError(
            "RELAY_INSTANTLY_CAMPAIGN_ID not configured — run "
            "`python -m src.services.relay --setup-email-channel` once, "
            "then set the printed campaign id in .env"
        )

    subject = (item.payload or {}).get("subject", "")
    body = (item.payload or {}).get("body", "")
    if not body:
        raise RuntimeError(f"item {item.id}: payload missing 'body' for email channel")

    result = instantly.add_leads(campaign_id, [{
        "email": item.recipient,
        "ra_subject": subject,
        "ra_body": body,
    }])

    if result is None:
        raise RuntimeError(f"item {item.id}: Instantly add_leads call failed (see logs)")

    if result.get("leads_skipped", 0):
        raise RuntimeError(
            f"item {item.id}: Instantly skipped {item.recipient} — already a "
            f"member of the Relay passthrough campaign (duplicate-contact "
            f"guard; a genuine repeat send needs a rotated/new campaign — "
            f"not yet built, see RELAY-R2-Implementation-Plan.md Locked decision #2)"
        )

    if result.get("leads_created", 0) == 0:
        raise RuntimeError(f"item {item.id}: Instantly reported 0 leads created for {item.recipient}")


register("email", send_email)
