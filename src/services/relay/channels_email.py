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

RELAY-v2.2 sub-task R3 appends a plain-text unsubscribe footer (postal
address + one-click link, same unsubscribe_url() the DBPR template uses)
before every send. Without it a Relay recipient has no way to opt out —
a CAN-SPAM requirement — and suppression_sync.py's Instantly poll would
never see a real unsubscribe event to sync back.
"""
from __future__ import annotations

from src.services import instantly_service as instantly
from src.services.email_unsubscribe import unsubscribe_url
from src.services.relay.channels import register
from src.services.relay.queue import QueueItem
from src.utils.venture_config import get_venture_config

PASSTHROUGH_CAMPAIGN_NAME = "Relay Passthrough (RELAY-v2.2 R2)"


def send_email(item: QueueItem) -> None:
    """Dispatch one approved item through its venture's passthrough campaign.

    Raises on any failure (including a duplicate-contact skip) so the
    engine records the item as 'failed' — never a false 'sent'.

    Campaign, brand name and postal address are resolved per venture
    (CLONE-v2.2 / CL3). Each venture needs its OWN passthrough campaign:
    Instantly's duplicate-contact guard is per-campaign, so sharing one
    across ventures would make venture B's first email to a prospect look
    like a repeat of venture A's and fail.
    """
    venture = get_venture_config(item.venture_key)
    campaign_id = venture.relay_instantly_campaign_id
    if not campaign_id:
        raise RuntimeError(
            f"item {item.id}: no Instantly campaign for venture "
            f"{item.venture_key!r} — run `python -m src.services.relay "
            f"--setup-email-channel` once for it, then set the printed id on "
            f"the venture's ventures.relay_instantly_campaign_id (or, for "
            f"venture #1, RELAY_INSTANTLY_CAMPAIGN_ID in .env)"
        )

    subject = (item.payload or {}).get("subject", "")
    body = (item.payload or {}).get("body", "")
    if not body:
        raise RuntimeError(f"item {item.id}: payload missing 'body' for email channel")

    nl_to_br = body.replace("\r\n", "\n").replace("\n", "<br>\n")
    unsub = unsubscribe_url(item.recipient)
    body = (
        f'<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;max-width:600px">'
        f"{nl_to_br}"
        f'<br><br>--<br>{venture.brand_name}<br>{venture.postal_address}<br><br>'
        f'<a href="{unsub}" style="color:#888;font-size:12px">Unsubscribe</a>'
        f"</div>"
    )

    result = instantly.add_leads(campaign_id, [{
        "email": item.recipient,
        "custom_variables": {
            "ra_subject": subject,
            "ra_body": body,
        },
    }])

    if result is None:
        raise RuntimeError(f"item {item.id}: Instantly add_leads call failed (see logs)")

    duplicated = result.get("duplicated_leads", 0) or result.get("leads_skipped", 0)
    if duplicated:
        raise RuntimeError(
            f"item {item.id}: Instantly skipped {item.recipient} — already a "
            f"member of venture {item.venture_key}'s passthrough campaign (duplicate-contact "
            f"guard; a genuine repeat send needs a rotated/new campaign — "
            f"not yet built, see RELAY-R2-Implementation-Plan.md Locked decision #2)"
        )

    created = result.get("leads_uploaded", 0) or result.get("leads_created", 0)
    if created == 0:
        raise RuntimeError(f"item {item.id}: Instantly reported 0 leads created for {item.recipient}")


register("email", send_email)
