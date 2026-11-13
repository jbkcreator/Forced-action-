"""P2.4 chase drafts: one message per step, by text when the borrower consented, else by email.

Payload keys match the agent job drafts (src/lending/agent_jobs/drafts.py) so Cora's existing ``ghl_sms`` /
``ghl_email`` executors send them unchanged. Every draft goes through ``SendGate.submit``: an approval card first,
and the relay re-checks consent, the 8 AM to 8 PM window, suppression, the lane deny list and the halt at send time.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Sequence

from config.lending_closing import REQUESTED_BY, TEMPLATES
from packages.agent_core.send_gate import SendGate
from packages.agent_core.tools import EgressDraft, ToolContext


@dataclass(frozen=True)
class Recipient:
    first_name: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    ghl_contact_id: Optional[str]


def chase_draft(kind: str, recipient: Recipient, *, link: Optional[str], missing: Sequence[str], text_ok: bool,
                contact_ref: str, deal_ref: str) -> Optional[EgressDraft]:
    """The ``kind`` message (a TEMPLATES key) for ``recipient``; None when neither a text nor an email can go."""
    template = TEMPLATES[kind]
    values = {"first_name": recipient.first_name or "there", "link": link or "", "missing": ", ".join(missing),
              "count": len(missing)}
    if text_ok and recipient.phone:
        channel, version = "ghl_sms", template.version_sms
        fields = {"to_phone": recipient.phone, "body": template.sms.format(**values)}
    elif recipient.email:
        channel, version = "ghl_email", template.version_email
        fields = {"to_email": recipient.email, "to_phone": recipient.phone,
                  "subject": template.email_subject, "body": template.email_body.format(**values)}
    else:
        return None
    last4 = f" (…{recipient.phone[-4:]})" if recipient.phone else ""
    summary = (f"📄 {kind} · {version}\n{recipient.first_name or 'Borrower'}{last4}: "
               f"{len(missing)} document(s) missing")
    payload = {"template_version": version, "ghl_contact_id": recipient.ghl_contact_id, "deal_ref": deal_ref, **fields}
    return EgressDraft(channel=channel, payload=payload, summary=summary, recipient_phone=recipient.phone,
                       recipient_email=recipient.email, contact_ref=contact_ref, deal_ref=deal_ref)


def submit_draft(gate: SendGate, draft: EgressDraft, *, key: str, channel: str, ttl: timedelta) -> int:
    """Hold ``draft`` for approval; ``key`` makes a repeat return the same action, so a step never queues twice."""
    context = ToolContext(user_id=REQUESTED_BY, is_approver=False, channel=channel, thread_ts="", tool_use_id=key)
    return gate.submit(draft, tool_name=REQUESTED_BY, context=context, ttl=ttl).action_id
