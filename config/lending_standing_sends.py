"""P1.4 Standing Sends Tracker: when an agent message type (template version) graduates to standing auto-send.

Brief P1.4: "Automatically graduate a message type to standing auto-send after 25 approvals with under 10% edits."
Read literally (client decision, Nov 12): only approvals count toward the 25, the edit rate is edited approvals over
approvals, and a rejection neither helps nor blocks graduation (it is shown on the tracker). Graduation happens on its
own, with no confirm step; it can be revoked with ``python -m src.lending.standing_sends --revoke``.
"""
from __future__ import annotations

GRADUATION_MIN_APPROVALS = 25
GRADUATION_MAX_EDIT_RATE = 0.10  # strictly under: 10% edited does not graduate

# The actor #429's job worker approves a graduated job as; such sends are not approvals by Josh.
AUTO_APPROVER_PREFIX = "auto:"
AUTO_GRADUATED_ACTOR = "auto:graduated"

# The draft fields an edit can change (agent_core Revise, cora_agent.EDITABLE_FIELDS). Recipient fields are never
# copied into an edit record.
EDIT_FIELDS = ("subject", "body", "stage", "tag", "title")

MORNING_CARD_LIMIT = 10

# Brief section 7 (Audit): every send carries a template version. Agent jobs bring their own (P1.1); Cora's own message
# drafts carry these. They are tracked like any other type but never graduate: Josh asked Cora for each of them (or,
# for Instantly answers, each one answers a different person's reply), so they always need an approval card. Bump the
# version when the drafting prompt or tool changes how these messages are written.
CORA_SMS_TEMPLATE = "CORA-SMS-v1"
CORA_EMAIL_TEMPLATE = "CORA-EMAIL-v1"
INSTANTLY_REPLY_TEMPLATE = "INSTANTLY-REPLY-v1"
APPROVAL_ONLY_TEMPLATES = frozenset({CORA_SMS_TEMPLATE, CORA_EMAIL_TEMPLATE, INSTANTLY_REPLY_TEMPLATE})
