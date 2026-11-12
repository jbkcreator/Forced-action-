"""P1.1 agent job queue: the job types the agents may file, the receipt statuses Cora writes back, and limits.

Format and statuses come from the Cora activation brief, section 6 ("Handoff Formats"). ``blocked_cap`` is ours
(decision D6): the brief's status list has no status for the cold-email cap.
"""
from __future__ import annotations

from datetime import timedelta

# Job type -> the Cora egress channel it becomes. A cold contact's send_email goes to COLD_EMAIL_CHANNEL instead.
JOB_TYPES: dict[str, str] = {
    "send_sms": "ghl_sms",
    "send_email": "ghl_email",
    "create_task": "ghl_task",
    "add_tag": "ghl_tag",
    "move_stage": "ghl_stage",
    "add_note": "ghl_note",
}
# Not "instantly_email": that is P1.3's channel for answers inside an Instantly reply thread.
COLD_EMAIL_CHANNEL = "instantly_cold_email"
MESSAGE_JOB_TYPES = frozenset({"send_sms", "send_email"})

RESULT_STATUSES = ("sent", "edited_sent", "rejected", "blocked_consent", "blocked_hours",
                   "blocked_lane", "blocked_cap", "expired", "error")

MAX_FILE_BYTES = 65_536
MAX_JOBS_PER_SWEEP = 50

# Brief section 7, Caps: "cold email at most once every 3 days and 4 times in 14 days".
COLD_EMAIL_MIN_GAP = timedelta(days=3)
COLD_EMAIL_WINDOW = timedelta(days=14)
COLD_EMAIL_MAX_IN_WINDOW = 4

# Brief section 7, Caps: "partners at most twice a week plus the Deal Drop". A partner is a GHL contact tagged
# PARTNER_TAG (the same tag as Josh's partner call list); a job is the Deal Drop when its template_version starts
# with DEAL_DROP_TEMPLATE_PREFIX (any case), and Deal Drops neither count toward nor are stopped by the cap.
PARTNER_TAG = "ndl-partner"
PARTNER_WINDOW = timedelta(days=7)
PARTNER_MAX_IN_WINDOW = 2
DEAL_DROP_TEMPLATE_PREFIX = "deal-drop"

# pending_actions.requested_by for drafts filed by an agent job (varchar(32)).
REQUESTED_BY = "agent_job"
# A held message job is re-checked this often until its window opens.
HOURS_RECHECK = timedelta(minutes=15)
# An approved send still 'sending' after this long has an unknown outcome; logged once, never retried.
STUCK_SENDING_AFTER = timedelta(minutes=15)
