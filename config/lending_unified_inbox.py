"""R1 1.5 unified inbox: channels, sources, timings and approved copy (values only; logic in src/lending/inbox/).

Gmail / Hunter: add an adapter that calls ``record_inbound`` and one entry in ``SOURCES`` (client Q1, 27 Nov).
HubSpot and Salesmsg are Backflip systems and are never added.
"""
from __future__ import annotations

CHANNEL_GHL_SMS = "ghl_sms"
CHANNEL_GHL_EMAIL = "ghl_email"
CHANNEL_GHL_OTHER = "ghl_other"  # Facebook, Instagram, web chat: triage only, no card
CHANNEL_MISSED_CALL = "missed_call"
CHANNELS = frozenset({CHANNEL_GHL_SMS, CHANNEL_GHL_EMAIL, CHANNEL_GHL_OTHER, CHANNEL_MISSED_CALL})

SOURCE_GHL = "ghl"
SOURCE_BATCHDIALER = "batchdialer"
SOURCES: dict[str, str] = {SOURCE_GHL: "GoHighLevel", SOURCE_BATCHDIALER: "BatchDialer"}

CHANNEL_LABELS = {"ghl_sms": "GHL text", "ghl_email": "GHL email", "ghl_other": "GHL message",
                  "missed_call": "Missed call", "instantly_email": "Instantly email"}

# UNVERIFIED: GHL's message-type key and values follow its public reference, not a captured payload. An absent
# type counts as a text.
GHL_MESSAGE_TYPE_PATHS = (("messageType",), ("message", "type"), ("type",))
GHL_EMAIL_TYPES = frozenset({"email", "type_email"})
GHL_SMS_TYPES = frozenset({"sms", "type_sms", ""})
GHL_INBOUND_DIRECTIONS = frozenset({"in", "inbound", "incoming"})

STATUSES = ("new", "done", "failed", "skipped_test")
INTENTS = ("deal", "question", "objection", "dead", "stop", "unclassified")

MAX_ATTEMPTS = 5
BATCH_SIZE = 10
TRIAGE_SLA_SECONDS = 120
CALLBACK_TASK_DUE_MINUTES = 30
# How long a GHL text waits for the reply agent or a person before Cora decides on a card: 30 s + the up-to-60 s
# sweep + the model call stays under TRIAGE_SLA_SECONDS.
AI_GRACE_SECONDS = 30

# Approved by the client 27 Nov. ``first_name`` falls back to "there".
MISSED_CALL_REPLY_TEXT = ("Hi {first_name}, this is Josh with Next Deal Lending. Sorry I missed your call. "
                          "When is a good time to call you back? Reply STOP to opt out.")

# The GHL "NDL Reply Agent" Stop Bot final messages (docs/lending/ghl-conversation-ai.md), lower-cased with whitespace
# collapsed. Edit them here when they change in GHL. A handoff message means the agent passed the text to Josh; a
# mismatch only costs a card (reply_guard's rate/terms and reschedule word checks are the backstop). The opt-out
# message is the agent's promise "we won't text you again": the sweep honours it (the phone is opted out, no card).
REPLY_AGENT_HANDOFF_MESSAGES = (
    "honestly, i'd rather get you on a call with josh than guess over text. he'll give you real numbers on your "
    "deal and follow up shortly.",
    "thanks for letting us know. josh will follow up with you shortly.",
)
REPLY_AGENT_OPT_OUT_MESSAGES = (
    "understood, sorry for the mix-up. we won't text you again.",
)

INBOX_TOOL_NAME = "unified_inbox_reply"
