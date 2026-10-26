"""WP-GL-11: Next Deal Lending website lead form (nextdeallending.com) rule values.

The disclosure copy below is the exact label shown next to each consent checkbox on the published
page. The form sends the label it actually displayed; the server stores that verbatim and records
whether it still matches the constant here. The page has three separate unticked boxes (Engineering
Pod Action List section 2): text messages only, calls only, and the optional Deal Drop by email.
Nobody is named in the copy: the sender is "Next Deal Lending" (client, Q18).
"""
from __future__ import annotations

# Box 1: text messages only, named "transactional". GHL's opt-in review fails a consent text that states no message
# type ("Message type disclosure": promotional, transactional, alerts), and the client's own wording for this box is
# "text-only transactional consent". The call consent is its own box (CALL_CONSENT_TEXT): the GHL 30896 rejection was
# one box covering calls and texts. The box does not carry "(operated by HEU AI LLC)" (the footer, Privacy and Terms
# do): if it is ever added, change it here and on the page together.
SMS_CONSENT_TEXT = (
    "I agree to receive transactional text messages from Next Deal Lending at the number above about my inquiry, "
    "including replies, missed-call follow-ups, and call confirmations and reminders. "
    "Message frequency varies. Message and data rates may apply. "
    "Reply STOP to opt out, HELP for help. Consent is not a condition of any financing."
)

# Box 2: calls only. Wording approved by the team (option B: includes how to stop calls). The client asked for the
# separate call-only box but supplied no wording of their own. A page that shows other wording is stored verbatim and
# flagged as not matching, so change this and the page together.
CALL_CONSENT_TEXT = (
    "I agree that Next Deal Lending may call me at the number above about my inquiry, "
    "including with automated technology. Calls are recorded. "
    "Consent is not a condition of any financing. "
    "To stop calls, tell us on any call or email optout@nextdeallending.com."
)

# A typed loan amount above this is a typo, not a deal (whole dollars).
MAX_LOAN_AMOUNT = 100_000_000

RATE_LIMIT_SCOPE = "lending_web_lead"
RATE_LIMIT_PER_WINDOW = 8
RATE_LIMIT_WINDOW_SECONDS = 3600

# A double-click or a retried request inside this window returns the stored lead.
DEDUP_WINDOW_MINUTES = 10

# Minutes to wait after the Nth failed delivery (the last value repeats) before the sweep tries again.
# A lead is never given up on by attempt count: it keeps retrying until GHL_GIVE_UP_AFTER_HOURS old.
GHL_RETRY_BACKOFF_MINUTES = (2, 5, 15, 60, 240)
GHL_GIVE_UP_AFTER_HOURS = 72
# A lead still not in GHL this long after arriving is posted to Slack once.
GHL_ALERT_AFTER_MINUTES = 60
# A rejected key or setup (not a problem with the lead) does not use up an attempt.
GHL_CONFIG_ERROR_STATUS_CODES = (401, 403)
SWEEP_BATCH_SIZE = 50

GHL_SOURCE = "Website form"
# the dropdown value written to the consent custom fields (their options are "Yes" / "No")
GHL_CUSTOM_FIELD_YES = "Yes"
GHL_TAG_WEB_LEAD = "web-lead"
GHL_TAG_SMS_CONSENT_YES = "web-sms-consent-yes"
GHL_TAG_SMS_CONSENT_NO = "web-sms-consent-no"
GHL_TAG_CALL_CONSENT_YES = "web-call-consent-yes"
GHL_TAG_CALL_CONSENT_NO = "web-call-consent-no"
GHL_TAG_DEAL_DROP_OPTIN = "web-deal-drop-optin"  # the weekly Deal Drop by email only; never a text opt-in

FIELD_MAX_LENGTHS = {
    "name": 120,
    "email": 255,
    "property_city": 80,
    "deal_type": 60,
    "completed_projects_3y": 30,
    "loan_amount": 40,  # the typed text, before it is parsed to whole dollars
    "property_address": 200,
    "property_state": 40,
    "credit_band": 20,  # the column's width
    "consent_text": 2000,
    "call_consent_text": 2000,
    "page_url": 300,
    "user_agent": 300,
}

GHL_STATUSES = ("pending", "synced", "contact_only", "failed")
