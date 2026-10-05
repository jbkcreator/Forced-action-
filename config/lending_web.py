"""WP-GL-11: Next Deal Lending website lead form (nextdeallending.com) rule values.

The disclosure copy below is the exact label shown next to the consent checkbox on the
published page (design export, Oct 2026). The form sends the label it actually displayed;
the server stores that verbatim and records whether it still matches this constant.
"""
from __future__ import annotations

SMS_CONSENT_TEXT = (
    "I agree that Next Deal Lending may call and text me at the number above about my inquiry, "
    "including with automated technology. Consent is not a condition of any financing. "
    "Calls are recorded. Message frequency varies and message and data rates may apply. "
    "Reply STOP to opt out, HELP for help."
)

RATE_LIMIT_SCOPE = "lending_web_lead"
RATE_LIMIT_PER_WINDOW = 8
RATE_LIMIT_WINDOW_SECONDS = 3600

# A double-click or a retried request inside this window returns the stored lead.
DEDUP_WINDOW_MINUTES = 10

GHL_MAX_ATTEMPTS = 5
GHL_RETRY_AFTER_MINUTES = 2
SWEEP_BATCH_SIZE = 50

GHL_SOURCE = "Website form"
GHL_TAG_WEB_LEAD = "web-lead"
GHL_TAG_SMS_CONSENT_YES = "web-sms-consent-yes"
GHL_TAG_SMS_CONSENT_NO = "web-sms-consent-no"
GHL_TAG_DEAL_DROP_OPTIN = "web-deal-drop-optin"

FIELD_MAX_LENGTHS = {
    "name": 120,
    "email": 255,
    "property_city": 80,
    "deal_type": 60,
    "completed_projects_3y": 30,
    "consent_text": 2000,
    "page_url": 300,
    "user_agent": 300,
}

GHL_STATUSES = ("pending", "synced", "contact_only", "failed")
