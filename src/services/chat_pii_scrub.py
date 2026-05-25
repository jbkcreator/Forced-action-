"""
PII scrubber for chat message content.

Strips emails, phone numbers, and card-number patterns before persistence.
Run on every user and assistant message body before writing to chat_messages.
"""

import re

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE)

# Matches US phone formats: (813)555-1234, 813-555-1234, 8135551234, +18135551234, etc.
_PHONE_RE = re.compile(
    r"(?:\+?1[-.\s]?)?"
    r"(?:\(?\d{3}\)?[-.\s]?)"
    r"\d{3}[-.\s]?\d{4}"
)

# 13–19 digit sequences (card numbers with optional spaces/dashes)
_CARD_RE = re.compile(r"(?:\d[ \-]?){13,19}")

_EMAIL_PLACEHOLDER = "[email redacted]"
_PHONE_PLACEHOLDER = "[phone redacted]"
_CARD_PLACEHOLDER = "[card redacted]"


def scrub(text: str) -> str:
    """Return text with PII patterns replaced by placeholders."""
    if not text:
        return text
    text = _EMAIL_RE.sub(_EMAIL_PLACEHOLDER, text)
    text = _PHONE_RE.sub(_PHONE_PLACEHOLDER, text)
    text = _CARD_RE.sub(_CARD_PLACEHOLDER, text)
    return text
