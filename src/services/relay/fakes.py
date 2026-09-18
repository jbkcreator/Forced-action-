"""
FakeMail / FakeSMS — the WP-T2-1 "immediate deliverable" (Forced_Action_MAX
Tier2_4_Developer_Split.md, WP-T2-1 Dependency Role).

Every other Tier 2 developer needs to write and run outbound-send tests
before the dedicated domain, DNS auth, and 10DLC registration exist. These
two classes are that test double: same call shape as the real vendor calls
(src.services.instantly_service.add_leads, src.services.telnyx_sms.send_message),
zero network, zero credentials, and every call recorded so a test can assert
on exactly what would have been sent.

Selection is by config/settings.py:fa_max_relay_send_mode ("fake" default,
"live" once the real lane is ready) — see channels_email.py / channels_sms.py
for where these are actually invoked. This module never talks to a real
vendor.

Distinguishing "accepted" from "delivered" (production-execution review,
finding 6): Instantly's add_leads() only proves the recipient was queued
into a campaign, not that an inbox received anything. Telnyx's Messaging API
returns "queued", not "delivered". Modeling both fakes with only a boolean
"sent" flag would let a test believe more than the real vendor ever promises.
Both fakes therefore report `accepted` (the call succeeded) and leave
`delivered` deliberately unset (None) — nothing in this codebase observes
real delivery today (see email_deliverability_monitor.py's proxy metrics),
so no fake should claim it does either.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class FakeSendReceipt:
    """What a Fake reports back — mirrors the real vendor's own limits."""
    accepted: bool
    vendor: str
    vendor_message_id: str
    accepted_at: str
    delivered: Optional[bool] = None  # never observed by either Fake — see module docstring
    reason: Optional[str] = None      # set when accepted=False


@dataclass
class FakeMail:
    """Stands in for src.services.instantly_service.add_leads() /
    src.services.relay.channels_email.send_email().

    `duplicate_recipients` lets a test reproduce Instantly's real
    duplicate-contact-skip behavior (channels_email.py's documented hard
    failure) without needing a live campaign.
    """
    sent: list[dict] = field(default_factory=list)
    duplicate_recipients: set[str] = field(default_factory=set)
    fail_recipients: set[str] = field(default_factory=set)

    def send(self, *, recipient: str, subject: str, body: str, campaign_id: str) -> FakeSendReceipt:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if recipient in self.fail_recipients:
            return FakeSendReceipt(
                accepted=False, vendor="fake_instantly", vendor_message_id="",
                accepted_at=now, reason="vendor_error",
            )
        if recipient in self.duplicate_recipients:
            return FakeSendReceipt(
                accepted=False, vendor="fake_instantly", vendor_message_id="",
                accepted_at=now, reason="duplicate_contact_guard",
            )
        self.sent.append({
            "recipient": recipient, "subject": subject, "body": body,
            "campaign_id": campaign_id, "sent_at": now,
        })
        return FakeSendReceipt(
            accepted=True, vendor="fake_instantly",
            vendor_message_id=f"fake-instantly-{len(self.sent)}", accepted_at=now,
        )


@dataclass
class FakeSMS:
    """Stands in for src.services.telnyx_sms.send_message().

    `fail_recipients` reproduces a Telnyx-side rejection (invalid number,
    account suspended, etc) without a live account.
    """
    sent: list[dict] = field(default_factory=list)
    fail_recipients: set[str] = field(default_factory=set)

    def send(self, *, to: str, body: str, message_type: str = "marketing") -> FakeSendReceipt:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if to in self.fail_recipients:
            return FakeSendReceipt(
                accepted=False, vendor="fake_telnyx", vendor_message_id="",
                accepted_at=now, reason="vendor_error",
            )
        self.sent.append({"to": to, "body": body, "message_type": message_type, "sent_at": now})
        return FakeSendReceipt(
            accepted=True, vendor="fake_telnyx",
            vendor_message_id=f"fake-telnyx-{len(self.sent)}", accepted_at=now,
        )


# Process-wide instances so a test can import and inspect the same fake the
# channel dispatchers used, without threading an instance through every call
# (mirrors fakeredis's module-level pattern already used elsewhere in this
# repo's test suite — CLAUDE.md: "Use fakeredis in tests/sandbox").
FAKE_MAIL = FakeMail()
FAKE_SMS = FakeSMS()


def reset_fakes() -> None:
    """Clear recorded sends between tests."""
    FAKE_MAIL.sent.clear()
    FAKE_MAIL.duplicate_recipients.clear()
    FAKE_MAIL.fail_recipients.clear()
    FAKE_SMS.sent.clear()
    FAKE_SMS.fail_recipients.clear()
