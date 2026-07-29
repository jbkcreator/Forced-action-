"""
10 seeded reply fixtures — real reply.py input shape (contracts.ReplyStubPayload
+ avenue hint), spanning all 5 top-level intents and several subtypes
including UNSUBSCRIBE. `expected_intent`/`expected_subtype` are the ground
truth a real classification run is checked against in integration tests;
unit tests instead force the classifier via conftest.classify_result and
only assert on downstream behavior.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

_NOW = datetime.now(timezone.utc).isoformat()


def _reply(
    n: int,
    *,
    body_text: str,
    subject: str = "Re: Founding seat",
    expected_intent: str,
    expected_subtype: str | None = None,
    opportunity_thread_id: str = "OPP-TEST-0001",
    avenue: str = "flippers",
) -> Dict[str, Any]:
    return {
        "opportunity_thread_id": opportunity_thread_id,
        "from_address": f"reply-test-{n}@example.com",
        "subject": subject,
        "body_text": body_text,
        "received_at": _NOW,
        "avenue": avenue,
        "expected_intent": expected_intent,
        "expected_subtype": expected_subtype,
    }


REPLIES: List[Dict[str, Any]] = [
    _reply(1, body_text="This looks interesting, tell me more and how do I book a call?",
           expected_intent="INTERESTED"),
    _reply(2, body_text="Yes I'd like to set up a time to talk this week.",
           expected_intent="INTERESTED", expected_subtype="BOOKING_REQUEST"),
    _reply(3, body_text="I already have a data source for county records, not sure I need this.",
           expected_intent="OBJECTION", avenue="flippers"),
    _reply(4, body_text="What does this cost? Seems like it might be too expensive for us.",
           expected_intent="OBJECTION", expected_subtype="PRICING_QUESTION"),
    _reply(5, body_text="Not actively buying right now, maybe check back later.",
           expected_intent="TIMING", expected_subtype="NOT_NOW", avenue="buy_and_hold"),
    _reply(6, body_text="Can you follow up with me again in about 3 months?",
           expected_intent="TIMING"),
    _reply(7, body_text="I'm not the right person for this — you should talk to our acquisitions lead instead.",
           expected_intent="REFERRAL", expected_subtype="WRONG_CONTACT"),
    _reply(8, body_text="You've got the wrong contact, this isn't my department.",
           expected_intent="REFERRAL", expected_subtype="WRONG_CONTACT"),
    _reply(9, body_text="Stop emailing me. Unsubscribe me from this list immediately.",
           expected_intent="HOSTILE", expected_subtype="UNSUBSCRIBE"),
    _reply(10, body_text="This is spam, take me off your list and don't contact me again.",
           expected_intent="HOSTILE", expected_subtype="UNSUBSCRIBE"),
]
