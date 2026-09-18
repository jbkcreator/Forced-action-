"""Fail-closed governance for every FA Max outbound action."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

FA_MAX_VENTURE = "fa_max_lending"
LANES = frozenset({"MONEY", "EXCEPTIONS", "RELATIONSHIPS"})
CONTACT_CHANNELS = frozenset({"email", "sms"})

_PROHIBITED_KEYS = re.compile(
    r"(^|_)(ssn|social_security|credit_score|fico|income|bank_statement|"
    r"tax_return|dti|debt_to_income|rate|interest_rate|term|commitment)(_|$)",
    re.IGNORECASE,
)
_PROHIBITED_TEXT = re.compile(
    r"\b(?:social security|SSN|credit score|FICO|bank statement|tax return|"
    r"interest rate|loan rate|loan terms?|commitment)\b",
    re.IGNORECASE,
)


class GovernanceBlocked(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class ConsentResult:
    allowed: bool
    reason: str


def validate_safe_payload(payload: dict[str, Any]) -> None:
    """Reject prohibited borrower financial fields and outbound claims."""
    def walk(value: Any, path: str = "payload") -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if _PROHIBITED_KEYS.search(str(key)):
                    raise GovernanceBlocked(f"prohibited_financial_field:{path}.{key}")
                walk(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")
        elif isinstance(value, str) and _PROHIBITED_TEXT.search(value):
            raise GovernanceBlocked(f"prohibited_financial_content:{path}")
    walk(payload)


def require_consent(session: Session, *, person_id: str, channel: str) -> ConsentResult:
    """Require an explicit current opt-in for this person and channel."""
    if channel not in CONTACT_CHANNELS:
        return ConsentResult(True, "not_a_contact_channel")
    row = session.execute(
        text("SELECT consented FROM fa_max_person_consent "
             "WHERE person_id = CAST(:person_id AS uuid) AND channel = :channel"),
        {"person_id": person_id, "channel": channel},
    ).fetchone()
    if row is None:
        return ConsentResult(False, "consent_absent")
    if not row.consented:
        return ConsentResult(False, "consent_withdrawn")
    return ConsentResult(True, "consent_granted")


def autonomous_tier_context_verified(
    session: Session, *, person_id: str, tier: str, thread_id: str | None,
) -> bool:
    """Verify an A/B auto-send against durable recipient context.

    A prior interaction alone is insufficient: a later cold pitch to the
    same person remains cold. Unverifiable replies and warm introductions
    continue through human approval. Tier C is the conservative default.

    thread_id is currently UNUSED for Tier A verification (WP-T2-2 review
    fix) -- kept in the signature so call sites and a future WP-T2-6 (Reply
    Agent as Portal Concierge, which owns inbound-reply-to-thread
    correlation) do not need a signature change to use it once that
    correlation actually exists. See the Tier A branch below for why.
    """
    if tier == "C":
        return True
    if tier == "A":
        funded = session.execute(text(
            "SELECT 1 FROM fa_max_opportunities WHERE person_id = CAST(:person_id AS uuid) "
            "AND outcome = 'funded' LIMIT 1"
        ), {"person_id": person_id}).scalar()
        if funded:
            return True
        # WP-T2-2 review fix: the prior "any later inbound interaction for
        # this person, after a sent item in this thread" heuristic did NOT
        # actually tie the inbound interaction to the SAME thread --
        # fa_max_interactions has no thread_id column at all, so the EXISTS
        # subquery could only ever check "this person replied to something,
        # at some point, in some conversation." A reply on an unrelated
        # thread (or an unrelated later inbound touch from a different
        # channel) would satisfy it and incorrectly authorize an autonomous
        # send on THIS thread. There is currently no production path in
        # this codebase that captures which thread an inbound reply
        # belongs to -- that correlation is WP-T2-6's (Reply Agent as
        # Portal Concierge) job, not this WP's. Rather than invent a
        # thread_id column with no real writer behind it, this falls back
        # to the review's own explicitly offered safe alternative: when the
        # link cannot be proved, the send stays unverified for autonomous
        # dispatch (auto_authorize is declined, and the item is queued for
        # ordinary human Slack approval instead) -- a funded-borrower
        # relationship is the only Tier A context this function can
        # currently prove from durable data.
        return False
    if tier == "B":
        active_partner = session.execute(text(
            "SELECT 1 FROM fa_max_partners WHERE person_id = CAST(:person_id AS uuid) "
            "AND status = 'active' LIMIT 1"
        ), {"person_id": person_id}).scalar()
        return bool(active_partner)
    return False


def suppression_reason(session: Session, *, recipient: str, channel: str) -> str | None:
    """First suppression gate, using the same stores as Relay's send gate."""
    campaign_reason = backflip_campaign_reason(session, recipient=recipient, channel=channel)
    if campaign_reason:
        return campaign_reason
    if channel == "email":
        from src.services.email_suppression import is_email_suppressed
        return "email_opt_out" if is_email_suppressed(session, recipient) else None
    if channel == "sms":
        from src.services.compliance_gator import validate_outbound
        result = validate_outbound(recipient, channel, session)
        return None if result.allowed else (result.reason or "compliance_blocked")
    return None


def backflip_campaign_reason(session: Session, *, recipient: str, channel: str) -> str | None:
    """Fail closed when the latest complete Backflip campaign snapshot is stale."""
    if channel not in CONTACT_CHANNELS:
        return None
    from config.settings import get_settings
    from src.services.phone_utils import normalize

    max_age = get_settings().fa_max_backflip_feed_max_age_hours
    fresh = session.execute(
        text("SELECT last_success_at >= now() - make_interval(hours => :max_age) "
             "FROM fa_max_backflip_campaign_feed WHERE id = 1"),
        {"max_age": max_age},
    ).scalar_one_or_none()
    if fresh is None:
        return "backflip_feed_unavailable"
    if not fresh:
        return "backflip_feed_stale"
    value = recipient.strip().lower() if channel == "email" else normalize(recipient)
    if value is None:
        return "invalid_contact_identifier"
    kind = "email" if channel == "email" else "phone"
    active = session.execute(
        text("SELECT 1 FROM fa_max_backflip_campaign_contacts "
             "WHERE identifier_kind = :kind AND identifier_value = :value "
             "AND active LIMIT 1"),
        {"kind": kind, "value": value},
    ).scalar_one_or_none()
    return "backflip_active_campaign" if active else None


def set_consent(
    session: Session, *, person_id: str, channel: str, consented: bool, source: str,
) -> None:
    """Record the latest channel consent with its source and timestamp."""
    if channel not in CONTACT_CHANNELS:
        raise ValueError(f"unsupported consent channel: {channel}")
    if not source.strip():
        raise ValueError("consent source is required")
    session.execute(
        text("INSERT INTO fa_max_person_consent "
             "(person_id, channel, consented, source, consented_at) "
             "VALUES (CAST(:person_id AS uuid), :channel, :consented, :source, now()) "
             "ON CONFLICT (person_id, channel) DO UPDATE SET "
             "consented = EXCLUDED.consented, source = EXCLUDED.source, "
             "consented_at = EXCLUDED.consented_at"),
        {"person_id": person_id, "channel": channel,
         "consented": consented, "source": source.strip()},
    )
