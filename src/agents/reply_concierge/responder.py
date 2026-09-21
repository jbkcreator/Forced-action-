"""
KB responder for WP-T2-4. Template lookup only — no LLM drafts reply text.

The compliance footer is appended to every outbound reply. It is stored in
settings so it can be updated without a deploy (pending Backflip sign-off).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

_DEFAULT_FOOTER = (
    "\n\n---\n"
    "Josh Kantor, Forced Action / 1320 W. Lemon St., Tampa, FL 33606 / "
    "(813) 361-8927 / "
    "This message is not an offer of credit and is not a solicitation to originate a loan. "
    "Reply STOP to opt out of future messages."
)


@dataclass
class ReplyResult:
    has_reply: bool
    reply_text: Optional[str]
    kb_topic_key: Optional[str]
    reason: str


def build_reply(
    kb_topic_key: str,
    db: Session,
    borrower_first_name: Optional[str] = None,
) -> ReplyResult:
    """
    Look up the KB template for the given topic key and return the reply text.

    Returns has_reply=False when:
      - topic_key is "none" (no match found)
      - the topic is inactive or missing from the table
    In those cases the caller routes to EXCEPTIONS.
    """
    if not kb_topic_key or kb_topic_key == "none":
        return ReplyResult(
            has_reply=False,
            reply_text=None,
            kb_topic_key=None,
            reason="no_kb_topic_matched",
        )

    try:
        row = db.execute(
            text("""
                SELECT answer_template
                FROM fa_max_concierge_kb
                WHERE topic_key = :key AND is_active = TRUE
                LIMIT 1
            """),
            {"key": kb_topic_key},
        ).fetchone()
    except Exception as exc:
        logger.error("responder: KB lookup failed (%s)", exc)
        return ReplyResult(
            has_reply=False,
            reply_text=None,
            kb_topic_key=kb_topic_key,
            reason=f"kb_lookup_error: {exc}",
        )

    if not row:
        logger.warning("responder: topic_key=%r not found or inactive in KB", kb_topic_key)
        return ReplyResult(
            has_reply=False,
            reply_text=None,
            kb_topic_key=kb_topic_key,
            reason="topic_inactive_or_missing",
        )

    template: str = row[0]

    # Personalise: substitute borrower name if available
    greeting = f"Hi {borrower_first_name}," if borrower_first_name else "Hi,"
    body = f"{greeting}\n\n{template}"

    footer = _get_compliance_footer()
    reply_text = body + footer

    return ReplyResult(
        has_reply=True,
        reply_text=reply_text,
        kb_topic_key=kb_topic_key,
        reason="kb_match",
    )


def _get_compliance_footer() -> str:
    try:
        from config.settings import get_settings
        settings = get_settings()
        footer = getattr(settings, "concierge_compliance_footer", None)
        if footer:
            return f"\n\n---\n{footer}"
    except Exception:
        pass
    return _DEFAULT_FOOTER
