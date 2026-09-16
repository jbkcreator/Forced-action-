"""Tracked links — WP-7 self-serve pre-fill path, WI-1.

Every partner, campaign, source, or per-property mailer gets its own opaque
slug. A click is recorded and attributed before the borrower types anything.

Attribution is durable server-side, not cookie-based: the click immediately
creates a selfserve_sessions row (see src/api/selfserve_router.py) and the
session's own UUID token is the URL the borrower continues on. This survives
cookie clearing and works across devices, at the cost of the borrower needing
the URL — the same tradeoff as any bookmarked/emailed link, and a stronger
durability guarantee than a 60-day cookie would have given.
"""
from __future__ import annotations

import logging
import secrets
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.core.models import TrackedLink, TrackedLinkClick

logger = logging.getLogger(__name__)

_SLUG_BYTES = 8              # secrets.token_urlsafe(8) -> ~11-char opaque slug
_MINT_RETRIES = 3


def mint_link(
    db: Session,
    kind: str,
    label: str,
    created_by: str,
    partner_ref: Optional[str] = None,
    campaign_ref: Optional[str] = None,
    property_id: Optional[int] = None,
    destination: Optional[str] = None,
) -> TrackedLink:
    """Create a TrackedLink with an opaque, non-enumerable slug.

    Retries on the vanishingly rare slug collision, same pattern as
    affiliate_engine.mint_affiliate.
    """
    last_err: Optional[IntegrityError] = None
    for _ in range(_MINT_RETRIES):
        link = TrackedLink(
            slug=secrets.token_urlsafe(_SLUG_BYTES),
            kind=kind,
            label=label,
            partner_ref=partner_ref,
            campaign_ref=campaign_ref,
            property_id=property_id,
            destination=destination,
            created_by=created_by,
        )
        db.add(link)
        try:
            db.flush()
            logger.info("Minted tracked_link id=%s kind=%s", link.id, kind)
            return link
        except IntegrityError as exc:
            last_err = exc
            db.rollback()
    raise RuntimeError("Failed to mint tracked link after retries") from last_err


def resolve_slug(db: Session, slug: str) -> Optional[TrackedLink]:
    """Look up an active tracked link by slug. Returns None for an unknown or
    inactive slug — callers must degrade to the generic flow, never 404 a
    borrower (a dead link in a printed mailer must not be a dead end)."""
    row = db.execute(
        text(
            "SELECT id, slug, kind, label, partner_ref, campaign_ref, "
            "property_id, destination, is_active, created_by, created_at "
            "FROM tracked_links WHERE slug = :slug AND is_active = true"
        ),
        {"slug": slug},
    ).mappings().first()
    if row is None:
        return None
    return TrackedLink(**dict(row))


def record_click(
    db: Session,
    tracked_link_id: int,
    session_token: str,
    ip_hash: Optional[str] = None,
    user_agent: Optional[str] = None,
    referer: Optional[str] = None,
) -> TrackedLinkClick:
    click = TrackedLinkClick(
        tracked_link_id=tracked_link_id,
        session_token=session_token,
        ip_hash=ip_hash,
        user_agent=user_agent,
        referer=referer,
    )
    db.add(click)
    db.flush()
    return click
