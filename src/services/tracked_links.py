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


# ---------------------------------------------------------------------------
# Slack slash command: /tracked-link — link generation without an engineer.
#
# Lives here, not in src/services/relay/, deliberately: WP-7 owns this logic,
# the shared Socket Mode connection (src/services/relay/socket_listener.py,
# PR #276) just imports and registers it. Contract matches that module's
# handle_socket_request exactly (client, request) -> bool, so it drops into
# the same socket_mode_request_listeners list without any changes there
# beyond one registration line — see the Phase B follow-up.
# ---------------------------------------------------------------------------

VALID_TRACKED_LINK_KINDS = ("partner", "campaign", "source", "property_mailer")

_SLASH_COMMAND = "/tracked-link"
_SLASH_USAGE = (
    "Usage: `/tracked-link <partner|campaign|source|property_mailer> <label or address>`\n"
    "Examples:\n"
    "  `/tracked-link partner Acme Title Co`\n"
    "  `/tracked-link property_mailer 123 Main St, Tampa FL 33602`"
)


def _slack_ephemeral(msg: str) -> dict:
    return {"response_type": "ephemeral", "text": msg}


def build_tracked_link_reply(db: Session, text_arg: str, user: str) -> dict:
    """Parse the slash command's text, mint the link, return the Slack
    response payload. Split out from the Socket Mode envelope handling below
    so it's testable with a plain db session — no fake Slack objects needed."""
    from src.services.prefill_assembly import find_property_by_address

    parts = (text_arg or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        return _slack_ephemeral(_SLASH_USAGE)

    kind, rest = parts[0].lower(), parts[1].strip()
    if kind not in VALID_TRACKED_LINK_KINDS:
        return _slack_ephemeral(
            f"Unknown kind {kind!r}. Use one of: {', '.join(VALID_TRACKED_LINK_KINDS)}\n\n{_SLASH_USAGE}"
        )

    property_id = None
    label = rest
    if kind == "property_mailer":
        property_id, _confidence = find_property_by_address(db, rest)
        if property_id is None:
            return _slack_ephemeral(
                f"Couldn't confidently match {rest!r} to a property — check the address and try again."
            )
        label = f"mailer: {rest}"

    link = mint_link(db, kind=kind, label=label, created_by=f"slack:{user}", property_id=property_id)

    from config.settings import settings
    base = settings.app_base_url.rstrip("/") if settings.app_base_url else ""
    return _slack_ephemeral(f"Created: {base}/go/{link.slug}")


def handle_tracked_link_socket_request(client, request) -> bool:
    """Socket Mode envelope handler for the /tracked-link slash command.

    Returns True only when this listener handled the request — every other
    app action is left untouched so a shared Socket Mode connection does not
    accidentally mutate another workflow (same contract as
    src/services/relay/socket_listener.py's handle_socket_request).
    """
    if request.type != "slash_commands":
        return False
    payload = request.payload or {}
    if payload.get("command") != _SLASH_COMMAND:
        return False

    from slack_sdk.socket_mode.response import SocketModeResponse
    from config.settings import settings
    from src.core.database import get_db_context

    allowed_channel = settings.fa_max_slack_channel_relationships
    if allowed_channel and payload.get("channel_id") != allowed_channel:
        client.send_socket_mode_response(
            SocketModeResponse(
                envelope_id=request.envelope_id,
                payload=_slack_ephemeral(f"{_SLASH_COMMAND} only works in the fa-max-relationships channel."),
            )
        )
        return True

    text_arg = payload.get("text") or ""
    user = payload.get("user_name") or "someone"

    with get_db_context() as db:
        reply = build_tracked_link_reply(db, text_arg, user)

    client.send_socket_mode_response(
        SocketModeResponse(envelope_id=request.envelope_id, payload=reply)
    )
    return True
