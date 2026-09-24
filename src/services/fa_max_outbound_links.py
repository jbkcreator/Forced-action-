"""Booking + portal links for FA Max borrower outbound.

Spec §Slack queues / §Calendar (item 9): "Calendar link on every outbound."

One tracked link per opportunity, reused across every touch, serves both
routes: `/book/{slug}` (calendar picker, one-live-booking guard per link) and
`/go/{slug}` (self-serve pre-fill → Backflip handoff). Reuse keeps click
attribution on one row instead of one row per email.

Fail closed: when a link cannot be built, callers must not send — a borrower
never receives placeholder text.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.tracked_links import mint_link

logger = logging.getLogger(__name__)

_CREATED_BY = "fa_max:outbound_links"


class LinkUnresolved(Exception):
    """A booking/portal link could not be built for this outbound."""


@dataclass(frozen=True)
class OutboundLinks:
    calendar_url: str
    portal_url: str


def _public_base_url() -> str:
    return (get_settings().app_base_url or "").rstrip("/")


BOOKING_LINE = "Want to talk it through? Grab a time: {calendar_link}"
_FOOTER_MARKER = "\n\n---\n"


def _outbound_link_label(person_id: str, opportunity_id: Optional[str]) -> str:
    if opportunity_id:
        return f"fa_max:outbound:{opportunity_id}"
    return f"fa_max:outbound:person:{person_id}"


def alert_link_unresolved(*, agent_name: str, person_id: str, opportunity_id: Optional[str], reason: str) -> None:
    """Surface a withheld touch to EXCEPTIONS. Never raises."""
    try:
        from src.services.relay import exceptions_alert_queue

        exceptions_alert_queue.enqueue_and_attempt(
            venture_key="fa_max_lending",
            rule=f"fa_max_link_unresolved:{agent_name}",
            message=(
                "*FA Max touch withheld — booking/portal link could not be built*\n"
                f"  • reason: `{reason}`\n"
                f"  • agent: `{agent_name}`  ·  person: `{person_id}`  ·  "
                f"opportunity: `{opportunity_id or '<none>'}`"
            ),
        )
    except Exception:
        logger.warning(
            "fa_max_outbound_links: alert failed person_id=%s reason=%s", person_id, reason, exc_info=True,
        )


def _existing_slug(db: Session, label: str) -> Optional[str]:
    return db.execute(
        text(
            "SELECT slug FROM tracked_links "
            "WHERE label = :label AND is_active = true ORDER BY id LIMIT 1"
        ),
        {"label": label},
    ).scalar()


def _subject_property_id(db: Session, opportunity_id: str) -> Optional[int]:
    return db.execute(
        text(
            "SELECT property_id FROM fa_max_opportunity_properties "
            "WHERE opportunity_id = :oid ::uuid AND role = 'subject' ORDER BY id LIMIT 1"
        ),
        {"oid": opportunity_id},
    ).scalar()


def _buyer_entity_id(db: Session, person_id: str) -> Optional[int]:
    return db.execute(
        text("SELECT buyer_entity_id FROM fa_max_person_profiles WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).scalar()


def resolve_links(db: Session, *, person_id: str, opportunity_id: Optional[str]) -> OutboundLinks:
    """Return the booking + portal URLs for this opportunity (or the person,
    when there is no opportunity), minting the tracked link on first use.
    Raises LinkUnresolved when no public base URL is configured or minting
    fails."""
    base = _public_base_url()
    if not base:
        raise LinkUnresolved("app_base_url_unset")

    label = _outbound_link_label(person_id, opportunity_id)
    slug = _existing_slug(db, label)
    if slug is None:
        try:
            link = mint_link(
                db,
                kind="source",
                label=label,
                created_by=_CREATED_BY,
                property_id=_subject_property_id(db, opportunity_id) if opportunity_id else None,
                buyer_entity_id=_buyer_entity_id(db, person_id),
            )
        except Exception as exc:
            logger.warning("fa_max_outbound_links: mint failed opportunity_id=%s: %s", opportunity_id, type(exc).__name__)
            raise LinkUnresolved("mint_failed") from exc
        slug = link.slug

    return OutboundLinks(calendar_url=f"{base}/book/{slug}", portal_url=f"{base}/go/{slug}")


def add_booking_line(body: str, links: OutboundLinks) -> str:
    """Insert the booking line above the compliance footer when the body
    already carries one, else append it."""
    line = BOOKING_LINE.format(calendar_link=links.calendar_url)
    head, marker, footer = body.rpartition(_FOOTER_MARKER)
    if not marker:
        return f"{body}\n\n{line}"
    return f"{head}\n\n{line}{marker}{footer}"


def resolve_or_alert(
    db: Session, *, agent_name: str, person_id: Optional[str], opportunity_id: Optional[str],
) -> Optional[OutboundLinks]:
    """Links for a borrower outbound, or None after alerting EXCEPTIONS.
    None means the caller must withhold the send (fail closed)."""
    try:
        if not person_id:
            raise LinkUnresolved("person_unknown")
        return resolve_links(db, person_id=person_id, opportunity_id=opportunity_id)
    except LinkUnresolved as exc:
        logger.error(
            "fa_max_outbound_links: %s send withheld person_id=%s opportunity_id=%s reason=link_unresolved:%s",
            agent_name, person_id, opportunity_id, exc,
        )
        alert_link_unresolved(
            agent_name=agent_name, person_id=person_id or "<none>",
            opportunity_id=opportunity_id, reason=str(exc),
        )
        return None
