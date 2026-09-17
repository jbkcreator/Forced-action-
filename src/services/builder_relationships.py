"""WP-T2-8 Stage F — RELATIONSHIPS Slack queue output.

Repeat and portfolio-expansion builders surface here after pattern detection.
This is a MONEY-style opportunity card (positive signal), not an EXCEPTIONS alert.

Spec (Forced_Action_MAX_Tier2_4_Developer_Split.md §WP-T2-8 Output):
  - Matched builders → Tier 1 dial list (Stage E, wired via WP-9)
  - Repeat + portfolio-expansion builders → `RELATIONSHIPS` queue

Channel: FA_MAX_SLACK_CHANNEL_RELATIONSHIPS
No-ops silently when unconfigured — same pattern as EXCEPTIONS lane.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.builder_patterns import BuilderHit, PatternType
from src.services.builder_sizing import BuilderSizingResult

logger = logging.getLogger(__name__)

# Patterns that qualify for the RELATIONSHIPS queue (high-value repeat/portfolio signals)
_RELATIONSHIPS_PATTERNS: frozenset[PatternType] = frozenset({
    "repeat_builder",
    "spec_cadence",
})

_PATTERN_LABELS: dict[str, str] = {
    "repeat_builder":    "Repeat Builder",
    "concurrent_builder": "Concurrent Builder",
    "townhome_infill":   "Townhome / Infill Developer",
    "land_to_permit":    "Land → Permit",
    "spec_cadence":      "Spec Cadence Builder",
}


def is_relationships_candidate(hit: BuilderHit) -> bool:
    """True when a BuilderHit should surface in the RELATIONSHIPS queue."""
    return hit.pattern in _RELATIONSHIPS_PATTERNS


# ─────────────────────────────────────────────────────────────────────────────
# Card builder (pure — unit-testable, no I/O)
# ─────────────────────────────────────────────────────────────────────────────

def _format_loan(amount: Optional[Decimal]) -> str:
    if amount is None:
        return "_Unknown_"
    m = amount / Decimal("1_000_000")
    if m >= Decimal("1"):
        return f"${m:.2f}M"
    return f"${int(amount):,}"


def _permit_count(hit: BuilderHit) -> int:
    return len(hit.evidence_permit_ids) + len(hit.staging_permit_ids)


def build_relationships_blocks(
    hit: BuilderHit,
    sizing: Optional[BuilderSizingResult] = None,
) -> list[dict]:
    """Render a Block Kit card for one RELATIONSHIPS-queue builder opportunity.

    Design follows Slack UI guide: bold primary, italic muted secondary,
    primary/ghost action buttons, dividers for structure.
    """
    pattern_label = _PATTERN_LABELS.get(hit.pattern, hit.pattern)
    loan_str = _format_loan(sizing.estimated_loan if sizing else None)
    confidence_str = sizing.confidence if sizing else "unknown"
    permit_count = _permit_count(hit)
    county_str = hit.county_id.replace("_", " ").title() if hit.county_id else "Unknown county"
    date_str = (
        f"{hit.latest_permit_date.month}/{hit.latest_permit_date.day}/{hit.latest_permit_date.year}"
        if hit.latest_permit_date else "—"
    )

    blocks: list[dict] = [
        # Opportunity header — primary bold, muted county/pattern context
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{hit.principal_name}*\n"
                    f"_{pattern_label}  ·  {county_str}_"
                ),
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "View profile", "emoji": False},
                "action_id": f"view_builder_profile_{hit.buyer_entity_id}",
                "value": str(hit.buyer_entity_id),
            },
        },
        {"type": "divider"},
        # Key metrics — two-column fields
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Permits (evidence)*\n{permit_count} permit{'s' if permit_count != 1 else ''}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Est. Loan Size*\n{loan_str}  _{confidence_str}_",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Latest Permit*\n{date_str}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Sizing Source*\n_{sizing.sizing_source if sizing else 'none'}_",
                },
            ],
        },
        # Actions: Add to dial list (primary accent), Snooze (ghost)
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Add to dial list", "emoji": False},
                    "style": "primary",
                    "action_id": f"add_builder_to_diallist_{hit.buyer_entity_id}",
                    "value": str(hit.buyer_entity_id),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Snooze 7 days", "emoji": False},
                    "action_id": f"snooze_builder_{hit.buyer_entity_id}",
                    "value": str(hit.buyer_entity_id),
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Not a fit", "emoji": False},
                    "style": "danger",
                    "action_id": f"dismiss_builder_{hit.buyer_entity_id}",
                    "value": str(hit.buyer_entity_id),
                },
            ],
        },
        {"type": "divider"},
        # Footer — tertiary muted, pipe-separated
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Lane: *RELATIONSHIPS*  |  Source: Builder Engine (WP-T2-8)  |  _85% LTC construction_",
                },
            ],
        },
    ]
    return blocks


def build_relationships_text(hit: BuilderHit, sizing: Optional[BuilderSizingResult] = None) -> str:
    """Plain-text fallback for notification/a11y."""
    pattern_label = _PATTERN_LABELS.get(hit.pattern, hit.pattern)
    loan_str = _format_loan(sizing.estimated_loan if sizing else None)
    permit_count = _permit_count(hit)
    return (
        f"Builder opportunity — {hit.principal_name} "
        f"({pattern_label}, {permit_count} permits, est. loan {loan_str})"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Emitter
# ─────────────────────────────────────────────────────────────────────────────

def emit_relationships_alert(
    hit: BuilderHit,
    sizing: Optional[BuilderSizingResult] = None,
) -> bool:
    """Post one builder opportunity card to the RELATIONSHIPS Slack queue.

    No-ops when the channel or token is unconfigured — mirrors the EXCEPTIONS
    lane pattern so nightly runs never fail due to missing Slack config.
    """
    settings = get_settings()
    token = settings.slack_bot_token
    channel = settings.fa_max_slack_channel_relationships

    if not token or not channel:
        logger.info(
            "RELATIONSHIPS Slack unconfigured — skipping post for builder %s (%s)",
            hit.principal_name,
            hit.pattern,
        )
        return False

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_postMessage(
            channel=channel,
            text=build_relationships_text(hit, sizing),
            blocks=build_relationships_blocks(hit, sizing),
        )
        logger.info(
            "RELATIONSHIPS posted for builder %s pattern=%s",
            hit.principal_name,
            hit.pattern,
        )
        return True
    except Exception:
        logger.exception(
            "Failed to post RELATIONSHIPS alert for builder %s", hit.principal_name
        )
        return False


_CLAIM_RELATIONSHIP_ALERT_SQL = text("""
    INSERT INTO builder_relationship_alerts
        (buyer_entity_id, pattern, latest_permit_date, surfaced_at)
    VALUES (:buyer_entity_id, :pattern, :latest_permit_date, CURRENT_TIMESTAMP)
    ON CONFLICT (buyer_entity_id, pattern, latest_permit_date) DO NOTHING
    RETURNING buyer_entity_id
""")

_RELEASE_RELATIONSHIP_ALERT_SQL = text("""
    DELETE FROM builder_relationship_alerts
    WHERE buyer_entity_id = :buyer_entity_id
      AND pattern = :pattern
      AND latest_permit_date = :latest_permit_date
""")


def load_builder_queue_state(session: Session) -> tuple[set[int], set[int]]:
    """Return (dismissed_entity_ids, actively_snoozed_entity_ids) from
    builder_dial_queue — the operator decisions made on RELATIONSHIPS cards.

    Dismissed builders ("Not a fit") are excluded from MONEY and RELATIONSHIPS.
    Snoozed builders are suppressed from RELATIONSHIPS until snoozed_until passes.
    A single query; both sets read from one pass.
    """
    now = datetime.now(timezone.utc)
    dismissed: set[int] = set()
    snoozed: set[int] = set()
    for row in session.execute(text(
        "SELECT buyer_entity_id, dismissed, snoozed_until FROM builder_dial_queue"
    )):
        if row.dismissed:
            dismissed.add(row.buyer_entity_id)
            continue
        su = row.snoozed_until
        if su is not None:
            # Normalize a possibly tz-naive value (SQLite) before comparing.
            if su.tzinfo is None:
                su = su.replace(tzinfo=timezone.utc)
            if su > now:
                snoozed.add(row.buyer_entity_id)
    return dismissed, snoozed


def surface_relationship_hits(session: Session, hits: Iterable[BuilderHit]) -> int:
    """Post qualifying builder events once, durably, across scheduled runs.

    The database claim is committed before Slack I/O so concurrent dial-list
    jobs cannot double-post. A failed or unconfigured delivery releases the
    claim, allowing the next scheduled run to retry.

    Operator decisions are honored: a dismissed builder is never surfaced; a
    snoozed builder is skipped until its snooze expires. Each surfaced card
    carries its 85% LTC construction sizing (Stage D).
    """
    from src.services.builder_sizing import size_builder_hit

    dismissed, snoozed = load_builder_queue_state(session)
    surfaced = 0
    for hit in hits:
        if not is_relationships_candidate(hit):
            continue
        if hit.buyer_entity_id in dismissed or hit.buyer_entity_id in snoozed:
            continue
        params = {
            "buyer_entity_id": hit.buyer_entity_id,
            "pattern": hit.pattern,
            "latest_permit_date": hit.latest_permit_date or date.min,
        }
        savepoint = session.begin_nested()
        try:
            claimed = session.execute(
                _CLAIM_RELATIONSHIP_ALERT_SQL, params,
            ).first()
            savepoint.commit()
            session.commit()
        except SQLAlchemyError:
            savepoint.rollback()
            logger.exception(
                "Could not claim RELATIONSHIPS event for builder %s",
                hit.buyer_entity_id,
            )
            continue
        if claimed is None:
            continue
        sizing = size_builder_hit(session, hit)
        if emit_relationships_alert(hit, sizing):
            surfaced += 1
            continue
        savepoint = session.begin_nested()
        try:
            session.execute(_RELEASE_RELATIONSHIP_ALERT_SQL, params)
            savepoint.commit()
            session.commit()
        except SQLAlchemyError:
            savepoint.rollback()
            logger.exception(
                "Could not release failed RELATIONSHIPS event for builder %s",
                hit.buyer_entity_id,
            )
    return surfaced
