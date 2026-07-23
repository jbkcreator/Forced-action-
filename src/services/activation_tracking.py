"""
Activation-event tracking — T-B12-05 (Tier2 trust/proof layer + 5-min
activation onboarding).

Stamps the three timestamps needed to measure "reached first value within
5 minutes of signup":
  signup_time            -- row creation (mirrors Subscriber.created_at)
  first_leads_shown_time -- first render of the free-tier 3-5 scored-lead
                            dashboard (contact locked)
  first_unlock_time      -- first time the subscriber unlocks any lead's
                            contact info (the activation event)

All writes are set-once (never overwrite an existing timestamp) and
best-effort — a failure here must never block the dashboard or unlock flow
it's instrumenting.
"""
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _ensure_row(subscriber_id: int, db: Session) -> None:
    db.execute(
        text(
            """
            INSERT INTO activation_events (subscriber_id, signup_time)
            SELECT :subscriber_id, s.created_at
            FROM subscribers s
            WHERE s.id = :subscriber_id
            ON CONFLICT (subscriber_id) DO NOTHING
            """
        ),
        {"subscriber_id": subscriber_id},
    )


def stamp_first_leads_shown(subscriber_id: int, db: Session) -> None:
    """Set-once stamp for the first free-tier dashboard render with real leads visible."""
    try:
        with db.begin_nested():
            _ensure_row(subscriber_id, db)
            db.execute(
                text(
                    """
                    UPDATE activation_events
                    SET first_leads_shown_time = now()
                    WHERE subscriber_id = :subscriber_id
                      AND first_leads_shown_time IS NULL
                    """
                ),
                {"subscriber_id": subscriber_id},
            )
    except Exception as exc:  # noqa: BLE001 — instrumentation must not break the dashboard
        logger.warning(
            "activation_tracking: first_leads_shown stamp failed for subscriber=%s: %s",
            subscriber_id, exc,
        )


def stamp_first_unlock(subscriber_id: int, db: Session) -> None:
    """Set-once stamp for the first lead-contact unlock — the activation event."""
    try:
        with db.begin_nested():
            _ensure_row(subscriber_id, db)
            db.execute(
                text(
                    """
                    UPDATE activation_events
                    SET first_unlock_time = now()
                    WHERE subscriber_id = :subscriber_id
                      AND first_unlock_time IS NULL
                    """
                ),
                {"subscriber_id": subscriber_id},
            )
    except Exception as exc:  # noqa: BLE001 — instrumentation must not break the unlock flow
        logger.warning(
            "activation_tracking: first_unlock stamp failed for subscriber=%s: %s",
            subscriber_id, exc,
        )


def get_activation_status(subscriber_id: int, db: Session) -> dict:
    """Return the activation timestamps for a subscriber, or all-None if no row yet."""
    row = db.execute(
        text(
            """
            SELECT signup_time, first_leads_shown_time, first_unlock_time
            FROM activation_events
            WHERE subscriber_id = :subscriber_id
            """
        ),
        {"subscriber_id": subscriber_id},
    ).mappings().first()

    if not row:
        return {"signup_time": None, "first_leads_shown_time": None, "first_unlock_time": None}

    return {
        "signup_time": row["signup_time"].isoformat() if row["signup_time"] else None,
        "first_leads_shown_time": row["first_leads_shown_time"].isoformat() if row["first_leads_shown_time"] else None,
        "first_unlock_time": row["first_unlock_time"].isoformat() if row["first_unlock_time"] else None,
    }
