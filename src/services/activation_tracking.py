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


def _activation_event_columns(db: Session) -> set[str]:
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'activation_events'
            """
        )
    ).fetchall()
    return {row[0] for row in rows}


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


def stamp_onboarding_completed(subscriber_id: int, db: Session) -> None:
    """Set-once stamp for the one-time preference form (PATCH /onboarding).

    The only step between signup and first_leads_shown_time — without this,
    "never opened onboarding" and "opened it, never saw a lead" were the same
    NULL in the funnel data (Section 4.10 gap)."""
    try:
        with db.begin_nested():
            _ensure_row(subscriber_id, db)
            db.execute(
                text(
                    """
                    UPDATE activation_events
                    SET onboarding_completed_time = now()
                    WHERE subscriber_id = :subscriber_id
                      AND onboarding_completed_time IS NULL
                    """
                ),
                {"subscriber_id": subscriber_id},
            )
    except Exception as exc:  # noqa: BLE001 — instrumentation must not break onboarding submit
        logger.warning(
            "activation_tracking: onboarding_completed stamp failed for subscriber=%s: %s",
            subscriber_id, exc,
        )


def stamp_welcome_email_sent(subscriber_id: int, db: Session) -> None:
    """Set-once stamp for the initial welcome email send."""
    try:
        if "welcome_email_sent_time" not in _activation_event_columns(db):
            return
        with db.begin_nested():
            _ensure_row(subscriber_id, db)
            db.execute(
                text(
                    """
                    UPDATE activation_events
                    SET welcome_email_sent_time = now()
                    WHERE subscriber_id = :subscriber_id
                      AND welcome_email_sent_time IS NULL
                    """
                ),
                {"subscriber_id": subscriber_id},
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "activation_tracking: welcome_email_sent stamp failed for subscriber=%s: %s",
            subscriber_id, exc,
        )


def stamp_magic_link_redeemed(subscriber_id: int, db: Session) -> None:
    """Set-once stamp for the first successful magic-link redemption."""
    try:
        if "magic_link_redeemed_time" not in _activation_event_columns(db):
            return
        with db.begin_nested():
            _ensure_row(subscriber_id, db)
            db.execute(
                text(
                    """
                    UPDATE activation_events
                    SET magic_link_redeemed_time = now()
                    WHERE subscriber_id = :subscriber_id
                      AND magic_link_redeemed_time IS NULL
                    """
                ),
                {"subscriber_id": subscriber_id},
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "activation_tracking: magic_link_redeemed stamp failed for subscriber=%s: %s",
            subscriber_id, exc,
        )


def get_activation_status(subscriber_id: int, db: Session) -> dict:
    """Return the activation timestamps for a subscriber, or all-None if no row yet."""
    columns = _activation_event_columns(db)
    select_cols = ["signup_time", "onboarding_completed_time", "first_leads_shown_time", "first_unlock_time"]
    if "welcome_email_sent_time" in columns:
        select_cols.insert(1, "welcome_email_sent_time")
    if "magic_link_redeemed_time" in columns:
        select_cols.insert(2 if "welcome_email_sent_time" in columns else 1, "magic_link_redeemed_time")
    row = db.execute(
        text(
            f"""
            SELECT {", ".join(select_cols)}
            FROM activation_events
            WHERE subscriber_id = :subscriber_id
            """
        ),
        {"subscriber_id": subscriber_id},
    ).mappings().first()

    if not row:
        return {
            "signup_time": None,
            "welcome_email_sent_time": None,
            "magic_link_redeemed_time": None,
            "onboarding_completed_time": None,
            "first_leads_shown_time": None,
            "first_unlock_time": None,
        }

    return {
        "signup_time": row["signup_time"].isoformat() if row["signup_time"] else None,
        "welcome_email_sent_time": row.get("welcome_email_sent_time").isoformat() if row.get("welcome_email_sent_time") else None,
        "magic_link_redeemed_time": row.get("magic_link_redeemed_time").isoformat() if row.get("magic_link_redeemed_time") else None,
        "onboarding_completed_time": row["onboarding_completed_time"].isoformat() if row["onboarding_completed_time"] else None,
        "first_leads_shown_time": row["first_leads_shown_time"].isoformat() if row["first_leads_shown_time"] else None,
        "first_unlock_time": row["first_unlock_time"].isoformat() if row["first_unlock_time"] else None,
    }
