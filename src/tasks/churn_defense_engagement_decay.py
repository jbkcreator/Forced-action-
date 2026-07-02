"""
Churn Defense — weekly engagement-decay scorer (Task 6.3, Phase 6).

For every non-churned subscriber, recompute portal-engagement counts from
webhook_events over a rolling 30-day window, score engagement decay against the
subscriber's own weekly-equivalent baseline, snapshot the result into
subscriber_session_metrics, and — for accounts whose activity has collapsed
(decay < DECAY_THRESHOLD) — stage a churn_defense_leads row, generate a
personalized pitch from the subscriber's purchased ZIP via the Pitch Generator,
and fire a GoHighLevel retention check-in by tagging the contact.

Spec formula (page 8):
    engagement_decay = (views_7 * 0.4 + downloads_7 * 0.6) / (baseline_30day_mean + 1)
    baseline_30day_mean = (views_30 * 0.4 + downloads_30 * 0.6) / (30 / 7)

Cold-start guard is history-based (not account age): an account is only eligible
to be flagged once its first-ever DASHBOARD_VIEW or LEAD_DOWNLOAD (never
SUBSCRIBER_LOGIN alone) is >= MIN_BASELINE_HISTORY_DAYS old. This prevents a
launch-day false-positive flood (every established account has zero tracked
events until the frontend listeners ship) AND prevents flagging an account that
has only ever logged in and never actually used the dashboard — logging in is
not engagement, so it must not count toward "has a real baseline to drop from."
A zero baseline_30day_mean is still flaggable once that history requirement is
met — a genuinely engaged account gone completely silent is exactly the signal
this worker exists to catch.

Sends nothing during --dry-run (no snapshot writes, no Claude calls, no GHL).

Cron: 0 14 * * 1  (weekly, Monday 14:00 UTC — after churn_scoring at 13:00)

Usage:
    python -m src.tasks.churn_defense_engagement_decay [--dry-run]
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.churn_defense import (
    BASELINE_WEEKS,
    DECAY_SCALAR_MAX,
    DECAY_THRESHOLD,
    DOWNLOAD_WEIGHT,
    GHL_CHURN_DEFENSE_TAG,
    MIN_BASELINE_HISTORY_DAYS,
    OUTREACH_COOLDOWN_DAYS,
    VIEW_WEIGHT,
)
from src.core.database import get_db_context
from src.core.models import ChurnDefenseLead, Subscriber

logger = logging.getLogger(__name__)


# ── SQL ──────────────────────────────────────────────────────────────────────

# Per-subscriber engagement aggregation over the rolling 30-day window.
_AGG_SQL = text("""
    SELECT
        w.subscriber_id,
        COUNT(*) FILTER (WHERE w.event_type='DASHBOARD_VIEW' AND w.processed_at >= now() - interval '7 days')  AS views_7,
        COUNT(*) FILTER (WHERE w.event_type='LEAD_DOWNLOAD'  AND w.processed_at >= now() - interval '7 days')  AS downloads_7,
        COUNT(*) FILTER (WHERE w.event_type='DASHBOARD_VIEW')                                                   AS views_30,
        COUNT(*) FILTER (WHERE w.event_type='LEAD_DOWNLOAD')                                                    AS downloads_30,
        MAX(w.processed_at) FILTER (WHERE w.event_type='SUBSCRIBER_LOGIN')                                      AS last_login_at,
        MIN(w.processed_at) FILTER (WHERE w.event_type='SUBSCRIBER_LOGIN')                                      AS first_login_at,
        COUNT(*)            FILTER (WHERE w.event_type='SUBSCRIBER_LOGIN')                                       AS login_count
    FROM webhook_events w
    WHERE w.source = 'frontend'
      AND w.subscriber_id IS NOT NULL
      AND w.event_type IN ('DASHBOARD_VIEW', 'LEAD_DOWNLOAD', 'SUBSCRIBER_LOGIN')
      AND w.processed_at >= now() - interval '30 days'
    GROUP BY w.subscriber_id
""")

# Lifetime (unbounded) first-tracked-ENGAGEMENT-event per subscriber — deliberately
# NOT limited to the 30-day window, and deliberately excludes SUBSCRIBER_LOGIN.
#
# Excluded from the 30-day window: an event older than 30 days is exactly what
# proves a subscriber has enough tracked history, but it is excluded from
# _AGG_SQL by design (that query computes the rolling baseline). Computing "has
# 30 days of history" from a query that only ever looks back 30 days is a
# contradiction — this second query exists to avoid that trap.
#
# Excludes SUBSCRIBER_LOGIN: the decay formula only ever measures DASHBOARD_VIEW
# and LEAD_DOWNLOAD, never logins. If logins counted toward "has history," a
# subscriber who logged in once 40 days ago and never once viewed the dashboard
# or downloaded a lead would pass the history guard with a genuine zero
# baseline and get flagged as "churned" despite never having been a real user
# of the product in the first place. Restricting this query to the same two
# event types the formula actually measures closes that gap.
_FIRST_TRACKED_SQL = text("""
    SELECT subscriber_id, MIN(processed_at) AS first_event_at
    FROM webhook_events
    WHERE source = 'frontend'
      AND subscriber_id IS NOT NULL
      AND event_type IN ('DASHBOARD_VIEW', 'LEAD_DOWNLOAD')
    GROUP BY subscriber_id
""")

_POPULATION_SQL = text("SELECT id FROM subscribers WHERE churned_at IS NULL")

# FAILED is excluded here (in addition to CONVERTED) so a transient outreach
# failure (GHL timeout, pitch-generation error, etc.) does not permanently
# suppress retries for the full cooldown window — see _fire_retention_outreach
# and the FAILED assignment in run() below.
_OPEN_LEADS_SQL = text("""
    SELECT DISTINCT subscriber_id
    FROM churn_defense_leads
    WHERE triggered_at >= now() - make_interval(days => :cooldown)
      AND outreach_status NOT IN ('CONVERTED', 'FAILED')
""")

_UPSERT_SNAPSHOT_SQL = text("""
    INSERT INTO subscriber_session_metrics
        (subscriber_id, last_login_at, dashboard_views_7_day, lead_downloads_7_day,
         auth_intervals_seconds, engagement_decay_scalar, updated_at)
    VALUES
        (:subscriber_id, :last_login_at, :views_7, :downloads_7,
         :auth_intervals_seconds, :engagement_decay_scalar, now())
    ON CONFLICT (subscriber_id) DO UPDATE SET
        last_login_at           = EXCLUDED.last_login_at,
        dashboard_views_7_day   = EXCLUDED.dashboard_views_7_day,
        lead_downloads_7_day    = EXCLUDED.lead_downloads_7_day,
        auth_intervals_seconds  = EXCLUDED.auth_intervals_seconds,
        engagement_decay_scalar = EXCLUDED.engagement_decay_scalar,
        updated_at              = now()
""")

_REP_PROPERTY_SQL = text("""
    SELECT p.id
    FROM properties p
    JOIN distress_scores d ON d.property_id = p.id
    WHERE p.zip = :zip
    ORDER BY d.score_date DESC
    LIMIT 1
""")

_SUBSCRIBER_ZIPS_SQL = text(
    "SELECT zip_code FROM zip_territories WHERE subscriber_id = :sid AND zip_code IS NOT NULL"
)


# ── Scoring ────────────────────────────────────────────────────────────────


def _compute_decay(agg: dict) -> tuple[float, bool]:
    """Return (engagement_decay, is_cold_start) for one subscriber's aggregates.

    is_cold_start is True only when the account's first-ever DASHBOARD_VIEW or
    LEAD_DOWNLOAD (agg['first_event_at'], from an UNBOUNDED, engagement-only
    lookup — see _FIRST_TRACKED_SQL) is missing or less than
    MIN_BASELINE_HISTORY_DAYS old. SUBSCRIBER_LOGIN never counts toward this —
    a login-only account has no real product engagement to have "dropped" from.
    A zero baseline_30day_mean on its own is NOT treated as cold-start once the
    history requirement is met: an account with a real, sufficiently old
    engagement event and zero activity in the current window is exactly the
    extreme drop-off case this worker exists to catch, not an absence of data.
    """
    views_7 = agg.get("views_7") or 0
    downloads_7 = agg.get("downloads_7") or 0
    views_30 = agg.get("views_30") or 0
    downloads_30 = agg.get("downloads_30") or 0
    first_event_at = agg.get("first_event_at")

    recent = views_7 * VIEW_WEIGHT + downloads_7 * DOWNLOAD_WEIGHT
    baseline_30day_mean = (views_30 * VIEW_WEIGHT + downloads_30 * DOWNLOAD_WEIGHT) / BASELINE_WEEKS
    engagement_decay = recent / (baseline_30day_mean + 1)

    now = datetime.now(timezone.utc)
    history_days = None
    if first_event_at is not None:
        # first_event_at is timezone-aware (timestamptz).
        history_days = (now - first_event_at).total_seconds() / 86400.0

    is_cold_start = (
        first_event_at is None
        or history_days < MIN_BASELINE_HISTORY_DAYS
    )
    return engagement_decay, is_cold_start


def _auth_intervals_seconds(agg: dict) -> int:
    """Best-effort average login interval in seconds; 0 when < 2 logins.

    Not used by the decay formula — stored per spec only.
    """
    login_count = agg.get("login_count") or 0
    first_login = agg.get("first_login_at")
    last_login = agg.get("last_login_at")
    if login_count >= 2 and first_login is not None and last_login is not None:
        span = (last_login - first_login).total_seconds()
        return max(0, int(span / (login_count - 1)))
    return 0


# ── Trigger side-effects ─────────────────────────────────────────────────────


def _generate_retention_pitch(db: Session, subscriber_id: int) -> Optional[dict]:
    """Generate a personalized pitch from a representative property in the
    subscriber's purchased ZIP(s). Returns the pitch dict, or None if no
    ZIP/property is available or generation fails."""
    from src.agents.pitch_builder import (
        build_property_pitch_context,
        generate_pitch_with_claude,
    )

    zips = [r[0] for r in db.execute(_SUBSCRIBER_ZIPS_SQL, {"sid": subscriber_id}).all()]
    property_id = None
    for zip_code in zips:
        row = db.execute(_REP_PROPERTY_SQL, {"zip": zip_code}).first()
        if row is not None:
            property_id = row[0]
            break

    if property_id is None:
        logger.warning(
            "[ChurnDefense] subscriber=%d has no scored property in purchased ZIPs — skipping pitch",
            subscriber_id,
        )
        return None

    context = build_property_pitch_context(db, property_id)
    return generate_pitch_with_claude(
        context=context,
        request_options={},  # defaults: email_subject / email_pitch / sms_pitch
        subscriber_id=subscriber_id,
        db=db,
    )


def _pitch_note_body(pitch: dict) -> str:
    """Format the Pitch Generator's output into a single GHL note body."""
    parts = []
    subject = pitch.get("email_subject")
    if subject:
        parts.append(f"Subject: {subject}")
    body = pitch.get("email_pitch") or pitch.get("sms_pitch")
    if body:
        parts.append(body)
    return "\n\n".join(parts).strip()


def _fire_retention_outreach(db: Session, lead: ChurnDefenseLead) -> bool:
    """Generate the pitch and fire the GHL check-in sequence for a staged lead.

    On a successful GHL push, the generated pitch copy is attached to the
    contact as a note — otherwise the Claude call would run and its output
    would never reach anyone (Pitch Generator per spec R10).

    Returns True if the GHL push succeeded (status advanced to SEQUENCE_TRIGGERED).
    Never raises — all external-call failures are logged and isolated.
    """
    sid = lead.subscriber_id

    # Pitch generation is best-effort — a failure must not block GHL outreach.
    pitch: Optional[dict] = None
    try:
        pitch = _generate_retention_pitch(db, sid)
    except Exception as exc:
        logger.warning("[ChurnDefense] pitch generation failed for subscriber=%d: %s", sid, exc)

    subscriber = db.get(Subscriber, sid)
    if subscriber is None:
        logger.warning("[ChurnDefense] subscriber=%d vanished before GHL push", sid)
        return False

    try:
        from src.services.ghl_webhook import add_contact_note, push_subscriber_to_ghl

        ok = push_subscriber_to_ghl(
            subscriber,
            stage=None,
            tags=[GHL_CHURN_DEFENSE_TAG],
            db=db,
        )
        if ok:
            lead.outreach_status = "SEQUENCE_TRIGGERED"
            if pitch and subscriber.ghl_contact_id:
                note_body = _pitch_note_body(pitch)
                if note_body:
                    try:
                        add_contact_note(subscriber.ghl_contact_id, note_body)
                    except Exception as exc:
                        logger.warning(
                            "[ChurnDefense] failed to attach pitch note for subscriber=%d: %s",
                            sid, exc,
                        )
        return ok
    except Exception as exc:
        logger.error("[ChurnDefense] GHL push failed for subscriber=%d: %s", sid, exc)
        return False


# ── Main run ───────────────────────────────────────────────────────────────


def run(dry_run: bool = False) -> dict:
    """Score every non-churned subscriber and stage at-risk accounts. Returns summary."""
    results = {"scored": 0, "flagged": 0, "skipped_coldstart": 0, "ghl_ok": 0, "ghl_failed": 0}

    with get_db_context() as db:
        agg_by_sub = {
            row["subscriber_id"]: dict(row)
            for row in db.execute(_AGG_SQL).mappings().all()
        }
        # Unbounded lookup — independent of agg_by_sub, so a subscriber with
        # ZERO activity in the last 30 days (the most extreme drop-off) still
        # gets correctly identified as having enough history to be judged,
        # rather than silently defaulting to cold-start via a missing key.
        first_tracked_by_sub = {
            row["subscriber_id"]: row["first_event_at"]
            for row in db.execute(_FIRST_TRACKED_SQL).mappings().all()
        }

        population = [r[0] for r in db.execute(_POPULATION_SQL).all()]
        open_leads = {
            r[0] for r in db.execute(_OPEN_LEADS_SQL, {"cooldown": OUTREACH_COOLDOWN_DAYS}).all()
        }

        snapshots: list[dict] = []
        to_flag: list[tuple[int, float]] = []  # (subscriber_id, engagement_decay)

        for sid in population:
            results["scored"] += 1
            agg = dict(agg_by_sub.get(sid, {}))
            agg["first_event_at"] = first_tracked_by_sub.get(sid)
            engagement_decay, is_cold_start = _compute_decay(agg)
            scalar = 1.00 if is_cold_start else min(round(engagement_decay, 2), DECAY_SCALAR_MAX)

            snapshots.append({
                "subscriber_id": sid,
                "last_login_at": agg.get("last_login_at"),
                "views_7": int(agg.get("views_7") or 0),
                "downloads_7": int(agg.get("downloads_7") or 0),
                "auth_intervals_seconds": _auth_intervals_seconds(agg),
                "engagement_decay_scalar": scalar,
            })

            if is_cold_start:
                results["skipped_coldstart"] += 1
                continue

            if engagement_decay < DECAY_THRESHOLD and sid not in open_leads:
                results["flagged"] += 1
                to_flag.append((sid, engagement_decay))

        if dry_run:
            logger.info(
                "[ChurnDefense] DRY RUN scored=%d flagged=%d skipped_coldstart=%d "
                "(no writes, no pitch, no GHL)",
                results["scored"], results["flagged"], results["skipped_coldstart"],
            )
            return results

        # Snapshot upsert — one batched statement (executemany).
        if snapshots:
            db.execute(_UPSERT_SNAPSHOT_SQL, snapshots)

        # Stage + fire outreach for each flagged subscriber.
        for sid, engagement_decay in to_flag:
            risk_score = round(1 - engagement_decay, 3)
            lead = ChurnDefenseLead(
                subscriber_id=sid,
                risk_score=risk_score,
                outreach_status="STAGED",
            )
            db.add(lead)
            db.flush()  # assign lead.id before the outreach update

            if _fire_retention_outreach(db, lead):
                results["ghl_ok"] += 1
            else:
                # Mark FAILED (not left at STAGED) so _OPEN_LEADS_SQL does not
                # treat this as "already handled" and block a retry next week.
                lead.outreach_status = "FAILED"
                results["ghl_failed"] += 1

    logger.info(
        "[ChurnDefense] scored=%d flagged=%d skipped_coldstart=%d ghl_ok=%d ghl_failed=%d dry_run=%s",
        results["scored"], results["flagged"], results["skipped_coldstart"],
        results["ghl_ok"], results["ghl_failed"], dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
