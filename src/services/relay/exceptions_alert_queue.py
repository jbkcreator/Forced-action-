"""
Durable retry queue for the FA Max EXCEPTIONS Slack lane (WP-T2-1 go-live
review, 2026-09).

Before this module existed, both fa_max_send_health_monitor.py and
relay/sweep.py's suppression-sync-failure path called
slack_post.post_exceptions_alert() directly and fire-and-forget: a Slack
outage, or a process crash between deciding to alert and the Slack call
completing, silently dropped the alert with nothing durable to retry.

enqueue_and_attempt() is the new entry point for both callers: it commits a
'pending' row to fa_max_exceptions_alert_queue BEFORE making the Slack call,
then attempts delivery immediately. drain_pending() is the retry worker body
(see src/tasks/fa_max_exceptions_alert_drain.py), run on a short cron cycle
to catch anything still 'pending' after a failed or interrupted attempt.

See FaMaxExceptionsAlertQueue's model docstring (src/core/models.py) for the
full design, including the documented (accepted, not solved) duplicate-post
risk on an ambiguous Slack result: a crash between Slack accepting the post
and this table recording 'sent' causes the next drain tick to re-post the
same content. That is judged an acceptable trade-off for an operational
alert (worst case: Josh sees the same warning twice) against the actual
problem being fixed (alert loss).

Concurrency (code-review finding, 2026-09): every row that's about to have
post_exceptions_alert() called on it is first atomically claimed via
_claim_row() — the same short-lived-lease pattern as relay's own
claim_slack_post()/release_slack_post() (queue.py). Without this,
enqueue_and_attempt()'s own immediate attempt and an overlapping
drain_pending() tick (or two overlapping drain ticks) could both select and
post the SAME 'pending' row, since a real network call sits between the row
being visible and it being finalized. This is a genuine race distinct from
the documented crash-after-Slack duplicate case above — that one is
accepted as unsolvable without history-reconciliation; this one is not, and
is fixed here.

Retry is intentionally unbounded — see testing-verification's failure/retry
guidance: a stale-source-style alert must not silently give up. `attempts`
is tracked for observability; past _ESCALATE_AFTER_ATTEMPTS a still-pending
row logs at ERROR instead of WARNING so a stuck queue doesn't stay quiet.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.core.database import get_db_context
from src.services.relay.slack_post import post_exceptions_alert

logger = logging.getLogger(__name__)

# Matches fa_max_send_health_monitor.ALERT_DEDUP_WINDOW_HOURS — same
# "don't page again for the same condition within N hours" intent, applied
# here to queuing rather than posting so a sustained outage (e.g. sweep.py
# retrying every 30 min while Instantly stays down for days) doesn't flood
# this table with duplicate pending rows for the same underlying condition.
DEDUP_WINDOW_HOURS = 12
_ESCALATE_AFTER_ATTEMPTS = 12  # ~1h of retries at the */5min drain cadence
_CLAIM_LEASE_MINUTES = 2  # comfortably longer than a single Slack call, well under the */5min drain cadence


def _recently_queued(session, *, venture_key: str, rule: str) -> bool:
    """True if a pending row, or one sent within DEDUP_WINDOW_HOURS, already
    covers this (venture, rule)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=DEDUP_WINDOW_HOURS)
    existing = session.execute(
        text(
            "SELECT id FROM fa_max_exceptions_alert_queue "
            "WHERE venture_key = :venture AND rule = :rule "
            "AND (status = 'pending' OR (status = 'sent' AND sent_at >= :cutoff)) "
            "LIMIT 1"
        ),
        {"venture": venture_key, "rule": rule, "cutoff": cutoff},
    ).first()
    return existing is not None


def _claim_row(row_id: int) -> bool:
    """Atomically lease one 'pending' row so only one caller ever posts it
    at a time -- mirrors queue.py's claim_slack_post() exactly. A crashed
    claimant's lease simply expires after _CLAIM_LEASE_MINUTES, so the row
    becomes claimable again rather than getting stuck."""
    with get_db_context() as session:
        claimed = session.execute(
            text(
                "UPDATE fa_max_exceptions_alert_queue SET "
                "claimed_until = now() + make_interval(mins => :lease_minutes) "
                "WHERE id = :id AND status = 'pending' "
                "AND (claimed_until IS NULL OR claimed_until < now()) "
                "RETURNING id"
            ),
            {"id": row_id, "lease_minutes": _CLAIM_LEASE_MINUTES},
        ).scalar_one_or_none()
        session.commit()
    return claimed is not None


def _attempt(row_id: int, *, venture_key: str, rule: str, message: str, attempts_so_far: int) -> bool:
    """Caller must already hold this row's claim (see _claim_row) — this
    function does not claim it itself, so enqueue_and_attempt() and
    drain_pending() share one attempt implementation without re-deciding
    who's allowed to call it."""
    delivered = post_exceptions_alert(venture_key=venture_key, rule=rule, message=message)
    with get_db_context() as session:
        if delivered:
            session.execute(
                text(
                    "UPDATE fa_max_exceptions_alert_queue "
                    "SET status = 'sent', sent_at = now(), last_attempt_at = now(), "
                    "attempts = attempts + 1, error = NULL, claimed_until = NULL "
                    "WHERE id = :id AND status = 'pending'"
                ),
                {"id": row_id},
            )
        else:
            session.execute(
                text(
                    "UPDATE fa_max_exceptions_alert_queue "
                    "SET last_attempt_at = now(), attempts = attempts + 1, "
                    "error = 'Slack post failed or not configured', claimed_until = NULL "
                    "WHERE id = :id AND status = 'pending'"
                ),
                {"id": row_id},
            )
        session.commit()

    next_attempts = attempts_so_far + 1
    if delivered:
        logger.info("[Relay][EXCEPTIONS] alert id=%d rule=%s delivered", row_id, rule)
    elif next_attempts >= _ESCALATE_AFTER_ATTEMPTS:
        logger.error(
            "[Relay][EXCEPTIONS] alert id=%d rule=%s still undelivered after %d attempts",
            row_id, rule, next_attempts,
        )
    else:
        logger.warning(
            "[Relay][EXCEPTIONS] alert id=%d rule=%s undelivered (attempt %d)",
            row_id, rule, next_attempts,
        )
    return delivered


def enqueue_and_attempt(*, venture_key: str, rule: str, message: str) -> bool:
    """Durably record an EXCEPTIONS alert, then make one immediate delivery
    attempt. Never raises — an alerting failure must not fail the caller
    (a monitor tick or a sweep) that triggered it. Returns True if this
    immediate attempt delivered it, False otherwise (including the dedup
    skip case, where nothing new was queued or attempted).

    Concurrency (code-review finding, third round, 2026-09): _recently_
    queued()'s SELECT-then-INSERT is a check-then-act race on its own --
    two concurrent callers can both see "nothing pending" before either
    commits, and both insert. Reproduced directly with a widened race
    window. The real fix is the DB-level partial unique index on
    (venture_key, rule) WHERE status='pending' (see the migration and
    FaMaxExceptionsAlertQueue's __table_args__) -- the SELECT here remains
    as a fast, cheap pre-check to avoid the round trip in the common case,
    but the actual guarantee is the constraint: a losing concurrent INSERT
    raises IntegrityError, caught below and treated exactly like the
    SELECT-based skip (another caller already has this covered).

    Residual risk, documented rather than further chased (code-review
    finding, fourth round, 2026-09): the partial unique index only
    constrains concurrent 'pending' rows for the same (venture_key, rule).
    If caller A's full round trip (insert -> claim -> post -> mark 'sent')
    completes fast enough to finish BEFORE caller B's own _recently_queued()
    SELECT even runs, B's SELECT correctly sees A's row as "sent within
    DEDUP_WINDOW_HOURS" and skips. But if B's SELECT happens to run in the
    narrow window BEFORE A's row exists at all (a race on the SELECT
    itself, not on two INSERTs), B has no way to see what hasn't been
    written yet, and after the unique index no longer blocks B (A's row is
    already 'sent', not 'pending'), B legitimately creates a second alert
    for the same underlying condition. This requires two producers'
    real-world triggers (a sweep tick, a monitor run) to land within
    single-digit milliseconds of each other -- given this repo's actual
    cadences (30-min sweep, daily monitor, 5-min drain), the honest
    assessment is this window is real but not worth chasing further with
    additional locking machinery: the cost (occasional duplicate
    operational alert -- Josh sees the same warning twice) is the same
    already-accepted trade-off as the ambiguous-Slack-result duplicate
    documented on FaMaxExceptionsAlertQueue itself, not a new category of
    harm.
    """
    try:
        with get_db_context() as session:
            if _recently_queued(session, venture_key=venture_key, rule=rule):
                logger.info(
                    "[Relay][EXCEPTIONS] %s (venture=%s) already queued/sent within %dh — skipping",
                    rule, venture_key, DEDUP_WINDOW_HOURS,
                )
                return False
            try:
                row_id = session.execute(
                    text(
                        "INSERT INTO fa_max_exceptions_alert_queue "
                        "(venture_key, rule, message, status) "
                        "VALUES (:venture, :rule, :message, 'pending') RETURNING id"
                    ),
                    {"venture": venture_key, "rule": rule, "message": message},
                ).scalar_one()
                session.commit()
            except IntegrityError:
                session.rollback()
                logger.info(
                    "[Relay][EXCEPTIONS] %s (venture=%s) already queued by a concurrent "
                    "caller (unique constraint) — skipping",
                    rule, venture_key,
                )
                return False

        if not _claim_row(row_id):
            # Lost the claim to an overlapping drain_pending() tick that
            # somehow already picked this brand-new row up between our
            # commit and this claim attempt -- vanishingly unlikely (the
            # drain worker only just started existing when this row did),
            # but correct to skip rather than attempt a row we don't hold.
            logger.info(
                "[Relay][EXCEPTIONS] alert id=%d rule=%s claimed by a concurrent "
                "worker before this immediate attempt — leaving it to them",
                row_id, rule,
            )
            return False

        return _attempt(row_id, venture_key=venture_key, rule=rule, message=message, attempts_so_far=0)
    except Exception:
        logger.exception(
            "[Relay][EXCEPTIONS] enqueue_and_attempt failed for rule=%s (venture=%s) — "
            "alert may not be durably recorded",
            rule, venture_key,
        )
        return False


def drain_pending(*, limit: int = 50) -> int:
    """Retry every still-'pending', unclaimed alert, oldest first. Called by
    the standalone drain task on a short cron cycle. Returns the number of
    rows this run actually claimed and attempted (a row visible in the
    initial SELECT but claimed by a concurrent worker before this run's own
    claim attempt is correctly NOT counted -- it's being handled, just not
    by this call)."""
    with get_db_context() as session:
        rows = session.execute(
            text(
                "SELECT id, venture_key, rule, message, attempts "
                "FROM fa_max_exceptions_alert_queue "
                "WHERE status = 'pending' "
                "AND (claimed_until IS NULL OR claimed_until < now()) "
                "ORDER BY created_at ASC LIMIT :limit"
            ),
            {"limit": limit},
        ).mappings().all()

    attempted = 0
    for row in rows:
        if not _claim_row(row["id"]):
            continue  # another worker (or this same run's own overlap) already has it
        attempted += 1
        _attempt(
            row["id"], venture_key=row["venture_key"], rule=row["rule"],
            message=row["message"], attempts_so_far=row["attempts"],
        )
    return attempted
