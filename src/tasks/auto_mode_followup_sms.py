"""
Auto Mode follow-up SMS (2nd/3rd touch) — Stage 5.

Separate from `auto_mode_followup.py` (the existing 30-min voicemail sweep) —
different idempotency key, different action, kept isolated so that working
sweep is never touched by this one.

For every `auto_mode_first_text` MessageOutcome with no reply since it was
sent, this job sends a generic second-touch SMS at day 2 (or day 3 for the
`followup_cadence_v1` A/B variant arm) and a third-touch at day 5 (day 6 for
variant), using the existing vertical-agnostic compliance pipe
(`sms_compliance.send_sms`) — zero changes to that file.

Reply detection is subscriber-level, not thread-level: the real inbound-SMS
webhook (`cora_suppression.record_generic_sms_reply`) stamps `replied_at` on
the newest unreplied `message_outcomes` row of `message_type='sms'` for a
subscriber, not necessarily the row this sweep is evaluating. So "no reply"
here means no sms MessageOutcome row for this subscriber, sent at/after
first-touch, has `replied_at` set — not just checking the first-touch row.

Idempotency and cadence are per-lead, not per-subscriber: a subscriber can have
several leads in flight (one Auto Mode invocation per delivered lead), each
with its own first-touch `message_outcomes` row. Every follow-up row carries
`context_snapshot['first_touch_id']` pointing back at its lead's first-touch
row, and "already sent"/phone-to-text-back are resolved off that, not off
`subscriber_id` + a time window — otherwise one lead's second-touch send would
read as "already done" for a sibling lead delivered to the same subscriber.
Third-touch additionally requires the second touch to already be on record
from an earlier run, so a sweep that runs after both thresholds have passed
(e.g. after scheduler downtime) never fires both in the same execution.

Run via `python -m src.tasks.auto_mode_followup_sms [--dry-run]`.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.core.models import MessageOutcome
from src.services.ab_engine import (
    FOLLOWUP_CADENCE_ARMS,
    FOLLOWUP_CADENCE_TEST_NAME,
    assign_rollout_arm,
    ensure_followup_cadence_test,
    record_outcome,
)
from src.services.auto_mode import _compose_second_text, _compose_third_text

logger = logging.getLogger(__name__)

_MIN_LOOKBACK_DAYS = 2   # earliest any arm's second touch can fire
_MAX_LOOKBACK_DAYS = 10  # staleness cap — covers variant's day-6 third touch + buffer


def run(dry_run: bool = False) -> dict:
    stats = {
        "checked": 0,
        "second_touch_sent": 0,
        "third_touch_sent": 0,
        "skipped_replied": 0,
        "skipped_no_phone": 0,
        "skipped_not_eligible_yet": 0,
        "skipped_already_done": 0,
        "errors": 0,
    }
    now = datetime.now(timezone.utc)

    with get_db_context() as db:
        ensure_followup_cadence_test(db)

        candidates = db.execute(
            sa_text(
                "SELECT id, subscriber_id, sent_at, context_snapshot FROM message_outcomes "
                "WHERE template_id = 'auto_mode_first_text' "
                "  AND sent_at <= :max_cutoff AND sent_at >= :min_cutoff "
                "ORDER BY subscriber_id, sent_at"
            ),
            {
                "max_cutoff": now - timedelta(days=_MIN_LOOKBACK_DAYS),
                "min_cutoff": now - timedelta(days=_MAX_LOOKBACK_DAYS),
            },
        ).mappings().all()

        for row in candidates:
            stats["checked"] += 1
            sub_id = row["subscriber_id"]
            first_touch_id = row["id"]
            first_touch_sent_at = row["sent_at"]
            phone = (row["context_snapshot"] or {}).get("phone")
            if first_touch_sent_at is not None and first_touch_sent_at.tzinfo is None:
                # message_outcomes.sent_at is a naive DateTime column (always
                # written as UTC) — raw SQL returns it naive; normalize before
                # comparing against a tz-aware `now`.
                first_touch_sent_at = first_touch_sent_at.replace(tzinfo=timezone.utc)
            if sub_id is None:
                continue

            try:
                if _has_replied_since(db, sub_id, first_touch_sent_at):
                    stats["skipped_replied"] += 1
                    if not dry_run:
                        record_outcome(sub_id, FOLLOWUP_CADENCE_TEST_NAME, "converted", db)
                    continue

                arm = assign_rollout_arm(sub_id, FOLLOWUP_CADENCE_TEST_NAME, db)
                second_days, third_days = FOLLOWUP_CADENCE_ARMS.get(arm, FOLLOWUP_CADENCE_ARMS["control"])

                age = now - first_touch_sent_at
                sent_any = False

                # Snapshot second-touch state BEFORE possibly sending it this run,
                # so a stale sweep (both thresholds already overdue) can never
                # send second and third touch for the same lead in one execution —
                # third only fires once second was recorded in an earlier run.
                had_second_touch = _touch_exists(db, first_touch_id, "auto_mode_second_text")

                if age >= timedelta(days=second_days) and not had_second_touch:
                    outcome = _send_touch_if_needed(
                        db, sub_id, first_touch_id, phone,
                        template_id="auto_mode_second_text",
                        body=_compose_second_text(),
                        dry_run=dry_run,
                        stats=stats,
                        sent_stat_key="second_touch_sent",
                    )
                    sent_any = sent_any or outcome
                elif had_second_touch:
                    stats["skipped_already_done"] += 1

                if age >= timedelta(days=third_days) and had_second_touch:
                    outcome = _send_touch_if_needed(
                        db, sub_id, first_touch_id, phone,
                        template_id="auto_mode_third_text",
                        body=_compose_third_text(),
                        dry_run=dry_run,
                        stats=stats,
                        sent_stat_key="third_touch_sent",
                    )
                    sent_any = sent_any or outcome

                if not sent_any and age < timedelta(days=second_days):
                    stats["skipped_not_eligible_yet"] += 1

            except Exception as exc:
                logger.error(
                    "[AutoModeFollowupSms] failed: subscriber=%s outcome=%s err=%s",
                    sub_id, row["id"], exc,
                )
                stats["errors"] += 1

    logger.info("[AutoModeFollowupSms] %s", stats)
    return stats


def _has_replied_since(db, subscriber_id: int, since) -> bool:
    row = db.execute(
        sa_text(
            "SELECT 1 FROM message_outcomes "
            "WHERE subscriber_id = :sid AND message_type = 'sms' "
            "  AND sent_at >= :since AND replied_at IS NOT NULL LIMIT 1"
        ),
        {"sid": subscriber_id, "since": since},
    ).first()
    return row is not None


def _touch_exists(db, first_touch_id: int, template_id: str) -> bool:
    """Has this specific lead's first-touch already gotten this follow-up?

    Scoped by first_touch_id (the parent first-touch outcome), not
    subscriber_id — a subscriber can have several leads in flight at once,
    each with its own first-touch row and its own follow-up cadence.
    """
    row = db.execute(
        sa_text(
            "SELECT 1 FROM message_outcomes "
            "WHERE template_id = :tid "
            "  AND (context_snapshot ->> 'first_touch_id')::bigint = :first_touch_id LIMIT 1"
        ),
        {"tid": template_id, "first_touch_id": first_touch_id},
    ).first()
    return row is not None


def _send_touch_if_needed(
    db, subscriber_id: int, first_touch_id: int, phone: str | None, *,
    template_id: str, body: str, dry_run: bool, stats: dict, sent_stat_key: str,
) -> bool:
    if _touch_exists(db, first_touch_id, template_id):
        stats["skipped_already_done"] += 1
        return False

    if not phone:
        stats["skipped_no_phone"] += 1
        logger.warning(
            "[AutoModeFollowupSms] no phone resolved for subscriber=%d template=%s",
            subscriber_id, template_id,
        )
        return False

    if dry_run:
        logger.info(
            "[AutoModeFollowupSms] DRY-RUN would send %s to subscriber=%d",
            template_id, subscriber_id,
        )
        return False

    outcome = MessageOutcome(
        subscriber_id=subscriber_id,
        message_type="sms",
        template_id=template_id,
        channel="telnyx",
        sent_at=datetime.now(timezone.utc),
        context_snapshot={"first_touch_id": first_touch_id, "phone": phone},
    )
    db.add(outcome)
    db.flush()

    from src.services.sms_compliance import send_sms
    sent = send_sms(
        to=phone,
        body=body,
        db=db,
        message_type="marketing",
        subscriber_id=subscriber_id,
        task_type="auto_mode_followup_sms",
        campaign=template_id,
    )
    if sent:
        outcome.delivered_at = datetime.now(timezone.utc)
        db.flush()
        stats[sent_stat_key] += 1
    return sent


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
