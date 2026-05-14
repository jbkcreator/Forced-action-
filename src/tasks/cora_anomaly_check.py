"""
Cora anomaly check — call volume + no-answer rate monitor.

Runs every 15 minutes via cron. Queries message_outcomes for Synthflow voice
calls in the last 60 minutes and pages the ops phone number via Twilio SMS if:
  - total calls exceed CORA_VOLUME_THRESHOLD (default 40)
  - no-answer/voicemail rate exceeds CORA_NO_ANSWER_THRESHOLD (default 0.85)
    (only fires when >= 5 calls observed, to avoid noise on quiet periods)

Alert target: ALERT_SMS_NUMBER if set, otherwise FOUNDER_PHONE.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone

from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_VOLUME_THRESHOLD_DEFAULT = 40
_NO_ANSWER_THRESHOLD_DEFAULT = 0.85
_MIN_CALLS_FOR_RATE_ALERT = 5


def run_anomaly_check(dry_run: bool = False) -> dict:
    from src.core.database import get_db_context
    from src.core.models import MessageOutcome
    from sqlalchemy import select, func, and_
    from config.settings import get_settings

    settings = get_settings()
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=60)

    volume_threshold = _VOLUME_THRESHOLD_DEFAULT
    no_answer_threshold = _NO_ANSWER_THRESHOLD_DEFAULT

    with get_db_context() as session:
        base_filter = and_(
            MessageOutcome.message_type == "voice",
            MessageOutcome.channel == "synthflow",
            MessageOutcome.sent_at >= window_start,
        )
        total = session.execute(
            select(func.count()).select_from(MessageOutcome).where(base_filter)
        ).scalar() or 0

        no_answer = session.execute(
            select(func.count()).select_from(MessageOutcome).where(
                and_(
                    base_filter,
                    MessageOutcome.template_id.in_(["no_answer", "voicemail"]),
                )
            )
        ).scalar() or 0

    no_answer_rate = no_answer / total if total > 0 else 0.0

    anomalies: list[str] = []
    if total > volume_threshold:
        anomalies.append(
            f"volume spike: {total} calls in 60 min (threshold: {volume_threshold})"
        )
    if total >= _MIN_CALLS_FOR_RATE_ALERT and no_answer_rate > no_answer_threshold:
        anomalies.append(
            f"high no-answer rate: {no_answer_rate:.0%} "
            f"({no_answer}/{total}, threshold: {no_answer_threshold:.0%})"
        )

    logger.info(
        "[CoraMonitor] calls_60m=%d no_answer=%d rate=%.0f%% anomalies=%d",
        total, no_answer, no_answer_rate * 100, len(anomalies),
    )

    if anomalies:
        if dry_run:
            logger.info("[CoraMonitor] DRY RUN — would alert: %s", anomalies)
        else:
            _send_alert(settings, anomalies, total, no_answer)

    return {
        "total_calls_60m": total,
        "no_answer": no_answer,
        "no_answer_rate": round(no_answer_rate, 4),
        "anomalies": anomalies,
    }


def _send_alert(settings, anomalies: list[str], total: int, no_answer: int) -> None:
    phone = settings.alert_sms_number or settings.founder_phone
    if not phone:
        logger.warning("[CoraMonitor] no alert phone configured (ALERT_SMS_NUMBER / FOUNDER_PHONE) — SMS skipped")
        return
    lines = ["[FA] Cora anomaly alert"]
    for a in anomalies:
        lines.append(f"• {a}")
    lines.append(f"Calls(60m): {total}  No-answer: {no_answer}")
    message = "\n".join(lines)

    # Route through the compliance gate so ops alerts honour opt-out + TCPA
    # quiet hours just like every other outbound SMS. Previously bypassed
    # by instantiating a Twilio client inline.
    from src.core.database import get_db_context
    from src.services.sms_compliance import send_sms
    try:
        with get_db_context() as db:
            sent = send_sms(
                to=phone,
                body=message[:320],
                db=db,
                message_type="transactional",
                task_type="cora_anomaly",
                campaign="cora_anomaly_alert",
            )
        if sent:
            logger.info("[CoraMonitor] alert SMS sent to %s", phone)
        else:
            logger.info("[CoraMonitor] alert SMS suppressed by compliance gate")
    except Exception as exc:
        logger.error("[CoraMonitor] alert SMS failed: %s", exc)


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    result = run_anomaly_check(dry_run=dry)
    print(
        f"Calls (60m): {result['total_calls_60m']}  "
        f"No-answer: {result['no_answer']}  "
        f"Rate: {result['no_answer_rate']:.0%}  "
        f"Anomalies: {result['anomalies'] or 'none'}"
    )
