"""
Enrichment match rate monitor.

Checks the skip-trace match rate over the past 48 hours and sends an ops
alert (email + optional SMS) if the rate drops below 65% for two consecutive
daily checks.

Run daily via cron:
    0 9 * * * $PROJECT/scripts/cron/run.sh src.tasks.match_rate_monitor

Settings required (via AppSettings / .env):
    ALERT_EMAIL      — ops recipient for low-match alerts
    ALERT_SMS_NUMBER — phone number for SMS via email-to-SMS gateway (optional)
    ALERT_SMS_CARRIER — carrier gateway domain, e.g. tmomail.net (optional)
    SMTP_HOST / SMTP_USER / SMTP_PASS — must be set for alerts to send
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.core.database import get_db_context
from src.core.models import EnrichedContact
from src.services.email import send_alert

logger = logging.getLogger(__name__)

# Below this rate → alert fires
MATCH_RATE_THRESHOLD = 0.65

# Minimum enriched records in window before we consider the rate meaningful
MIN_SAMPLE_SIZE = 10

# Track consecutive low-rate days in a state file (simple, no extra DB table)
_STATE_FILE = Path(__file__).parent.parent.parent / "data" / "match_rate_state.json"


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception:
        pass
    return {"consecutive_low_days": 0, "last_check": None}


def _save_state(state: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state))
    except Exception as exc:
        logger.warning("Could not save match rate state: %s", exc)


def run_match_rate_monitor() -> dict:
    """
    Check 48-hour enrichment match rate and alert if below threshold.

    Returns:
        dict with keys: total, matched, rate, alerted
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

    with get_db_context() as db:
        total = db.query(EnrichedContact).filter(
            EnrichedContact.enriched_at >= cutoff
        ).count()

        matched = db.query(EnrichedContact).filter(
            EnrichedContact.enriched_at >= cutoff,
            EnrichedContact.match_success == True,  # noqa: E712
        ).count()

    if total < MIN_SAMPLE_SIZE:
        logger.info(
            "[MatchRateMonitor] Only %d enriched records in last 48h — skipping (min: %d)",
            total, MIN_SAMPLE_SIZE,
        )
        return {"total": total, "matched": matched, "rate": None, "alerted": False}

    rate = matched / total
    logger.info(
        "[MatchRateMonitor] 48h match rate: %.1f%% (%d/%d)",
        rate * 100, matched, total,
    )

    state = _load_state()
    alerted = False

    if rate < MATCH_RATE_THRESHOLD:
        state["consecutive_low_days"] += 1
        logger.warning(
            "[MatchRateMonitor] Match rate %.1f%% below %.0f%% threshold "
            "(consecutive low days: %d)",
            rate * 100, MATCH_RATE_THRESHOLD * 100, state["consecutive_low_days"],
        )

        if state["consecutive_low_days"] >= 2:
            subject = (
                f"[Forced Action] ALERT: Enrichment match rate {rate*100:.1f}% "
                f"(threshold {MATCH_RATE_THRESHOLD*100:.0f}%)"
            )
            body = (
                f"The BatchSkipTracing match rate has been below "
                f"{MATCH_RATE_THRESHOLD*100:.0f}% for {state['consecutive_low_days']} "
                f"consecutive days.\n\n"
                f"Current rate (48h window): {rate*100:.1f}%\n"
                f"Records checked: {total}\n"
                f"Matched: {matched}\n\n"
                f"Action required:\n"
                f"  1. Check BATCH_SKIP_TRACING_API_KEY in .env (may be expired/out of credits)\n"
                f"  2. Review recent EnrichedContact rows for error patterns\n"
                f"  3. Run: python -m src.services.skip_trace --dry-run --limit 5\n\n"
                f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
            )
            alerted = send_alert(subject=subject, body=body)
    else:
        # Rate is healthy — reset counter
        state["consecutive_low_days"] = 0

    state["last_check"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)

    return {"total": total, "matched": matched, "rate": rate, "alerted": alerted}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_match_rate_monitor()
    print(result)
