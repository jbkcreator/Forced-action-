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
from src.core.models import EnrichedContact, EnrichmentAnomalyLog
from src.services.cora_slack import post_incident_alert
from src.services.email import send_alert

logger = logging.getLogger(__name__)


# ── A4: per-provider degraded-enrichment detection ──────────────────────────

def evaluate_provider_health(provider, rate, sample_size, floor, *, min_sample):
    """Pure decision: is this provider degraded?

    Below `min_sample` attempts the rate is not trustworthy → skipped (never
    degraded). Otherwise degraded when the hit rate falls strictly below the
    configured floor.
    """
    skipped = sample_size < min_sample
    degraded = (not skipped) and rate is not None and rate < floor
    return {
        "provider": provider,
        "hit_rate": rate,
        "sample_size": sample_size,
        "floor": floor,
        "degraded": degraded,
        "skipped": skipped,
    }


def recent_hit_rate(db, provider, hours):
    """(hit_rate, sample_count) over the last `hours` of enrichment_usage_logs
    for `provider`. rate is None when there are no rows in the window."""
    from sqlalchemy import text

    row = db.execute(
        text(
            """
            SELECT AVG(CASE WHEN success THEN 1.0 ELSE 0.0 END) AS rate,
                   COUNT(*) AS n
            FROM enrichment_usage_logs
            WHERE vendor = :vendor
              AND created_at >= :cutoff
            """
        ),
        {
            "vendor": provider,
            "cutoff": datetime.now(timezone.utc) - timedelta(hours=hours),
        },
    ).one()
    rate = float(row.rate) if row.rate is not None else None
    return rate, int(row.n)


def apply_degraded_discount(db, provider, hours, multiplier):
    """Haircut the quality rating of a degraded provider's batch.

    Multiplies owners.contact_info_confidence_score (the column CDS reads) by
    `multiplier` for every paid hit this provider produced inside the window,
    so a degraded batch can't poison scoring. Returns the number of owner rows
    discounted. Batch/window-scoped, not per-lead."""
    from sqlalchemy import text

    result = db.execute(
        text(
            """
            UPDATE owners
            SET contact_info_confidence_score = contact_info_confidence_score * :mult
            WHERE contact_info_confidence_score IS NOT NULL
              AND property_id IN (
                  SELECT eul.property_id
                  FROM enrichment_usage_logs eul
                  WHERE eul.vendor = :vendor
                    AND eul.success = TRUE
                    AND eul.property_id IS NOT NULL
                    AND eul.created_at >= :cutoff
              )
            """
        ),
        {
            "mult": multiplier,
            "vendor": provider,
            "cutoff": datetime.now(timezone.utc) - timedelta(hours=hours),
        },
    )
    return result.rowcount


def _alerted_recently(db, provider, cooldown_hours):
    """True if a degraded row for `provider` was written inside the cooldown —
    the table doubles as the re-alert guard (no state file)."""
    from sqlalchemy import text

    found = db.execute(
        text(
            """
            SELECT 1 FROM enrichment_anomaly_log
            WHERE provider = :provider AND detected_at >= :cutoff
            LIMIT 1
            """
        ),
        {
            "provider": provider,
            "cutoff": datetime.now(timezone.utc) - timedelta(hours=cooldown_hours),
        },
    ).first()
    return found is not None


def run_provider_health_check(
    db, *, floors=None, window_hours=None, min_sample=None, cooldown_hours=None,
    multiplier=None, discount_enabled=None,
):
    """Per-provider degraded-enrichment check (A4).

    For each provider, compute the recent hit rate and decide health. When a
    provider is degraded and not already alerted inside the cooldown: discount
    the batch's quality ratings, write an EnrichmentAnomalyLog row, and fire one
    ops alert (Slack + email fallback). Returns the per-provider health dicts.
    Never raises into the caller.
    """
    from types import SimpleNamespace

    from config.settings import get_settings

    settings = get_settings()
    floors = floors if floors is not None else settings.enrichment_provider_floors
    window_hours = window_hours or settings.enrichment_window_hours
    min_sample = min_sample or settings.enrichment_min_sample
    cooldown_hours = cooldown_hours or settings.enrichment_realert_cooldown_hours
    multiplier = multiplier if multiplier is not None else settings.enrichment_degraded_confidence_multiplier
    discount_enabled = (
        discount_enabled if discount_enabled is not None
        else settings.enrichment_degraded_discount_enabled
    )

    results = []
    for provider, floor in floors.items():
        rate, n = recent_hit_rate(db, provider, window_hours)
        health = evaluate_provider_health(provider, rate, n, floor, min_sample=min_sample)
        results.append(health)

        if not health["degraded"]:
            continue
        if _alerted_recently(db, provider, cooldown_hours):
            logger.info("[EnrichmentMonitor] %s degraded but within cooldown — not re-alerting", provider)
            continue

        affected = (
            apply_degraded_discount(db, provider, window_hours, multiplier)
            if discount_enabled else 0
        )
        row = EnrichmentAnomalyLog(
            provider=provider,
            observed_hit_rate=rate,
            floor_hit_rate=floor,
            sample_size=n,
            records_affected=affected,
        )
        db.add(row)
        db.flush()

        incident = SimpleNamespace(
            metric_name=f"enrichment_hit_rate:{provider}",
            severity="high",
            observed_value=round(rate, 4),
            threshold_value=floor,
            feature_name="enrichment",
        )
        try:
            sent = post_incident_alert(
                incident,
                kind="enrichment_degraded",
                action_summary=(
                    f"{provider} hit rate {rate:.1%} below floor {floor:.0%} "
                    f"over last {window_hours}h ({n} attempts). Check vendor API "
                    f"key/credits and recent enrichment_usage_logs errors."
                ),
            )
        except Exception:
            logger.warning("[EnrichmentMonitor] alert send failed for %s", provider, exc_info=True)
            sent = None
        row.alert_sent = bool(sent)
        db.flush()

    return results


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


def run_provider_health_check_job() -> list:
    """Cron wrapper: open a committing session and run the per-provider check.
    Feature-flagged; never raises (logs and returns [] on failure)."""
    from config.settings import get_settings

    if not get_settings().enrichment_anomaly_enabled:
        logger.info("[EnrichmentMonitor] disabled via settings — skipping")
        return []
    try:
        with get_db_context() as db:
            return run_provider_health_check(db)
    except Exception:
        logger.warning("[EnrichmentMonitor] provider health check failed", exc_info=True)
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_match_rate_monitor()
    print(result)
    health = run_provider_health_check_job()
    print({"provider_health": health})
