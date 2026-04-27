"""
Load validation and anomaly alerting — M6.

Checks daily scraper run counts against a rolling 7-day baseline.
Fires an ops alert if any scraper's count drops more than 70% below baseline
for 2 consecutive days (avoids noisy single-day false positives).

Zero-record runs (total_scraped == 0 on a successful run) fire an immediate
single-day alert — no consecutive-day grace period — because a scraper that
ran without errors but produced nothing is always actionable.

Also validates:
  - scraper run success flags (failed runs)
  - enrichment pipeline match rate (delegates to match_rate_monitor)

Run daily via cron after scrapers + enrichment complete:
  0 6 * * * cd /path/to/app && python -m src.tasks.load_validator

State file (consecutive low-day tracking):
  data/load_validator_state.json
"""

import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

from src.core.database import get_db_context
from src.core.models import ScraperAlertLog, ScraperRunStats
from src.services.email import send_alert

logger = logging.getLogger(__name__)

# A scraper whose count drops below this fraction of its 7d average triggers an alert
_DROP_THRESHOLD = 0.30      # count must be >= 30% of baseline to pass
_BASELINE_DAYS = 7          # rolling window for baseline
_CONSECUTIVE_DAYS_ALERT = 2 # alert after N consecutive low days

# Minimum baseline average before we bother checking (ignore brand-new scrapers)
_MIN_BASELINE_AVG = 3

_STATE_FILE = Path(__file__).parent.parent.parent / "data" / "load_validator_state.json"

# Scrapers that MUST produce a stats row on weekdays. Alert immediately if absent.
# These are keyed to scraper_run_stats.source_type values.
REQUIRED_DAILY_SCRAPERS: list = [
    'violations',
    'bankruptcy',
    'evictions',
    'probate',
    'foreclosures',
    'permits',
    'lien_tcl',
    'lien_ccl',
    'lien_hoa',
    'lien_ml',
]
# source_types required only on specific weekday (0=Monday … 6=Sunday)
REQUIRED_WEEKLY_SCRAPERS: dict = {
    0: ['tax_delinquencies'],  # Monday only
}


def _was_recently_alerted(source_type: str, county_id: str, alert_type: str) -> bool:
    """Return True if an alert of this type was already sent within ALERT_COOLDOWN_HOURS."""
    from config.settings import get_settings
    cooldown = timedelta(hours=get_settings().alert_cooldown_hours)
    cutoff = datetime.now(timezone.utc) - cooldown
    try:
        with get_db_context() as session:
            row = (
                session.query(ScraperAlertLog)
                .filter(
                    ScraperAlertLog.source_type == source_type,
                    ScraperAlertLog.county_id == county_id,
                    ScraperAlertLog.alert_type == alert_type,
                    ScraperAlertLog.alerted_at >= cutoff,
                )
                .first()
            )
            return row is not None
    except Exception as exc:
        logger.warning("[LoadValidator] Cooldown check failed — not suppressing: %s", exc)
        return False


def _record_alert_sent(source_type: str, county_id: str, alert_type: str) -> None:
    """Write a row to scraper_alert_log after successfully sending an alert."""
    try:
        with get_db_context() as session:
            session.add(ScraperAlertLog(
                source_type=source_type,
                county_id=county_id,
                alert_type=alert_type,
                alerted_at=datetime.now(timezone.utc),
            ))
            session.commit()
    except Exception as exc:
        logger.warning("[LoadValidator] Could not record alert log: %s", exc)


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception:
        pass
    return {"consecutive_low": {}, "last_check": None}


def _save_state(state: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state))
    except Exception as exc:
        logger.warning("Could not save load validator state: %s", exc)


def _get_recent_stats(session, county_id: str, days: int) -> Dict[str, list]:
    """
    Returns {source_type: [matched_count, ...]} for the last `days` days.
    Only includes successful runs.
    """
    cutoff = date.today() - timedelta(days=days)
    rows = (
        session.query(ScraperRunStats)
        .filter(
            ScraperRunStats.run_date >= cutoff,
            ScraperRunStats.county_id == county_id,
            ScraperRunStats.run_success == True,    # noqa: E712
        )
        .order_by(ScraperRunStats.run_date.asc())
        .all()
    )

    by_type: Dict[str, list] = defaultdict(list)
    for row in rows:
        by_type[row.source_type].append(row.total_scraped)
    return dict(by_type)


def _get_failed_runs(session, county_id: str, run_date: date) -> list:
    """
    Return list of (source_type, error_message) for real scraper failures today.

    Excludes 'no_data' rows — those are legitimate empty-filing days, not errors.
    NULL error_type rows (pre-migration) are treated as 'scraper_error' for safety.
    """
    rows = (
        session.query(ScraperRunStats)
        .filter(
            ScraperRunStats.run_date == run_date,
            ScraperRunStats.county_id == county_id,
            ScraperRunStats.run_success == False,   # noqa: E712
        )
        .all()
    )
    return [
        (r.source_type, r.error_message or "unknown error")
        for r in rows
        if r.error_type != 'no_data'  # NULL treated as scraper_error (backward compat)
    ]


def _get_missing_scrapers(session, county_id: str, today: date) -> list:
    """
    Return source_types from REQUIRED_DAILY_SCRAPERS (and today's REQUIRED_WEEKLY_SCRAPERS)
    that have no stats row for today. Skips entirely on weekends for daily scrapers.
    """
    weekday = today.weekday()  # 0=Mon … 6=Sun
    is_weekend = weekday >= 5

    required = set()
    if not is_weekend:
        required.update(REQUIRED_DAILY_SCRAPERS)
    weekly = REQUIRED_WEEKLY_SCRAPERS.get(weekday, [])
    required.update(weekly)

    if not required:
        return []

    ran_today = {
        row[0]
        for row in session.query(ScraperRunStats.source_type).filter(
            ScraperRunStats.run_date == today,
            ScraperRunStats.county_id == county_id,
        ).all()
    }
    return sorted(required - ran_today)


def run_load_validator(county_id: str = "hillsborough") -> dict:
    """
    Validate today's scraper loads against 7-day rolling baseline.

    Returns:
        dict with keys: anomalies (list), failed_runs (list), alerts_sent (int)
    """
    today = date.today()
    results = {"anomalies": [], "zero_record_scrapers": [], "failed_runs": [], "missing_scrapers": [], "alerts_sent": 0, "checked_date": str(today)}

    state = _load_state()

    with get_db_context() as session:
        # ── Check for failed runs ──────────────────────────────────────────
        failed = _get_failed_runs(session, county_id, today)
        results["failed_runs"] = failed

        if failed:
            logger.warning("[LoadValidator] %d scrapers reported failure today: %s",
                           len(failed), [f[0] for f in failed])

        # ── Check for missing required scrapers ────────────────────────────
        # Alert if a required scraper produced no stats row at all today
        # (distinct from zero-record or failure — it simply never ran).
        missing = _get_missing_scrapers(session, county_id, today)
        results["missing_scrapers"] = missing

        if missing:
            logger.warning("[LoadValidator] Required scrapers did not run today: %s", missing)
            if not _was_recently_alerted('_batch', county_id, 'missing_scraper'):
                sent = send_alert(
                    subject=f"[Forced Action] ALERT: {len(missing)} required scraper(s) did not run ({today})",
                    body=(
                        f"The following scrapers are required to run today but produced no stats row "
                        f"in scraper_run_stats ({today}):\n\n"
                        + "\n".join(f"  - {s}" for s in missing)
                        + "\n\nPossible causes:\n"
                        "  - Cron job did not fire (check crontab and cron logs)\n"
                        "  - Scraper exited before recording stats (check scraper logs)\n"
                        "  - Server was down or out of memory during the scheduled window\n"
                        f"\nForced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    ),
                )
                if sent:
                    results["alerts_sent"] = results.get("alerts_sent", 0) + 1
                    _record_alert_sent('_batch', county_id, 'missing_scraper')
            else:
                logger.info("[LoadValidator] Missing-scraper alert suppressed — already sent within cooldown")

        # ── Check for count anomalies ──────────────────────────────────────
        # Get baseline: stats from last 7 days (excludes today)
        baseline_data = _get_recent_stats(session, county_id, days=_BASELINE_DAYS + 1)

        # Get today's stats
        today_rows = (
            session.query(ScraperRunStats)
            .filter(
                ScraperRunStats.run_date == today,
                ScraperRunStats.county_id == county_id,
                ScraperRunStats.run_success == True,    # noqa: E712
            )
            .all()
        )
        today_data = {r.source_type: r.total_scraped for r in today_rows}

    # Compute baseline averages (excluding today)
    baseline_avgs: Dict[str, float] = {}
    for source_type, counts in baseline_data.items():
        if counts:
            # Exclude today from baseline if present
            baseline_avgs[source_type] = sum(counts[:-1]) / max(len(counts) - 1, 1)

    # ── Zero-record immediate alert ───────────────────────────────────────
    # A successful run that returned 0 records is always suspicious — alert
    # immediately (no 2-day wait) when there is enough baseline to expect data.
    zero_record_lines = []
    for source_type, today_count in today_data.items():
        baseline_avg = baseline_avgs.get(source_type)
        if today_count == 0 and baseline_avg is not None and baseline_avg >= _MIN_BASELINE_AVG:
            zero_record_lines.append(
                f"  {source_type}: 0 records today (7d_avg={baseline_avg:.1f})"
            )
            logger.warning(
                "[LoadValidator] %s: ZERO records today (7d_avg=%.1f) — immediate alert",
                source_type, baseline_avg,
            )

    if zero_record_lines:
        if _was_recently_alerted('_batch', county_id, 'zero_records'):
            logger.info("[LoadValidator] Zero-record alert suppressed — already sent within cooldown")
        else:
            sent = send_alert(
                subject=f"[Forced Action] ALERT: {len(zero_record_lines)} scraper(s) returned ZERO records ({today})",
                body=(
                    f"The following scraper(s) ran successfully but loaded 0 records today ({today}):\n\n"
                    + "\n".join(zero_record_lines)
                    + "\n\nPossible causes:\n"
                    "  - Source website changed structure or returned empty results\n"
                    "  - Scraper ran before new data was published (timing issue)\n"
                    "  - Authentication/session expired silently\n"
                    "\nCheck individual scraper logs in logs/cron/ and re-run failing modules.\n"
                    f"\nForced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                ),
            )
            if sent:
                results["alerts_sent"] += 1
                _record_alert_sent('_batch', county_id, 'zero_records')

    results["zero_record_scrapers"] = [
        line.strip().split(":")[0] for line in zero_record_lines
    ]

    # ── Check for count anomalies (< 30% of 7-day baseline) ──────────────
    # Skip scrapers already caught by the zero-record check above.
    anomalies = []
    for source_type, today_count in today_data.items():
        if today_count == 0:
            continue  # handled by zero-record check above
        baseline_avg = baseline_avgs.get(source_type)
        if baseline_avg is None or baseline_avg < _MIN_BASELINE_AVG:
            continue   # not enough history or too sparse to baseline

        ratio = today_count / baseline_avg if baseline_avg > 0 else 1.0
        if ratio < _DROP_THRESHOLD:
            anomalies.append({
                "source_type": source_type,
                "today_count": today_count,
                "baseline_avg": round(baseline_avg, 1),
                "ratio": round(ratio, 3),
            })
            logger.warning(
                "[LoadValidator] %s: today=%d vs 7d_avg=%.1f (%.0f%% of baseline)",
                source_type, today_count, baseline_avg, ratio * 100,
            )

    results["anomalies"] = anomalies

    # ── Update consecutive low-day counters ───────────────────────────────
    if "consecutive_low" not in state:
        state["consecutive_low"] = {}

    anomaly_types = {a["source_type"] for a in anomalies}

    # Reset counter for scrapers that are back to normal
    for source_type in list(state["consecutive_low"].keys()):
        if source_type not in anomaly_types:
            state["consecutive_low"].pop(source_type, None)

    # Increment counter for anomalous scrapers
    alert_lines = []
    for a in anomalies:
        st = a["source_type"]
        state["consecutive_low"][st] = state["consecutive_low"].get(st, 0) + 1
        if state["consecutive_low"][st] >= _CONSECUTIVE_DAYS_ALERT:
            alert_lines.append(
                f"  {st}: today={a['today_count']} vs 7d_avg={a['baseline_avg']} "
                f"({a['ratio']*100:.0f}% — {state['consecutive_low'][st]} consecutive low days)"
            )

    # ── Send anomaly alert ─────────────────────────────────────────────────
    if alert_lines:
        if _was_recently_alerted('_batch', county_id, 'low_count'):
            logger.info("[LoadValidator] Low-count alert suppressed — already sent within cooldown")
        else:
            body_parts = [
                f"Scraper load anomaly detected for {len(alert_lines)} source(s) "
                f"({_CONSECUTIVE_DAYS_ALERT}+ consecutive low days):\n",
                "\n".join(alert_lines),
                "\n\nPossible causes:",
                "  - Source website/API changed (check scraper logs)",
                "  - Upstream data temporarily unavailable",
                "  - DB matching regression (check unmatched counts)",
                "\nCheck individual scraper logs in logs/cron/ and re-run failing modules.",
                f"\nForced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            ]
            sent = send_alert(
                subject=f"[Forced Action] ALERT: {len(alert_lines)} scraper(s) low count ({today})",
                body="\n".join(body_parts),
            )
            if sent:
                results["alerts_sent"] += 1
                _record_alert_sent('_batch', county_id, 'low_count')

    # ── Send failed-run alert ──────────────────────────────────────────────
    # Only real errors (scraper_error) reach here — no_data rows are filtered
    # out by _get_failed_runs(). Suppressed if already alerted within cooldown.
    if failed:
        if _was_recently_alerted('_batch', county_id, 'scraper_error'):
            logger.info("[LoadValidator] Failed-run alert suppressed — already sent within cooldown")
        else:
            failed_lines = [f"  {ft}: {fm}" for ft, fm in failed]
            sent = send_alert(
                subject=f"[Forced Action] ALERT: {len(failed)} scraper(s) FAILED ({today})",
                body=(
                    f"The following scrapers failed today ({today}):\n\n"
                    + "\n".join(failed_lines)
                    + "\n\nCheck logs and re-run manually if needed."
                    f"\n\nForced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                ),
            )
            if sent:
                results["alerts_sent"] += 1
                _record_alert_sent('_batch', county_id, 'scraper_error')

    state["last_check"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)

    logger.info(
        "[LoadValidator] Done. anomalies=%d failed_runs=%d missing=%d alerts_sent=%d",
        len(anomalies), len(failed), len(results["missing_scrapers"]), results["alerts_sent"],
    )
    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    county = sys.argv[1] if len(sys.argv) > 1 else "hillsborough"
    result = run_load_validator(county)
    print(result)
