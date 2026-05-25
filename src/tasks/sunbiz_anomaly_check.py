"""
Sunbiz LLC piercing observability — daily anomaly check (fa031).

Metrics captured:
  - owners.sunbiz_status distribution (all statuses)
  - Enriched owners in the last 24h
  - Multi-property LLC group count (same doc_number on > 1 owner row)
  - parser_failed_rate in the last 24h

Alert threshold: parser_failed_rate > 5% when >= 10 outcomes observed.

Run daily at 07:50 UTC (after sunbiz_enrichment at 07:40) via cron.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_PARSER_FAILED_THRESHOLD = 0.05
_MIN_OUTCOMES_FOR_ALERT = 10


def _collect_metrics(session: Session) -> dict:
    from src.core.models import Owner

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=24)

    status_rows = session.execute(
        select(Owner.sunbiz_status, func.count().label("n"))
        .group_by(Owner.sunbiz_status)
    ).all()
    status_dist = {r.sunbiz_status: r.n for r in status_rows}
    total_owners = sum(status_dist.values())

    recent_rows = session.execute(
        select(Owner.sunbiz_status, func.count().label("n"))
        .where(Owner.sunbiz_enriched_at >= window_start)
        .group_by(Owner.sunbiz_status)
    ).all()
    recent_dist = {r.sunbiz_status: r.n for r in recent_rows}
    enriched_24h = recent_dist.get("matched", 0)
    failed_24h = recent_dist.get("parser_failed", 0)
    total_24h = sum(recent_dist.values())

    multi_prop_count = session.execute(
        text(
            "SELECT COUNT(*) FROM ("
            "  SELECT sunbiz_doc_number"
            "  FROM owners"
            "  WHERE sunbiz_doc_number IS NOT NULL"
            "  GROUP BY sunbiz_doc_number"
            "  HAVING COUNT(*) > 1"
            ") sub"
        )
    ).scalar() or 0

    return {
        "status_dist": status_dist,
        "total_owners": total_owners,
        "enriched_24h": enriched_24h,
        "failed_24h": failed_24h,
        "total_24h": total_24h,
        "multi_prop_llcs": multi_prop_count,
    }


def run_sunbiz_anomaly_check(dry_run: bool = False, session: Optional[Session] = None) -> dict:
    from config.settings import get_settings
    from src.core.database import get_db_context

    settings = get_settings()

    if session is not None:
        metrics = _collect_metrics(session)
    else:
        with get_db_context() as db:
            metrics = _collect_metrics(db)

    enriched_24h = metrics["enriched_24h"]
    failed_24h = metrics["failed_24h"]
    total_24h = metrics["total_24h"]
    multi_prop_count = metrics["multi_prop_llcs"]
    status_dist = metrics["status_dist"]

    parser_failed_rate = failed_24h / total_24h if total_24h > 0 else 0.0

    logger.info(
        "[SunbizMonitor] status_dist=%s enriched_24h=%d failed_24h=%d "
        "total_24h=%d parser_failed_rate=%.1f%% multi_prop_llcs=%d total_owners=%d",
        status_dist, enriched_24h, failed_24h,
        total_24h, parser_failed_rate * 100, multi_prop_count, metrics["total_owners"],
    )

    anomalies: list[str] = []
    if total_24h >= _MIN_OUTCOMES_FOR_ALERT and parser_failed_rate > _PARSER_FAILED_THRESHOLD:
        anomalies.append(
            f"Sunbiz parser_failed rate: {parser_failed_rate:.0%} "
            f"({failed_24h}/{total_24h} outcomes in 24h, threshold: {_PARSER_FAILED_THRESHOLD:.0%})"
        )

    if anomalies:
        if dry_run:
            logger.info("[SunbizMonitor] DRY RUN — would alert: %s", anomalies)
        else:
            _send_alert(settings, anomalies, status_dist, enriched_24h, multi_prop_count)

    return {
        "status_dist": status_dist,
        "total_owners": metrics["total_owners"],
        "enriched_24h": enriched_24h,
        "failed_24h": failed_24h,
        "total_24h": total_24h,
        "parser_failed_rate": round(parser_failed_rate, 4),
        "multi_prop_llcs": multi_prop_count,
        "anomalies": anomalies,
    }


def _send_alert(settings, anomalies: list[str], status_dist: dict, enriched_24h: int, multi_prop_count: int) -> None:
    phone = settings.alert_sms_number or settings.founder_phone
    if not phone:
        logger.warning("[SunbizMonitor] no alert phone configured — SMS skipped")
        return

    matched = status_dist.get("matched", 0)
    pending = status_dist.get("pending", 0)
    not_found = status_dist.get("not_found", 0)

    lines = [
        "[FA] Sunbiz anomaly alert",
        *[f"• {a}" for a in anomalies],
        f"Portfolio: matched={matched} pending={pending} not_found={not_found}",
        f"Enriched(24h)={enriched_24h}  Multi-prop LLCs={multi_prop_count}",
    ]
    body = "\n".join(lines)

    from src.core.database import get_db_context
    from src.services.sms_compliance import send_sms
    try:
        with get_db_context() as db:
            sent = send_sms(
                to=phone,
                body=body[:320],
                db=db,
                message_type="transactional",
                task_type="sunbiz_anomaly",
                campaign="sunbiz_anomaly_alert",
            )
        if sent:
            logger.info("[SunbizMonitor] alert SMS sent to %s", phone)
        else:
            logger.info("[SunbizMonitor] alert SMS suppressed by compliance gate")
    except Exception as exc:
        logger.error("[SunbizMonitor] alert SMS failed: %s", exc)


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    result = run_sunbiz_anomaly_check(dry_run=dry)
    pending = result["status_dist"].get("pending", 0)
    matched = result["status_dist"].get("matched", 0)
    not_found = result["status_dist"].get("not_found", 0)
    failed_status = result["status_dist"].get("parser_failed", 0)
    print(
        f"Status dist  — pending={pending} matched={matched} "
        f"not_found={not_found} parser_failed={failed_status}\n"
        f"Last 24h     — enriched={result['enriched_24h']} failed={result['failed_24h']} "
        f"rate={result['parser_failed_rate']:.1%}\n"
        f"Multi-prop LLCs: {result['multi_prop_llcs']}\n"
        f"Anomalies: {result['anomalies'] or 'none'}"
    )
