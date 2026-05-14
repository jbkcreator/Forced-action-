"""
NWS Revenue Alert Poller — autonomous 5-minute poll of api.weather.gov.

Replaces the manual-webhook dependency for the storm-pack revenue trigger.
Fetches active alerts for all configured counties, delegates each alert to
process_alert() (which owns idempotency, ZIP resolution, Redis flags, SMS, and
logging), then dispatches Cora urgency messages for qualifying subscribers.

Usage:
    python -m src.tasks.nws_poll
    python -m src.tasks.nws_poll --county-id hillsborough --dry-run

Cron (every 5 minutes):
    */5 * * * * cd /app && python -m src.tasks.nws_poll >> /var/log/cron/nws_poll.log 2>&1
"""

import argparse
import logging
import uuid
from datetime import datetime, timezone

import requests

from config.settings import get_settings
from src.core.database import get_db_context
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_NWS_ALERTS_ZONE_URL = "https://api.weather.gov/alerts/active"
_NWS_USER_AGENT = "ForcedAction/1.0 (distressed-property-intelligence)"


# ──────────────────────────────────────────────────────────────────────────────
# NWS fetch
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_alerts_for_zones(zone_ids: list) -> list:
    """Fetch active NWS alerts for the given UGC zone list."""
    if not zone_ids:
        return []
    zone_param = ",".join(zone_ids)
    from src.utils.http_helpers import get_requests_proxies
    proxies = get_requests_proxies()
    try:
        resp = requests.get(
            _NWS_ALERTS_ZONE_URL,
            params={"zone": zone_param, "status": "actual", "message_type": "alert"},
            headers={
                "User-Agent": _NWS_USER_AGENT,
                "Accept": "application/geo+json",
            },
            timeout=(5, 15),
            proxies=proxies,
        )
        resp.raise_for_status()
        features = resp.json().get("features", [])
        logger.info("[NWSPoll] Fetched %d features for zones %s", len(features), zone_param)
        return features
    except Exception as e:
        logger.warning("[NWSPoll] NWS API fetch failed: %s", e)
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Cora urgency dispatch
# ──────────────────────────────────────────────────────────────────────────────

def _dispatch_cora_urgency(alert_id: str, event_type: str, headline: str,
                            area_desc: str, expires: str, affected_zips: list,
                            db) -> int:
    """
    For each active subscriber with locked territories in affected ZIPs and
    at least one Gold+ lead in those ZIPs, dispatch a Cora urgency message —
    unless already sent for this alert+subscriber combination.

    Returns number of Cora messages dispatched.
    """
    from sqlalchemy import select, and_, func as sa_func
    from src.core.models import (
        Subscriber, ZipTerritory, Property, DistressScore, AgentDecision,
    )
    from src.agents.router import get_graph_spec

    if not affected_zips:
        return 0

    spec = get_graph_spec("nws_storm_alert_active")
    if not spec:
        logger.warning("[NWSPoll] No graph registered for nws_storm_alert_active")
        return 0

    # Subscribers with locked territories in affected ZIPs
    territories = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code.in_(affected_zips),
            ZipTerritory.status == "locked",
            ZipTerritory.subscriber_id.isnot(None),
        )
    ).scalars().all()

    sub_ids = list({t.subscriber_id for t in territories})
    if not sub_ids:
        return 0

    subscribers = db.execute(
        select(Subscriber).where(
            Subscriber.id.in_(sub_ids),
            Subscriber.status == "active",
        )
    ).scalars().all()

    dispatched = 0
    for sub in subscribers:
        sub_zips = [t.zip_code for t in territories if t.subscriber_id == sub.id]

        # Count Gold+ leads in this subscriber's affected ZIPs
        lead_count = db.execute(
            select(sa_func.count(DistressScore.id))
            .join(Property, Property.id == DistressScore.property_id)
            .where(
                Property.zip.in_(sub_zips),
                DistressScore.lead_tier.in_(["Gold", "Platinum", "Ultra Platinum"]),
            )
        ).scalar() or 0

        if lead_count == 0:
            continue

        # Per-subscriber/per-alert duplicate check via agent_decisions
        try:
            already_sent = db.execute(
                select(AgentDecision).where(
                    AgentDecision.subscriber_id == sub.id,
                    AgentDecision.graph_name == "nws_urgency",
                    AgentDecision.payload_summary["alert_id"].astext == alert_id,
                )
            ).scalar_one_or_none()
        except Exception:
            already_sent = None

        if already_sent:
            logger.debug("[NWSPoll] Cora urgency already sent sub=%d alert=%s", sub.id, alert_id[:40])
            continue

        payload = {
            "alert_id": alert_id,
            "event": event_type,
            "headline": headline or event_type,
            "area_desc": area_desc or "",
            "expires": expires or "",
            "affected_zips": sub_zips,
            "lead_count": lead_count,
        }

        try:
            result = spec.runner(
                event_payload=payload,
                subscriber_id=sub.id,
                decision_id=str(uuid.uuid4()),
            )
            if (result or {}).get("terminal_status") == "completed":
                dispatched += 1
                logger.info(
                    "[NWSPoll] Cora urgency dispatched: sub=%d, leads=%d, alert=%s",
                    sub.id, lead_count, alert_id[:40],
                )
            else:
                logger.warning(
                    "[NWSPoll] Cora urgency non-completed: sub=%d status=%s reason=%s",
                    sub.id,
                    (result or {}).get("terminal_status"),
                    (result or {}).get("failure_reason"),
                )
        except Exception as e:
            logger.warning("[NWSPoll] Cora urgency dispatch failed sub=%d: %s", sub.id, e)

    return dispatched


# ──────────────────────────────────────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────────────────────────────────────

def run_nws_poll(county_id: str = "hillsborough", dry_run: bool = False) -> dict:
    """
    Poll api.weather.gov for active alerts covering configured counties and
    process each qualifying alert through process_alert().

    Returns stats dict.
    """
    settings = get_settings()
    stats = {
        "polled": 0,
        "new_alerts": 0,
        "duplicates_skipped": 0,
        "non_qualifying_skipped": 0,
        "errors": 0,
        "cora_dispatched": 0,
    }

    if not settings.nws_weather_enabled:
        logger.info("[NWSPoll] nws_weather_enabled=False — poll skipped")
        return stats

    from src.services.nws_same_to_zip import UGC_TO_ZIPS
    zone_ids = list(UGC_TO_ZIPS.keys())

    features = _fetch_alerts_for_zones(zone_ids)
    stats["polled"] = len(features)

    if not features:
        logger.info("[NWSPoll] No active alerts found for zones %s", zone_ids)
        return stats

    if dry_run:
        for f in features:
            props = f.get("properties", {})
            logger.info(
                "[NWSPoll DRY RUN] Would process: event=%s id=%s",
                props.get("event", "?"),
                f.get("id", "?")[:60],
            )
        return stats

    new_alert_ids = []

    with get_db_context() as db:
        from src.services.nws_webhook import process_alert

        for feature in features:
            # Merge top-level feature id into properties so process_alert has it
            props = {**(feature.get("properties") or {}), "id": feature.get("id", "")}

            try:
                result = process_alert(props, db)
            except Exception as e:
                logger.error("[NWSPoll] process_alert failed: %s", e)
                stats["errors"] += 1
                continue

            status = result.get("status")
            if status == "processed":
                stats["new_alerts"] += 1
                new_alert_ids.append({
                    "alert_id": result["alert_id"],
                    "event": result.get("event", ""),
                    "affected_zips": result.get("affected_zips", []),
                    "headline": props.get("headline", ""),
                    "area_desc": props.get("areaDesc", ""),
                    "expires": props.get("expires", ""),
                })
            elif status == "duplicate":
                stats["duplicates_skipped"] += 1
            elif status == "skipped":
                stats["non_qualifying_skipped"] += 1

    # Cora urgency dispatch — outside the main DB session to avoid long transactions
    if (
        new_alert_ids
        and settings.nws_revenue_polling_enabled
        and settings.nws_cora_urgency_enabled
    ):
        with get_db_context() as db:
            for alert in new_alert_ids:
                count = _dispatch_cora_urgency(
                    alert_id=alert["alert_id"],
                    event_type=alert["event"],
                    headline=alert["headline"],
                    area_desc=alert["area_desc"],
                    expires=alert["expires"],
                    affected_zips=alert["affected_zips"],
                    db=db,
                )
                stats["cora_dispatched"] += count

    logger.info(
        "[NWSPoll] Complete — polled=%d new=%d dupe=%d skip=%d cora=%d errors=%d",
        stats["polled"], stats["new_alerts"], stats["duplicates_skipped"],
        stats["non_qualifying_skipped"], stats["cora_dispatched"], stats["errors"],
    )
    return stats


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NWS revenue alert poller")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch alerts and log them without writing to DB or triggering offers",
    )
    args = parser.parse_args()

    result = run_nws_poll(county_id=args.county_id, dry_run=args.dry_run)
    print(result)
