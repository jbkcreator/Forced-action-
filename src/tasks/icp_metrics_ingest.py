"""
ICP Channel Metrics Ingestion (fa066).

Computes raw metric counts per ICP channel and writes to icp_daily_stats.
Attribution uses icp_channel_key — verticals alone are not safe (they overlap ICPs).

Kill-switch percentages (first_payment_rate, saved_card_rate, etc.) are derived
from raw counts at read time in icp_kill_switch.py — never stored directly.

Cron: 15 7 * * *  (07:15 UTC daily, after kill_switch_metric_ingest at 06:00)
Gate: always runs; no env kill-switch (metrics are read-only writes).

Usage:
    python -m src.tasks.icp_metrics_ingest
    python -m src.tasks.icp_metrics_ingest --dry-run
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text as sa_text

from config.icp_channels import ICP_CHANNELS, DEFAULT_ICP_CHANNEL_KEY
from src.core.database import get_db_context
from src.tasks.kill_switch_metric_ingest import _cache_metric

logger = logging.getLogger(__name__)


def _compute_channel_stats(db, channel_key: str, county_id: str) -> dict:
    """
    Compute raw counts for one ICP channel + county for today.
    Attribution by icp_channel_key — not verticals.
    """
    today = date.today()
    since_30d = today - timedelta(days=30)
    since_7d = today - timedelta(days=7)

    # ── signup_count: new subscribers in last 30d ─────────────────────────
    row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM subscribers
        WHERE icp_channel_key = :key
          AND county_id = :county
          AND created_at::date >= :since
    """), {"key": channel_key, "county": county_id, "since": since_30d}).first()
    signup_count = int(row.c or 0)

    # ── payer_count: subscribers who paid at least once in last 30d ───────
    row = db.execute(sa_text("""
        SELECT COUNT(DISTINCT s.id) AS c
        FROM subscribers s
        WHERE s.icp_channel_key = :key
          AND s.county_id = :county
          AND s.tier NOT IN ('free','data_only')
          AND s.created_at::date >= :since
    """), {"key": channel_key, "county": county_id, "since": since_30d}).first()
    payer_count = int(row.c or 0)

    # ── saved_card_count: payers who have a saved card ────────────────────
    row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM subscribers
        WHERE icp_channel_key = :key
          AND county_id = :county
          AND has_saved_card = true
          AND tier NOT IN ('free','data_only')
    """), {"key": channel_key, "county": county_id}).first()
    saved_card_count = int(row.c or 0)

    # ── active_subscriber_count: active non-free subs right now ──────────
    row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM subscribers
        WHERE icp_channel_key = :key
          AND county_id = :county
          AND status = 'active'
          AND tier NOT IN ('free','data_only')
    """), {"key": channel_key, "county": county_id}).first()
    active_subscriber_count = int(row.c or 0)

    # ── mrr_cents: sum of plan_price in cents for active subs ────────────
    row = db.execute(sa_text("""
        SELECT COALESCE(SUM(ROUND(plan_price * 100)), 0) AS c FROM subscribers
        WHERE icp_channel_key = :key
          AND county_id = :county
          AND status = 'active'
          AND tier NOT IN ('free','data_only')
          AND plan_price IS NOT NULL
    """), {"key": channel_key, "county": county_id}).first()
    mrr_cents = int(row.c or 0)

    # ── sms counts: last 7d ───────────────────────────────────────────────
    sms_row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE m.message_type = 'sms') AS sent,
            COUNT(*) FILTER (WHERE m.message_type = 'sms' AND m.replied_at IS NOT NULL) AS replied
        FROM message_outcomes m
        JOIN subscribers s ON s.id = m.subscriber_id
        WHERE s.icp_channel_key = :key
          AND s.county_id = :county
          AND m.sent_at >= :since7d
    """), {"key": channel_key, "county": county_id, "since7d": datetime.now(timezone.utc) - timedelta(days=7)}).first()
    sms_sent_count = int(sms_row.sent or 0)
    sms_reply_count = int(sms_row.replied or 0)

    # ── cancel_count: cancellations in last 30d ───────────────────────────
    row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM subscribers
        WHERE icp_channel_key = :key
          AND county_id = :county
          AND status IN ('cancelled','churned')
          AND updated_at::date >= :since
    """), {"key": channel_key, "county": county_id, "since": since_30d}).first()
    cancel_count = int(row.c or 0)

    # ── refund_count: refunded leads/packs in last 30d ────────────────────
    row = db.execute(sa_text("""
        SELECT COUNT(sl.id) AS c
        FROM sent_leads sl
        JOIN subscribers s ON s.id = sl.subscriber_id
        WHERE s.icp_channel_key = :key
          AND s.county_id = :county
          AND sl.refunded_at IS NOT NULL
          AND sl.refunded_at::date >= :since
    """), {"key": channel_key, "county": county_id, "since": since_30d}).first()
    refund_count = int(row.c or 0)

    return {
        "signup_count": signup_count,
        "payer_count": payer_count,
        "saved_card_count": saved_card_count,
        "sms_sent_count": sms_sent_count,
        "sms_reply_count": sms_reply_count,
        "active_subscriber_count": active_subscriber_count,
        "cancel_count": cancel_count,
        "refund_count": refund_count,
        "mrr_cents": mrr_cents,
    }


def _upsert_stats(db, channel_key: str, county_id: str, stats: dict) -> None:
    today = date.today()
    db.execute(sa_text("""
        INSERT INTO icp_daily_stats
            (run_date, county_id, icp_channel_key,
             signup_count, payer_count, saved_card_count,
             sms_sent_count, sms_reply_count, active_subscriber_count,
             cancel_count, refund_count, mrr_cents,
             created_at, updated_at)
        VALUES
            (:run_date, :county, :channel,
             :signup_count, :payer_count, :saved_card_count,
             :sms_sent_count, :sms_reply_count, :active_subscriber_count,
             :cancel_count, :refund_count, :mrr_cents,
             NOW(), NOW())
        ON CONFLICT (run_date, county_id, icp_channel_key) DO UPDATE SET
            signup_count = EXCLUDED.signup_count,
            payer_count = EXCLUDED.payer_count,
            saved_card_count = EXCLUDED.saved_card_count,
            sms_sent_count = EXCLUDED.sms_sent_count,
            sms_reply_count = EXCLUDED.sms_reply_count,
            active_subscriber_count = EXCLUDED.active_subscriber_count,
            cancel_count = EXCLUDED.cancel_count,
            refund_count = EXCLUDED.refund_count,
            mrr_cents = EXCLUDED.mrr_cents,
            updated_at = NOW()
    """), {
        "run_date": today,
        "county": county_id,
        "channel": channel_key,
        **stats,
    })


def _cache_channel_metrics(channel_key: str, county_id: str, stats: dict) -> None:
    """Cache derived rates to Redis for fast kill-switch reads."""
    signup = stats.get("signup_count") or 0
    payer = stats.get("payer_count") or 0
    saved_card = stats.get("saved_card_count") or 0
    sms_sent = stats.get("sms_sent_count") or 0
    sms_reply = stats.get("sms_reply_count") or 0
    mrr_cents = stats.get("mrr_cents") or 0

    # Only cache if denominator > 0; otherwise leave as None (→ "unknown")
    prefix = f"{channel_key}"
    if signup > 0:
        _cache_metric(f"icp:{prefix}:first_payment_rate", round(payer / signup * 100, 2), county_id)
    if payer > 0:
        _cache_metric(f"icp:{prefix}:saved_card_rate", round(saved_card / payer * 100, 2), county_id)
    if sms_sent > 0:
        _cache_metric(f"icp:{prefix}:sms_reply_rate", round(sms_reply / sms_sent * 100, 2), county_id)
    # MRR in USD for the contractor MRR gate
    if channel_key == DEFAULT_ICP_CHANNEL_KEY:
        _cache_metric("contractor_mrr_usd", round(mrr_cents / 100, 2), county_id)


def run_icp_metrics_ingest(dry_run: bool = False) -> dict:
    from config.settings import get_settings
    settings = get_settings()
    county_id = settings.county_launch_source_county or "hillsborough"

    results = {}
    with get_db_context() as db:
        for channel_key in ICP_CHANNELS:
            try:
                stats = _compute_channel_stats(db, channel_key, county_id)
                results[channel_key] = stats
                if not dry_run:
                    _upsert_stats(db, channel_key, county_id, stats)
                    _cache_channel_metrics(channel_key, county_id, stats)
                    db.flush()
            except Exception:
                logger.exception("[icp-metrics] failed for channel=%s county=%s", channel_key, county_id)

    logger.info("[icp-metrics] %s", json.dumps(results, default=str))
    return {"dry_run": dry_run, "county_id": county_id, "channels": results}


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    dry_run = "--dry-run" in set(argv or sys.argv[1:])
    result = run_icp_metrics_ingest(dry_run=dry_run)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
