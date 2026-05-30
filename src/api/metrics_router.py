"""
Stage 10 — Prometheus /metrics exposition endpoint.

Exposes Cora kill-switch business metrics and 3-variant A/B slot performance
in Prometheus text format so Alertmanager can scrape them.

Metrics emitted:
  cora_first_payment_rate        — % free→paid in 30d (kill-switch)
  cora_saved_card_rate           — % payers with saved card
  cora_wallet_adoption           — % payers with wallet
  cora_lock_conversion           — % wallet→lock
  cora_retention_30d             — 30-day payer retention
  cora_sms_reply_rate            — % marketing SMS replied
  cora_offer_acceptance_rate     — % wallet-push offers accepted
  cora_variant_sends             — sends per (sequence, slot)
  cora_variant_conversions       — conversions per (sequence, slot)
  cora_variant_conv_rate         — conversion rate per (sequence, slot)
  cora_pricing_cohort_active     — 1 if cohort active, 0 otherwise

Values are read from Redis (kill-switch cache) and Postgres (variant tests,
cohorts). Each scrape rebuilds the registry — no background threads.

Gate: only active when settings.prometheus_enabled is True. Returns 404
otherwise so an unconfigured Prometheus config fails loudly.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

router = APIRouter(tags=["metrics"])

_KILL_SWITCH_METRICS = [
    "first_payment_rate",
    "saved_card_rate",
    "wallet_adoption",
    "lock_conversion",
    "retention_30d",
    "sms_reply_rate",
    "offer_acceptance_rate",
]

_PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _get_db():
    from src.core.database import get_db_context
    with get_db_context() as db:
        yield db


@router.get("/metrics", include_in_schema=False)
def prometheus_metrics(db: Session = Depends(_get_db)):
    from config.settings import get_settings
    from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST
    from src.tasks.kill_switch_metric_ingest import get_cached_metric

    settings = get_settings()
    if not getattr(settings, "prometheus_enabled", False):
        raise HTTPException(status_code=404, detail="Prometheus metrics not enabled")

    reg = CollectorRegistry(auto_describe=False)

    # ── Kill-switch business metrics ─────────────────────────────────────────
    for metric_name in _KILL_SWITCH_METRICS:
        val = get_cached_metric(metric_name)
        if val is not None:
            g = Gauge(
                f"cora_{metric_name}",
                f"Cora kill-switch metric: {metric_name}",
                registry=reg,
            )
            g.set(val)

    # ── 3-variant slot performance ────────────────────────────────────────────
    try:
        rows = db.execute(sa_text("""
            SELECT
                sequence_name,
                slot_a_sends,   slot_a_conversions,
                slot_b_sends,   slot_b_conversions,
                slot_c_sends,   slot_c_conversions,
                slot_a_status,  slot_b_status,  slot_c_status
            FROM message_variant_tests
            WHERE status = 'active'
        """)).fetchall()

        sends_g = Gauge(
            "cora_variant_sends",
            "Total sends per variant slot",
            ["sequence_name", "slot"],
            registry=reg,
        )
        convs_g = Gauge(
            "cora_variant_conversions",
            "Total conversions per variant slot",
            ["sequence_name", "slot"],
            registry=reg,
        )
        rate_g = Gauge(
            "cora_variant_conv_rate",
            "Conversion rate per variant slot (0.0–1.0)",
            ["sequence_name", "slot"],
            registry=reg,
        )
        active_g = Gauge(
            "cora_variant_slot_active",
            "1 if slot is active, 0 if retired",
            ["sequence_name", "slot"],
            registry=reg,
        )

        for row in rows:
            for slot in ("a", "b", "c"):
                sends = getattr(row, f"slot_{slot}_sends") or 0
                convs = getattr(row, f"slot_{slot}_conversions") or 0
                rate = convs / sends if sends > 0 else 0.0
                is_active = 1 if getattr(row, f"slot_{slot}_status") == "active" else 0
                labels = [row.sequence_name, slot]
                sends_g.labels(*labels).set(sends)
                convs_g.labels(*labels).set(convs)
                rate_g.labels(*labels).set(rate)
                active_g.labels(*labels).set(is_active)

    except Exception:
        logger.exception("[metrics] failed to collect variant stats")

    # ── Pricing cohort status ─────────────────────────────────────────────────
    try:
        cohort_rows = db.execute(sa_text("""
            SELECT county_id, trade_vertical, price_type, status
            FROM pricing_cohorts
            WHERE status IN ('active', 'pending')
        """)).fetchall()

        cohort_g = Gauge(
            "cora_pricing_cohort_active",
            "1 if pricing cohort is active, 0 if pending/rolled_back",
            ["county_id", "trade_vertical", "price_type"],
            registry=reg,
        )
        for row in cohort_rows:
            cohort_g.labels(row.county_id, row.trade_vertical, row.price_type).set(
                1 if row.status == "active" else 0
            )

    except Exception:
        logger.exception("[metrics] failed to collect pricing cohort stats")

    body = generate_latest(reg)
    return Response(content=body, media_type=CONTENT_TYPE_LATEST)
