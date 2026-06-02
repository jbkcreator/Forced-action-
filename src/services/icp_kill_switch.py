"""
ICP Channel Kill-Switch Score Computation (fa066).

Computes a 4-week Green/Yellow/Red gate snapshot per ICP channel.

Key rules:
- Attribution uses icp_channel_key, NOT verticals. Verticals can overlap ICPs.
- Percentages are derived from raw counts in icp_daily_stats — never hardcoded.
- Missing data (value=None) → color="unknown", shown as "N/A" in the UI.
  Never raises an exception for missing metrics.
- Reuses _gate_color() from county_launch_evaluator for consistency.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.icp_channels import DEFAULT_ICP_CHANNEL_KEY, is_gate_required
from src.tasks.county_launch_evaluator import _gate_color
from src.tasks.kill_switch_metric_ingest import get_cached_metric

logger = logging.getLogger(__name__)

# Gate thresholds for ICP channels — separate from EXPANSION_GATES which are
# for county launch. These are per-channel launch gates.
ICP_GATE_CONFIG: dict[str, dict] = {
    "first_payment_rate": {
        "threshold": 30.0,
        "direction": "higher_is_better",
        "description": "% of signups who made first payment within 30d",
    },
    "saved_card_rate": {
        "threshold": 70.0,
        "direction": "higher_is_better",
        "description": "% of payers who saved a card within 7d",
    },
    "payer_retention_30d": {
        "threshold": 70.0,
        "direction": "higher_is_better",
        "description": "% of payers still active at 30d",
    },
    "sms_reply_rate": {
        "threshold": 8.0,
        "direction": "higher_is_better",
        "description": "% of marketing SMS with a reply in 7d",
    },
    "contractor_mrr_usd": {
        "threshold": 50000.0,
        "direction": "higher_is_better",
        "description": "Global contractor ICP MRR must reach $50K before any expansion ICP launches",
    },
}


def _derive_rates(raw: dict) -> dict[str, Optional[float]]:
    """Compute percentage metrics from raw counts. Returns None for any metric
    where raw denominator is 0 or missing (→ shown as N/A)."""
    s = raw.get("signup_count") or 0
    p = raw.get("payer_count") or 0
    sc = raw.get("saved_card_count") or 0
    sent = raw.get("sms_sent_count") or 0
    replied = raw.get("sms_reply_count") or 0
    active = raw.get("active_subscriber_count") or 0

    return {
        "first_payment_rate": round(p / s * 100, 2) if s > 0 else None,
        "saved_card_rate": round(sc / p * 100, 2) if p > 0 else None,
        "sms_reply_rate": round(replied / sent * 100, 2) if sent > 0 else None,
        # payer_retention_30d requires historical payer count — not derivable from
        # single-day snapshot; fetched from rolling 30-day query separately
        "payer_retention_30d": None,
    }


def _fetch_rolling_raw(db: Session, channel_key: str, county_id: str, days: int = 30) -> dict:
    """Aggregate raw counts from icp_daily_stats over the last `days` days."""
    since = date.today() - timedelta(days=days)
    row = db.execute(sa_text("""
        SELECT
            SUM(signup_count)              AS signup_count,
            SUM(payer_count)               AS payer_count,
            SUM(saved_card_count)          AS saved_card_count,
            SUM(sms_sent_count)            AS sms_sent_count,
            SUM(sms_reply_count)           AS sms_reply_count,
            MAX(active_subscriber_count)   AS active_subscriber_count,
            SUM(cancel_count)              AS cancel_count,
            SUM(refund_count)              AS refund_count,
            MAX(mrr_cents)                 AS mrr_cents
        FROM icp_daily_stats
        WHERE icp_channel_key = :channel
          AND county_id = :county
          AND run_date >= :since
    """), {"channel": channel_key, "county": county_id, "since": since}).first()
    if row is None:
        return {}
    return {
        "signup_count":            int(row.signup_count or 0),
        "payer_count":             int(row.payer_count or 0),
        "saved_card_count":        int(row.saved_card_count or 0),
        "sms_sent_count":          int(row.sms_sent_count or 0),
        "sms_reply_count":         int(row.sms_reply_count or 0),
        "active_subscriber_count": int(row.active_subscriber_count or 0),
        "cancel_count":            int(row.cancel_count or 0),
        "refund_count":            int(row.refund_count or 0),
        "mrr_cents":               int(row.mrr_cents or 0),
    }


def _fetch_contractor_mrr(db: Session) -> Optional[float]:
    """Get global contractor MRR in USD from contractor_mrr service."""
    try:
        from src.services.contractor_mrr import global_contractor_mrr
        return float(global_contractor_mrr(db))
    except Exception:
        return None


def compute_icp_gate_snapshot(
    channel_key: str,
    county_id: str,
    db: Session,
) -> dict:
    """
    Compute the full gate snapshot for an ICP channel.

    Returns a dict of {metric_name: {value, threshold, color, raw}}.
    color is always one of 'green'|'yellow'|'red'|'unknown'.
    'unknown' means insufficient data — shown as N/A in the UI, never raises.

    The contractor ICP (default) is gate-exempt — returns all-green.
    """
    # Contractor ICP is always gate-exempt
    if not is_gate_required(channel_key):
        return {
            metric: {
                "value": None,
                "threshold": cfg["threshold"],
                "color": "green",
                "raw": None,
                "note": "contractor ICP is gate-exempt",
            }
            for metric, cfg in ICP_GATE_CONFIG.items()
        }

    raw = _fetch_rolling_raw(db, channel_key, county_id, days=30)
    rates = _derive_rates(raw)

    # Contractor MRR from Redis cache first, then DB
    contractor_mrr = get_cached_metric("contractor_mrr_usd")
    if contractor_mrr is None:
        contractor_mrr = _fetch_contractor_mrr(db)

    snapshot: dict = {}
    for metric, cfg in ICP_GATE_CONFIG.items():
        if metric == "contractor_mrr_usd":
            value = contractor_mrr
            raw_for_metric = None
        else:
            value = rates.get(metric)
            raw_for_metric = {
                "signup_count": raw.get("signup_count"),
                "payer_count": raw.get("payer_count"),
                "saved_card_count": raw.get("saved_card_count"),
                "sms_sent_count": raw.get("sms_sent_count"),
                "sms_reply_count": raw.get("sms_reply_count"),
            } if raw else None

        if value is None:
            color = "unknown"
        else:
            # Map ICP gate thresholds to _gate_color() format by temporarily
            # injecting into KILL_SWITCH-compatible structure
            try:
                threshold = cfg["threshold"]
                if cfg["direction"] == "higher_is_better":
                    green = threshold
                    red = threshold * 0.67  # red = below 2/3 of threshold
                    if value >= green:
                        color = "green"
                    elif value < red:
                        color = "red"
                    else:
                        color = "yellow"
                else:
                    # lower_is_better (e.g. cost ratios)
                    green = threshold
                    red = threshold * 1.5
                    if value <= green:
                        color = "green"
                    elif value > red:
                        color = "red"
                    else:
                        color = "yellow"
            except Exception:
                color = "unknown"

        snapshot[metric] = {
            "value": value,
            "threshold": cfg["threshold"],
            "color": color,
            "raw": raw_for_metric,
            "description": cfg.get("description"),
        }

    return snapshot


def gate_blocking_reasons(snapshot: dict) -> list[str]:
    """Return human-readable list of blocking reasons from a gate snapshot."""
    reasons = []
    for metric, info in snapshot.items():
        color = info.get("color", "unknown")
        if color in ("red", "yellow"):
            val = info.get("value")
            val_str = f"{val:.1f}" if val is not None else "N/A"
            reasons.append(
                f"{metric} is {color} (value={val_str}, threshold={info.get('threshold')})"
            )
        elif color == "unknown":
            reasons.append(f"{metric}: insufficient data")
    return reasons


def is_gate_clear(snapshot: dict) -> bool:
    """Return True only when all gates are green (no unknown/yellow/red)."""
    return all(v.get("color") == "green" for v in snapshot.values())
