"""
Stage 10 — Alertmanager → Cora webhook receiver.

Alertmanager POSTs to this endpoint when a firing alert matches a receiver
rule. The handler:

  1. Validates the shared secret (PROMETHEUS_ALERT_WEBHOOK_SECRET in .env).
  2. Parses the Alertmanager JSON payload (standard v2 format).
  3. Dispatches each firing alert to the appropriate Cora action:

     alert_name == "CoraFirstPaymentRateLow"
       → variant_engine.promote_winner(sequence="wallet_push_v1")

     alert_name == "CoraVariantSigmaRollback"
       → variant_engine.check_sigma_rollback(sequence from label)

     alert_name == "CoraLockConversionLow" | "CoraWalletAdoptionLow"
       → cora_self_healing._process_metric (writes incident, fallback if eligible)

  4. Logs every action to Postgres (cora_incident or variant_retirement_log)
     via the existing engines — no direct writes here.

  5. Returns 200 with a JSON summary. Alertmanager marks the webhook as
     "succeeded" and stops retrying after the first 200.

n8n alternative: replace this endpoint with an n8n HTTP Request node that
  calls POST /alert-webhook with the same payload, then connects to an n8n
  variant-engine node via N8N_WEBHOOK_URL in .env. No code change needed —
  just point Alertmanager to the n8n webhook URL instead of this endpoint.

Endpoint: POST /webhooks/alerts/prometheus
Auth: Bearer token = PROMETHEUS_ALERT_WEBHOOK_SECRET (or absent = open when secret unset)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.api.deps import get_db as _get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/alerts", tags=["alerts"])


# ── auth ──────────────────────────────────────────────────────────────────────

def _verify_secret(request: Request) -> None:
    settings = get_settings()
    secret = getattr(settings, "prometheus_alert_webhook_secret", None)
    if not secret:
        return  # open when not configured
    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip()
    if not hmac.compare_digest(token, secret):
        raise HTTPException(status_code=401, detail="Invalid alert webhook secret")


# ── alert dispatch ────────────────────────────────────────────────────────────

_KNOWN_ALERT_NAMES = {
    "CoraFirstPaymentRateLow",
    "CoraLockConversionLow",
    "CoraWalletAdoptionLow",
    "CoraVariantSigmaRollback",
    "CoraSMSReplyRateLow",
    "CoraOfferAcceptanceRateLow",
}


def _dispatch_alert(alert: dict, db: Session) -> dict:
    """Route a single Alertmanager alert to the appropriate Cora action.

    Returns a dict describing what was done.
    """
    name = alert.get("labels", {}).get("alertname", "")
    status = alert.get("status", "firing")  # firing | resolved

    if status != "firing":
        return {"alert": name, "action": "skipped_resolved"}

    if name == "CoraFirstPaymentRateLow":
        return _handle_first_payment_low(db)

    if name == "CoraVariantSigmaRollback":
        sequence = alert.get("labels", {}).get("sequence_name", "")
        return _handle_sigma_rollback(sequence, db)

    if name in ("CoraLockConversionLow", "CoraWalletAdoptionLow",
                "CoraSMSReplyRateLow", "CoraOfferAcceptanceRateLow"):
        metric_map = {
            "CoraLockConversionLow": "lock_conversion",
            "CoraWalletAdoptionLow": "wallet_adoption",
            "CoraSMSReplyRateLow": "sms_reply_rate",
            "CoraOfferAcceptanceRateLow": "offer_acceptance_rate",
        }
        metric = metric_map[name]
        return _handle_metric_breach(metric, db)

    return {"alert": name, "action": "unrecognised"}


def _handle_first_payment_low(db: Session) -> dict:
    """first_payment_rate < 25% for 48h → promote winner variant in wallet_push_v1."""
    from config.stage10_config import STAGE10_KILL_SWITCH_OVERRIDES
    from src.services.variant_engine import promote_winner

    overrides = STAGE10_KILL_SWITCH_OVERRIDES.get("first_payment_rate", {})
    sequence = overrides.get("variant_sequence_name", "wallet_push_v1")

    result = promote_winner(sequence, db)
    logger.info("[alert-webhook] first_payment_rate low → promote_winner: %s", result)
    return {"alert": "CoraFirstPaymentRateLow", "action": "variant_promoted", "detail": result}


def _handle_sigma_rollback(sequence_name: str, db: Session) -> dict:
    """Slot >2σ below control → sigma rollback for the named sequence."""
    from src.services.variant_engine import check_sigma_rollback

    if not sequence_name:
        return {"alert": "CoraVariantSigmaRollback", "action": "skipped_no_sequence"}

    result = check_sigma_rollback(sequence_name, db)
    logger.info("[alert-webhook] sigma-rollback seq=%s: %s", sequence_name, result)
    return {
        "alert": "CoraVariantSigmaRollback",
        "action": "sigma_rollback_checked",
        "sequence": sequence_name,
        "detail": result,
    }


def _handle_metric_breach(metric_name: str, db: Session) -> dict:
    """Generic kill-switch metric breach → run one self-healing step."""
    from src.tasks.cora_self_healing import _process_metric, _Counters, _count_today_kill_recommendations
    from config.settings import get_settings

    settings = get_settings()
    county_id = getattr(settings, "county_launch_source_county", None) or "hillsborough"

    counters = _Counters(kill_recs_today=_count_today_kill_recommendations(db))
    result = _process_metric(
        db,
        metric_name=metric_name,
        county_id=county_id,
        feature_name=None,
        counters=counters,
        dry_run=False,
    )
    logger.info("[alert-webhook] metric_breach %s → self-healing: %s", metric_name, result)
    return {"alert": metric_name, "action": "self_healing_step", "detail": result}


# ── webhook endpoint ──────────────────────────────────────────────────────────

@router.post("/prometheus")
async def receive_alertmanager_webhook(
    request: Request,
    db: Session = Depends(_get_db),
    _auth: None = Depends(_verify_secret),
):
    """Receive Alertmanager v2 webhook payload and dispatch Cora actions."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    alerts: list[dict] = body.get("alerts", [])
    if not alerts:
        return JSONResponse({"status": "ok", "processed": 0})

    results = []
    for alert in alerts:
        try:
            result = _dispatch_alert(alert, db)
            results.append(result)
        except Exception as exc:
            logger.exception("[alert-webhook] dispatch failed for alert %s", alert)
            results.append({
                "alert": alert.get("labels", {}).get("alertname", "unknown"),
                "action": "error",
                "error": str(exc),
            })

    logger.info(
        "[alert-webhook] processed %d alerts: %s",
        len(results),
        json.dumps(results, default=str)[:500],
    )
    return JSONResponse({"status": "ok", "processed": len(results), "results": results})


# ── manual test endpoint (admin only) ─────────────────────────────────────────

@router.post("/prometheus/test", include_in_schema=False)
async def test_alert_dispatch(
    request: Request,
    db: Session = Depends(_get_db),
):
    """Simulate a firing alert without going through Alertmanager.

    Body: {"alertname": "CoraFirstPaymentRateLow"}
    Returns the same response as the real webhook.
    """
    settings = get_settings()
    if not getattr(settings, "dev_tools_enabled", False):
        raise HTTPException(status_code=403, detail="Test endpoint disabled in production")

    body = await request.json()
    alertname = body.get("alertname", "")
    sequence = body.get("sequence_name", "")

    mock_alert = {
        "status": "firing",
        "labels": {"alertname": alertname, "sequence_name": sequence},
    }
    result = _dispatch_alert(mock_alert, db)
    return JSONResponse({"status": "ok", "result": result})
