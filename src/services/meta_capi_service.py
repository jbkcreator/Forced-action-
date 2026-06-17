"""
Meta Conversions API (CAPI) — server-side Purchase reporting (S2).

`fire_purchase_event()` reports a confirmed paid purchase (subscription or lead
pack) to Meta's Conversions API so Meta can attribute ad spend to real revenue.

Design rules (Sprint S2):
  - Observer only — Stripe owns revenue truth. This module NEVER raises into the
    payment/webhook flow; every failure path returns a structured status dict.
  - Feature-gated. When META_CAPI_ENABLED is false, or META_PIXEL_ID /
    META_ACCESS_TOKEN are missing, the call is skipped safely (no network).
  - PII is hashed (SHA-256 hex) before it leaves the process. Raw email/phone,
    the access token, and the full token-bearing URL are NEVER logged.
  - test_event_code is included only when META_CAPI_TEST_MODE is true and a code
    is configured.

Returns one of:
  {"status": "sent",    "event_id": ..., "reason": "success"}
  {"status": "skipped", "event_id": ..., "reason": "disabled" | "missing_config"}
  {"status": "failed",  "event_id": ..., "reason": "meta_error"}
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import requests

from config.settings import get_settings

logger = logging.getLogger(__name__)

_GRAPH_BASE = "https://graph.facebook.com"
_TIMEOUT_SECONDS = 10
# Stripe metadata caps values at 500 chars; Meta has no hard UA limit, but keep
# the same compact bound we stamp at checkout time.
_MAX_UA_LEN = 480


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_email(email: Optional[str]) -> Optional[str]:
    """strip → lowercase → SHA-256 hex. Returns None when no email."""
    if not email:
        return None
    cleaned = email.strip().lower()
    if not cleaned:
        return None
    return _sha256_hex(cleaned)


def _hash_phone(phone: Optional[str]) -> Optional[str]:
    """Normalize to E.164, reduce to digits, SHA-256 hex. None when unparseable."""
    if not phone:
        return None
    try:
        from src.services.phone_utils import normalize as _normalize_phone
        normalized = _normalize_phone(phone)
    except Exception:
        normalized = phone
    digits = re.sub(r"\D", "", normalized or "")
    if not digits:
        return None
    return _sha256_hex(digits)


def fire_purchase_event(
    subscriber,
    amount,
    source: str,
    request_context: Optional[dict] = None,
    event_id: Optional[str] = None,
) -> dict:
    """Report a confirmed Purchase to Meta CAPI. Never raises.

    Args:
        subscriber: Subscriber row (reads .email, .phone, .id).
        amount: purchase value in DOLLARS (float/Decimal). Callers convert cents.
        source: "subscription" | "lead_pack" — used for content_name + logging.
        request_context: dict with buyer_ip, buyer_user_agent, fbclid,
            utm_campaign, campaign_id, currency (default "USD").
        event_id: stable id for Meta's event de-duplication (~48h window).

    Returns a structured status dict (see module docstring).
    """
    ctx = request_context or {}
    settings = get_settings()

    # ── Feature gate (no network, no raise) ──────────────────────────────────
    if not settings.meta_capi_enabled:
        logger.info("meta_capi_skipped event_id=%s source=%s reason=disabled", event_id, source)
        return {"status": "skipped", "event_id": event_id, "reason": "disabled"}

    pixel_id = settings.meta_pixel_id
    token_secret = settings.meta_access_token
    if not pixel_id or not token_secret:
        logger.info(
            "meta_capi_skipped event_id=%s source=%s reason=missing_config", event_id, source
        )
        return {"status": "skipped", "event_id": event_id, "reason": "missing_config"}

    event_time = int(datetime.now(timezone.utc).timestamp())

    # ── Build hashed user_data ───────────────────────────────────────────────
    user_data: dict = {}
    em = _hash_email(getattr(subscriber, "email", None))
    if em:
        user_data["em"] = [em]
    ph = _hash_phone(getattr(subscriber, "phone", None))
    if ph:
        user_data["ph"] = [ph]

    buyer_ip = ctx.get("buyer_ip")
    if buyer_ip:
        user_data["client_ip_address"] = buyer_ip
    buyer_ua = ctx.get("buyer_user_agent")
    if buyer_ua:
        user_data["client_user_agent"] = buyer_ua[:_MAX_UA_LEN]
    fbclid = ctx.get("fbclid")
    if fbclid:
        # Meta's click-id cookie format: fb.1.<unix_ts>.<fbclid>
        user_data["fbc"] = f"fb.1.{event_time}.{fbclid}"

    # ── Build custom_data ────────────────────────────────────────────────────
    currency = (ctx.get("currency") or "USD").upper()
    custom_data: dict = {
        "currency": currency,
        "value": float(amount or 0),
        "content_name": source,
        "source": "forced_action",
        "domain": "forcedactionleads.com",
    }
    if ctx.get("campaign_id"):
        custom_data["campaign_id"] = ctx["campaign_id"]
    if ctx.get("utm_campaign"):
        custom_data["utm_campaign"] = ctx["utm_campaign"]

    event = {
        "event_name": "Purchase",
        "event_time": event_time,
        "action_source": "website",
        "user_data": user_data,
        "custom_data": custom_data,
    }
    if event_id:
        event["event_id"] = event_id

    payload: dict = {
        "data": [event],
        "access_token": token_secret.get_secret_value(),
    }
    if settings.meta_capi_test_mode and settings.meta_test_event_code:
        payload["test_event_code"] = settings.meta_test_event_code

    logger.info(
        "meta_capi_payload_built event_id=%s source=%s value=%s currency=%s "
        "has_email=%s has_phone=%s has_fbc=%s test_mode=%s",
        event_id, source, custom_data["value"], currency,
        bool(em), bool(ph), bool(fbclid), settings.meta_capi_test_mode,
    )

    # URL carries the pixel id only — the token lives in the JSON body so it
    # never lands in a logged URL.
    url = f"{_GRAPH_BASE}/{settings.meta_graph_api_version}/{pixel_id}/events"

    # ── POST to Meta — never raise into the caller ───────────────────────────
    try:
        resp = requests.post(url, json=payload, timeout=_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        logger.warning(
            "meta_capi_failed_non_blocking event_id=%s source=%s reason=network err=%s",
            event_id, source, exc,
        )
        return {"status": "failed", "event_id": event_id, "reason": "meta_error"}

    if resp.status_code >= 400:
        fbtrace_id = None
        err_message = None
        try:
            body = resp.json()
            error = body.get("error", {}) if isinstance(body, dict) else {}
            fbtrace_id = error.get("fbtrace_id")
            err_message = error.get("message")
        except ValueError:
            err_message = resp.text[:200]
        logger.warning(
            "meta_capi_failed_non_blocking event_id=%s source=%s status=%s "
            "fbtrace_id=%s message=%s",
            event_id, source, resp.status_code, fbtrace_id, err_message,
        )
        return {"status": "failed", "event_id": event_id, "reason": "meta_error"}

    fbtrace_id = None
    try:
        body = resp.json()
        if isinstance(body, dict):
            fbtrace_id = body.get("fbtrace_id")
    except ValueError:
        pass

    logger.info(
        "meta_capi_sent event_id=%s source=%s status=%s fbtrace_id=%s",
        event_id, source, resp.status_code, fbtrace_id,
    )
    return {"status": "sent", "event_id": event_id, "reason": "success"}
