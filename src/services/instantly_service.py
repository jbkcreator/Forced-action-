"""
Instantly.ai v2 API client.

All Instantly I/O is centralized here. Bearer token auth (v2 only — not
compatible with v1). Documented limits: 100 req/sec, 6000 req/min per
workspace; 429 on exceed.

Throttle + exponential 429 backoff mirrors ghl_webhook._ghl_request.
Every public function is a no-op (returns None/[]/{}) when INSTANTLY_API_KEY
is not set or INSTANTLY_ENABLED=false — callers must check _is_configured()
before acting on the return value.

API reference: https://developer.instantly.ai/api/v2
"""

import logging
import time
from typing import Any, Optional

import requests
from requests.exceptions import ConnectionError, RequestException, Timeout

from config.settings import get_settings

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 20  # seconds
_BASE_THROTTLE = 0.05  # 20 req/s sustained — well under 100/s limit


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_configured() -> bool:
    s = get_settings()
    return bool(s.instantly_enabled and s.instantly_api_key)


def _headers() -> dict[str, str]:
    s = get_settings()
    if not s.instantly_api_key:
        raise RuntimeError("INSTANTLY_API_KEY not configured")
    return {
        "Authorization": f"Bearer {s.instantly_api_key.get_secret_value()}",
        "Content-Type": "application/json",
    }


def _base_url() -> str:
    return get_settings().instantly_base_url.rstrip("/")


def _request(method: str, path: str, **kwargs) -> requests.Response:
    """
    Throttled HTTP wrapper with exponential 429 backoff (4 attempts).
    Raises RuntimeError on 401/402. Raises RequestException after retry
    exhaustion on network errors. Returns the last 429 response if retries
    are exhausted on rate-limit (caller decides).
    """
    url = f"{_base_url()}{path}"
    kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
    headers = _headers()
    # Instantly (Fastify) rejects a JSON content-type with an empty body
    # (FST_ERR_CTP_EMPTY_JSON_BODY) — only declare it when we send a body.
    if "json" not in kwargs and "data" not in kwargs:
        headers.pop("Content-Type", None)
    kwargs["headers"] = headers

    time.sleep(_BASE_THROTTLE)

    last_exc: Optional[Exception] = None
    resp: Optional[requests.Response] = None

    for attempt in range(4):
        try:
            resp = requests.request(method, url, **kwargs)
        except (ConnectionError, Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            logger.warning(
                "[Instantly] Network error %s %s (attempt %d/4) — retry in %ds: %s",
                method, path, attempt + 1, wait, exc,
            )
            time.sleep(wait)
            continue

        if resp.status_code in (401, 402):
            raise RuntimeError(
                f"[Instantly] Auth/billing error {resp.status_code} on {method} {path}: "
                f"{resp.text[:200]}"
            )

        if resp.status_code != 429:
            return resp

        wait = 2 ** attempt  # 1s, 2s, 4s, 8s
        logger.warning(
            "[Instantly] 429 rate limit — retrying in %ds (attempt %d/4)", wait, attempt + 1
        )
        time.sleep(wait)

    if last_exc:
        raise RequestException(
            f"[Instantly] Request failed after 4 attempts: {method} {path}"
        ) from last_exc

    return resp  # last 429


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------

# Instantly interest_status / lead status → our engagement_status enum
_LEAD_STATUS_MAP: dict[str, str] = {
    "active":          "active",
    "completed":       "completed",
    "bounced":         "bounced",
    "unsubscribed":    "unsubscribed",
    "interested":      "interested",
    "not interested":  "not_interested",
    "not_interested":  "not_interested",
}

# Instantly analytics field → our campaign_daily_analytics column
_ANALYTICS_FIELD_MAP: dict[str, str] = {
    "emails_sent_count":    "emails_sent",
    "open_count_unique":    "opens",
    "reply_count_unique":   "replies",
    "link_click_count":     "clicks",
    "bounced_count":        "bounces",
    "unsubscribed_count":   "unsubscribes",
    # "interested" count not exposed in analytics endpoint — derived from leads
}


def map_lead_status(instantly_status: str) -> str:
    """Map an Instantly lead/interest status string to our engagement_status."""
    return _LEAD_STATUS_MAP.get((instantly_status or "").lower(), "active")


def map_analytics(raw: dict) -> dict:
    """Map Instantly analytics payload to our snapshot column names."""
    out: dict[str, Any] = {}
    for src, dst in _ANALYTICS_FIELD_MAP.items():
        if src in raw:
            out[dst] = raw[src]
    # open_rate / reply_rate derived from totals when available
    sent = out.get("emails_sent", 0) or 0
    if sent:
        out["open_rate"]  = round((out.get("opens",   0) or 0) / sent, 4)
        out["reply_rate"] = round((out.get("replies", 0) or 0) / sent, 4)
    else:
        out["open_rate"]  = 0.0
        out["reply_rate"] = 0.0
    return out


# ---------------------------------------------------------------------------
# Campaign methods
# ---------------------------------------------------------------------------

def create_campaign(
    name: str,
    schedule: dict,
    sequence_steps: list[dict],
    email_list: Optional[list[str]] = None,
) -> Optional[dict]:
    """
    POST /api/v2/campaigns

    schedule  — {schedules: [{name, timing:{from,to}, days:{}, timezone}],
                  start_date, end_date}
    sequence_steps — [{step_number, subject, body, delay_days}]
    email_list — sending inbox email addresses; campaign sends from these.
                 Omitted → campaign has no mailbox attached and cannot send.

    Returns the created campaign dict (includes 'id'), or None on failure.
    """
    if not _is_configured():
        logger.debug("[Instantly] not configured — create_campaign skipped")
        return None

    payload = {
        "name": name,
        "campaign_schedule": schedule,
        "sequences": [{"steps": sequence_steps}],
    }
    if email_list:
        payload["email_list"] = email_list
    try:
        resp = _request("POST", "/api/v2/campaigns", json=payload)
        resp.raise_for_status()
        return resp.json()
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] create_campaign failed: %s", exc)
        return None


def update_campaign(campaign_id: str, data: dict) -> bool:
    """PATCH /api/v2/campaigns/{id}. Returns True on success."""
    if not _is_configured():
        return False
    try:
        resp = _request("PATCH", f"/api/v2/campaigns/{campaign_id}", json=data)
        resp.raise_for_status()
        return True
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] update_campaign %s failed: %s", campaign_id, exc)
        return False


def delete_campaign(campaign_id: str) -> bool:
    """DELETE /api/v2/campaigns/{id}. Returns True on success."""
    if not _is_configured():
        return False
    try:
        resp = _request("DELETE", f"/api/v2/campaigns/{campaign_id}")
        resp.raise_for_status()
        return True
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] delete_campaign %s failed: %s", campaign_id, exc)
        return False


def activate_campaign(campaign_id: str) -> bool:
    """POST /api/v2/campaigns/{id}/activate. Returns True on success."""
    if not _is_configured():
        return False
    try:
        resp = _request("POST", f"/api/v2/campaigns/{campaign_id}/activate")
        resp.raise_for_status()
        return True
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] activate_campaign %s failed: %s", campaign_id, exc)
        return False


def pause_campaign(campaign_id: str) -> bool:
    """POST /api/v2/campaigns/{id}/pause. Returns True on success."""
    if not _is_configured():
        return False
    try:
        resp = _request("POST", f"/api/v2/campaigns/{campaign_id}/pause")
        resp.raise_for_status()
        return True
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] pause_campaign %s failed: %s", campaign_id, exc)
        return False


def duplicate_campaign(campaign_id: str) -> Optional[dict]:
    """POST /api/v2/campaigns/{id}/duplicate. Returns new campaign dict or None."""
    if not _is_configured():
        return None
    try:
        resp = _request("POST", f"/api/v2/campaigns/{campaign_id}/duplicate")
        resp.raise_for_status()
        return resp.json()
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] duplicate_campaign %s failed: %s", campaign_id, exc)
        return None


# ---------------------------------------------------------------------------
# Lead (contact) methods
# ---------------------------------------------------------------------------

def add_leads(campaign_id: str, leads: list[dict]) -> Optional[dict]:
    """
    POST /api/v2/leads/add  (campaign_id in body, not path)
    leads — list of dicts with at minimum {"email": ...}; also accepts
            first_name, last_name, company_name, phone, personalization vars.
    Up to 1000 leads per call. Server validates emails and skips duplicates.
    Returns the import-summary dict or None on failure.
    """
    if not _is_configured():
        return None
    if not leads:
        return {"leads_created": 0, "leads_skipped": 0}

    payload = {"campaign_id": campaign_id, "leads": leads[:1000]}
    try:
        resp = _request("POST", "/api/v2/leads/add", json=payload)
        resp.raise_for_status()
        return resp.json()
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] add_leads to %s failed: %s", campaign_id, exc)
        return None


def list_leads(
    campaign_id: str,
    cursor: Optional[str] = None,
    limit: int = 100,
) -> Optional[dict]:
    """
    POST /api/v2/leads/list  (POST — complex filter args)
    Returns {"leads": [...], "next_starting_after": cursor|None}.
    Pagination: pass next_starting_after value as cursor for next page.
    """
    if not _is_configured():
        return None
    payload: dict[str, Any] = {
        "campaign": campaign_id,
        "limit": min(limit, 100),
    }
    if cursor:
        payload["starting_after"] = cursor
    try:
        resp = _request("POST", "/api/v2/leads/list", json=payload)
        resp.raise_for_status()
        return resp.json()
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] list_leads for %s failed: %s", campaign_id, exc)
        return None


def remove_lead(lead_id: str) -> bool:
    """DELETE /api/v2/leads/{id}."""
    if not _is_configured():
        return False
    try:
        resp = _request("DELETE", f"/api/v2/leads/{lead_id}")
        resp.raise_for_status()
        return True
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] remove_lead %s failed: %s", lead_id, exc)
        return False


# ---------------------------------------------------------------------------
# Analytics methods
# ---------------------------------------------------------------------------

def get_daily_analytics(
    campaign_ids: list[str],
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    GET /api/v2/campaigns/analytics/daily
    campaign_ids — list of Instantly campaign IDs
    start_date / end_date — 'YYYY-MM-DD'
    Returns list of analytics dicts (one per campaign per day), [] on failure.
    """
    if not _is_configured():
        return []
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "exclude_total_leads_count": "true",
    }
    # Instantly accepts repeated 'id' params for multiple campaigns
    for cid in campaign_ids:
        params.setdefault("id", [])
        if isinstance(params["id"], list):
            params["id"].append(cid)
        else:
            params["id"] = [params["id"], cid]

    try:
        resp = _request("GET", "/api/v2/campaigns/analytics/daily", params=params)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] get_daily_analytics failed: %s", exc)
        return []


def get_analytics_overview(campaign_ids: list[str]) -> list[dict]:
    """
    GET /api/v2/campaigns/analytics/overview
    Returns summary analytics per campaign, [] on failure.
    """
    if not _is_configured():
        return []
    params: dict[str, Any] = {}
    for cid in campaign_ids:
        params.setdefault("id", [])
        if isinstance(params["id"], list):
            params["id"].append(cid)
        else:
            params["id"] = [params["id"], cid]
    try:
        resp = _request("GET", "/api/v2/campaigns/analytics/overview", params=params)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] get_analytics_overview failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Inbox / warmup methods
# ---------------------------------------------------------------------------

def list_accounts() -> list[dict]:
    """GET /api/v2/accounts — list connected sending inboxes."""
    if not _is_configured():
        return []
    try:
        resp = _request("GET", "/api/v2/accounts")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("items", data.get("data", []))
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] list_accounts failed: %s", exc)
        return []


def get_warmup_analytics(account_emails: list[str]) -> list[dict]:
    """
    POST /api/v2/accounts/warmup-analytics
    account_emails — list of inbox email addresses.
    Returns list of warmup-health dicts, [] on failure.
    """
    if not _is_configured():
        return []
    if not account_emails:
        return []
    try:
        resp = _request("POST", "/api/v2/accounts/warmup-analytics",
                        json={"emails": account_emails})
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except RuntimeError:
        raise
    except Exception as exc:
        logger.error("[Instantly] get_warmup_analytics failed: %s", exc)
        return []
