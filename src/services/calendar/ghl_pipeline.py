"""WP-GL-5: push gate outcomes into Next Deal Lending's "Booked Calls" GHL
pipeline — a passing gate + successful booking to the Booked stage, a
failing gate to the Nurture stage. Both stages live in one pipeline
(confirmed against the real account 2026-10-05, pipeline id
OzuRH2ELgAQJZy3bBVDr), not two separate pipelines.

Uses LENDING_GHL_API_KEY/LENDING_GHL_LOCATION_ID — Next Deal Lending's own
sub-account, not the generic GHL_API_KEY/GHL_LOCATION_ID that
src/services/ghl_webhook.py uses for Bay Street Capital's distressed-
property lead scoring. Same reasoning as GHLCalendarClient: reusing the
generic pair would either push into the wrong GHL account or silently
repoint Bay Street's existing GHL usage.

Never raises — a failed pipeline push must not undo an already-stored gate
result or an already-committed booking. Callers wrap this and log/alert on
failure themselves (gate.py's enqueue_nurture already pages EXCEPTIONS).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests
from requests.exceptions import ConnectionError, RequestException, Timeout

logger = logging.getLogger(__name__)

_GHL_BASE = "https://services.leadconnectorhq.com"
_GHL_API_VERSION = "2021-07-28"
_DEFAULT_TIMEOUT = 15


def _settings():
    from config.settings import get_settings

    return get_settings()


def _is_configured() -> bool:
    s = _settings()
    return bool(
        s.lending_ghl_api_key
        and s.lending_ghl_location_id
        and s.lending_ghl_pipeline_id
    )


def _headers() -> dict:
    key = _settings().lending_ghl_api_key.get_secret_value()
    return {
        "Authorization": f"Bearer {key}",
        "Version": _GHL_API_VERSION,
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, **kwargs) -> requests.Response:
    """Same retry-on-429 pattern as ghl_client.py / ghl_webhook.py."""
    kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
    url = f"{_GHL_BASE}{path}"
    last_exc: Optional[Exception] = None
    resp: Optional[requests.Response] = None

    for attempt in range(4):
        try:
            resp = requests.request(method, url, headers=_headers(), **kwargs)
        except (ConnectionError, Timeout) as exc:
            last_exc = exc
            wait = 2**attempt
            logger.warning(
                "lending_ghl_pipeline: network error on %s %s (attempt %d/4) — retrying in %ds",
                method, path, attempt + 1, wait,
            )
            time.sleep(wait)
            continue

        if resp.status_code != 429:
            return resp

        wait = 2**attempt
        time.sleep(wait)

    if last_exc:
        raise RequestException(
            f"lending_ghl_pipeline request failed after 4 attempts: {method} {path}"
        ) from last_exc
    return resp


def _upsert_contact(*, phone: Optional[str], email: Optional[str], first_name: Optional[str]) -> Optional[str]:
    """Create or find the GHL contact for this phone/email. Returns contact id or None.

    Needs at least one of phone/email — GHL contacts are deduplicated by
    either. Returns None (not raises) on any failure or if neither is given.
    """
    if not phone and not email:
        logger.warning("lending_ghl_pipeline: no phone or email — cannot resolve a GHL contact")
        return None

    location_id = _settings().lending_ghl_location_id
    payload: dict[str, Any] = {"locationId": location_id}
    if phone:
        payload["phone"] = phone
    if email:
        payload["email"] = email
    if first_name:
        payload["firstName"] = first_name

    try:
        resp = _request("POST", "/contacts/", json=payload)
        if resp.status_code == 400:
            # GHL's duplicate-prevention response carries the existing id.
            dup_id = (resp.json().get("meta") or {}).get("contactId")
            if dup_id:
                return dup_id
        if not resp.ok:
            logger.warning(
                "lending_ghl_pipeline: contact upsert HTTP %d: %s",
                resp.status_code, resp.text[:300],
            )
            return None
        return (resp.json().get("contact") or {}).get("id")
    except Exception:
        logger.exception("lending_ghl_pipeline: contact upsert failed")
        return None


def _find_opportunity_for_contact(contact_id: str) -> Optional[str]:
    try:
        resp = _request(
            "GET", "/opportunities/search",
            params={"location_id": _settings().lending_ghl_location_id, "contact_id": contact_id},
        )
        if not resp.ok:
            return None
        opps = resp.json().get("opportunities", [])
        return opps[0].get("id") if opps else None
    except Exception:
        logger.exception("lending_ghl_pipeline: opportunity search failed")
        return None


def push_to_stage(
    *,
    phone: Optional[str],
    email: Optional[str],
    first_name: Optional[str],
    stage_id: str,
    opportunity_name: str,
) -> bool:
    """Create/update an opportunity for this contact in the given pipeline stage.

    Never raises. Returns True only on a confirmed successful push — callers
    must not assume a push happened just because this was called.
    """
    if not _is_configured():
        logger.warning(
            "lending_ghl_pipeline: not configured (LENDING_GHL_API_KEY/"
            "LOCATION_ID/PIPELINE_ID) — push skipped"
        )
        return False

    contact_id = _upsert_contact(phone=phone, email=email, first_name=first_name)
    if contact_id is None:
        return False

    settings = _settings()
    payload = {
        "pipelineId": settings.lending_ghl_pipeline_id,
        "locationId": settings.lending_ghl_location_id,
        "name": opportunity_name,
        "pipelineStageId": stage_id,
        "contactId": contact_id,
        "status": "open",
    }

    try:
        existing_opp_id = _find_opportunity_for_contact(contact_id)
        if existing_opp_id:
            put_payload = {k: v for k, v in payload.items() if k not in ("locationId", "contactId")}
            resp = _request("PUT", f"/opportunities/{existing_opp_id}", json=put_payload)
        else:
            resp = _request("POST", "/opportunities/", json=payload)
        if not resp.ok:
            logger.warning(
                "lending_ghl_pipeline: opportunity upsert HTTP %d: %s",
                resp.status_code, resp.text[:300],
            )
            return False
        return True
    except Exception:
        logger.exception("lending_ghl_pipeline: opportunity upsert failed")
        return False


def push_booking_to_booked_stage(
    *, phone: Optional[str], email: Optional[str], first_name: Optional[str], opportunity_name: str,
) -> bool:
    stage_id = _settings().lending_ghl_stage_booked
    if not stage_id:
        logger.warning("lending_ghl_pipeline: LENDING_GHL_STAGE_BOOKED not set — push skipped")
        return False
    return push_to_stage(
        phone=phone, email=email, first_name=first_name,
        stage_id=stage_id, opportunity_name=opportunity_name,
    )


def push_gate_fail_to_nurture_stage(
    *, phone: Optional[str], email: Optional[str], first_name: Optional[str], opportunity_name: str,
) -> bool:
    stage_id = _settings().lending_ghl_stage_nurture
    if not stage_id:
        logger.warning("lending_ghl_pipeline: LENDING_GHL_STAGE_NURTURE not set — push skipped")
        return False
    return push_to_stage(
        phone=phone, email=email, first_name=first_name,
        stage_id=stage_id, opportunity_name=opportunity_name,
    )
