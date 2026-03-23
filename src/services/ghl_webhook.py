"""
GoHighLevel (GHL) CRM integration.

Fires when a lead scores >= ROUTING_THRESHOLDS["daily"] (60) after scoring.
Creates or updates a GHL Contact, then upserts an Opportunity in the
configured pipeline with the urgency-mapped stage.

Configuration (all via environment variables, see config/settings.py):
    GHL_API_KEY           — GHL private API key (v2)
    GHL_LOCATION_ID       — Location/sub-account ID
    GHL_PIPELINE_ID       — Pipeline ID for distressed-property leads
    GHL_STAGE_IMMEDIATE   — Stage ID for Immediate (SMS) tier (score ≥ 80)
    GHL_STAGE_HIGH        — Stage ID for High (daily email) tier (score 60–79)
    GHL_STAGE_MEDIUM      — Stage ID for Medium (weekly digest) tier (score 40–59)

If GHL_API_KEY or GHL_LOCATION_ID is not set the module is a no-op —
no exception is raised, a debug log line is emitted instead.

GHL API v2 reference:
    https://highlevel.stoplight.io/docs/integrations/
"""

import logging
import time
from typing import Dict, List, Optional

import requests
from requests.exceptions import ConnectionError, Timeout, RequestException

from config.settings import settings
from config.scoring import ROUTING_THRESHOLDS

logger = logging.getLogger(__name__)

_GHL_BASE = "https://services.leadconnectorhq.com"
_GHL_HEADERS_BASE = {
    "Version": "2021-07-28",
    "Content-Type": "application/json",
}
_DEFAULT_TIMEOUT = 15  # seconds

# Minimum score to push a lead to GHL — Medium stage is configured so include it
_MIN_SCORE_TO_PUSH = ROUTING_THRESHOLDS["weekly"]  # 40


def _ghl_request(method: str, url: str, **kwargs) -> requests.Response:
    """
    Wrapper around requests that retries on 429 with exponential backoff.
    Adds a 0.5s base delay between every call to stay under rate limits.

    Always sets a timeout — never hangs indefinitely.
    Raises RequestException if all retries are exhausted due to network errors.
    """
    kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
    time.sleep(0.5)  # baseline throttle — ~2 req/s sustained

    last_exc: Optional[Exception] = None
    resp: Optional[requests.Response] = None

    for attempt in range(4):
        try:
            resp = requests.request(method, url, **kwargs)
        except (ConnectionError, Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            logger.warning(
                "[GHL] Network error on %s %s (attempt %d/4) — retrying in %ds: %s",
                method, url, attempt + 1, wait, exc,
            )
            time.sleep(wait)
            continue

        if resp.status_code != 429:
            return resp

        wait = 2 ** attempt  # 1s, 2s, 4s, 8s
        logger.debug("[GHL] 429 rate limit — retrying in %ds (attempt %d/4)", wait, attempt + 1)
        time.sleep(wait)

    # Exhausted retries
    if last_exc:
        raise RequestException(
            f"GHL request failed after 4 attempts: {method} {url}"
        ) from last_exc

    # resp is the last 429 response after retry exhaustion
    return resp


def _is_configured() -> bool:
    """Return True only if the minimum required GHL env vars are present."""
    return bool(settings.ghl_api_key is not None and settings.ghl_location_id)


def _headers() -> Dict[str, str]:
    return {
        **_GHL_HEADERS_BASE,
        "Authorization": f"Bearer {settings.ghl_api_key.get_secret_value()}",
    }


def _stage_id_for_urgency(urgency: str) -> Optional[str]:
    """Map urgency level to configured GHL pipeline stage ID."""
    mapping = {
        "Immediate": settings.ghl_stage_immediate,
        "High":      settings.ghl_stage_high,
        "Medium":    settings.ghl_stage_medium,
    }
    return mapping.get(urgency)


def _top_signals(score_data: Dict, n: int = 2) -> List[str]:
    """Return the top N signal types by vertical weight contribution."""
    return score_data.get("distress_types", [])[:n]


def _best_vertical(score_data: Dict) -> str:
    """Return the vertical name with the highest score."""
    vs = score_data.get("vertical_scores", {})
    if not vs:
        return "unknown"
    return max(vs, key=vs.get)


def _find_contact_by_parcel(parcel_id: str) -> Optional[str]:
    """
    Search GHL for an existing contact by parcel ID (custom field tag search).
    Returns the contact ID if found, None otherwise.
    """
    try:
        resp = _ghl_request(
            "GET",
            f"{_GHL_BASE}/contacts/search",
            headers=_headers(),
            params={
                "locationId": settings.ghl_location_id,
                "query": parcel_id,
            },
        )
        resp.raise_for_status()
        contacts = resp.json().get("contacts", [])
        for c in contacts:
            for cf in c.get("customFields", []):
                if cf.get("value") == parcel_id:
                    return c["id"]
    except RequestException as exc:
        logger.debug("[GHL] contact search by parcel failed (network): %s", exc)
    except Exception:
        logger.debug("[GHL] contact search by parcel failed", exc_info=True)
    return None


def _upsert_contact(score_data: Dict) -> Optional[str]:
    """
    Create or update a GHL contact from score_data.
    Returns the GHL contact ID, or None on failure.
    """
    phone = score_data.get("owner_phone")
    email = score_data.get("owner_email")
    owner_name = score_data.get("owner_name") or ""
    address = score_data.get("address") or ""
    summaries = score_data.get("signal_summaries") or {}

    name_parts = owner_name.strip().split(None, 1)
    first_name = name_parts[0] if name_parts else "Unknown"
    last_name = name_parts[1] if len(name_parts) > 1 else "Owner"

    payload = {
        "locationId":  settings.ghl_location_id,
        "firstName":   first_name,
        "lastName":    last_name,
        "address1":    address,
        "city":        score_data.get("city") or "",
        "state":       score_data.get("state") or "",
        "postalCode":  score_data.get("zip") or "",
        "customFields": [
            {"id": "CyqT2fZ2VS9hANAKqDFB", "value": str(score_data.get("parcel_id") or "")},
            {"id": "eApA0zTDLatkrEjiRsSj", "value": str(score_data.get("final_cds_score") or 0)},
            {"id": "x2gdIlD8v1mMTt1kZKEI", "value": score_data.get("lead_tier") or ""},
            {"id": "3AHU9KWEyXaDNKFy3azC", "value": score_data.get("urgency_level") or ""},
            {"id": "QrohTQclVzyGNdeX31K9", "value": _best_vertical(score_data)},
            {"id": "9biCuTixgCWZ6HZcemig", "value": ", ".join(_top_signals(score_data))},
            {"id": "minc73GThMkfiTp6cXCv", "value": str(score_data.get("signal_count") or 0)},
            # Signal summaries
            {"id": "MqgcANWDFOsUsXVuFCxD", "value": summaries.get("code_violations_summary", "")},
            {"id": "8uWDAuGPpoJJ4bJKSPqL", "value": summaries.get("judgment_summary", "")},
            {"id": "LkW0sJXPscLl9nAofohO", "value": summaries.get("mechanics_lien_summary", "")},
            {"id": "WYyLBiBNJRG2EW1XYWW7", "value": summaries.get("tax_lien_summary", "")},
            {"id": "V7pfFTlC2ffDpFTo8rvz", "value": summaries.get("hoa_lien_summary", "")},
            {"id": "6ZcnH00wLVaE0xJ9ko3Z", "value": summaries.get("code_lien_summary", "")},
            {"id": "bgoi3sr6OZYjkXqPC0AB", "value": summaries.get("foreclosure_summary", "")},
            {"id": "wuaaHejZ0gvnhEy5EFHf", "value": summaries.get("tax_delinquency_summary", "")},
            {"id": "ietB3MZl1MFXx3O9zRlP", "value": summaries.get("probate_summary", "")},
            {"id": "X7GUuVaObzf5sLNMEbNO", "value": summaries.get("eviction_summary", "")},
            {"id": "LGeIFWXCBS8Ztcj9wqog", "value": summaries.get("bankruptcy_summary", "")},
            {"id": "RHVGRnEhuFdS6rhygOkq", "value": summaries.get("deed_summary", "")},
            {"id": "oJiNVlnJYVQAjpFQ1YG8", "value": summaries.get("permit_summary", "")},
            # Owner details
            {"id": "LIr3EBZZUGkqVppjzDNx", "value": score_data.get("owner_type") or ""},
            {"id": "F4O3QfIeQPbJ6leRjBGY", "value": score_data.get("absentee_status") or ""},
            {"id": "cGlyUQqIfYgW0r2qjnhE", "value": score_data.get("mailing_address") or ""},
            {"id": "BSa356i4zEcbC7Yv3pYj", "value": str(score_data.get("ownership_years") or "")},
            # Financial
            {"id": "BxALCQ89P4O37wnfsrtj", "value": f"${score_data['assessed_value_mkt']:,.0f}" if score_data.get("assessed_value_mkt") else ""},
            {"id": "LsLyMSDr1ihsWx5Avv1S", "value": "Yes" if score_data.get("homestead_exempt") else "No"},
            {"id": "MJMsPyM1AKTUTAf0ZTKF", "value": f"${score_data['est_equity']:,.0f}" if score_data.get("est_equity") else ""},
            {"id": "AjqzIAyBeWuXahL5KPZl", "value": f"{score_data['equity_pct']:.1f}%" if score_data.get("equity_pct") else ""},
            {"id": "wKZ3Wr7Mewgk6yyUKJ9P", "value": f"${score_data['last_sale_price']:,.0f}" if score_data.get("last_sale_price") else ""},
            {"id": "F4oNtv1KtWjXj2Kr88pg", "value": score_data.get("last_sale_date") or ""},
            # Property specs
            {"id": "yeAsHE59UYVJ4vlM9QAP", "value": str(int(score_data["sq_ft"])) if score_data.get("sq_ft") else ""},
            {"id": "aIL1zpBaFesqbgPHGrNq", "value": str(score_data.get("beds") or "")},
            {"id": "E0ukBkLgpb9WRarktAzN", "value": str(score_data.get("baths") or "")},
            {"id": "TvsOZjBAtpftOZWtxUFQ", "value": str(score_data.get("year_built") or "")},
            {"id": "U4AVIb21Q1ScMTEKtZuD", "value": f"{score_data['lot_size']:.2f} acres" if score_data.get("lot_size") else ""},
        ],
        "tags": [
            f"cds-{score_data.get('lead_tier', '').lower().replace(' ', '-')}",
            f"vertical-{_best_vertical(score_data)}",
            "distressed-property",
            *( ["synthflow-suppress"] if score_data.get("lead_tier") == "Silver" else ["synthflow-eligible"] ),
        ],
    }
    if phone:
        payload["phone"] = phone
    if email:
        payload["email"] = email

    # Dedup priority: 1) stored GHL contact ID on property, 2) parcel ID search
    parcel_id = score_data.get("parcel_id") or ""
    existing_id = score_data.get("ghl_contact_id") or (
        _find_contact_by_parcel(parcel_id) if parcel_id else None
    )

    try:
        if existing_id:
            put_payload = {k: v for k, v in payload.items() if k != "locationId"}
            resp = _ghl_request(
                "PUT",
                f"{_GHL_BASE}/contacts/{existing_id}",
                headers=_headers(),
                json=put_payload,
            )
        else:
            resp = _ghl_request(
                "POST",
                f"{_GHL_BASE}/contacts/",
                headers=_headers(),
                json=payload,
            )
            # GHL returns 400 with meta.contactId when duplicate prevention is on
            if resp.status_code == 400:
                try:
                    dup_id = resp.json().get("meta", {}).get("contactId")
                except Exception:
                    dup_id = None
                if dup_id:
                    logger.debug("[GHL] duplicate contact detected (%s), retrying as PUT", dup_id)
                    put_payload = {k: v for k, v in payload.items() if k != "locationId"}
                    resp = _ghl_request(
                        "PUT",
                        f"{_GHL_BASE}/contacts/{dup_id}",
                        headers=_headers(),
                        json=put_payload,
                    )
                    existing_id = dup_id

        if not resp.ok:
            logger.warning(
                "[GHL] contact upsert HTTP %d for %s: %s",
                resp.status_code, score_data.get("parcel_id"), resp.text[:500],
            )
        resp.raise_for_status()
        contact = resp.json().get("contact", {})
        return contact.get("id") or existing_id

    except RequestException as exc:
        logger.warning(
            "[GHL] contact upsert network error for %s: %s",
            score_data.get("parcel_id"), exc,
        )
        return None
    except Exception:
        logger.warning(
            "[GHL] contact upsert failed for %s",
            score_data.get("parcel_id"),
            exc_info=True,
        )
        return None


def _find_opportunity_for_contact(contact_id: str) -> Optional[str]:
    """Return existing opportunity ID for a contact, or None."""
    try:
        resp = _ghl_request(
            "GET",
            f"{_GHL_BASE}/opportunities/search",
            headers=_headers(),
            params={"location_id": settings.ghl_location_id, "contact_id": contact_id},
        )
        resp.raise_for_status()
        opps = resp.json().get("opportunities", [])
        if opps:
            return opps[0].get("id")
    except RequestException as exc:
        logger.debug("[GHL] opportunity search network error: %s", exc)
    except Exception:
        logger.debug("[GHL] opportunity search failed", exc_info=True)
    return None


def _upsert_opportunity(contact_id: str, score_data: Dict) -> bool:
    """
    Create or update a GHL pipeline opportunity linked to the contact.
    Returns True on success.
    """
    if not settings.ghl_pipeline_id:
        logger.debug("[GHL] GHL_PIPELINE_ID not set — skipping opportunity creation")
        return False

    stage_id = _stage_id_for_urgency(score_data.get("urgency_level", ""))
    if not stage_id:
        logger.debug(
            "[GHL] No stage configured for urgency '%s' — skipping opportunity",
            score_data.get("urgency_level"),
        )
        return False

    parcel_id = score_data.get("parcel_id") or score_data.get("property_id")
    best_v = _best_vertical(score_data)
    title = (
        f"{score_data.get('address', 'Unknown Address')} "
        f"| {score_data.get('lead_tier')} "
        f"| {best_v.title()}"
    )

    payload = {
        "pipelineId":      settings.ghl_pipeline_id,
        "locationId":      settings.ghl_location_id,
        "name":            title,
        "pipelineStageId": stage_id,
        "contactId":       contact_id,
        "monetaryValue":   score_data.get("final_cds_score") or 0,
        "status":          "open",
    }

    try:
        existing_opp_id = _find_opportunity_for_contact(contact_id)
        if existing_opp_id:
            put_payload = {k: v for k, v in payload.items() if k not in ("locationId", "contactId")}
            resp = _ghl_request(
                "PUT",
                f"{_GHL_BASE}/opportunities/{existing_opp_id}",
                headers=_headers(),
                json=put_payload,
            )
        else:
            resp = _ghl_request(
                "POST",
                f"{_GHL_BASE}/opportunities/",
                headers=_headers(),
                json=payload,
            )
        resp.raise_for_status()
        return True

    except RequestException as exc:
        logger.warning("[GHL] opportunity upsert network error for %s: %s", parcel_id, exc)
        return False
    except Exception:
        logger.warning("[GHL] opportunity upsert failed for %s", parcel_id, exc_info=True)
        return False


def push_lead_to_ghl(score_data: Dict) -> Optional[str]:
    """
    Push a scored lead to GHL CRM.

    Called from src/tasks/ghl_sync.py for every pending_sync lead.

    Returns the GHL contact_id string on success (truthy), or None on failure/skip.
    The caller (ghl_sync) is responsible for persisting contact_id back to the
    Property row in a batch commit — this function is purely API I/O.
    Raises nothing — all errors are logged internally.
    """
    if not _is_configured():
        logger.debug("[GHL] Not configured — skipping CRM push")
        return None

    final_score = score_data.get("final_cds_score", 0)
    if final_score < _MIN_SCORE_TO_PUSH:
        return None

    parcel_id = score_data.get("parcel_id")
    logger.info(
        "[GHL] Pushing lead: %s | score=%s | %s | %s",
        parcel_id, final_score, score_data.get("lead_tier"), score_data.get("urgency_level"),
    )

    contact_id = _upsert_contact(score_data)
    if not contact_id:
        logger.warning("[GHL] Failed to upsert contact for %s — opportunity skipped", parcel_id)
        return None

    # NOTE: the DB write (gohighlevel_contact_id / sync_status / last_crm_sync) has been
    # removed from this function. Previously it opened a separate session and committed
    # an individual UPDATE per lead — with thousands of leads this created thousands of
    # long-lived transactions on the properties table that blocked all scraper sessions.
    # The DB write is now handled in bulk by src/tasks/ghl_sync.py (100 rows per commit).

    _upsert_opportunity(contact_id, score_data)
    logger.info("[GHL] Lead pushed successfully: %s", parcel_id)
    return contact_id


# ---------------------------------------------------------------------------
# M1 — Subscriber pipeline (stages 5 + 7)
# ---------------------------------------------------------------------------

def push_subscriber_to_ghl(subscriber, stage: Optional[int], tags: Optional[List[str]] = None) -> bool:
    """
    Create or update a GHL contact for a subscriber and optionally move them
    to a pipeline stage and/or apply tags.

    stage=5 → Paid Subscriber (GHL_STAGE_PAID_SUBSCRIBER)
    stage=7 → Churned (GHL_STAGE_CHURNED)
    stage=None → tag-only update (e.g. payment_failed)

    Returns True on success, False on any failure.
    Raises nothing — all errors are logged internally.
    """
    if not _is_configured():
        return False

    contact_payload: Dict = {
        "locationId": settings.ghl_location_id,
        "email": subscriber.email or "",
        "name": subscriber.name or "",
        "tags": tags or [],
    }

    contact_id = subscriber.ghl_contact_id

    try:
        if contact_id:
            put_payload = {k: v for k, v in contact_payload.items() if k != "locationId"}
            resp = _ghl_request(
                "PUT",
                f"{_GHL_BASE}/contacts/{contact_id}",
                headers=_headers(),
                json=put_payload,
            )
        else:
            resp = _ghl_request(
                "POST",
                f"{_GHL_BASE}/contacts/",
                headers=_headers(),
                json=contact_payload,
            )
            contact_id = resp.json().get("contact", {}).get("id")
            if contact_id:
                subscriber.ghl_contact_id = contact_id

        if not resp.ok:
            logger.error(
                "[GHL] Subscriber contact upsert failed %d: %s",
                resp.status_code, resp.text[:200],
            )
            return False

    except RequestException as exc:
        logger.error(
            "[GHL] Network error upserting subscriber contact (id=%s): %s",
            getattr(subscriber, "id", "?"), exc,
        )
        return False
    except Exception:
        logger.error(
            "[GHL] Unexpected error upserting subscriber contact (id=%s)",
            getattr(subscriber, "id", "?"),
            exc_info=True,
        )
        return False

    if not contact_id:
        logger.error(
            "[GHL] No contact_id after subscriber upsert (id=%s)",
            getattr(subscriber, "id", "?"),
        )
        return False

    # ── Move to pipeline stage ────────────────────────────────────────────
    if stage is not None:
        stage_id_map = {
            5: settings.ghl_stage_paid_subscriber,
            7: settings.ghl_stage_churned,
        }
        stage_id = stage_id_map.get(stage)

        if stage_id and settings.ghl_pipeline_id:
            opp_payload = {
                "pipelineId":      settings.ghl_pipeline_id,
                "locationId":      settings.ghl_location_id,
                "pipelineStageId": stage_id,
                "contactId":       contact_id,
                "name":            f"Subscriber — {subscriber.tier} / {subscriber.vertical}",
                "status":          "open" if stage == 5 else "lost",
                "monetaryValue":   {"starter": 600, "pro": 1100, "dominator": 2000}.get(
                    subscriber.tier, 0
                ),
            }
            try:
                existing_opp_id = _find_opportunity_for_contact(contact_id)
                if existing_opp_id:
                    put_payload = {k: v for k, v in opp_payload.items() if k not in ("locationId", "contactId")}
                    opp_resp = _ghl_request(
                        "PUT",
                        f"{_GHL_BASE}/opportunities/{existing_opp_id}",
                        headers=_headers(),
                        json=put_payload,
                    )
                else:
                    opp_resp = _ghl_request(
                        "POST",
                        f"{_GHL_BASE}/opportunities/",
                        headers=_headers(),
                        json=opp_payload,
                    )
                opp_resp.raise_for_status()
            except RequestException as exc:
                logger.warning(
                    "[GHL] Network error upserting subscriber opportunity (stage=%s): %s",
                    stage, exc,
                )
            except Exception:
                logger.warning(
                    "[GHL] Failed to upsert subscriber opportunity (stage=%s)",
                    stage,
                    exc_info=True,
                )

    logger.info(
        "[GHL] Subscriber pushed: contact=%s stage=%s tags=%s",
        contact_id, stage, tags,
    )
    return True
