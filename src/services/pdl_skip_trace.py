"""
PeopleDataLabs skip trace — Tier 3 in the waterfall.

API:  GET https://api.peopledatalabs.com/v5/person/enrich
Cost: $0.28 per successful match (PDL Pro plan)

Returns SkipTraceResult(skipped=True) when PDL_API_KEY is not set.
"""
import time

import requests

from config.settings import get_settings
from src.services.skip_trace_result import SkipTraceResult, compute_confidence
from src.utils.logger import get_logger

logger = get_logger(__name__)

_PDL_ENRICH_URL = "https://api.peopledatalabs.com/v5/person/enrich"
_PDL_COST_CENTS = 28
_REQUEST_DELAY  = 1.0   # PDL standard plan: 1 req/sec


def run_pdl_lookup(
    first_name: str,
    last_name: str,
    street: str,
    city: str,
    state: str,
    zip_code: str,
) -> SkipTraceResult:
    """
    Enrich a single person via PeopleDataLabs.
    Returns skipped=True when PDL_API_KEY is not configured.
    """
    settings = get_settings()

    if not settings.pdl_api_key:
        return SkipTraceResult(
            provider="pdl", success=False, skipped=True, confidence=0.0, cost_cents=0,
        )

    params = {
        "first_name":     first_name,
        "last_name":      last_name,
        "street_address": street,
        "locality":       city,
        "region":         state,
        "postal_code":    zip_code,
        "pretty":         False,
    }
    headers = {
        "X-Api-Key":    settings.pdl_api_key.get_secret_value(),
        "Content-Type": "application/json",
    }

    try:
        resp = requests.get(_PDL_ENRICH_URL, params=params, headers=headers, timeout=30)

        if resp.status_code == 404:
            # PDL returns 404 when no match — not an error
            return SkipTraceResult(
                provider="pdl", success=False, skipped=False, confidence=0.0, cost_cents=0,
            )
        if resp.status_code == 401:
            logger.error("[PDL] Invalid API key — check PDL_API_KEY in .env")
            return SkipTraceResult(
                provider="pdl", success=False, skipped=False, confidence=0.0,
                cost_cents=0, error="invalid_api_key",
            )

        resp.raise_for_status()
        data = resp.json()

        phones = data.get("phone_numbers") or []
        mobile, landline = None, None
        for ph in phones:
            num  = ph.get("number") or ph.get("e164_format")
            kind = (ph.get("type") or "").lower()
            if not num:
                continue
            if "mobile" in kind or "cell" in kind:
                mobile = mobile or num
            else:
                landline = landline or num

        emails = data.get("emails") or []
        email  = emails[0].get("address") if emails else None

        locs    = data.get("locations") or []
        mailing = None
        if locs:
            a = locs[0]
            mailing = ", ".join(
                p for p in [
                    a.get("street_address"), a.get("locality"),
                    a.get("region"), a.get("postal_code"),
                ] if p
            )

        confidence = compute_confidence(mobile, landline, email, mailing)
        success    = bool(mobile or landline or email)

        return SkipTraceResult(
            provider="pdl",
            success=success,
            skipped=False,
            confidence=confidence,
            cost_cents=_PDL_COST_CENTS if success else 0,
            mobile_phone=mobile,
            landline=landline,
            email=email,
            mailing_address=mailing,
            raw_metadata={"pdl_likelihood": data.get("likelihood")},
        )

    except requests.RequestException as exc:
        logger.error("[PDL] Request failed: %s", exc)
        return SkipTraceResult(
            provider="pdl", success=False, skipped=False, confidence=0.0,
            cost_cents=0, error=str(exc),
        )
    finally:
        time.sleep(_REQUEST_DELAY)
