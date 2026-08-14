"""
Idempotent script to create the 7 UTM attribution custom fields in GHL.

Run once per environment:
    PYTHONPATH=. python scripts/ghl_create_utm_fields.py

Prints the field IDs to stdout — copy them into .env as GHL_CF_UTM_* vars.
Skips fields that already exist (matched by name, case-insensitive).
"""

import sys
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

UTM_FIELDS = [
    ("utm_source",   "UTM Source"),
    ("utm_medium",   "UTM Medium"),
    ("utm_campaign", "UTM Campaign"),
    ("utm_content",  "UTM Content"),
    ("utm_term",     "UTM Term"),
    ("landing_path", "Landing Path"),
    ("referrer",     "Referrer"),
]

ENV_VAR_MAP = {
    "utm_source":   "GHL_CF_UTM_SOURCE",
    "utm_medium":   "GHL_CF_UTM_MEDIUM",
    "utm_campaign": "GHL_CF_UTM_CAMPAIGN",
    "utm_content":  "GHL_CF_UTM_CONTENT",
    "utm_term":     "GHL_CF_UTM_TERM",
    "landing_path": "GHL_CF_UTM_LANDING_PATH",
    "referrer":     "GHL_CF_UTM_REFERRER",
}


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Version": "2021-07-28",
    }


def list_custom_fields(api_key: str, location_id: str) -> list[dict]:
    url = f"https://services.leadconnectorhq.com/locations/{location_id}/customFields"
    resp = requests.get(url, headers=_headers(api_key), timeout=15)
    resp.raise_for_status()
    return resp.json().get("customFields", [])


def create_custom_field(api_key: str, location_id: str, name: str, key: str) -> dict:
    url = f"https://services.leadconnectorhq.com/locations/{location_id}/customFields"
    payload = {
        "name": name,
        "dataType": "TEXT",
        "model": "contact",
    }
    resp = requests.post(url, headers=_headers(api_key), json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json().get("customField", {})


def main() -> None:
    from config.settings import get_settings
    settings = get_settings()

    api_key = settings.ghl_api_key
    if not api_key:
        logger.error("GHL_API_KEY not set — cannot create fields")
        sys.exit(1)

    location_id = settings.ghl_location_id
    if not location_id:
        logger.error("GHL_LOCATION_ID not set")
        sys.exit(1)

    raw_key = api_key.get_secret_value()

    logger.info("Fetching existing custom fields from GHL...")
    existing = list_custom_fields(raw_key, location_id)
    existing_by_name = {f["name"].lower(): f for f in existing}
    logger.info("Found %d existing fields", len(existing))

    results: dict[str, str] = {}
    field_keys: dict[str, str] = {}

    for key, label in UTM_FIELDS:
        if label.lower() in existing_by_name:
            field = existing_by_name[label.lower()]
            logger.info("EXISTS  %-20s → %s", label, field["id"])
        else:
            field = create_custom_field(raw_key, location_id, label, key)
            logger.info("CREATED %-20s → %s", label, field["id"])
        results[key] = field["id"]
        # GHL derives its own fieldKey from the display name — capture the
        # real one so Demi builds workflows against the actual key, not a guess.
        field_keys[key] = field.get("fieldKey") or field.get("fieldKeyRaw") or "(unknown)"

    print("\n# Add to .env:")
    for key, field_id in results.items():
        print(f"{ENV_VAR_MAP[key]}={field_id}")

    print("\n# Send to Demi — build GHL workflows against these actual fieldKeys:")
    for key, label in UTM_FIELDS:
        print(f"  {label:<14} spec_name={key:<14} ghl_fieldKey={field_keys[key]}")


if __name__ == "__main__":
    main()
