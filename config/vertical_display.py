"""
Vertical display metadata — single source of truth for all human-readable
vertical labels, descriptions, and defaults used across services and tasks.

Import from here instead of defining inline dicts in individual files.
These are PRODUCT VERTICALS (categories inside the contractor lead product),
NOT ICP channels. See config/icp_channels.py for ICP definitions.
"""

VERTICAL_DISPLAY: dict[str, dict] = {
    "roofing": {
        "label": "Roofing",
        "label_plural": "Roofers",
        "clay_description": "roofing contractor",
        "sms_copy": "free roofing leads",
    },
    "restoration": {
        "label": "Restoration",
        "label_plural": "Restorers",
        "clay_description": "water damage restoration contractor",
        "sms_copy": "free restoration leads",
    },
    "public_adjusters": {
        "label": "Public Adjusters",
        "label_plural": "Public Adjusters",
        "clay_description": "public adjuster insurance",
        "sms_copy": "free public adjuster leads",
    },
    "wholesalers": {
        "label": "Wholesalers",
        "label_plural": "Wholesalers",
        "clay_description": "real estate investor wholesaler",
        "sms_copy": "free wholesale deal leads",
    },
    "fix_flip": {
        "label": "Fix & Flip",
        "label_plural": "Fix & Flip Investors",
        "clay_description": "house flipper real estate investor",
        "sms_copy": "free fix-and-flip leads",
    },
    "attorneys": {
        "label": "Attorneys",
        "label_plural": "Attorneys",
        "clay_description": "real estate attorney",
        "sms_copy": "free real estate attorney leads",
    },
}

# Human-readable label dict — direct replacement for all `{vertical: label}` inline dicts.
VERTICAL_LABELS: dict[str, str] = {k: v["label"] for k, v in VERTICAL_DISPLAY.items()}

# Default vertical for fallback paths. Centralizes all `or "roofing"` patterns.
DEFAULT_VERTICAL = "roofing"


def get_label(vertical: str) -> str:
    """Return human-readable label for a vertical, or the raw key if unknown."""
    return VERTICAL_DISPLAY.get(vertical, {}).get("label", vertical)


def get_clay_description(vertical: str) -> str:
    """Return Clay enrichment description for a vertical."""
    return VERTICAL_DISPLAY.get(vertical, {}).get("clay_description", vertical)
