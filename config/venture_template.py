"""
Venture configuration template — CLONE-v2.2 / CL3.

A "venture" is one business running on this agent fleet: its own Relay
sending identity (Slack approval channel, Instantly campaign, sender
address, send window, daily ceiling, kill switch) and its own geography
(state, bankruptcy court, the set of counties it scrapes). Venture #1 is
`hillsborough_distress` — the Hillsborough County distressed-property
business this platform was built for.

This module is the copy-and-fill artifact. Nothing here reads the DB:
VENTURE_TEMPLATE is the shape, validate_venture_config() is the gate, and
src/services/venture_provisioning.py is what turns a filled-in copy into
`ventures` + `counties` + `county_sources` rows. The resolver that reads
those rows back at runtime is src/utils/venture_config.py.

Usage:
    python -m src.services.venture_provisioning --emit-template > venture2.json
    # edit venture2.json
    python -m src.services.venture_provisioning --config venture2.json --dry-run
    python -m src.services.venture_provisioning --config venture2.json --apply

See docs/venture-onboarding.md for the full runbook.
"""
from __future__ import annotations

from typing import Any

# Venture #1 — the key every pre-CL3 row is backfilled to. Every Python
# default in the Relay/county stack resolves to this, so a deployment that
# never creates a second venture behaves exactly as it did before CL3.
# The DB carries the same literal as its column server_default; this
# constant is the single source of truth for application code.
DEFAULT_VENTURE_KEY = "hillsborough_distress"

# Kill-switch feature key a venture uses unless it opts into its own. Shared
# with src/services/relay/config.py (which re-exports it as
# KILL_SWITCH_FEATURE, Relay's long-standing public name for it) and with
# src/utils/venture_config.py's env fallback, so the string is declared once.
DEFAULT_KILL_SWITCH_FEATURE = "relay_global"

# Signal types a venture needs `county_sources` rows for before its pipeline
# is functional. Derived from what src/utils/county_config.py:_load_from_db()
# actually reads when building the `urls` sub-dict every scraper consumes —
# a venture missing one of these has a scraper that will resolve an empty URL.
REQUIRED_SIGNAL_TYPES: tuple[str, ...] = (
    "foreclosures",
    "tax_delinquency",
    "violations",
    "permits",
    "liens",
    "court_records",
    "master_data",
)

# Fields that must be present and non-empty in any venture config.
_REQUIRED_FIELDS: tuple[str, ...] = (
    "venture_key",
    "display_name",
    "brand_name",
    "state",
    "bankruptcy_court_code",
    "relay_send_window_timezone",
)

# ── The template ─────────────────────────────────────────────────────────────
# Copy this whole dict, change every value marked CHANGE ME, and leave the
# rest unless you have a reason. Emitted as JSON by
# `python -m src.services.venture_provisioning --emit-template`.

VENTURE_TEMPLATE: dict[str, Any] = {
    # Identity ---------------------------------------------------------------
    # CHANGE ME — stable slug, lowercase, no spaces. Never reused, never
    # renamed: it is the join key on counties, relay_approval_queue and
    # golden_close_chains.
    "venture_key": "venture_two",
    # CHANGE ME — human label for admin UIs and logs.
    "display_name": "Venture Two",
    # CHANGE ME — the name that appears in the CAN-SPAM footer of every
    # Relay email this venture sends. Must be the real operating entity.
    "brand_name": "Venture Two",
    # CHANGE ME — postal address for the same footer (CAN-SPAM requires one).
    # Leave null to fall back to settings.company_postal_address.
    "postal_address": None,

    # Geography --------------------------------------------------------------
    # CHANGE ME IF NOT FLORIDA — two-letter state code. Read by the
    # flood/insurance/storm scrapers for NWS + FEMA lookups.
    "state": "FL",
    # CHANGE ME IF NOT FLORIDA MIDDLE DISTRICT — CourtListener bankruptcy
    # court code (e.g. 'flmb' = Florida Middle Bankruptcy).
    "bankruptcy_court_code": "flmb",
    # Division prefix used when a county has no bankruptcy_division of its
    # own (e.g. '8:' = Tampa division).
    "default_bankruptcy_division": "8:",
    # CHANGE ME — the county whose county_sources rows every new county in
    # this venture clones from. Leave null and each county's sources must be
    # created by hand. For a brand-new venture in a new state, set this after
    # the first county's sources are built and verified.
    "template_county_id": None,

    # Relay — approval surface ----------------------------------------------
    # CHANGE ME — this venture's own Slack channel. Sharing one channel
    # across ventures makes approvals ambiguous.
    "relay_slack_channel": "",
    # Slack user IDs allowed to approve/reject this venture's items.
    "relay_approvers": [],

    # Relay — email channel --------------------------------------------------
    # CHANGE ME — run `python -m src.services.relay --setup-email-channel`
    # once per venture and paste the printed campaign id here. Two ventures
    # sharing one campaign would cross-contaminate Instantly's
    # duplicate-contact guard.
    "relay_instantly_campaign_id": None,
    # CHANGE ME — the from-address for this venture's sends.
    "relay_instantly_sender_email": None,

    # Relay — execution guards ----------------------------------------------
    # Local-hour send window (deliverability, not legal — CAN-SPAM sets no
    # time restriction). Set start=0/end=24 to disable.
    "relay_send_window_start": 11,
    "relay_send_window_end": 18,
    # CHANGE ME IF NOT EASTERN — the timezone the window above is measured in.
    "relay_send_window_timezone": "America/New_York",
    # Per-channel sends per calendar day. Counted in a Redis key scoped to
    # this venture, so each venture gets its own independent cap.
    "relay_daily_ceiling": 20,
    # Kill-switch feature key checked before every batch and every item.
    # Leave as 'relay_global' to share the fleet-wide Relay stop, or give
    # this venture its own key to stop it independently. The fleet-wide
    # 'global' override always takes precedence either way.
    "kill_switch_feature": DEFAULT_KILL_SWITCH_FEATURE,
}

# Counties block — provisioning accepts a list of these alongside the venture
# config. `source_url_overrides` maps signal_type -> the new county's portal
# URL; anything not overridden inherits the template county's URL, which is
# almost always wrong for a different county, so override every one you can.
COUNTY_TEMPLATE: dict[str, Any] = {
    "county_id": "CHANGE_ME",
    "display_name": "CHANGE ME County",
    "fips": None,
    "nws_zone": None,
    "parcel_id_format": "folio",
    "bankruptcy_division": None,
    "zip_prefixes": [],
    "city_filer_keywords": [],
    "address_city_tokens": [],
    "source_url_overrides": {},
}


def _is_int(value: Any) -> bool:
    """True for a real int. `bool` is an int subclass in Python, so a config
    carrying `true` for an hour or a ceiling would otherwise pass as 1."""
    return isinstance(value, int) and not isinstance(value, bool)


def validate_venture_config(cfg: dict[str, Any]) -> list[str]:
    """Return a list of human-readable problems with `cfg`, empty if valid.

    Pure — no DB, no settings, no side effects. Called by
    src/services/venture_provisioning.py before any row is written, and
    usable standalone to check a hand-edited JSON file.
    """
    problems: list[str] = []

    unknown = set(cfg) - set(VENTURE_TEMPLATE)
    if unknown:
        problems.append(f"unknown field(s): {', '.join(sorted(unknown))}")

    for field in _REQUIRED_FIELDS:
        value = cfg.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            problems.append(f"{field} is required and must be non-empty")

    venture_key = cfg.get("venture_key")
    if isinstance(venture_key, str) and venture_key.strip():
        if venture_key != venture_key.strip().lower() or " " in venture_key:
            problems.append("venture_key must be lowercase with no spaces")

    state = cfg.get("state")
    if isinstance(state, str) and state.strip() and len(state.strip()) != 2:
        problems.append("state must be a two-letter code (e.g. 'FL')")

    start = cfg.get("relay_send_window_start", VENTURE_TEMPLATE["relay_send_window_start"])
    end = cfg.get("relay_send_window_end", VENTURE_TEMPLATE["relay_send_window_end"])
    start_ok = _is_int(start) and 0 <= start <= 24
    end_ok = _is_int(end) and 0 <= end <= 24
    if not start_ok:
        problems.append("relay_send_window_start must be an int in 0..24")
    if not end_ok:
        problems.append("relay_send_window_end must be an int in 0..24")
    if start_ok and end_ok and start >= end:
        problems.append(
            f"relay_send_window_start ({start}) must be less than "
            f"relay_send_window_end ({end}) — an empty window sends nothing"
        )

    ceiling = cfg.get("relay_daily_ceiling", VENTURE_TEMPLATE["relay_daily_ceiling"])
    if not _is_int(ceiling) or ceiling <= 0:
        problems.append("relay_daily_ceiling must be a positive int")

    if cfg.get("relay_instantly_campaign_id") and not cfg.get("relay_instantly_sender_email"):
        problems.append(
            "relay_instantly_sender_email is required when "
            "relay_instantly_campaign_id is set — the email channel cannot "
            "send without a from-address"
        )

    approvers = cfg.get("relay_approvers", [])
    if not isinstance(approvers, list):
        problems.append("relay_approvers must be a list of Slack user IDs")

    return problems


def new_venture_config(**overrides: Any) -> dict[str, Any]:
    """A fresh copy of VENTURE_TEMPLATE with `overrides` applied.

    Copying matters: VENTURE_TEMPLATE holds mutable list values, and handing
    callers the module-level dict would let one caller's edit leak into the
    next one's template.
    """
    cfg = {
        key: (list(value) if isinstance(value, list) else value)
        for key, value in VENTURE_TEMPLATE.items()
    }
    cfg.update(overrides)
    return cfg
