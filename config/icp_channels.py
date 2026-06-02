"""
ICP Channel Registry — static metadata for every ICP channel.

ICP (Ideal Customer Profile) = the CUSTOMER GROUP: contractor, rei_investor, etc.
Vertical = the PRODUCT CATEGORY inside the lead product: roofing, restoration, etc.

These are SEPARATE axes. A vertical like `wholesalers` may belong to BOTH the
`contractor` ICP and a future `rei_investor` ICP. All attribution, metrics, and
gating use `icp_channel_key` explicitly — never verticals alone.

Rules:
- `contractor` is the default ICP, always active, never gate-required.
- Do NOT add future ICPs as CDS scoring verticals unless the scoring engine
  genuinely needs them (they are a different axis).
- Add new ICPs here in `draft` status; promote to `gated` after seeding the DB row.
- All dynamic state (status transitions, gate results, timestamps) lives in the DB
  table `expansion_icp_channels`. This file is static metadata only.
"""

ICP_CHANNELS: dict[str, dict] = {
    "contractor": {
        "display_name": "Property-Service Contractor",
        "slug": "contractor",
        "status": "active",
        "is_default": True,
        "gate_required": False,
        "price_monthly_cents": None,  # existing Stripe tier pricing unchanged
        "stripe_price_key": None,
        "verticals": [
            "roofing", "restoration", "public_adjusters",
            "wholesalers", "fix_flip", "attorneys",
        ],
        "description": (
            "The original contractor/trade ICP. Roofers, restorers, public adjusters, "
            "wholesalers, fix-and-flip investors, and attorneys."
        ),
        "target_audience": "Roofers, restorers, adjusters, investors, attorneys",
        "lead_filter_summary": "All 6 contractor verticals in licensed territory.",
    },
    "rei_investor": {
        "display_name": "REI Investor",
        "slug": "rei-investor",
        "status": "draft",
        "is_default": False,
        "gate_required": True,
        "price_monthly_cents": 19700,   # $197/mo
        "stripe_price_key": "icp_rei_investor",
        "verticals": ["wholesalers", "fix_flip"],
        "description": "Real estate investors buying distressed properties wholesale or fix-and-flip.",
        "target_audience": "Real estate investors seeking wholesale/fix-flip deals",
        "lead_filter_summary": "Investment-grade distress signals; REI-specific scoring.",
    },
    "insurance_adjuster": {
        "display_name": "Insurance Adjuster",
        "slug": "insurance-adjuster",
        "status": "draft",
        "is_default": False,
        "gate_required": True,
        "price_monthly_cents": 9700,    # $97/mo
        "stripe_price_key": "icp_insurance_adjuster",
        "verticals": ["public_adjusters", "restoration"],
        "description": "Insurance adjusters and public adjusters seeking storm/distress claims.",
        "target_audience": "Insurance adjusters and public adjusters",
        "lead_filter_summary": "Insurance-claim and storm-damage signals.",
    },
    "hard_money_lender": {
        "display_name": "Hard Money Lender",
        "slug": "hard-money-lender",
        "status": "draft",
        "is_default": False,
        "gate_required": True,
        "price_monthly_cents": 39700,   # $397/mo
        "stripe_price_key": "icp_hard_money_lender",
        "verticals": ["wholesalers", "fix_flip", "attorneys"],
        "description": "Hard money lenders funding distressed property acquisitions.",
        "target_audience": "Hard money lenders seeking deal flow",
        "lead_filter_summary": "High-equity distress signals; pre-foreclosure focus.",
    },
    "property_manager": {
        "display_name": "Property Manager",
        "slug": "property-manager",
        "status": "draft",
        "is_default": False,
        "gate_required": True,
        "price_monthly_cents": 19700,   # $197/mo
        "stripe_price_key": "icp_property_manager",
        "verticals": ["wholesalers", "restoration"],
        "description": "Property managers seeking distressed properties to manage or acquire.",
        "target_audience": "Property managers and management companies",
        "lead_filter_summary": "Vacant, absentee-owner, and distress signals.",
    },
    "bankruptcy_attorney": {
        "display_name": "Bankruptcy Attorney",
        "slug": "bankruptcy-attorney",
        "status": "draft",
        "is_default": False,
        "gate_required": True,
        "price_monthly_cents": 19700,   # $197/mo
        "stripe_price_key": "icp_bankruptcy_attorney",
        "verticals": ["attorneys"],
        "description": "Bankruptcy attorneys seeking clients with distressed property situations.",
        "target_audience": "Bankruptcy and real estate attorneys",
        "lead_filter_summary": "Bankruptcy, foreclosure, and legal-proceeding signals.",
    },
    "title_company": {
        "display_name": "Title Company",
        "slug": "title-company",
        "status": "draft",
        "is_default": False,
        "gate_required": True,
        "price_monthly_cents": 9700,    # $97/mo
        "stripe_price_key": "icp_title_company",
        "verticals": ["wholesalers", "fix_flip", "attorneys"],
        "description": "Title companies tracking distressed property transactions in their markets.",
        "target_audience": "Title companies and escrow officers",
        "lead_filter_summary": "Foreclosure, deed, and lien signals.",
    },
}

# The key used when no ICP is explicitly attributed (backwards-compat).
DEFAULT_ICP_CHANNEL_KEY = "contractor"


def get_icp_channel(key: str) -> dict:
    """Return ICP channel metadata. Raises KeyError if not found."""
    return ICP_CHANNELS[key]


def is_default_icp(key: str) -> bool:
    return ICP_CHANNELS.get(key, {}).get("is_default", False)


def is_gate_required(key: str) -> bool:
    return ICP_CHANNELS.get(key, {}).get("gate_required", True)


def all_channel_keys() -> list[str]:
    return list(ICP_CHANNELS.keys())


def get_icp_price_cents(key: str) -> int | None:
    """Return the monthly price in cents for a non-contractor ICP, or None if not applicable."""
    return ICP_CHANNELS.get(key, {}).get("price_monthly_cents")


def get_icp_stripe_price_key(key: str) -> str | None:
    """Return the Stripe lookup key for an ICP's price, or None for contractor."""
    return ICP_CHANNELS.get(key, {}).get("stripe_price_key")


# Map stripe_price_key → icp_channel_key (for checkout validation)
STRIPE_KEY_TO_ICP: dict[str, str] = {
    v["stripe_price_key"]: k
    for k, v in ICP_CHANNELS.items()
    if v.get("stripe_price_key")
}
