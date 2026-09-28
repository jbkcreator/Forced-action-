"""Lead ownership: which campaign owns a person when several qualify.

One priority order covers every campaign that can run a sequence for a person
— PropertyRadar campaigns and the FA Max outbound campaigns (WP-T3-4) alike —
so the two systems can never both own the same person. Adding a campaign is
one entry here.
"""

# Highest priority first. The FA Max outbound campaigns keep their own order
# (config/fa_max_campaigns.py); PropertyRadar cold leads rank below them.
CAMPAIGN_PRIORITY: tuple[str, ...] = (
    "exit_desk",
    "capital_desk_loop",
    "rescue_circuit",
    "maturity_target_lender",
)

# Campaigns run by the FA Max campaign-selection engine. Their ownership lives
# in fa_max_campaign_enrollments; lead ownership reads it but never writes it.
FA_MAX_ENGINE_CAMPAIGNS: frozenset[str] = frozenset({"exit_desk", "capital_desk_loop", "rescue_circuit"})

LEAD_SOURCE_PROPERTY_RADAR = "property_radar"


def priority_rank(campaign: str) -> int:
    """0 = highest priority. Unknown campaigns are rejected, never ranked."""
    if campaign not in CAMPAIGN_PRIORITY:
        raise ValueError(f"campaign {campaign!r} is not in CAMPAIGN_PRIORITY")
    return CAMPAIGN_PRIORITY.index(campaign)
