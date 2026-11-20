"""PropertyRadar → FA Max handoff configuration.

State- and county-agnostic by design: adding a state or campaign is a config
entry here, never a code change.
"""

# Opportunity type each campaign creates in FA Max. Must be one of the values
# allowed by ck_fa_max_opp_type. A campaign without an entry is never handed off.
CAMPAIGN_OPPORTUNITY_TYPES: dict[str, str] = {
    "maturity_target_lender": "refinance",
}

# FA Max channel-split source for each campaign. Must be in
# fa_max_send_governance.FA_MAX_ALLOWED_SOURCE_TYPES, otherwise every outbound
# touch on the lead is blocked by the channel-split gate.
CAMPAIGN_SOURCE_TYPES: dict[str, str] = {
    "maturity_target_lender": "maturity",
}

# Campaigns traced for the dialer only: the runner traces them live and the calling-pool bridge stages them, but
# they create no FA Max opportunity (a cash buyer or a portfolio owner is not a refinance lead). A campaign must not
# be in both this set and CAMPAIGN_OPPORTUNITY_TYPES.
DIALER_TRACE_CAMPAIGNS: frozenset[str] = frozenset({"cash_purchase", "investor_portfolio"})

# County FIPS codes the thin path admits (Hillsborough, Pinellas).
THIN_PATH_COUNTY_FIPS: frozenset[str] = frozenset({"12057", "12103"})

# Consent source recorded when contact rules are switched on.
CONSENT_SOURCE = "property_radar_cold_lead"

# Channels that receive a consent row when contact rules are switched on.
# SMS is deliberately absent: it needs opt-in and 10DLC registration first.
# Calls are not a consent channel; they go through the dial list and DNC checks.
CONSENTED_CHANNELS: tuple[str, ...] = ("email",)

# Staged records are read in pages of this size.
STAGED_RECORD_PAGE_SIZE = 500
