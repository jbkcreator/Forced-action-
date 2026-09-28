"""PropertyRadar adapter configuration.

FIPS codes and abbreviation expansions are the only county/owner knowledge
that lives here — no county names in application code.
"""

# FA county slug for each FIPS code we have loaded in the DB.
# Only these counties will get a property_id link during staging.
# Miami-Dade: legacy FIPS 12025 (not the current 12086) — Dev 1 maps it there.
COUNTY_FIPS_TO_SLUG: dict[str, str] = {
    "12057": "hillsborough",
    "12101": "pasco",
    "12103": "pinellas",
}

# PropertyRadar abbreviates entity names in owner fields.
# Expand these before comparing owner names so "KIAVI FNDG INC" and
# "KIAVI FUNDING INC" are treated as the same owner (no false sold signal).
# Keys must be upper-case whole words only.
OWNER_ABBREVIATIONS: dict[str, str] = {
    "INV": "INVESTMENTS",
    "COML": "COMMERCIAL",
    "SVCS": "SERVICES",
    "FNDG": "FUNDING",
    "CAP": "CAPITAL",
    "MGMT": "MANAGEMENT",
    "PROP": "PROPERTIES",
    "ASSOC": "ASSOCIATES",
    "GRP": "GROUP",
    "INTL": "INTERNATIONAL",
}
