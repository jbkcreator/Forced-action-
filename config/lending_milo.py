"""P3.3 Central Lending Milo auto-fill: field names and value lists from the Milo new-loan form.

Captured from Central Lending's broker view (miloservicing.com/central/new-loan), 2026-11-17; step 2
dropdown options (FICO, property type, GEO restrictions, refinance, foreign national, product) confirmed
2026-11-23 from the loan-sizer page. Step 3 (after "Choose" on a loan option) is not captured.
"""
from __future__ import annotations

MILO_NEW_LOAN_PATH = "/central/new-loan"

# Step 1 "What are you financing?" — Milo label per product key.
MILO_PRODUCTS: dict[str, str] = {
    "fix_and_flip": "Fix & flip / fix to rent",
    "ground_up": "Ground-up construction",
    "bridge": "Stabilized bridge",
    "dscr": "Rental loan (DSCR)",
}
# Products the mapping fills today. The others need their step-2 field lists.
MILO_SUPPORTED_PRODUCTS: frozenset[str] = frozenset({"fix_and_flip"})

# Step 2 "Product" dropdown labels (they differ from the step 1 cards). Milo also offers "I'm Not Sure".
MILO_STEP2_PRODUCTS: dict[str, str] = {
    "fix_and_flip": "Fix & Flip / Fix to Rent",
    "bridge": "Bridge",
    "ground_up": "Ground-Up Construction",
    "dscr": "DSCR Rental Loan",
}

# Step 2 "Property Type": every option in Milo's dropdown.
MILO_PROPERTY_TYPES: dict[str, str] = {
    "sfr": "1-4 Unit",
    "1_4_unit": "1-4 Unit",
    "5_plus_multifamily": "5+ MultiFamily",
    "mixed_use": "Mixed Use",
    "commercial": "Commercial",
    "vacant_land": "Vacant Land (No Construction)",
    "other": "Other",
}
MILO_PROPERTY_TYPES_CONFIRMED = True

# Step 2 "GEO Restrictions": every option in Milo's dropdown.
MILO_GEO_RESTRICTIONS: dict[str, str] = {
    "na": "N/A",
    "partial_rural": "Partial Rural",
    "rural": "Rural",
    "restricted_city": "DETROIT, CHICAGO, BALTIMORE, PHILADELPHIA",
}
MILO_GEO_RESTRICTION_DEFAULT = "na"

# Step 2 "Est. FICO" bands: (min FICO, Milo label), highest first; every option in Milo's dropdown.
MILO_FICO_BANDS: tuple[tuple[int, str], ...] = (
    (780, "780+"),
    (760, "760-779"),
    (740, "740-759"),
    (720, "720-739"),
    (700, "700-719"),
    (680, "680-699"),
    (660, "660-679"),
    (640, "640-659"),
    (620, "620-639"),
    (600, "600-619"),
    (0, "<600"),
)
MILO_FICO_BANDS_CONFIRMED = True

MILO_PACKET_TEMPLATE = "milo_packet.html"
MILO_PACKET_WATERMARK = "PREPARED FOR LENDER SUBMISSION — FIGURES AS PROVIDED BY THE BORROWER AND FORCED ACTION ESTIMATES"

# The Slack Confirm / Cancel card (src/lending/milo/slack_card.py), posted in #approvals by Cora's bot. Only Cora's
# approvers (LENDING_CORA_APPROVER_USER_IDS) may press either button.
ACTION_CONFIRM = "milo_confirm"
ACTION_CANCEL = "milo_cancel"
CARD_FLAG_TEXT = {
    "fico_band_unconfirmed": "FICO band list not yet checked against Milo's dropdown",
    "property_type_unconfirmed": "Property type list not yet checked against Milo's dropdown",
    "no_comps": "No comps attached",
    "arv_below_total_cost": "ARV is below purchase price plus rehab",
}
