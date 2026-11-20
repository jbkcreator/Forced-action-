"""P3.3 Central Lending Milo auto-fill: field names and value lists from the Milo new-loan form.

Captured from Central Lending's broker view (miloservicing.com/central/new-loan), 2026-11-17.
Dropdown option lists not yet captured are marked unconfirmed; the mapping refuses nothing on them,
it flags the package instead.
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

# Step 2 "Property Type" options seen so far.
MILO_PROPERTY_TYPES: dict[str, str] = {"sfr": "1-4 Unit", "1_4_unit": "1-4 Unit"}
MILO_PROPERTY_TYPES_CONFIRMED = False

MILO_GEO_RESTRICTION_DEFAULT = "N/A"

# Step 2 "Est. FICO Score" bands: (min FICO, Milo label), highest first. Placeholder until the dropdown
# options are captured; a package mapped with them is flagged ``fico_band_unconfirmed``.
MILO_FICO_BANDS: tuple[tuple[int, str], ...] = (
    (780, "780+"),
    (760, "760-779"),
    (740, "740-759"),
    (720, "720-739"),
    (700, "700-719"),
    (680, "680-699"),
    (660, "660-679"),
    (0, "Below 660"),
)
MILO_FICO_BANDS_CONFIRMED = False

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
