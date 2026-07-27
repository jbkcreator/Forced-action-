"""
Cora's objection library — basic structure grouped by vertical (avenue),
not the full Week 3 learning engine. A static seed list; accretion of real
objections (per Cora's constitution: "objections accrete into the library")
is future work, not built here.
"""
from __future__ import annotations

from typing import Dict, List, TypedDict


class ObjectionEntry(TypedDict):
    objection: str
    response_strategy: str


OBJECTION_LIBRARY: Dict[str, List[ObjectionEntry]] = {
    "flippers": [
        {
            "objection": "Already have a data source / county records myself",
            "response_strategy": "Acknowledge, differentiate on speed-to-signal (auction/filing "
                                  "freshness) rather than data existence itself.",
        },
        {
            "objection": "Price too high for a subscription",
            "response_strategy": "Offer to walk through ROI on their actual purchase volume, not a "
                                  "generic pitch — never discount without founder sign-off.",
        },
    ],
    "buy_and_hold": [
        {
            "objection": "Not actively buying right now",
            "response_strategy": "Shift to a lower-commitment offer (lead pack) or ask to follow up "
                                  "at a stated future date — never push.",
        },
    ],
    "wholesalers": [
        {
            "objection": "Already has a buyer's list, doesn't need more deal flow",
            "response_strategy": "Reframe around speed/exclusivity of the signal, not volume.",
        },
    ],
    "lender_types": [
        {
            "objection": "Compliance/legal needs to review before engaging",
            "response_strategy": "Offer the booking link for a no-commitment intro call — this is "
                                  "lead-handoff only, no fee mechanics until RESPA clearance.",
        },
    ],
}


def get_objections_for_avenue(avenue: str) -> List[ObjectionEntry]:
    return OBJECTION_LIBRARY.get(avenue, [])
