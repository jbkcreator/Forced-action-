"""
M6 Lead Quality Truth Engine — grading configuration.

Numeric grade thresholds (CDS score bands + contactability floors) live in the
``grade_thresholds`` DB table so they can be tuned without a deploy (spec §3.1a,
§190). This module holds the parts that are structural business rules rather than
tunable parameters: the grade ordering, the grade → revenue-channel routing map,
and the cohort-key derivation.

Scale note: ``grade_thresholds.cds_min/cds_max`` are stored on the 0–100 scale to
match ``distress_scores.final_cds_score``. The 414 spec §3.1a expressed CDS cut-offs
on a 0–1 scale; the ``notes`` column on each seeded row records the original value.
"""

GRADE_CHANNEL_ROUTING: dict[str, list[str]] = {
    "Ultra":     ["loan_lane", "contractor_subscription"],
    "Platinum":  ["loan_lane", "contractor_subscription"],
    "Gold":      ["contractor_subscription", "storm_retainer"],
    "Silver":    ["data_pack_bulk"],
    "Bronze":    ["free_hand_delivered"],
    "sub_grade": ["recycle_suppress"],
}

GRADE_ORDER: list[str] = ["sub_grade", "Bronze", "Silver", "Gold", "Platinum", "Ultra"]

GOLD_PLUS_GRADES: frozenset[str] = frozenset({"Gold", "Platinum", "Ultra"})


def primary_channel(grade: str) -> str:
    """Return the highest-priority routed channel for a grade."""
    return GRADE_CHANNEL_ROUTING.get(grade, ["recycle_suppress"])[0]


def grade_rank(grade: str) -> int:
    """Return the ordinal of a grade on GRADE_ORDER (unknown grades rank lowest)."""
    try:
        return GRADE_ORDER.index(grade)
    except ValueError:
        return 0


def lower_grade(grade_a: str, grade_b: str) -> str:
    """Return the lower of two grades on GRADE_ORDER (contactability can only pull down)."""
    return grade_a if grade_rank(grade_a) <= grade_rank(grade_b) else grade_b


def compute_cohort_key(cds_lead_tier: str | None, county_id: str | None, source: str | None) -> str:
    """Build the contactability cohort key.

    grade_band is the CDS ``lead_tier`` (from ``distress_scores``), deliberately
    independent of the Truth Engine verdict so the cohort fallback that supplies
    contactability does not depend on the grade it helps compute (spec §12.1).
    """
    return f"{cds_lead_tier or 'unknown'}|{county_id or 'unknown'}|{source or 'unknown'}"
