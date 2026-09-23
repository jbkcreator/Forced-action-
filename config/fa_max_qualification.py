"""
FA Max Qualification Agent — checklist configuration (WP-T3-7).

Defines the per-opportunity-type sufficiency checklist: which facts must be
present (and non-None) before the Scenario Builder (WP-8A/8B) has enough to
produce a usable output.

IMPORTANT — Dev 4 sign-off required:
    The exact FactSpec list per opportunity_type is a shared contract with
    Developer 4 (WP-8A/8B). The specs below represent the minimum viable set
    derived from QuoteReadyInput's required fields. Any change to what the
    Scenario Builder needs must be coordinated with Dev 4 before this config
    is updated in production, and checklist updates must bump CHECKLIST_VERSION
    so the backstop sweep reevaluates all non-terminal opportunities.

CHECKLIST_VERSION must be bumped whenever any FactSpec changes (add/remove/
modify). A checklist version bump automatically triggers a backstop sweep in
the qualification worker over all non-terminal opportunities.

Compliance boundary (SOT.md Part 1):
    FactSpec may never reference credit_score, income, bank_statement,
    tax_return, or ssn. Any spec touching those is rejected at code review.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

# Bump when any checklist entry changes — triggers backstop reevaluation
# of all non-terminal opportunities. Semver: major.minor.patch where:
#   major = backward-incompatible checklist removal/rename
#   minor = new required fact added
#   patch = gap reason text / enrichment annotation change only
CHECKLIST_VERSION = "1.2.1"

GapType = Literal["client_gap", "pending_enrichment"]

# Single source of truth for the forbidden-financial-term substring check,
# shared between the API request validator (admin_router.py) and the
# service-layer write path (fa_max_qualification.set_facts) — the
# authoritative write path (SOT.md Part 1 requires the boundary enforced at
# the schema level, not just the prompt/API layer). Previously this check
# existed ONLY in the Pydantic validator, so a direct set_facts() call
# (e.g. a future T3-8 enrichment writer) could still store a value
# containing one of these terms (code-review finding, fifth round, 2026-09).
FORBIDDEN_FINANCIAL_TERMS: frozenset[str] = frozenset({
    "credit_score", "income", "bank_statement", "tax_return", "ssn",
    "fico", "dti", "debt_to_income",
})

# Facts requiring a STRICTLY POSITIVE value, not merely presence. Matches
# src/services/quote_ready/compute.py's own ground truth: _purchase_basis()
# only accepts "value > _ZERO", and arv_usable requires "inp.arv > _ZERO" —
# a $0 purchase price or ARV is not a usable scenario input (code-review
# finding, 2026-09: presence-only checking let 0.00 through as "sufficient").
POSITIVE_NUMERIC_FACT_KEYS: frozenset[str] = frozenset({
    "purchase_price", "estimated_value", "assessed_value_mkt", "last_sale_price", "arv",
})

# rehab_estimate is the one numeric fact where 0 IS a genuinely usable value
# (a pure acquisition or extension deal with no planned rehab work) — matches
# compute.py's own "rehab_usable = inp.rehab_estimate is not None and
# inp.rehab_estimate >= _ZERO". A second code-review round (2026-09) caught
# that folding this into POSITIVE_NUMERIC_FACT_KEYS would have rejected a
# legitimate $0 rehab_estimate as insufficient — verify against compute.py
# before ever widening this set.
NONNEGATIVE_NUMERIC_FACT_KEYS: frozenset[str] = frozenset({"rehab_estimate"})

NUMERIC_FACT_KEYS: frozenset[str] = POSITIVE_NUMERIC_FACT_KEYS | NONNEGATIVE_NUMERIC_FACT_KEYS

# current_use is descriptive only — it is not consumed anywhere in
# src/services/quote_ready (QuoteReadyInput has no such field). A free-text
# column here is an unrestricted-text path around the "property/project
# facts only" schema boundary: extra="forbid" on the request model blocks
# unknown FIELD NAMES, but says nothing about what a caller puts inside an
# allowed text field's VALUE (code-review finding, second round, 2026-09).
# A fixed vocabulary closes that path structurally — a value outside this
# set is rejected by the Pydantic Literal type, not merely discouraged.
CurrentUse = Literal[
    "single_family", "multi_family_2_4", "multi_family_5plus",
    "condo", "townhouse", "vacant_land", "commercial", "mixed_use", "other",
]

# Opportunity types whose FactSpec list has NOT been confirmed against a
# real downstream consumer. compute_quote_ready() (src/services/quote_ready)
# is verified ground truth for acquisition/rehab/construction/extension —
# its rehab/ARV-centric LTC+LTV math is exactly what those types need, and
# its only real production caller (builder_sizing.py, WP-T2-8 Stage D) is
# itself a construction/rehab-specific feature. There is zero evidence in
# this repo that refinance/dscr_takeout/repeat are meant to flow through
# that SAME function (an after-REPAIR value has no obvious meaning for a
# refinance leg with no repair happening) — applying compute.py's rehab/ARV
# requirements to these three types would be guessing at an unconfirmed
# cross-team contract in either direction (code-review finding, fourth
# round, 2026-09; withdrawn instruction from third round confirmed this
# analysis was correct rather than resolving it).
#
# Until Dev 4 (WP-8A/8B) confirms these three types' real checklist AND
# consumer mapping, a "sufficient" verdict for them must not produce an
# authoritative handoff — see evaluate_sufficiency()'s
# "sufficient_pending_contract" verdict and the qualification worker's
# dispatch on it.
PENDING_CONTRACT_APPROVAL_TYPES: frozenset[str] = frozenset({
    "refinance", "dscr_takeout", "repeat",
})


@dataclass(frozen=True)
class FactSpec:
    fact_key: str
    display_name: str
    gap_type: GapType
    gap_reason: str
    # When True, one of the alternative_to group satisfying makes this optional.
    # None = standalone required fact.
    alternative_group: Optional[str] = None
    # opportunity_types this spec applies to. Empty set = all types.
    applies_to: frozenset = field(default_factory=frozenset)

    @property
    def applies_to_all(self) -> bool:
        return not self.applies_to


# ---------------------------------------------------------------------------
# Purchase-basis group — at least one must be present.
# The Scenario Builder's QuoteReadyInput fallback chain is:
#   purchase_price → estimated_value → assessed_value_mkt → last_sale_price
# A gap fires only when ALL four are absent for a type that needs a basis.
# ---------------------------------------------------------------------------

_PURCHASE_BASIS_TYPES = frozenset({
    "acquisition", "rehab", "construction", "extension",
})

_PURCHASE_PRICE_REQUIRED_TYPES = frozenset({
    "acquisition", "rehab", "construction", "extension",
})

_VALUE_BASIS_TYPES = frozenset({
    "refinance", "dscr_takeout", "repeat",
})

CHECKLIST: dict[str, list[FactSpec]] = {
    # ── acquisition ──────────────────────────────────────────────────────────
    "acquisition": [
        FactSpec(
            fact_key="purchase_price",
            display_name="Purchase Price",
            gap_type="client_gap",
            gap_reason="Purchase price is required for an acquisition scenario.",
            applies_to=frozenset({"acquisition"}),
        ),
        FactSpec(
            fact_key="rehab_estimate",
            display_name="Rehab Estimate",
            gap_type="client_gap",
            # compute_quote_ready() checks rehab_estimate universally (not
            # per opportunity_type) — a None rehab_estimate is flagged
            # "missing" by the builder even for a no-rehab acquisition.
            # $0 is the correct, usable answer here (code-review finding,
            # 2026-09: acquisition never asked for this fact at all, so the
            # builder always saw it as missing downstream of a "sufficient"
            # qualification verdict).
            gap_reason="Confirm rehab budget (enter $0 if no rehab work is planned) — "
                       "the Scenario Builder requires this to compute project cost.",
            applies_to=frozenset({"acquisition"}),
        ),
        FactSpec(
            fact_key="arv",
            display_name="After-Repair Value (ARV)",
            gap_type="pending_enrichment",
            gap_reason="ARV must be established (comp-derived or override) before the "
                       "Scenario Builder can compute proposed loan and LTV.",
            applies_to=frozenset({"acquisition"}),
        ),
    ],

    # ── rehab ─────────────────────────────────────────────────────────────────
    "rehab": [
        FactSpec(
            fact_key="purchase_price",
            display_name="Purchase Price",
            gap_type="client_gap",
            gap_reason="Purchase price is required for a rehab scenario.",
            applies_to=frozenset({"rehab"}),
        ),
        FactSpec(
            fact_key="rehab_estimate",
            display_name="Rehab Estimate",
            gap_type="client_gap",
            gap_reason="Rehab scope estimate is required to compute project cost and LTC.",
            applies_to=frozenset({"rehab"}),
        ),
        FactSpec(
            fact_key="arv",
            display_name="After-Repair Value (ARV)",
            gap_type="pending_enrichment",
            gap_reason="ARV must be established before the Scenario Builder can compute LTV.",
            applies_to=frozenset({"rehab"}),
        ),
    ],

    # ── construction ──────────────────────────────────────────────────────────
    "construction": [
        FactSpec(
            fact_key="purchase_price",
            display_name="Purchase Price / Lot Value",
            gap_type="client_gap",
            gap_reason="Purchase price or lot value is required for a construction scenario.",
            applies_to=frozenset({"construction"}),
        ),
        FactSpec(
            fact_key="rehab_estimate",
            display_name="Construction Budget",
            gap_type="client_gap",
            gap_reason="Construction budget is required to compute project cost and LTC.",
            applies_to=frozenset({"construction"}),
        ),
        FactSpec(
            fact_key="arv",
            display_name="After-Construction Value (ARV)",
            gap_type="pending_enrichment",
            gap_reason="ARV must be established before the Scenario Builder can compute LTV.",
            applies_to=frozenset({"construction"}),
        ),
        FactSpec(
            fact_key="expected_exit_strategy",
            display_name="Exit Strategy",
            gap_type="client_gap",
            gap_reason="Exit strategy (sale / rent / dscr) is required for a "
                       "construction scenario to size the take-out assumption.",
            applies_to=frozenset({"construction"}),
        ),
    ],

    # ── extension ─────────────────────────────────────────────────────────────
    "extension": [
        FactSpec(
            fact_key="purchase_price",
            display_name="Original Purchase Price",
            gap_type="client_gap",
            # NOT "outstanding principal" — that's a loan-balance concept
            # with no corresponding field in QuoteReadyInput or
            # compute_quote_ready()'s cost-basis chain (purchase_price →
            # estimated_value → assessed_value_mkt → last_sale_price).
            # Storing an outstanding loan balance under purchase_price would
            # feed the builder a different number than every other type's
            # purchase_price means, silently corrupting the LTC calculation
            # (code-review finding, third round, 2026-09: the prior
            # dual-meaning display_name/gap_reason implied outstanding
            # principal was an acceptable answer here — it is not, under
            # the current single-column schema).
            gap_reason="Original purchase price is required to establish the cost "
                       "basis for an extension.",
            applies_to=frozenset({"extension"}),
        ),
        FactSpec(
            fact_key="rehab_estimate",
            display_name="Remaining Rehab Budget",
            gap_type="client_gap",
            gap_reason="Confirm remaining rehab budget (enter $0 if rehab is already "
                       "complete) — the Scenario Builder requires this to compute "
                       "project cost.",
            applies_to=frozenset({"extension"}),
        ),
        FactSpec(
            fact_key="arv",
            display_name="Current As-Is or Post-Rehab Value",
            gap_type="pending_enrichment",
            gap_reason="Current value must be established before the Scenario Builder "
                       "can compute the extension LTV.",
            applies_to=frozenset({"extension"}),
        ),
    ],

    # ── refinance ─────────────────────────────────────────────────────────────
    "refinance": [
        FactSpec(
            fact_key="estimated_value",
            display_name="Estimated Property Value",
            gap_type="pending_enrichment",
            gap_reason="Property value is required to compute refinance LTV. Will be "
                       "auto-populated from existing enrichment data if available.",
            alternative_group="value_basis",
            applies_to=frozenset({"refinance"}),
        ),
        FactSpec(
            fact_key="assessed_value_mkt",
            display_name="Assessed Market Value (fallback)",
            gap_type="pending_enrichment",
            gap_reason="Assessed market value used when no estimated value is available.",
            alternative_group="value_basis",
            applies_to=frozenset({"refinance"}),
        ),
    ],

    # ── dscr_takeout ──────────────────────────────────────────────────────────
    "dscr_takeout": [
        FactSpec(
            fact_key="estimated_value",
            display_name="Estimated Property Value",
            gap_type="pending_enrichment",
            gap_reason="Property value is required to compute DSCR take-out LTV.",
            alternative_group="value_basis",
            applies_to=frozenset({"dscr_takeout"}),
        ),
        FactSpec(
            fact_key="assessed_value_mkt",
            display_name="Assessed Market Value (fallback)",
            gap_type="pending_enrichment",
            gap_reason="Assessed market value used when no estimated value is available.",
            alternative_group="value_basis",
            applies_to=frozenset({"dscr_takeout"}),
        ),
        FactSpec(
            fact_key="expected_exit_strategy",
            display_name="Exit Strategy",
            gap_type="client_gap",
            gap_reason="Confirm exit strategy is 'dscr' (hold for rental / DSCR take-out).",
            applies_to=frozenset({"dscr_takeout"}),
        ),
    ],

    # ── repeat ────────────────────────────────────────────────────────────────
    "repeat": [
        FactSpec(
            fact_key="purchase_price",
            display_name="Purchase Price",
            gap_type="client_gap",
            gap_reason="Purchase price for the new deal is required for a repeat borrower scenario.",
            alternative_group="repeat_basis",
            applies_to=frozenset({"repeat"}),
        ),
        FactSpec(
            fact_key="estimated_value",
            display_name="Estimated Property Value (fallback for refinance leg)",
            gap_type="pending_enrichment",
            gap_reason="Estimated value used when the repeat scenario is a refinance leg.",
            alternative_group="repeat_basis",
            applies_to=frozenset({"repeat"}),
        ),
        FactSpec(
            fact_key="arv",
            display_name="After-Repair Value (ARV)",
            gap_type="pending_enrichment",
            gap_reason="ARV required to size the repeat loan.",
            applies_to=frozenset({"repeat"}),
        ),
    ],
}


def get_checklist(opportunity_type: str) -> list[FactSpec]:
    """Return the ordered FactSpec list for an opportunity_type.

    Raises KeyError for an unknown type — the CHECK constraint on
    fa_max_opportunities.opportunity_type prevents unknown types reaching
    here from the DB, but the caller should still handle the error.
    """
    if opportunity_type not in CHECKLIST:
        raise KeyError(f"No checklist defined for opportunity_type={opportunity_type!r}")
    return CHECKLIST[opportunity_type]
