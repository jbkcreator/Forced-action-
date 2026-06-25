"""
Closer Cockpit (Sprint S1b) — controlled vocabularies and routing constants.

Single source of truth for the closer-call objection taxonomy, pitch variants,
and the enums used by both the AI tagger (Call Tagging) and the human one-tap
feedback panel. Referenced by the tagging prompt/validation, the DB CHECK
constraints, the API endpoints, and (eventually) the frontend cockpit so all
three stay in lockstep.

See: CLOSER_COCKPIT_BACKEND_DESIGN.md, ADR
"closer-telemetry-separate-from-agent-decisions".
"""
from __future__ import annotations

# Controlled objection vocabulary. Shared by the AI tagger's `objections`
# (0..n) and the closer's single one-tap `objection_type`.
OBJECTION_TAXONOMY: list[str] = [
    "price_too_high",
    "lead_quality_doubt",
    "no_roi_yet",
    "already_has_leads",
    "capacity_too_busy",
    "timing_seasonal",
    "needs_partner_approval",
    "exclusivity_concern",
    "commitment_fear",
    "trust_skepticism",
    "tech_friction",
    "wants_to_think",
    "other",
]

# Sales angle the closer used on the call (one-tap feedback).
PITCH_VARIANTS: list[str] = [
    "roi_first",
    "scarcity",
    "social_proof",
    "discount_offer",
]

# AI-derived: how the call ended.
CALL_OUTCOMES: list[str] = [
    "committed",
    "callback_scheduled",
    "undecided",
    "declined",
    "no_meaningful_conversation",
]

# AI-derived: whether the closer overcame the primary objection.
OBJECTION_RESOLUTIONS: list[str] = ["resolved", "unresolved", "none"]

# Sentiment values (sourced from Aircall's native /sentiments endpoint/event).
SENTIMENTS: list[str] = ["positive", "neutral", "negative", "mixed"]

# Human lead-quality rating scale (inclusive).
LEAD_QUALITY_MIN: int = 1
LEAD_QUALITY_MAX: int = 5

# task_type key passed to claude_router.call_claude for the tagging pass.
CLOSER_CALL_TAGGING_TASK_TYPE: str = "closer_call_tagging"


# Convenience sets for O(1) validation.
OBJECTION_TAXONOMY_SET = frozenset(OBJECTION_TAXONOMY)
PITCH_VARIANTS_SET = frozenset(PITCH_VARIANTS)
CALL_OUTCOMES_SET = frozenset(CALL_OUTCOMES)
OBJECTION_RESOLUTIONS_SET = frozenset(OBJECTION_RESOLUTIONS)
SENTIMENTS_SET = frozenset(SENTIMENTS)


# ── A6: Closer-to-Cora Teaching Corrections ──────────────────────────────────
# Controlled vocabulary for Teaching Corrections (see CONTEXT.md → Score Dampener,
# ADR 0006, ADR 0007).

CORRECTION_REASON_NON_RESIDENTIAL = "non_residential"
CORRECTION_REASON_OWNER_NOT_MOTIVATED = "owner_not_motivated"
CORRECTION_REASON_WRONG_DISTRESS = "wrong_distress"
CORRECTION_REASON_BAD_CONTACT = "bad_contact"
CORRECTION_REASON_OTHER = "other"

CORRECTION_REASONS: list[str] = [
    CORRECTION_REASON_NON_RESIDENTIAL,
    CORRECTION_REASON_OWNER_NOT_MOTIVATED,
    CORRECTION_REASON_WRONG_DISTRESS,
    CORRECTION_REASON_BAD_CONTACT,
    CORRECTION_REASON_OTHER,
]

CORRECTION_REASONS_SET: frozenset[str] = frozenset(CORRECTION_REASONS)

# Reasons that cause a score dampener to be applied; bad_contact and other are
# label-only and trigger no suppression in the CDS engine.
DAMPENING_REASONS_SET: frozenset[str] = frozenset({
    CORRECTION_REASON_NON_RESIDENTIAL,
    CORRECTION_REASON_OWNER_NOT_MOTIVATED,
    CORRECTION_REASON_WRONG_DISTRESS,
})

# Signal types a closer can flag via wrong_distress — the exhaustive set of
# signal keys emitted by _collect_signals() in cds_engine.py.
TEACHABLE_SIGNAL_TYPES: list[str] = [
    "code_violations",
    "judgment_liens",
    "code_lien",
    "hoa_liens",
    "mechanics_liens",
    "irs_tax_liens",
    "deed_transfers",
    "probate",
    "evictions",
    "bankruptcy",
    "tax_delinquencies",
    "foreclosures",
    "building_permits",
    "enforcement_permit",
]

TEACHABLE_SIGNAL_TYPES_SET: frozenset[str] = frozenset(TEACHABLE_SIGNAL_TYPES)
