"""
Hunter — confidence gating (single enforcement point).

Hunter's constitution: "<70 confidence = UNVERIFIED, never surfaces in
Lifecycle's drafts." Every capability module (buyer_entity_resolution.py,
whale_detection.py, buyer_type_classification.py, ...) calls into this
rather than re-implementing the threshold, so the rule can't drift between
modules as more of them get built.
"""
from __future__ import annotations

UNVERIFIED_FLOOR = 70


def is_citable(confidence: int) -> bool:
    """
    True if a record at this confidence may surface in a Lifecycle draft or any
    customer-facing output. Below UNVERIFIED_FLOOR, the record may exist in
    the DB (for audit/traceability) but must never be cited.
    """
    return confidence >= UNVERIFIED_FLOOR


def verification_status(confidence: int) -> str:
    """The buyer_entities.verification_status value for a given confidence score."""
    return "verified" if is_citable(confidence) else "unverified"
