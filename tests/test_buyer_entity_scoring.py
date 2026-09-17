"""
Pure unit tests for score_candidate_pair() (WP-3, WI-2) -- no DB.

Covers the contact-corroboration tiers added on top of the existing
name/address scoring: exact email/phone match as corroboration, never as a
sole match signal. Pattern: build minimal CandidateRecord fixtures directly
(no extraction/DB round trip needed to exercise the pure scoring function).

Run:
    pytest tests/test_buyer_entity_scoring.py -v
"""
from __future__ import annotations

from src.services.buyer_entity_resolution import CandidateRecord, score_candidate_pair


def _cand(
    name: str,
    address: str | None = None,
    emails: frozenset[str] = frozenset(),
    phones: frozenset[str] = frozenset(),
    source_id: int = 1,
) -> CandidateRecord:
    return CandidateRecord(
        source_table="owners",
        source_id=source_id,
        raw_name=name,
        normalized_name=name,
        mailing_address=address,
        entity_type_hint="Individual",
        managing_members=None,
        county_id="ztest",
        emails=emails,
        phones=phones,
    )


def test_address_disagreement_checked_before_name_score():
    """A near-identical name at a clearly different address stays a no_match
    (or ambiguous, for the near-identical carve-out) -- the address-disagree
    check must run before any name-based auto-match tier, regardless of
    contact evidence being absent."""
    a = _cand("JOHN A SMITH", "100 MAIN ST, TAMPA FL 33601", source_id=1)
    b = _cand("JOHN A SMITH", "900 OAK AVE, TAMPA FL 33602", source_id=2)
    verdict = score_candidate_pair(a, b)
    assert verdict.is_match is False
    assert verdict.method in ("no_match", "ambiguous")


def test_contact_match_is_set_intersection_not_scalar():
    """Two candidates share a phone in different slots (a.phones has two
    numbers, only one overlaps with b.phones) -- intersection must still
    find it."""
    a = _cand(
        "MARIA GARCIA", "100 MAIN ST, TAMPA FL 33601",
        phones=frozenset({"+18135551111", "+18135552222"}), source_id=1,
    )
    b = _cand(
        "MARIA GARCIA", "100 MAIN ST, TAMPA FL 33601",
        phones=frozenset({"+18135552222"}), source_id=2,
    )
    verdict = score_candidate_pair(a, b)
    assert verdict.is_match is True
    assert "+18135552222" in verdict.explanation


def test_shared_phone_boosts_fuzzy_name_to_exact_tier():
    """Name score in the fuzzy band (85-89), same phone, no address to
    corroborate -- contact corroboration promotes fuzzy_name up to the
    exact_name_address tier, per the handoff's hard rule."""
    a = _cand("ROBERT J MILLER", phones=frozenset({"+18135559999"}), source_id=1)
    b = _cand("ROBERT MILLER", phones=frozenset({"+18135559999"}), source_id=2)
    verdict = score_candidate_pair(a, b)
    assert verdict.is_match is True
    assert verdict.method == "exact_name_address"
    assert "boosted by" in verdict.explanation or "no address" in verdict.explanation


def test_shared_email_confirms_high_name_score_without_address():
    """Name score >= NAME_AUTO_MATCH_MIN, no address on either side, but a
    shared email -- confirms the match at exact_name_address tier."""
    a = _cand("PATRICIA ANN WEBER", emails=frozenset({"pweber@example.com"}), source_id=1)
    b = _cand("PATRICIA A WEBER", emails=frozenset({"pweber@example.com"}), source_id=2)
    verdict = score_candidate_pair(a, b)
    assert verdict.is_match is True
    assert verdict.method == "exact_name_address"


def test_contact_match_with_weak_name_stays_ambiguous():
    """A shared phone number alone, with a name score below the ambiguous
    floor, must NOT auto-confirm -- contact is corroboration only, never a
    sole match signal. This is the handoff's hard rule, unconditional."""
    a = _cand("ROBERT JAMES MILLER", phones=frozenset({"+18135559999"}), source_id=1)
    b = _cand("SUSAN ELAINE CARTER", phones=frozenset({"+18135559999"}), source_id=2)
    verdict = score_candidate_pair(a, b)
    assert verdict.is_match is False


def test_contact_match_does_not_override_disagreeing_address():
    """A shared phone cannot rescue a pair whose addresses clearly
    disagree -- the address-disagree check runs first and is not
    overridable by contact evidence."""
    a = _cand(
        "JOHN A SMITH", "100 MAIN ST, TAMPA FL 33601",
        phones=frozenset({"+18135559999"}), source_id=1,
    )
    b = _cand(
        "JOHN A SMITH", "900 OAK AVE, TAMPA FL 33602",
        phones=frozenset({"+18135559999"}), source_id=2,
    )
    verdict = score_candidate_pair(a, b)
    assert verdict.is_match is False


def test_no_contact_overlap_falls_back_to_name_only_scoring():
    """Disjoint contacts on both sides behave identically to no contacts at
    all -- confirms the intersection logic doesn't accidentally treat
    "both have some phone" as a match."""
    a = _cand("DAVID LEE CHEN", phones=frozenset({"+18135551111"}), source_id=1)
    b = _cand("DAVID CHEN", phones=frozenset({"+18135552222"}), source_id=2)
    verdict = score_candidate_pair(a, b)
    assert verdict.method == "fuzzy_name"
    assert "no address to corroborate" in verdict.explanation
