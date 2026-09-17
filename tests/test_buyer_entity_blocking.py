"""
Pure unit tests for block_candidates() / score_blocked_pairs() contact
blocking (WP-3, WI-2) -- no DB.

Covers the contact-blocking pass added alongside the existing name-token+ZIP
blocking: a shared email/phone must create a comparison group even when the
two records have different (or absent) ZIPs -- which name-token+ZIP blocking
alone can never do. Also covers the frequency cap applying to contact values
exactly as it already does to name tokens.

Run:
    pytest tests/test_buyer_entity_blocking.py -v
"""
from __future__ import annotations

from src.services.buyer_entity_resolution import (
    MAX_BLOCK_FREQUENCY,
    CandidateRecord,
    block_candidates,
    score_blocked_pairs,
)


def _cand(
    name: str,
    address: str | None = None,
    phones: frozenset[str] = frozenset(),
    emails: frozenset[str] = frozenset(),
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
        phones=phones,
        emails=emails,
    )


def test_shared_phone_creates_cross_zip_comparison():
    """Two records with the SAME phone, DIFFERENT ZIPs (deed-sourced side has
    no address at all -- typical for a property whose current owner proxy
    doesn't match), and no shared name tokens beyond a fuzzy variant --
    name-token+ZIP blocking alone would never put these in the same group.
    The contact blocking pass must produce exactly this one comparison, and
    the eventual verdict is a genuine match (fuzzy name + shared phone,
    corroborated per score_candidate_pair's rules -- see
    test_buyer_entity_scoring.py for the scoring-tier assertions)."""
    a = _cand(
        "WILLIAM T ANDERSON", "100 MAIN ST, TAMPA FL 33601",
        phones=frozenset({"+18135551234"}), source_id=1,
    )
    b = _cand(
        "WILLIAM ANDERSON", None,
        phones=frozenset({"+18135551234"}), source_id=2,
    )
    blocks = block_candidates([a, b])
    scored = score_blocked_pairs(blocks)
    assert len(scored) == 1
    _, _, verdict = scored[0]
    assert verdict.is_match is True
    assert verdict.method == "exact_name_address"


def test_no_shared_contact_and_different_zip_produces_no_pair():
    """Sanity check: without a shared contact, two candidates with different
    ZIPs and only one shared name token still get filtered by the existing
    co-occurrence gate (unchanged behavior)."""
    a = _cand("PATRICIA JONES", "100 MAIN ST, TAMPA FL 33601", source_id=1)
    b = _cand("PATRICIA SMITH", "500 GULF BLVD, CLEARWATER FL 33755", source_id=2)
    blocks = block_candidates([a, b])
    scored = score_blocked_pairs(blocks)
    assert scored == []


def test_contact_frequency_cap_drops_shared_office_line():
    """A phone number shared by more than MAX_BLOCK_FREQUENCY records (a
    realtor office line, an `info@` mailbox) must be excluded as a blocking
    key entirely, exactly like an over-frequent name token -- otherwise a
    single shared contact could create a runaway comparison group."""
    shared_phone = frozenset({"+18135550000"})
    candidates = [
        _cand(f"OWNER NUMBER {i}", phones=shared_phone, source_id=i)
        for i in range(MAX_BLOCK_FREQUENCY + 100)
    ]
    blocks = block_candidates(candidates)
    assert ("phone:+18135550000", None) not in blocks
