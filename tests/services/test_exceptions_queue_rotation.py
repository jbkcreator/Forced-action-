"""
Regression test: EXCEPTIONS queue no longer starves pairs beyond the cap.

Before the fix, record_ambiguous_pair_exceptions() always took
ambiguous_pairs[:max_new] in the same deterministic order every sweep, so a
persistent set of >max_new ambiguous pairs let the same early pairs win the
cap forever -- any pair past position max_new never reached EXCEPTIONS. The
fix checks which pairs are already recorded and prioritizes unseen ones
below the cap, so every pair reaches the queue within a bounded number of
sweeps.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.services.buyer_entity_resolution import (
    CandidateRecord,
    MatchVerdict,
    _record_key,
    record_ambiguous_pair_exceptions,
)


def _pair(i: int) -> tuple[CandidateRecord, CandidateRecord, MatchVerdict]:
    a = CandidateRecord(
        source_table="owners", source_id=i, raw_name=f"OWNER {i}",
        normalized_name=f"OWNER {i}", mailing_address=None,
        entity_type_hint=None, managing_members=None, county_id="ztest",
    )
    b = CandidateRecord(
        source_table="owners", source_id=i + 1_000_000, raw_name=f"OWNER {i}B",
        normalized_name=f"OWNER {i}B", mailing_address=None,
        entity_type_hint=None, managing_members=None, county_id="ztest",
    )
    verdict = MatchVerdict(is_match=False, confidence=60, method="fuzzy_name", explanation="ambiguous")
    return a, b, verdict


def _pair_ref(cand_a: CandidateRecord, cand_b: CandidateRecord) -> tuple[str, str]:
    left_key, right_key = _record_key(cand_a), _record_key(cand_b)
    left_ref, right_ref = f"{left_key[0]}#{left_key[1]}", f"{right_key[0]}#{right_key[1]}"
    if right_ref < left_ref:
        left_ref, right_ref = right_ref, left_ref
    return left_ref, right_ref


def _mock_session(existing_refs: list[tuple[str, str]]) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.mappings.return_value = [
        {"left_ref": lr, "right_ref": rr} for lr, rr in existing_refs
    ]
    return session


def test_unseen_pairs_are_prioritized_under_the_cap(monkeypatch):
    # 5 pairs, cap of 2. Pair 0 is already in EXCEPTIONS; the other 4 are
    # unseen. Unseen pairs must win the cap over the seen one.
    pairs = [_pair(i) for i in range(5)]
    already_recorded = {_pair_ref(pairs[0][0], pairs[0][1])}

    captured_rows: list[dict] = []
    monkeypatch.setattr(
        "src.services.buyer_entity_resolution.record_exceptions_batch",
        lambda session, rows: captured_rows.extend(rows),
    )

    session = _mock_session(sorted(already_recorded))
    recorded = record_ambiguous_pair_exceptions(session, pairs, max_new=2)

    assert recorded == 2
    inserted_refs = {(row["left_ref"], row["right_ref"]) for row in captured_rows}
    assert inserted_refs.isdisjoint(already_recorded), (
        "already-recorded pair must not consume cap slots while unseen pairs exist"
    )


def test_no_cap_needed_records_every_pair(monkeypatch):
    pairs = [_pair(i) for i in range(3)]

    captured_rows: list[dict] = []
    monkeypatch.setattr(
        "src.services.buyer_entity_resolution.record_exceptions_batch",
        lambda session, rows: captured_rows.extend(rows),
    )

    session = _mock_session([])
    recorded = record_ambiguous_pair_exceptions(session, pairs, max_new=10)

    assert recorded == 3
    assert len(captured_rows) == 3
    # under the cap, no existence lookup should even be needed
    session.execute.assert_not_called()
