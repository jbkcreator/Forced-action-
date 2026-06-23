"""M10 / B2 Lead Delivery — pure matching-logic unit tests (no DB)."""

from datetime import datetime, timezone

from src.services.lead_delivery import (
    Candidate,
    bucket_for,
    headroom,
    pick_winner,
    tier_rank,
)


class TestHeadroom:
    def test_headroom_is_bucket_minus_delivered(self):
        assert headroom(bucket=20, credits=0, delivered_this_cycle=5) == 15

    def test_credits_add_to_headroom(self):
        assert headroom(bucket=20, credits=2, delivered_this_cycle=20) == 2

    def test_headroom_never_negative(self):
        assert headroom(bucket=5, credits=0, delivered_this_cycle=9) == 0

    def test_bucket_for_missing_grade_is_zero(self):
        assert bucket_for({"gold": 20}, "platinum") == 0

    def test_bucket_for_present_grade(self):
        assert bucket_for({"gold": 20}, "gold") == 20

    def test_bucket_for_empty_entitlement(self):
        assert bucket_for(None, "gold") == 0


class TestPickWinner:
    def test_no_candidates_returns_none(self):
        assert pick_winner([]) is None

    def test_zero_headroom_excluded(self):
        c = Candidate(account_id="a", plan_tier="pro", headroom=0, last_delivered_at=None)
        assert pick_winner([c]) is None

    def test_tier_priority_wins_first(self):
        starter = Candidate("starter", "starter", headroom=50, last_delivered_at=None)
        pro = Candidate("pro", "pro", headroom=1, last_delivered_at=None)
        # pro has less headroom but higher tier — tier wins
        assert pick_winner([starter, pro]).account_id == "pro"

    def test_headroom_breaks_tier_tie(self):
        a = Candidate("a", "pro", headroom=10, last_delivered_at=None)
        b = Candidate("b", "pro", headroom=40, last_delivered_at=None)
        assert pick_winner([a, b]).account_id == "b"

    def test_round_robin_breaks_remaining_tie(self):
        older = Candidate("older", "pro", headroom=10,
                          last_delivered_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        newer = Candidate("newer", "pro", headroom=10,
                          last_delivered_at=datetime(2026, 6, 1, tzinfo=timezone.utc))
        # older last delivery = waited longest = wins
        assert pick_winner([older, newer]).account_id == "older"

    def test_never_delivered_wins_round_robin(self):
        fresh = Candidate("fresh", "pro", headroom=10, last_delivered_at=None)
        delivered = Candidate("delivered", "pro", headroom=10,
                              last_delivered_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert pick_winner([fresh, delivered]).account_id == "fresh"


def test_tier_rank_ordering():
    assert tier_rank("dominator") > tier_rank("pro") > tier_rank("starter") > tier_rank("free_trial")
    assert tier_rank(None) == 0
    assert tier_rank("unknown") == 0
