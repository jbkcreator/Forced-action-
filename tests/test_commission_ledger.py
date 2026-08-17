"""Unit tests for commission-split math (TEST-02).

Mocks the DB session (same convention as test_guarantee_shortfall_sweep.py)
rather than using the `fresh_db` Postgres fixture, so these always run in CI
(no DATABASE_URL / live Postgres available on a bare GitHub Actions runner) —
`fresh_db`-based tests silently skip there instead of failing.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.services.commission_ledger import (
    compute_net_lines,
    post_commission,
    resolve_split_config,
)


def _exec_results(mock_db, *results):
    """Make successive mock_db.execute(...) calls return these results in
    order (each already the object .fetchone() is called on)."""
    mock_db.execute.side_effect = list(results)


# ---------------------------------------------------------------------------
# resolve_split_config
# ---------------------------------------------------------------------------

class TestResolveSplitConfig:
    def test_returns_matching_tier(self, mock_db):
        _exec_results(
            mock_db,
            MagicMock(fetchone=MagicMock(
                return_value=MagicMock(split_config_id="tier_platinum")
            )),
        )
        assert resolve_split_config(mock_db, 1_000_000) == "tier_platinum"

    def test_returns_none_when_no_tier_matches(self, mock_db):
        _exec_results(mock_db, MagicMock(fetchone=MagicMock(return_value=None)))
        assert resolve_split_config(mock_db, 999_999_999) is None


# ---------------------------------------------------------------------------
# compute_net_lines
# ---------------------------------------------------------------------------

class TestComputeNetLines:
    def test_even_split(self, mock_db):
        parties = [{"party": "platform", "pct": 50}, {"party": "broker", "pct": 50}]
        _exec_results(mock_db, MagicMock(fetchone=MagicMock(
            return_value=MagicMock(parties=parties)
        )))

        lines = compute_net_lines(mock_db, 500_000, "platform_50_broker_50")

        assert lines == [
            {"party": "platform", "amount_cents": 250_000},
            {"party": "broker", "amount_cents": 250_000},
        ]

    def test_remainder_cents_go_to_first_party(self, mock_db):
        # 33.33 / 33.33 / 33.34 of 100 cents -> 33/33/33 with 1 remainder cent,
        # which must land on the first party rather than being dropped.
        parties = [
            {"party": "a", "pct": 33.33},
            {"party": "b", "pct": 33.33},
            {"party": "c", "pct": 33.34},
        ]
        _exec_results(mock_db, MagicMock(fetchone=MagicMock(
            return_value=MagicMock(parties=parties)
        )))

        lines = compute_net_lines(mock_db, 100, "three_way")

        amounts = [l["amount_cents"] for l in lines]
        assert amounts == [34, 33, 33]
        assert sum(amounts) == 100  # no cent lost or fabricated

    def test_raises_when_split_config_not_found(self, mock_db):
        _exec_results(mock_db, MagicMock(fetchone=MagicMock(return_value=None)))
        with pytest.raises(ValueError, match="Split config not found"):
            compute_net_lines(mock_db, 1_000, "missing_config")


# ---------------------------------------------------------------------------
# post_commission
# ---------------------------------------------------------------------------

class TestPostCommission:
    def test_idempotent_on_replay(self, mock_db):
        _exec_results(mock_db, MagicMock(fetchone=MagicMock(
            return_value=MagicMock(entry_id="already-posted")
        )))

        result = post_commission(mock_db, "tid-1", 500_000, "platform_50_broker_50")

        assert result is None
        assert mock_db.execute.call_count == 1  # short-circuits before any further lookup

    def test_happy_path_with_explicit_split_config(self, mock_db):
        parties = [{"party": "platform", "pct": 50}, {"party": "broker", "pct": 50}]
        _exec_results(
            mock_db,
            MagicMock(fetchone=MagicMock(return_value=None)),  # no existing entry
            MagicMock(fetchone=MagicMock(return_value=MagicMock(
                lane_id="lane-1", broker_id="broker-1",
            ))),  # transition lookup
            MagicMock(fetchone=MagicMock(return_value=MagicMock(
                parties=parties,
            ))),  # compute_net_lines' own split-config lookup
            MagicMock(fetchone=MagicMock(return_value=MagicMock(
                entry_id="new-entry-id",
            ))),  # insert
        )

        result = post_commission(mock_db, "tid-2", 500_000, "platform_50_broker_50")

        assert result == "new-entry-id"

    def test_raises_when_no_split_config_and_no_matching_tier(self, mock_db):
        _exec_results(
            mock_db,
            MagicMock(fetchone=MagicMock(return_value=None)),  # no existing entry
            MagicMock(fetchone=MagicMock(return_value=None)),  # resolve_split_config: no tier matches
        )

        with pytest.raises(ValueError, match="No split_config_id supplied"):
            post_commission(mock_db, "tid-3", 999_999_999_999, None)

    def test_raises_when_transition_not_found(self, mock_db):
        _exec_results(
            mock_db,
            MagicMock(fetchone=MagicMock(return_value=None)),  # no existing entry
            MagicMock(fetchone=MagicMock(return_value=None)),  # transition lookup: not found
        )

        with pytest.raises(ValueError, match="Transition not found"):
            post_commission(mock_db, "tid-4", 500_000, "platform_50_broker_50")
