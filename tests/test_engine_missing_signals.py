"""
Stage D — engine reframe.

Replaces the multiplicative `signal_coverage_pct` discount with feature
suppression: signals listed in `cfg.missing_signals` are dropped before
primary selection and stacking counts, so a Pinellas property with a strong
primary signal now scores comparably to a Hillsborough property carrying
the same primary signal.

These tests pin the new behavior at the _score_vertical level so a future
refactor can't quietly reintroduce the false-zero / multiplicative discount.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock

from src.services.cds_engine import MultiVerticalScorer


def _signal(sig_type: str, days_ago: int, amount: float | None = None) -> dict:
    return {
        "type":   sig_type,
        "date":   date.today() - timedelta(days=days_ago),
        "amount": amount,
    }


def _scorer() -> MultiVerticalScorer:
    # MultiVerticalScorer doesn't touch DB during _score_vertical; the session
    # arg is unused for the unit under test.
    return MultiVerticalScorer(session=MagicMock())


# ---------------------------------------------------------------------------
# Signal type already in missing_signals must not participate
# ---------------------------------------------------------------------------

class TestMissingSignalsExclusion:

    def test_missing_signal_excluded_from_primary_candidates(self):
        """A signal type listed as missing for this county can't be the primary."""
        scorer = _scorer()
        signals = [
            _signal("foreclosures", days_ago=10),
            _signal("code_violations", days_ago=5),  # would normally outrank foreclosures
        ]
        # With code_violations forcibly absent, foreclosures must win.
        result = scorer._score_vertical(
            vertical="wholesalers",
            signals=signals,
            owner=None,
            financial=None,
            missing_signals=frozenset({"code_violations"}),
        )
        assert result["primary_signal"] == "foreclosures"

    def test_missing_signal_excluded_from_stacking_count(self):
        """A missing signal type must not inflate the stacking count.

        Uses three signals whose wholesalers weights all sit comfortably
        above STACKING_MIN_WEIGHT (30) so the test doesn't accidentally
        depend on a hand-tuneable boundary. Currently:
          foreclosures=68, judgment_liens=68, irs_tax_liens=55.
        """
        scorer = _scorer()
        signals = [
            _signal("foreclosures",   days_ago=10),
            _signal("judgment_liens", days_ago=20),
            _signal("irs_tax_liens",  days_ago=15),
        ]
        result_with_all = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
            missing_signals=frozenset(),
        )
        result_without_one = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
            missing_signals=frozenset({"irs_tax_liens"}),
        )
        assert result_with_all["signals_within_window"] == 3
        assert result_without_one["signals_within_window"] == 2
        assert result_with_all["stacking_bonus"] > result_without_one["stacking_bonus"]

    def test_default_missing_signals_is_empty(self):
        """When missing_signals isn't passed, behavior is unchanged from before Stage D."""
        scorer = _scorer()
        signals = [_signal("foreclosures", days_ago=10)]
        # Call without the kwarg — default empty frozenset.
        result_default = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
        )
        result_explicit = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
            missing_signals=frozenset(),
        )
        assert result_default["score"] == result_explicit["score"]
        assert result_default["primary_signal"] == result_explicit["primary_signal"]


# ---------------------------------------------------------------------------
# Cross-county parity — the original Pinellas bug
# ---------------------------------------------------------------------------

class TestCrossCountyParity:

    def test_same_primary_signal_scores_equally_with_or_without_missing_axes(self):
        """
        The structural fix: a Pinellas property with a strong primary signal
        should score the same as a Hillsborough property with the same signal.

        Before Stage D: Pinellas's score was multiplied by ~0.6 because of
        the coverage normalizer, which is exactly the "discount the lead
        because we didn't scrape another signal type" behavior that produced
        the inversion. The score for a foreclosure-only property should be
        identical regardless of which county's missing_signals set we pass.
        """
        scorer = _scorer()
        signals = [_signal("foreclosures", days_ago=10)]

        hillsborough_result = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
            missing_signals=frozenset(),
        )
        pinellas_result = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
            missing_signals=frozenset({
                "code_violations", "code_lien", "enforcement_permit",
                "tax_delinquencies", "bankruptcy", "evictions",
            }),
        )
        # The actual numeric score must be identical — both counties observe
        # foreclosure, neither observes code_violations on this property.
        assert hillsborough_result["score"] == pinellas_result["score"]
        assert hillsborough_result["primary_signal"] == pinellas_result["primary_signal"]

    def test_pinellas_signal_in_missing_set_drops_out_even_if_present(self):
        """
        If a signal is in cfg.missing_signals AND happens to be present on
        the record (data inconsistency / partial backfill), the engine must
        still treat it as unobserved. This keeps Pinellas inference robust
        to legacy rows that slipped through before the missing_signals mask
        was tightened.

        Using `restoration` here: code_violations base weight (75) is the
        highest of any signal for that vertical, so when present it wins
        primary cleanly. With missing_signals={code_violations}, the second
        candidate (foreclosures at base 30) must take over.
        """
        scorer = _scorer()
        signals = [
            _signal("code_violations", days_ago=5),
            _signal("foreclosures",    days_ago=60),
        ]
        # Hillsborough — code_violations is a valid axis and wins on weight.
        h = scorer._score_vertical(
            vertical="restoration", signals=signals, owner=None, financial=None,
            missing_signals=frozenset(),
        )
        # Pinellas — code_violations listed as missing, foreclosures wins.
        p = scorer._score_vertical(
            vertical="restoration", signals=signals, owner=None, financial=None,
            missing_signals=frozenset({"code_violations"}),
        )
        assert h["primary_signal"] == "code_violations"
        assert p["primary_signal"] == "foreclosures"


# ---------------------------------------------------------------------------
# Sanity: missing_signals doesn't break the happy path
# ---------------------------------------------------------------------------

class TestSanity:

    def test_no_signals_with_missing_set_returns_zero_score(self):
        scorer = _scorer()
        result = scorer._score_vertical(
            vertical="wholesalers", signals=[], owner=None, financial=None,
            missing_signals=frozenset({"code_violations"}),
        )
        assert result["score"] == 0.0
        assert result["primary_signal"] is None

    def test_only_missing_signals_present_returns_zero_score(self):
        """If every signal the property carries is listed missing, score = 0."""
        scorer = _scorer()
        signals = [
            _signal("code_violations", days_ago=5),
            _signal("tax_delinquencies", days_ago=10),
        ]
        result = scorer._score_vertical(
            vertical="wholesalers", signals=signals, owner=None, financial=None,
            missing_signals=frozenset({"code_violations", "tax_delinquencies"}),
        )
        assert result["score"] == 0.0
        assert result["primary_signal"] is None


# ---------------------------------------------------------------------------
# Coverage discount block removed from score_property
# ---------------------------------------------------------------------------

class TestCoverageDiscountRemoved:

    def test_signal_coverage_pct_no_longer_called_in_score_property(self):
        """
        Stage D removed the multiplicative `signal_coverage_pct` discount.
        Verify by reading score_property's source: it should no longer call
        signal_coverage_pct or assign the `coverage_pct` field on result dicts.
        """
        import inspect
        from src.services.cds_engine import MultiVerticalScorer
        src = inspect.getsource(MultiVerticalScorer.score_property)
        assert "signal_coverage_pct(" not in src, (
            "score_property still calls signal_coverage_pct — the multiplicative "
            "discount must be removed in Stage D."
        )
        assert '"coverage_pct"' not in src, (
            "score_property still writes coverage_pct onto vertical_results — "
            "the coverage discount path should be fully gone."
        )

    def test_score_property_resolves_county_cfg_once(self):
        """
        Stage D moves the for_county() lookup to a single location (before
        the per-vertical scoring loop). Verify there's exactly one lookup so
        we don't accidentally re-fetch on every iteration.
        """
        import inspect
        from src.services.cds_engine import MultiVerticalScorer
        src = inspect.getsource(MultiVerticalScorer.score_property)
        assert src.count("for_county(prop.county_id)") == 1, (
            "for_county(prop.county_id) should be called exactly once in score_property."
        )
