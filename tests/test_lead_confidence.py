"""Unit tests for src/services/lead_confidence.py — A2 Lead Confidence gating.

The pure computation (compute_lead_confidence) takes a list of contributing
SignalRecords and returns a 0-1 Lead Confidence plus an is_guess_lead flag.
No DB access — fixtures are hand-built signal lists matching the worked
examples in tasks/A2-implementation-plan.md §6.
"""
from datetime import date

from config.confidence import MIN_CONFIDENCE_THRESHOLD
from src.services.lead_confidence import SignalRecord, compute_lead_confidence


AS_OF = date(2026, 6, 24)


def _sig(match_confidence, *, signal_type="code_violations", days_ago=10):
    return SignalRecord(
        signal_type=signal_type,
        signal_date=date(2026, 6, 14),  # 10 days before AS_OF, inside window
        match_confidence=match_confidence,
    )


class TestComputeLeadConfidence:
    def test_fuzzy_single_signal_is_guess(self):
        # 456 Oak Ave: one signal, pending-band match (0.78), thin file.
        # match_comp = (0.78-0.75)/(0.92-0.75) = 0.176 ; corr_comp(N=1) = 0.0
        # lead_confidence = 0.6*0.176 + 0.4*0 = 0.11  -> below 0.40 -> guess
        result = compute_lead_confidence([_sig(0.78)], as_of=AS_OF)
        assert round(result.lead_confidence, 2) == 0.11
        assert result.is_guess_lead is True

    def test_high_match_thick_file_is_confident(self):
        # 123 Main St: strong match (0.95) + 3 distinct signals.
        # match_comp=1.0 ; corr_comp(N=3)=0.8 ; 0.6*1.0 + 0.4*0.8 = 0.92
        signals = [
            _sig(0.95, signal_type="foreclosures"),
            _sig(0.95, signal_type="code_violations"),
            _sig(0.95, signal_type="tax_delinquencies"),
        ]
        result = compute_lead_confidence(signals, as_of=AS_OF)
        assert round(result.lead_confidence, 2) == 0.92
        assert result.is_guess_lead is False

    def test_solid_single_signal_borderline_sells(self):
        # 789 Pine Rd: matched (0.93) but a single signal (thin).
        # match_comp=1.0 ; corr_comp(N=1)=0.0 ; 0.6*1.0 = 0.60 -> not a guess
        result = compute_lead_confidence([_sig(0.93)], as_of=AS_OF)
        assert round(result.lead_confidence, 2) == 0.60
        assert result.is_guess_lead is False

    def test_pending_match_two_signals_passes(self):
        # 22 Elm Ct: pending-band match (0.90) + 2 distinct signals.
        # match_comp=(0.90-0.75)/0.17=0.882 ; corr_comp(N=2)=0.5
        # 0.6*0.882 + 0.4*0.5 = 0.73 -> not a guess
        signals = [
            _sig(0.90, signal_type="foreclosures"),
            _sig(0.90, signal_type="probate"),
        ]
        result = compute_lead_confidence(signals, as_of=AS_OF)
        assert round(result.lead_confidence, 2) == 0.73
        assert result.is_guess_lead is False

    def test_missing_match_confidence_defaults_to_matched(self):
        # Spokes without a stored match_confidence (tax_delinquencies,
        # building_permits, incidents) pass match_confidence=None. A persisted
        # record cleared the loader's match gate, so None -> matched (1.0).
        result = compute_lead_confidence(
            [_sig(None, signal_type="tax_delinquencies")], as_of=AS_OF
        )
        assert round(result.lead_confidence, 2) == 0.60  # 0.6*1.0 + N=1
        assert result.is_guess_lead is False

    def test_staleness_does_not_affect_confidence(self):
        # D2: staleness is handled in CDS scoring, NOT here. Two leads identical
        # except signal age (both inside the stacking window) score the same.
        fresh = SignalRecord("foreclosures", date(2026, 6, 20), 0.95)
        old = SignalRecord("foreclosures", date(2026, 1, 20), 0.95)  # ~155d, in window
        r_fresh = compute_lead_confidence([fresh], as_of=AS_OF)
        r_old = compute_lead_confidence([old], as_of=AS_OF)
        assert r_fresh.lead_confidence == r_old.lead_confidence

    def test_threshold_uses_strict_less_than(self):
        # is_guess_lead must be (confidence < threshold), not <=. Pin the
        # operator as a law that holds for every input shape.
        for signals in ([_sig(0.78)], [_sig(0.93)], [_sig(None)]):
            r = compute_lead_confidence(signals, as_of=AS_OF)
            assert r.is_guess_lead == (r.lead_confidence < MIN_CONFIDENCE_THRESHOLD)

    def test_below_review_floor_record_is_not_corroboration(self):
        # A record below the review floor is not real corroboration. Adding one
        # to a solid single signal must NOT raise N from 1 to 2.
        solid = _sig(0.95, signal_type="foreclosures")
        weak = _sig(0.50, signal_type="code_violations")  # below review_min 0.75
        r = compute_lead_confidence([solid, weak], as_of=AS_OF)
        # still N=1 -> corr 0.0 -> 0.6*1.0 = 0.60, unchanged by the weak record
        assert round(r.lead_confidence, 2) == 0.60
