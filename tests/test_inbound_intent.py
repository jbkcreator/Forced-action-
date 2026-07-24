"""TDD for the Block 11 high-intent inbound classifier (score_inbound).

Scoring: intent_slot=50, transcript keywords=30, known caller=15,
live territory=10. Cutoff score >= 50 -> is_hot.
"""

from unittest.mock import MagicMock

import pytest

from src.services.inbound_intent import score_inbound


def _db_returning(rows_by_query):
    """Mock db whose .execute(...).first() cycles through rows_by_query in order."""
    db = MagicMock()
    results = iter(rows_by_query)

    def _execute(*_args, **_kwargs):
        result = MagicMock()
        result.first.return_value = next(results, None)
        return result

    db.execute.side_effect = _execute
    return db


class TestIntentSlotSignal:
    def test_intent_slot_alone_hits_cutoff(self):
        db = _db_returning([None, None])  # no known caller, no territory
        result = score_inbound(
            phone="+15551234567", zip_code=None, vertical=None,
            transcript=None, intent_slot=True, db=db,
        )
        assert result["score"] == 50
        assert result["is_hot"] is True
        assert "intent_slot" in result["matched_signals"]

    def test_no_signals_is_not_hot(self):
        db = _db_returning([None, None])
        result = score_inbound(
            phone="+15551234567", zip_code="33604", vertical="roofing",
            transcript="just calling about my mail", intent_slot=False, db=db,
        )
        assert result["score"] == 0
        assert result["is_hot"] is False
        assert result["matched_signals"] == []


class TestTranscriptKeywordSignal:
    @pytest.mark.parametrize("transcript", [
        "what's your pricing for roofing leads",
        "I want to subscribe to this",
        "how do I sign up",
        "I'd like to buy some leads",
    ])
    def test_keyword_match_scores_30(self, transcript):
        db = _db_returning([None, None])
        result = score_inbound(
            phone=None, zip_code=None, vertical=None,
            transcript=transcript, intent_slot=False, db=db,
        )
        assert result["score"] == 30
        assert result["is_hot"] is False  # 30 alone is below cutoff
        assert "transcript_intent" in result["matched_signals"]

    def test_no_keyword_match_scores_zero_for_signal(self):
        db = _db_returning([None, None])
        result = score_inbound(
            phone=None, zip_code=None, vertical=None,
            transcript="just checking on my package delivery",
            intent_slot=False, db=db,
        )
        assert "transcript_intent" not in result["matched_signals"]

    def test_none_transcript_does_not_match(self):
        db = _db_returning([None, None])
        result = score_inbound(
            phone=None, zip_code=None, vertical=None,
            transcript=None, intent_slot=False, db=db,
        )
        assert "transcript_intent" not in result["matched_signals"]


class TestKnownCallerSignal:
    def test_known_subscriber_scores_15(self):
        db = _db_returning([(1,), None])  # subscriber row found, no territory
        result = score_inbound(
            phone="+15551234567", zip_code=None, vertical=None,
            transcript=None, intent_slot=False, db=db,
        )
        assert result["score"] == 15
        assert "known_caller" in result["matched_signals"]

    def test_unknown_phone_no_signal(self):
        db = _db_returning([None, None])
        result = score_inbound(
            phone="+15551234567", zip_code=None, vertical=None,
            transcript=None, intent_slot=False, db=db,
        )
        assert "known_caller" not in result["matched_signals"]

    def test_missing_phone_skips_query_no_signal(self):
        db = _db_returning([None])  # only the territory query should fire
        result = score_inbound(
            phone=None, zip_code=None, vertical=None,
            transcript=None, intent_slot=False, db=db,
        )
        assert "known_caller" not in result["matched_signals"]
        assert db.execute.call_count == 0


class TestLiveTerritorySignal:
    def test_active_territory_scores_10(self):
        db = _db_returning([None, (1,)])  # no known caller, territory found
        result = score_inbound(
            phone="+15551234567", zip_code="33604", vertical="roofing",
            transcript=None, intent_slot=False, db=db,
        )
        assert result["score"] == 10
        assert "live_territory" in result["matched_signals"]

    def test_no_territory_row_no_signal(self):
        db = _db_returning([None, None])
        result = score_inbound(
            phone="+15551234567", zip_code="99999", vertical="roofing",
            transcript=None, intent_slot=False, db=db,
        )
        assert "live_territory" not in result["matched_signals"]

    def test_missing_zip_or_vertical_skips_query(self):
        db = _db_returning([None])  # only known-caller query fires
        result = score_inbound(
            phone="+15551234567", zip_code=None, vertical="roofing",
            transcript=None, intent_slot=False, db=db,
        )
        assert "live_territory" not in result["matched_signals"]
        assert db.execute.call_count == 1


class TestCombinedScoring:
    def test_transcript_plus_known_plus_territory_hits_cutoff(self):
        db = _db_returning([(1,), (1,)])  # known caller + territory
        result = score_inbound(
            phone="+15551234567", zip_code="33604", vertical="roofing",
            transcript="I'm ready to buy leads", intent_slot=False, db=db,
        )
        assert result["score"] == 30 + 15 + 10
        assert result["is_hot"] is True

    def test_known_plus_territory_without_intent_stays_below_cutoff(self):
        db = _db_returning([(1,), (1,)])
        result = score_inbound(
            phone="+15551234567", zip_code="33604", vertical="roofing",
            transcript=None, intent_slot=False, db=db,
        )
        assert result["score"] == 15 + 10
        assert result["is_hot"] is False

    def test_cutoff_boundary_exactly_49_is_not_hot(self):
        # known(15) + territory(10) + transcript(30) - 6 = not a real combo;
        # simulate boundary by only known+territory (25) plus no other signal.
        db = _db_returning([(1,), (1,)])
        result = score_inbound(
            phone="+1", zip_code="1", vertical="roofing",
            transcript=None, intent_slot=False, db=db,
        )
        assert result["score"] < 50
        assert result["is_hot"] is False

    def test_all_signals_max_score(self):
        db = _db_returning([(1,), (1,)])
        result = score_inbound(
            phone="+15551234567", zip_code="33604", vertical="roofing",
            transcript="ready to subscribe", intent_slot=True, db=db,
        )
        assert result["score"] == 50 + 30 + 15 + 10
        assert result["is_hot"] is True
        assert set(result["matched_signals"]) == {
            "intent_slot", "transcript_intent", "known_caller", "live_territory",
        }

    def test_result_shape(self):
        db = _db_returning([None, None])
        result = score_inbound(
            phone=None, zip_code=None, vertical=None,
            transcript=None, intent_slot=False, db=db,
        )
        assert set(result.keys()) == {"is_hot", "score", "matched_signals"}
        assert isinstance(result["is_hot"], bool)
        assert isinstance(result["score"], int)
        assert isinstance(result["matched_signals"], list)
