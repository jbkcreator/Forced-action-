"""tests/services/test_fa_max_person_search.py"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.services.fa_max_person_search import search_fa_max_persons


def _mock_db(rows):
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = rows
    return db


def _row(mapping: dict):
    m = MagicMock()
    m._mapping = mapping
    return m


class TestSearchFaMaxPersons:
    def test_empty_query_returns_empty_list(self):
        db = MagicMock()
        assert search_fa_max_persons(db, "") == []
        db.execute.assert_not_called()

    def test_returns_matches_shaped_correctly(self):
        db = _mock_db([
            _row({
                "person_id": "p-1", "full_name": "Jane Doe", "email": "jane@example.com",
                "phone": "8135550100", "last_stage": "under_review",
            }),
        ])
        results = search_fa_max_persons(db, "Jane Doe")
        assert results == [{
            "person_id": "p-1", "full_name": "Jane Doe", "email": "jane@example.com",
            "phone": "8135550100", "last_stage": "under_review",
        }]

    def test_query_is_passed_as_bind_param_not_interpolated(self):
        db = _mock_db([])
        search_fa_max_persons(db, "O'Brien")  # a name with a quote -- must not break the query
        call_args = db.execute.call_args
        assert call_args.args[1]["query"] == "O'Brien"

    def test_phone_query_is_normalized_to_e164_for_the_phone_comparison(self):
        db = _mock_db([])
        search_fa_max_persons(db, "(813) 555-0100")
        params = db.execute.call_args.args[1]
        assert params["normalized_query"] == "+18135550100"
        # the ILIKE name/email clauses keep the raw typed text
        assert params["query"] == "(813) 555-0100"

    def test_non_phone_query_normalizes_to_none_and_cannot_match(self):
        db = _mock_db([])
        search_fa_max_persons(db, "Jane Doe")
        assert db.execute.call_args.args[1]["normalized_query"] is None

    def test_respects_limit(self):
        db = _mock_db([])
        search_fa_max_persons(db, "Jane", limit=3)
        call_args = db.execute.call_args
        assert call_args.args[1]["limit"] == 3
