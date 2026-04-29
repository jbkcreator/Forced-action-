"""
Unit tests for src/utils/lead_filters.py — the shared contact-filter and
phone-priority ordering helpers used by every lead-delivery path.
"""

from types import SimpleNamespace

import pytest
from sqlalchemy import desc, select
from sqlalchemy.sql.elements import BooleanClauseList, Case

from src.core.models import DistressScore, Owner
from src.utils.lead_filters import has_contact_filter, phone_priority_order


class TestHasContactFilter:
    def test_returns_none_when_debug_true(self):
        settings = SimpleNamespace(debug=True)
        assert has_contact_filter(settings) is None

    def test_returns_clause_when_debug_false(self):
        settings = SimpleNamespace(debug=False)
        clause = has_contact_filter(settings)
        assert clause is not None

    def test_clause_compiles_against_owner_columns(self):
        settings = SimpleNamespace(debug=False)
        clause = has_contact_filter(settings)
        # Compiling the clause against the dialect proves the column refs are valid
        compiled = str(clause.compile(compile_kwargs={"literal_binds": True}))
        for col in ("phone_1", "phone_2", "phone_3", "email_1", "email_2"):
            assert col in compiled, f"Compiled filter missing reference to {col}"

    def test_clause_usable_in_select_where(self):
        settings = SimpleNamespace(debug=False)
        clause = has_contact_filter(settings)
        # Should not raise — proves the clause is a valid SQLAlchemy expression
        stmt = select(Owner).where(clause)
        assert stmt is not None


class TestPhonePriorityOrder:
    def test_returns_two_expressions(self):
        score_col = DistressScore.final_cds_score
        order = phone_priority_order(score_col)
        assert isinstance(order, list)
        assert len(order) == 2

    def test_first_element_is_case_expression(self):
        score_col = DistressScore.final_cds_score
        order = phone_priority_order(score_col)
        # The case() expression sorts phone-bearing leads to rank 0
        assert isinstance(order[0], Case)

    def test_compiles_and_references_phone_columns(self):
        score_col = DistressScore.final_cds_score
        order = phone_priority_order(score_col)
        compiled_case = str(order[0].compile(compile_kwargs={"literal_binds": True}))
        for col in ("phone_1", "phone_2", "phone_3"):
            assert col in compiled_case

    def test_usable_as_order_by(self):
        score_col = DistressScore.final_cds_score
        order = phone_priority_order(score_col)
        stmt = select(Owner).order_by(*order)
        # If we got here without raising, the order_by is structurally valid
        assert stmt is not None
