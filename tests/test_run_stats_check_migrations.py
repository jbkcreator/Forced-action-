"""
Regression coverage for PR #150 review finding #2: apply_cde10_run_stats_source_type.py
and apply_cde07_run_stats_dor_sale_outcomes.py must not silently drop a
source_type either one already added to check_run_stats_source_type,
regardless of run order.

Pure unit tests only — the union arithmetic and constraint-def parsing, not
the DDL itself. These scripts execute real ALTER TABLE against the shared
DB; that side effect is deliberately not exercised on every pytest run (it
is a one-time, manually-invoked migration per CLAUDE.md / ADR 0024), so this
suite mocks the connection instead of running the migrations live.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from migrations import apply_cde07_run_stats_dor_sale_outcomes as cde07
from migrations import apply_cde10_run_stats_source_type as cde10


def _mock_conn_with_constraint(values: set[str]) -> MagicMock:
    conn = MagicMock()
    def_str = "CHECK (source_type IN (" + ",".join(f"'{v}'" for v in values) + "))"
    conn.execute.return_value.first.return_value = MagicMock(_mapping={"def": def_str})
    return conn


class TestExistingCheckValuesParsing:
    def test_extracts_all_quoted_values(self):
        conn = _mock_conn_with_constraint({"deeds", "foreclosures", "dor_sale_outcomes"})
        result = cde10._existing_check_values(conn, "check_run_stats_source_type", "scraper_run_stats")
        assert result == {"deeds", "foreclosures", "dor_sale_outcomes"}

    def test_returns_empty_set_when_constraint_absent(self):
        conn = MagicMock()
        conn.execute.return_value.first.return_value = None
        result = cde10._existing_check_values(conn, "check_run_stats_source_type", "scraper_run_stats")
        assert result == set()


class TestCDE10NeverDropsSiblingAdditions:
    """
    The exact bug: CDE-10's REQUIRED_SOURCE_TYPES omits dor_sales,
    dor_sale_outcomes, deed_flip_outcomes, probate_lien_outcomes -- if this
    migration hardcoded and overwrote the constraint, running it after
    CDE-07's migration would reject existing dor_sale_outcomes stat rows.
    The union fix must preserve them regardless.
    """

    def test_union_preserves_values_required_list_does_not_know_about(self):
        existing_from_cde07 = {
            "deeds", "foreclosures", "outcome_label_layer",
            "dor_sales", "dor_sale_outcomes", "deed_flip_outcomes", "probate_lien_outcomes",
        }
        union = existing_from_cde07 | cde10.REQUIRED_SOURCE_TYPES
        assert "dor_sale_outcomes" in union
        assert "deed_flip_outcomes" in union
        assert "probate_lien_outcomes" in union
        # And nothing CDE-10 requires is lost either.
        assert cde10.REQUIRED_SOURCE_TYPES.issubset(union)

    def test_required_set_alone_is_a_strict_subset_of_cde07s_list(self):
        # Documents the exact gap the reviewer found: CDE-10's own list is
        # narrower than what's already live once CDE-07 has run.
        assert cde10.REQUIRED_SOURCE_TYPES < cde07.REQUIRED_SOURCE_TYPES


class TestCDE07NeverDropsSiblingAdditions:
    def test_union_preserves_values_required_list_does_not_know_about(self):
        existing_from_some_future_migration = {"a_brand_new_source_type"}
        union = existing_from_some_future_migration | cde07.REQUIRED_SOURCE_TYPES
        assert "a_brand_new_source_type" in union
        assert cde07.REQUIRED_SOURCE_TYPES.issubset(union)
