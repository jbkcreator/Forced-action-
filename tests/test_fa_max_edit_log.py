"""
WP-T3-2 — Weekly Edit Log: unit tests (no DB required).

Coverage:
  1. token_change_ratio — parity with old _is_material_edit cases
  2. is_material_edit — threshold boundary
  3. word_diff — format and identity cases
  4. categorize — every category rule, multi-label, wording fallback

Run: pytest tests/test_fa_max_edit_log.py -v
"""
from __future__ import annotations

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# 1. token_change_ratio
# ─────────────────────────────────────────────────────────────────────────────

class TestTokenChangeRatio:
    def test_identical_texts_return_zero(self):
        from src.services.fa_max_edit_log import token_change_ratio
        assert token_change_ratio("hello world", "hello world") == 0.0

    def test_completely_different_texts_return_one(self):
        from src.services.fa_max_edit_log import token_change_ratio
        assert token_change_ratio("hello world", "foo bar") == 1.0

    def test_partial_change(self):
        from src.services.fa_max_edit_log import token_change_ratio
        # "hello world" vs "hello earth": 'world'↔'earth' = 2 diff / 3 union = 0.666
        ratio = token_change_ratio("hello world", "hello earth")
        assert abs(ratio - 2 / 3) < 0.001

    def test_empty_both_zero(self):
        from src.services.fa_max_edit_log import token_change_ratio
        assert token_change_ratio("", "") == 0.0

    def test_case_insensitive(self):
        from src.services.fa_max_edit_log import token_change_ratio
        assert token_change_ratio("Hello World", "hello world") == 0.0

    def test_punctuation_ignored(self):
        from src.services.fa_max_edit_log import token_change_ratio
        # \w+ strips punctuation, so "hello!" and "hello" should match
        assert token_change_ratio("hello!", "hello") == 0.0

    def test_old_implementation_parity_small_change(self):
        """Verify parity with admin_router._is_material_edit for a change that
        is not material (≤ 15% of union tokens differ)."""
        from src.services.fa_max_edit_log import token_change_ratio, MATERIAL_EDIT_THRESHOLD
        # Changing one word in a long sentence: ratio should be < threshold
        old = "Hi Mike, I wanted to follow up on the property deal we discussed"
        new = "Hi Mike, I wanted to follow up on the property deal we explored"
        ratio = token_change_ratio(old, new)
        assert ratio < MATERIAL_EDIT_THRESHOLD  # small change, not material

    def test_old_implementation_parity_large_change(self):
        """Verify parity for a change that IS material."""
        from src.services.fa_max_edit_log import token_change_ratio, MATERIAL_EDIT_THRESHOLD
        old = "Hi Mike, rates are 7.2%"
        new = "Hi Mike, rates start at 6.9% for your flip project"
        ratio = token_change_ratio(old, new)
        assert ratio > MATERIAL_EDIT_THRESHOLD


# ─────────────────────────────────────────────────────────────────────────────
# 2. is_material_edit
# ─────────────────────────────────────────────────────────────────────────────

class TestIsMaterialEdit:
    def test_identical_not_material(self):
        from src.services.fa_max_edit_log import is_material_edit
        assert not is_material_edit("hello world", "hello world")

    def test_large_change_is_material(self):
        from src.services.fa_max_edit_log import is_material_edit
        assert is_material_edit("Hi Mike, rates are 7.2%", "Hi Mike, rates start at 6.9% for your flip")

    def test_single_word_change_in_long_text_not_material(self):
        from src.services.fa_max_edit_log import is_material_edit
        old = "Hi Mike, I wanted to follow up on the property deal we discussed last week"
        new = "Hi Mike, I wanted to follow up on the property deal we explored last week"
        assert not is_material_edit(old, new)

    def test_empty_both_not_material(self):
        from src.services.fa_max_edit_log import is_material_edit
        assert not is_material_edit("", "")

    def test_empty_old_non_empty_new_is_material(self):
        from src.services.fa_max_edit_log import is_material_edit
        assert is_material_edit("", "Hi Mike how are you doing")

    def test_threshold_constant_is_015(self):
        from src.services.fa_max_edit_log import MATERIAL_EDIT_THRESHOLD
        assert MATERIAL_EDIT_THRESHOLD == 0.15


# ─────────────────────────────────────────────────────────────────────────────
# 3. word_diff
# ─────────────────────────────────────────────────────────────────────────────

class TestWordDiff:
    def test_identical_texts_no_markers(self):
        from src.services.fa_max_edit_log import word_diff
        result = word_diff("hello world", "hello world")
        assert "[-" not in result
        assert "{+" not in result

    def test_deletion_marked(self):
        from src.services.fa_max_edit_log import word_diff
        result = word_diff("hello world foo", "hello world")
        assert "[-foo-]" in result

    def test_addition_marked(self):
        from src.services.fa_max_edit_log import word_diff
        result = word_diff("hello world", "hello world bar")
        assert "{+bar+}" in result

    def test_replacement_marked(self):
        from src.services.fa_max_edit_log import word_diff
        result = word_diff("rates are 7.2%", "rates start at 6.9%")
        assert "[-are-]" in result or "[-7.2%-]" in result
        assert "{+start+}" in result or "{+6.9%-]" in result or "{+6.9%+}" in result

    def test_empty_old_shows_addition(self):
        from src.services.fa_max_edit_log import word_diff
        result = word_diff("", "hello world")
        assert "{+hello+}" in result
        assert "{+world+}" in result

    def test_empty_new_shows_deletion(self):
        from src.services.fa_max_edit_log import word_diff
        result = word_diff("hello world", "")
        assert "[-hello-]" in result
        assert "[-world-]" in result


# ─────────────────────────────────────────────────────────────────────────────
# 4. categorize
# ─────────────────────────────────────────────────────────────────────────────

class TestCategorize:
    def test_no_change_wording_fallback(self):
        from src.services.fa_max_edit_log import categorize
        result = categorize("hello world", "hello world")
        assert result == ["wording"]

    def test_numbers_changed(self):
        from src.services.fa_max_edit_log import categorize
        result = categorize("rate is 7.2%", "rate is 6.9%")
        assert "numbers" in result

    def test_dollar_amount_changed(self):
        from src.services.fa_max_edit_log import categorize
        result = categorize("loan is $500,000", "loan is $450,000")
        assert "numbers" in result

    def test_url_changed(self):
        from src.services.fa_max_edit_log import categorize
        result = categorize("see https://old.com for details", "see https://new.com for details")
        assert "links" in result

    def test_opening_changed(self):
        from src.services.fa_max_edit_log import categorize
        old = "Hi Mike,\nhope you are well"
        new = "Hello Mike,\nhope you are well"
        result = categorize(old, new)
        assert "opening" in result

    def test_sign_off_changed(self):
        from src.services.fa_max_edit_log import categorize
        # Last non-empty line differs → sign_off fires.
        # "Best, Josh" vs "Thanks, Josh" — last line differs.
        old = "Hi Mike, let me know if you have questions.\nBest, Josh"
        new = "Hi Mike, let me know if you have questions.\nThanks, Josh"
        result = categorize(old, new)
        assert "sign_off" in result

    def test_shortened(self):
        from src.services.fa_max_edit_log import categorize
        old = " ".join(["word"] * 10)
        new = " ".join(["word"] * 7)  # 70% — under 80% threshold
        result = categorize(old, new)
        assert "shortened" in result

    def test_lengthened(self):
        from src.services.fa_max_edit_log import categorize
        old = " ".join(["word"] * 10)
        new = " ".join(["word"] * 13)  # 130% — over 120% threshold
        result = categorize(old, new)
        assert "lengthened" in result

    def test_multi_label_numbers_and_sign_off(self):
        from src.services.fa_max_edit_log import categorize
        old = "rate is 7% — best, Josh"
        new = "rate is 6% — thanks, Josh"
        result = categorize(old, new)
        assert "numbers" in result
        assert "sign_off" in result

    def test_wording_fallback_when_no_rule_matches(self):
        from src.services.fa_max_edit_log import categorize
        # Only middle lines change; opening and sign_off are the same.
        # No numbers/links/length change → wording fallback.
        old = "Hi Mike,\nI think the deal looks quite promising.\nBest, Josh"
        new = "Hi Mike,\nI believe the deal seems rather interesting.\nBest, Josh"
        result = categorize(old, new)
        assert result == ["wording"]

    def test_order_is_fixed_numbers_first(self):
        """When multiple categories match, order is: numbers, links, opening, sign_off,
        shortened, lengthened, then wording."""
        from src.services.fa_max_edit_log import categorize
        # Change a number AND a link
        old = "rate 7% see https://old.com"
        new = "rate 6% see https://new.com"
        result = categorize(old, new)
        assert result.index("numbers") < result.index("links")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Weekly report rendering (WP-T3-2 §5)
# Pure format tests — mock build_rollup so no DB required.
# ─────────────────────────────────────────────────────────────────────────────

class TestWeeklyReportRendering:
    """Tests for the enriched build_report() format (WP-T3-2 §5)."""

    def _make_rollup(self, *, agent="stage_monitor", tier="A", rate_this_week=0.143,
                     rate_prior_4w=0.06, has_prior=True, n_decided=14, n_edited=3,
                     n_material=2, top_categories=None, biggest_edit=None, over_gate=False):
        from src.services.fa_max_edit_log import AgentRollup
        return AgentRollup(
            agent_name=agent, tier=tier,
            rate_this_week=rate_this_week, rate_prior_4w=rate_prior_4w,
            rate_prior_4w_has_data=has_prior,
            n_decided=n_decided, n_edited=n_edited, n_material=n_material,
            top_categories=top_categories or ["numbers", "sign_off"],
            biggest_edit=biggest_edit,
            over_gate=over_gate,
        )

    def test_report_shows_agent_name_and_tier(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch
        rollup = self._make_rollup()
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[rollup]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=0):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
                    mock_db.return_value.__enter__ = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock(return_value=False)
                    report = build_report()
        assert "stage_monitor" in report
        assert "tier A" in report

    def test_over_gate_shows_warning_symbol(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch, MagicMock
        rollup = self._make_rollup(rate_this_week=0.143, over_gate=True)
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[rollup]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=0):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = MagicMock()
                    mock_db.return_value.__enter__ = MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = MagicMock(return_value=False)
                    report = build_report()
        assert "⚠" in report

    def test_under_gate_no_warning_symbol(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch, MagicMock
        rollup = self._make_rollup(rate_this_week=0.05, over_gate=False)
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[rollup]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=0):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = MagicMock()
                    mock_db.return_value.__enter__ = MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = MagicMock(return_value=False)
                    report = build_report()
        assert "⚠" not in report

    def test_uncaptured_footer_shown_when_nonzero(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch, MagicMock
        rollup = self._make_rollup()
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[rollup]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=2):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = MagicMock()
                    mock_db.return_value.__enter__ = MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = MagicMock(return_value=False)
                    report = build_report()
        assert "2" in report
        assert "captured" in report.lower() or "original" in report.lower()

    def test_uncaptured_footer_hidden_when_zero(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch, MagicMock
        rollup = self._make_rollup()
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[rollup]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=0):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = MagicMock()
                    mock_db.return_value.__enter__ = MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = MagicMock(return_value=False)
                    report = build_report()
        # should not mention uncaptured when 0
        assert "not captured" not in report and "uncaptured" not in report.lower()

    def test_top_categories_shown(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch, MagicMock
        rollup = self._make_rollup(top_categories=["numbers", "sign_off"])
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[rollup]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=0):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = MagicMock()
                    mock_db.return_value.__enter__ = MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = MagicMock(return_value=False)
                    report = build_report()
        assert "numbers" in report
        assert "sign_off" in report

    def test_no_approvals_unchanged_message(self):
        from src.tasks.fa_max_weekly_edit_rate_report import build_report
        from unittest.mock import patch, MagicMock
        with patch("src.tasks.fa_max_weekly_edit_rate_report.build_rollup", return_value=[]):
            with patch("src.tasks.fa_max_weekly_edit_rate_report.count_uncaptured", return_value=0):
                with patch("src.tasks.fa_max_weekly_edit_rate_report.get_db_context") as mock_db:
                    s = MagicMock()
                    mock_db.return_value.__enter__ = MagicMock(return_value=s)
                    mock_db.return_value.__exit__ = MagicMock(return_value=False)
                    report = build_report()
        assert "No FA Max human approvals" in report
