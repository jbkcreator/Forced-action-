"""
WP-T3-2 — Weekly Edit Log.

Coverage:
  1. Pure functions (no DB): token_change_ratio, is_material_edit, word_diff,
     categorize, iso_week_bounds.
  2. Report rendering (no DB): build_report over given rollups.
  3. Shared-DB (fresh_db, rolled back): seeded rows across 2 agents in a fixed
     historical week — rollup rate == get_weekly_edit_rate, categories,
     uncaptured count, ⚠ only for the agent over the Tier B gate.
  4. Shared-DB capture fix: a row queued without original_draft, then revised
     through the Slack Revise handler, gets its pre-revision body captured and
     material_edit measured against that body.

Run: pytest tests/test_fa_max_edit_log.py -v
"""
from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

from src.services.fa_max_autonomy import EditCounts, get_weekly_edit_rate, iso_week_bounds
from src.services.fa_max_edit_log import (
    MATERIAL_EDIT_THRESHOLD,
    AgentRollup,
    EditLogEntry,
    build_rollup,
    categorize,
    count_uncaptured,
    get_edit_log,
    is_material_edit,
    token_change_ratio,
    word_diff,
)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Pure functions
# ─────────────────────────────────────────────────────────────────────────────

class TestTokenChangeRatio:
    def test_identical_texts_return_zero(self):
        assert token_change_ratio("hello world", "hello world") == 0.0

    def test_completely_different_texts_return_one(self):
        assert token_change_ratio("hello world", "foo bar") == 1.0

    def test_partial_change(self):
        assert token_change_ratio("hello world", "hello earth") == pytest.approx(2 / 3)

    def test_empty_both_zero(self):
        assert token_change_ratio("", "") == 0.0

    def test_case_and_punctuation_ignored(self):
        assert token_change_ratio("Hello, World!", "hello world") == 0.0


class TestIsMaterialEdit:
    def test_threshold_is_015(self):
        assert MATERIAL_EDIT_THRESHOLD == 0.15

    def test_identical_not_material(self):
        assert not is_material_edit("hello world", "hello world")

    def test_changed_rate_is_material(self):
        assert is_material_edit("Hi Mike, rates are 7.2%", "Hi Mike, rates start at 6.9% for your flip")

    def test_one_word_in_long_text_not_material(self):
        old = "Hi Mike, I wanted to follow up on the property deal we discussed last week"
        new = "Hi Mike, I wanted to follow up on the property deal we explored last week"
        assert not is_material_edit(old, new)

    def test_empty_baseline_is_material(self):
        assert is_material_edit("", "Hi Mike how are you doing")

    def test_admin_router_uses_the_same_function(self):
        from src.api import admin_router
        assert admin_router._is_material_edit is is_material_edit


class TestWordDiff:
    def test_identical_texts_no_markers(self):
        assert word_diff("hello world", "hello world") == "hello world"

    def test_deletion(self):
        assert word_diff("hello world foo", "hello world") == "hello world [-foo-]"

    def test_addition(self):
        assert word_diff("hello world", "hello world bar") == "hello world {+bar+}"

    def test_replacement(self):
        assert word_diff("rates are 7.2%", "rates start at 6.9%") == (
            "rates [-are-] [-7.2%-] {+start+} {+at+} {+6.9%+}"
        )

    def test_empty_sides(self):
        assert word_diff("", "hi there") == "{+hi+} {+there+}"
        assert word_diff("hi there", "") == "[-hi-] [-there-]"


class TestCategorize:
    def test_no_change_is_wording(self):
        assert categorize("hello world", "hello world") == ["wording"]

    def test_changed_rate_is_numbers(self):
        assert "numbers" in categorize("rate is 7.2%", "rate is 6.9%")

    def test_changed_amount_is_numbers(self):
        assert "numbers" in categorize("loan is $500,000", "loan is $450,000")

    def test_removed_comma_in_prose_is_not_numbers(self):
        assert "numbers" not in categorize("Hi Mike, rates are good", "Hi Mike rates are good")

    def test_reformatted_same_amount_is_not_numbers(self):
        assert "numbers" not in categorize("loan of 5,000 today", "loan of 5000 today")

    def test_changed_url_is_links(self):
        assert "links" in categorize("see https://old.com now", "see https://new.com now")

    def test_opening_changed(self):
        assert "opening" in categorize("Hi Mike,\nhope you are well", "Hello Mike,\nhope you are well")

    def test_sign_off_changed(self):
        old = "Hi Mike, let me know if you have questions.\nBest, Josh"
        new = "Hi Mike, let me know if you have questions.\nThanks, Josh"
        assert "sign_off" in categorize(old, new)

    def test_single_line_edit_has_no_opening_or_sign_off(self):
        result = categorize("Running 10 min late", "Running a bit late")
        assert "opening" not in result and "sign_off" not in result

    def test_single_line_wording_edit_is_wording(self):
        assert categorize("I think the deal looks good", "I believe the deal seems good") == ["wording"]

    def test_shortened(self):
        assert "shortened" in categorize(" ".join(["word"] * 10), " ".join(["word"] * 7))

    def test_lengthened(self):
        assert "lengthened" in categorize(" ".join(["word"] * 10), " ".join(["word"] * 13))

    def test_multi_label_in_fixed_order(self):
        old = "Hi Mike,\nrate is 7%, see https://old.com\nBest, Josh"
        new = "Hi Mike,\nrate is 6%, see https://new.com\nThanks, Josh"
        assert categorize(old, new) == ["numbers", "links", "sign_off"]

    def test_middle_line_wording_only(self):
        old = "Hi Mike,\nI think the deal looks quite promising.\nBest, Josh"
        new = "Hi Mike,\nI believe the deal seems rather interesting.\nBest, Josh"
        assert categorize(old, new) == ["wording"]


class TestIsoWeekBounds:
    def test_week_starts_monday_midnight_eastern(self):
        start, end = iso_week_bounds(datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc))  # Thu
        assert start == datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc)  # Mon 00:00 EDT
        assert end == datetime(2026, 9, 28, 4, 0, tzinfo=timezone.utc)

    def test_week_across_dst_end_is_monday_to_monday(self):
        start, end = iso_week_bounds(datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc))
        assert start == datetime(2026, 10, 26, 4, 0, tzinfo=timezone.utc)  # EDT
        assert end == datetime(2026, 11, 2, 5, 0, tzinfo=timezone.utc)  # EST

    def test_weeks_back(self):
        now = datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc)
        start, end = iso_week_bounds(now, weeks_back=4)
        assert start == datetime(2026, 8, 24, 4, 0, tzinfo=timezone.utc)
        assert end == datetime(2026, 8, 31, 4, 0, tzinfo=timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Report rendering (no DB)
# ─────────────────────────────────────────────────────────────────────────────

_REPORT_NOW = datetime(2026, 9, 25, 14, 0, tzinfo=timezone.utc)


def _entry(**overrides) -> EditLogEntry:
    base = dict(
        item_id=812, agent_name="stage_monitor", tier="A", channel="email",
        original_draft="Hi Mike, rates are 7.2%", final_text="Hi Mike, rates start at 6.9%",
        diff="Hi Mike, rates [-are-] [-7.2%-] {+start+} {+at+} {+6.9%+}",
        change_ratio=0.41, categories=["numbers"], material=True, revision_count=1,
        last_revised_at=None, decided_at=None, status="sent",
    )
    base.update(overrides)
    return EditLogEntry(**base)


def _rollup(**overrides) -> AgentRollup:
    base = dict(
        agent_name="stage_monitor", tier="A",
        this_week=EditCounts(material=2, revised=3, decided=14),
        prior_4w=EditCounts(material=3, revised=4, decided=50),
        top_categories=[("numbers", 2), ("sign_off", 1)],
        biggest_edit=_entry(),
    )
    base.update(overrides)
    return AgentRollup(**base)


@pytest.fixture
def render():
    """Render build_report over the given rollups and uncaptured count."""
    def _render(rollups, uncaptured=0):
        from src.tasks import fa_max_weekly_edit_rate_report as report
        with patch.object(report, "get_db_context") as ctx, \
                patch.object(report, "build_rollup", return_value=rollups), \
                patch.object(report, "count_uncaptured", return_value=uncaptured):
            ctx.return_value.__enter__.return_value = MagicMock()
            return report.build_report(now=_REPORT_NOW)
    return _render


class TestWeeklyReportRendering:
    def test_line_matches_spec_example(self, render):
        report = render([_rollup()])
        assert "*FA Max weekly edit log* — week of Sep 21" in report
        assert "• `stage_monitor` (tier A) — 14.3% ⚠ (prior 4w 6.0% ↑) · 2 material / 3 edited / 14 decided" in report
        assert "    top: numbers ×2, sign_off ×1" in report
        assert '    biggest: #812 (41% changed) "Hi Mike, rates [-are-]' in report

    def test_no_prior_data_shows_dash(self, render):
        report = render([_rollup(prior_4w=None, this_week=EditCounts(0, 0, 5),
                                 top_categories=[], biggest_edit=None)])
        assert "0.0% (prior 4w — ) · 0 material / 0 edited / 5 decided" in report
        assert "top:" not in report and "biggest:" not in report

    def test_warning_only_for_agent_over_gate(self, render):
        report = render([
            _rollup(agent_name="hot", this_week=EditCounts(1, 1, 10)),   # 10% = gate
            _rollup(agent_name="cool", this_week=EditCounts(0, 1, 11)),  # 0%
        ])
        hot, cool = [ln for ln in report.splitlines() if ln.startswith("• ")]
        assert "⚠" in hot and "⚠" not in cool

    def test_flat_trend_arrow(self, render):
        report = render([_rollup(this_week=EditCounts(1, 1, 20), prior_4w=EditCounts(2, 2, 40))])
        assert "(prior 4w 5.0% →)" in report

    def test_snippet_collapses_newlines_and_truncates(self, render):
        long_diff = "line one\nline two " + "x " * 400
        report = render([_rollup(biggest_edit=_entry(diff=long_diff))])
        biggest = next(ln for ln in report.splitlines() if "biggest:" in ln)
        snippet = biggest.split('"')[1]
        assert "\n" not in snippet and snippet.startswith("line one line two")
        assert len(snippet) == 300 and snippet.endswith("…")

    def test_uncaptured_footer(self, render):
        assert "_2 revised drafts this week had no captured original (pre-fix rows)._" in render([_rollup()], 2)
        assert "no captured original" not in render([_rollup()], 0)

    def test_no_approvals_message_unchanged(self, render):
        assert render([]) == "No FA Max human approvals recorded yet this week."


# ─────────────────────────────────────────────────────────────────────────────
# 3/4. Shared DB (fresh_db — one transaction, always rolled back)
# ─────────────────────────────────────────────────────────────────────────────

# A historical week no live row can fall in, so seeded rows are the only data.
_SEED_NOW = datetime(2020, 3, 5, 15, 0, tzinfo=timezone.utc)  # Thu
_SEED_WEEK_START, _SEED_WEEK_END = iso_week_bounds(_SEED_NOW)


def _relay_queue_available(db) -> bool:
    try:
        db.execute(text(
            "SELECT original_draft, final_content, material_edit, revision_count "
            "FROM relay_approval_queue LIMIT 0"
        ))
        db.execute(text("SELECT 1 FROM ventures WHERE venture_key = 'fa_max_lending'")).one()
        return True
    except Exception:
        db.rollback()
        return False


@pytest.fixture
def queue_db(fresh_db):
    if not _relay_queue_available(fresh_db):
        pytest.skip("relay_approval_queue with WP-T2-2 columns / fa_max_lending venture not available")
    return fresh_db


@pytest.fixture
def person_id(queue_db):
    """fa_max_lending queue rows must name a person (ck_relay_fa_max_governance_fields)."""
    return queue_db.execute(
        text("INSERT INTO fa_max_persons (source) VALUES ('wp_t3_2_test') RETURNING person_id")
    ).scalar_one()


def _seed(db, person, *, agent, tier="A", status="sent", decided_at=None, original=None, final=None,
          body=None, revisions=0, material=None, decided_by="slack:U_JOSH") -> int:
    return db.execute(
        text(
            "INSERT INTO relay_approval_queue "
            "(idempotency_key, venture_key, channel, recipient, payload, status, agent_name, "
            " autonomy_tier_at_send, decided_by, decided_at, original_draft, final_content, "
            " revision_count, material_edit, lane, person_id) "
            "VALUES (:k, 'fa_max_lending', 'email', 'test@example.com', CAST(:payload AS jsonb), "
            " :status, :agent, :tier, :decided_by, :decided_at, :original, :final, :revisions, :material, "
            " 'RELATIONSHIPS', :person) "
            "RETURNING id"
        ),
        {
            "k": f"wp-t3-2-test-{uuid.uuid4()}",
            "payload": json.dumps({"subject": "s", "body": body if body is not None else (final or original or "")}),
            "status": status, "agent": agent, "tier": tier,
            "decided_by": decided_by if decided_at else None, "decided_at": decided_at,
            "original": original, "final": final, "revisions": revisions, "material": material,
            "person": person,
        },
    ).scalar_one()


@pytest.fixture
def seeded(queue_db, person_id):
    """Two agents in the seed week, plus prior-week and out-of-population rows.

    hot  (tier A): 10 decided — 2 material edits (numbers; numbers+sign_off),
                   1 minor edit, 1 uncaptured revised row, 6 unedited → 20% ⚠
    cool (tier A): 10 decided — 1 minor edit, 9 unedited → 0%
    """
    suffix = uuid.uuid4().hex[:8]
    hot, cool = f"t32_hot_{suffix}", f"t32_cool_{suffix}"
    day = _SEED_WEEK_START + timedelta(days=1, hours=15)

    _seed(queue_db, person_id, agent=hot, decided_at=day, revisions=1, material=True,
          original="Hi Mike,\nRates are 7.2% on this flip.\nBest, Josh",
          final="Hi Mike,\nRates start at 6.9% for your flip.\nBest, Josh")
    _seed(queue_db, person_id, agent=hot, decided_at=day + timedelta(hours=1), revisions=2, material=True,
          original="Hi Ann,\nLoan of $300,000 approved.\nBest, Josh",
          final="Hi Ann,\nLoan of $250,000 is approved pending title.\nThanks, Josh")
    _seed(queue_db, person_id, agent=hot, decided_at=day + timedelta(hours=2), revisions=1, material=False,
          original="Hi Sam,\nFollowing up on the duplex deal we discussed last week at the office.\nBest, Josh",
          final="Hi Sam,\nFollowing up on the duplex deal we covered last week at the office.\nBest, Josh")
    _seed(queue_db, person_id, agent=hot, decided_at=day + timedelta(hours=3), revisions=1, material=False,
          original=None, final="revised before the capture fix")
    for i in range(6):
        _seed(queue_db, person_id, agent=hot, decided_at=day + timedelta(hours=4 + i),
              original="unedited draft", final=None)
    _seed(queue_db, person_id, agent=cool, decided_at=day, revisions=1, material=False,
          original="Hi Jo,\nThanks for the intro to the builder on Oak Street last month.\nBest, Josh",
          final="Hi Jo,\nThanks for the intro to the builder on Oak Street last month!\nBest, Josh")
    for i in range(9):
        _seed(queue_db, person_id, agent=cool, decided_at=day + timedelta(hours=1 + i), original="unedited", final=None)

    # Prior 4 weeks: hot had 1 material in 4 decided (25%); cool had none.
    prior_day = _SEED_WEEK_START - timedelta(days=10)
    _seed(queue_db, person_id, agent=hot, decided_at=prior_day, revisions=1, material=True, original="a b c", final="x y z")
    for i in range(3):
        _seed(queue_db, person_id, agent=hot, decided_at=prior_day + timedelta(hours=i + 1), original="d", final=None)

    # Outside the population: autonomous decision, still-pending row, later week.
    _seed(queue_db, person_id, agent=hot, decided_at=day, material=True, revisions=1, original="a", final="b",
          decided_by="system:autonomous:A")
    _seed(queue_db, person_id, agent=hot, status="pending", material=True, revisions=1, original="a", final="b")
    _seed(queue_db, person_id, agent=hot, decided_at=_SEED_WEEK_END + timedelta(days=1), material=True,
          revisions=1, original="a", final="b")
    return queue_db, hot, cool


class TestRollupAgainstSharedDb:
    def _by_agent(self, db):
        return {r.agent_name: r for r in build_rollup(db, now=_SEED_NOW)}

    def test_rollup_rate_equals_gate_weekly_rate(self, seeded):
        db, hot, cool = seeded
        rollups = self._by_agent(db)
        for agent in (hot, cool):
            gate_rate = get_weekly_edit_rate(agent, "A", db, week_start=_SEED_WEEK_START, week_end=_SEED_WEEK_END)
            assert rollups[agent].rate_this_week == gate_rate
        assert rollups[hot].rate_this_week == pytest.approx(0.2)
        assert rollups[cool].rate_this_week == 0.0

    def test_counts_exclude_autonomous_pending_and_other_weeks(self, seeded):
        db, hot, cool = seeded
        rollups = self._by_agent(db)
        assert rollups[hot].this_week == EditCounts(material=2, revised=4, decided=10)
        assert rollups[cool].this_week == EditCounts(material=0, revised=1, decided=10)

    def test_prior_four_weeks(self, seeded):
        db, hot, cool = seeded
        rollups = self._by_agent(db)
        assert rollups[hot].rate_prior_4w == pytest.approx(0.25)
        assert rollups[cool].prior_4w is None

    def test_categories_and_biggest_edit(self, seeded):
        db, hot, _ = seeded
        hot_rollup = self._by_agent(db)[hot]
        assert dict(hot_rollup.top_categories) == {"numbers": 2, "sign_off": 1, "wording": 1}
        assert hot_rollup.top_categories[0] == ("numbers", 2)
        assert hot_rollup.biggest_edit.material

    def test_edit_log_excludes_uncaptured_and_counts_them(self, seeded):
        db, hot, _ = seeded
        entries = get_edit_log(db, window_start=_SEED_WEEK_START, window_end=_SEED_WEEK_END, agent_name=hot)
        assert len(entries) == 3
        assert all(e.original_draft for e in entries)
        assert count_uncaptured(db, window_start=_SEED_WEEK_START, window_end=_SEED_WEEK_END) == 1

    def test_report_warns_only_for_agent_over_gate(self, seeded):
        db, hot, cool = seeded
        from src.tasks import fa_max_weekly_edit_rate_report as report

        @contextmanager
        def _same_session():
            yield db

        with patch.object(report, "get_db_context", _same_session):
            body = report.build_report(now=_SEED_NOW)
        lines = {ln.split("`")[1]: ln for ln in body.splitlines() if ln.startswith("• ")}
        assert "⚠" in lines[hot] and "(prior 4w 25.0% ↓)" in lines[hot]
        assert "⚠" not in lines[cool] and "(prior 4w — )" in lines[cool]
        assert "_1 revised draft this week had no captured original (pre-fix rows)._" in body


class TestCaptureFixThroughReviseHandler:
    """A row queued by a raw-INSERT path (no original_draft), revised through
    the Slack Revise modal handler, keeps its pre-revision body as the original
    and has material_edit measured against it — not against ''."""

    def _submit_revision(self, db, item_id: int, new_text: str) -> dict:
        from src.api import admin_router

        @contextmanager
        def _same_session():
            yield db

        payload = {
            "user": {"id": "U_JOSH"},
            "view": {
                "private_metadata": json.dumps({"item_id": item_id}),
                "state": {"values": {"revised_content_block": {"revised_content": {"value": new_text}}}},
            },
        }
        with patch("src.services.relay.queue.get_db_context", _same_session), \
                patch.object(admin_router, "_relay_approver_authorized", return_value=True), \
                patch.object(admin_router, "_post_relay_thread_note"), \
                patch("src.services.relay.slack_post.refresh_card_after_revision", return_value=True):
            return admin_router._handle_relay_revise_submission(payload)

    def _row(self, db, item_id: int):
        return db.execute(
            text("SELECT original_draft, final_content, material_edit, revision_count, payload "
                 "FROM relay_approval_queue WHERE id = :id"),
            {"id": item_id},
        ).mappings().one()

    def test_small_edit_to_raw_insert_row_is_not_material(self, queue_db, person_id):
        body = "Hi Mike,\nFollowing up on the duplex deal we discussed last week at the office.\nBest, Josh"
        revised = "Hi Mike,\nFollowing up on the duplex deal we covered last week at the office.\nBest, Josh"
        item_id = _seed(queue_db, person_id, agent="stage_monitor_test", status="pending", body=body)

        assert self._submit_revision(queue_db, item_id, revised) == {"response_action": "clear"}

        row = self._row(queue_db, item_id)
        assert row["original_draft"] == body
        assert row["final_content"] == revised
        assert row["payload"]["body"] == revised
        assert row["revision_count"] == 1
        assert row["material_edit"] is False

    def test_second_revision_keeps_first_captured_original(self, queue_db, person_id):
        body = "Hi Ann,\nLoan of $300,000 approved.\nBest, Josh"
        item_id = _seed(queue_db, person_id, agent="stage_monitor_test", status="pending", body=body)

        self._submit_revision(queue_db, item_id, "Hi Ann,\nLoan of $300,000 approved!\nBest, Josh")
        self._submit_revision(queue_db, item_id, "Hi Ann,\nLoan of $250,000 is approved pending title.\nThanks, Josh")

        row = self._row(queue_db, item_id)
        assert row["original_draft"] == body
        assert row["revision_count"] == 2
        assert row["material_edit"] is True
