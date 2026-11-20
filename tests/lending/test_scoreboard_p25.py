"""P2.5: the closing scoreboard on a zero-dial day, its pipeline section and the Drive export. Database-free; the
Postgres reads of the pipeline are in test_scoreboard_pipeline.py."""
from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime, timezone

import pytest

from src.lending.scoreboard import (
    NO_CALLS,
    PIPELINE_UNAVAILABLE,
    Row,
    ScoreboardData,
    format_blocks,
    format_slack,
    pipeline_text,
)
from src.lending.scoreboard_export import COLUMNS, build_csv, export
from src.lending.scoreboard_pipeline import Blocker, PipelineData, StageCount

DAY = date(2026, 11, 16)
ZERO = Row("TOTAL", 0, 0, 0, 0, 0)


def board(total: Row = ZERO, pipeline: PipelineData | None = None, **kw) -> ScoreboardData:
    callers = kw.pop("by_caller", [])
    return ScoreboardData(by_caller=callers, by_campaign=kw.pop("by_campaign", []), by_hook=[], total=total,
                          pipeline=pipeline, **kw)


def pipe(blockers: int = 2) -> PipelineData:
    stages = [StageCount("application", "Application", 4), StageCount("submitted to lender", "Submitted to lender", 3),
              StageCount("funded", "Funded", 9), StageCount("lost", "Lost", 2)]
    owners = ["borrower", "lender", "title"]
    return PipelineData(by_stage=stages, blockers=[
        Blocker(first_name="Mark", phone_last4="0101", stage_name="Application", days=6 + i,
                owner=owners[i % 3], missing=2 if i == 0 else 0) for i in range(blockers)])


def _all_text(blocks: list[dict]) -> str:
    out = []
    for b in blocks:
        if "text" in b:
            out.append(b["text"]["text"])
        for e in b.get("elements", []):
            out.append(e["text"])
    return "\n".join(out)


# ── zero-dial day ────────────────────────────────────────────────────────────

def test_the_closing_board_posts_cleanly_on_a_day_with_no_calls():
    data = board(pipeline=pipe())
    text_ = format_slack(data, DAY)
    blocks = format_blocks(data, DAY)
    assert NO_CALLS in text_ and NO_CALLS in _all_text(blocks)
    assert "*0* dials" in _all_text(blocks) and "_no dials_" in text_  # no division error on 0 dials
    assert "*Pipeline* — 7 active deal(s), 7 open" in text_ and "*Pipeline* — 7 active" in _all_text(blocks)


def test_the_open_count_leaves_out_funded_lost_and_nurture_and_keeps_leads():
    p = PipelineData(by_stage=[StageCount("application", "Application", 2), StageCount("booked", "Booked", 5),
                               StageCount("funded", "Funded", 40), StageCount("lost", "Lost", 30),
                               StageCount("nurture", "Nurture", 20)])
    assert (p.active, p.open_deals) == (2, 7)
    assert "2 active deal(s), 7 open (funded, lost and nurture left out)" in pipeline_text(p)


def test_the_hourly_board_is_unchanged_no_zero_line_and_no_pipeline():
    hourly = datetime(2026, 11, 16, 15, 0, tzinfo=timezone.utc)
    data = board(pipeline=pipe())
    assert NO_CALLS not in format_slack(data, DAY, as_of=hourly)
    assert "Pipeline" not in _all_text(format_blocks(data, DAY, as_of=hourly))


def test_a_day_with_calls_has_no_zero_line():
    busy = Row("TOTAL", 12, 4, 0, 1, 0)
    data = board(total=busy, by_caller=[Row("Josh", 12, 4, 0, 1, 0)], pipeline=pipe())
    assert NO_CALLS not in format_slack(data, DAY) and NO_CALLS not in _all_text(format_blocks(data, DAY))


def test_a_board_without_pipeline_data_is_exactly_as_before():
    assert "Pipeline" not in format_slack(board(), DAY) and "Pipeline" not in _all_text(format_blocks(board(), DAY))


# ── the pipeline section ─────────────────────────────────────────────────────

def test_pipeline_section_shows_active_stages_and_stuck_deals_by_owner():
    text_ = pipeline_text(pipe(blockers=3))
    assert "Application 4 · Submitted to lender 3 · Funded 9 · Lost 2" in text_
    assert "*Stuck more than 3 days* — 3 (borrower 1 · lender 1 · closing (title) 1)" in text_
    assert "• Mark (…0101) · Application · 6 days · waiting on borrower · 2 document(s) missing" in text_
    assert "waiting on closing (title)" in text_  # P2.4's owner label, not the raw key


def test_no_stuck_deals_says_none_and_an_empty_pipeline_says_so():
    assert "*Stuck more than 3 days* — none" in pipeline_text(pipe(blockers=0))
    assert "_no deals in the pipeline_" in pipeline_text(PipelineData())


def test_an_unreadable_pipeline_is_marked_and_the_board_still_renders():
    data = board(pipeline=PipelineData(available=False))
    assert PIPELINE_UNAVAILABLE in format_slack(data, DAY) and PIPELINE_UNAVAILABLE in _all_text(format_blocks(data, DAY))


def test_more_than_ten_stuck_deals_are_listed_oldest_first_then_counted():
    text_ = pipeline_text(pipe(blockers=14))
    assert text_.count("• ") == 10 and "_… and 4 more_" in text_


def test_a_name_without_a_first_name_shows_borrower_and_markup_is_stripped():
    p = PipelineData(by_stage=[StageCount("application", "Application", 1)],
                     blockers=[Blocker(None, None, "App*lic`ation", 4, "borrower", 0),
                               Blocker("*Bob*", "0202", "Application", 5, "borrower", 0)])
    text_ = pipeline_text(p)
    assert "• Borrower · Applic'ation · 4 days" in text_ and "• Bob (…0202)" in text_


def test_the_pipeline_section_fits_one_slack_section_at_the_limits():
    long = "x" * 200
    p = PipelineData(by_stage=[StageCount(f"s{i}", long, 99) for i in range(12)],
                     blockers=[Blocker(long, "9999", long, 999, "title", 99) for _ in range(40)])
    assert len(pipeline_text(p)) < 3000


# ── the Drive CSV ────────────────────────────────────────────────────────────

def _csv_rows(data: ScoreboardData) -> list[dict]:
    return list(csv.DictReader(io.StringIO(build_csv(data, DAY).decode("utf-8"))))


def test_the_csv_holds_the_totals_breakdowns_and_pipeline_counts():
    busy = Row("TOTAL", 12, 4, 1, 2, 0, showed=1)
    data = board(total=busy, by_caller=[Row("Josh Kantor", 12, 4, 1, 2, 0, showed=1)],
                 by_campaign=[Row("Website", 12, 4, 1, 2, 0)], pipeline=pipe(blockers=3), booked_stage=3)
    rows = _csv_rows(data)
    assert list(rows[0]) == list(COLUMNS) and {r["day"] for r in rows} == {"2026-11-16"}
    got = {(r["section"], r["name"], r["metric"]): r["value"] for r in rows}
    assert got[("total", "TOTAL", "dials")] == "12" and got[("total", "TOTAL", "booked_stage")] == "3"
    assert got[("caller", "Josh Kantor", "live")] == "4" and got[("campaign", "Website", "booked")] == "2"
    assert got[("pipeline", "pipeline", "active_deals")] == "7" and got[("stage", "Funded", "deals")] == "9"
    assert got[("pipeline", "pipeline", "open_deals")] == "7" and ("pipeline", "pipeline", "total_deals") not in got
    assert got[("blockers", "blockers", "stuck_deals")] == "3" and got[("blockers", "lender", "stuck_deals")] == "1"


def test_the_csv_never_holds_a_borrower_name_phone_or_email():
    raw = build_csv(board(pipeline=pipe(blockers=5)), DAY).decode("utf-8")
    assert "Mark" not in raw and "0101" not in raw and "@" not in raw
    assert not re.search(r"\d{7,}", raw)


def test_a_zero_dial_day_exports_zeros_and_the_pipeline():
    got = {(r["section"], r["metric"]): r["value"] for r in _csv_rows(board(pipeline=pipe())) if r["name"] in
           ("TOTAL", "pipeline")}
    assert got[("total", "dials")] == "0" and got[("pipeline", "active_deals")] == "7"


def test_an_unreadable_pipeline_exports_only_its_flag():
    rows = [r for r in _csv_rows(board(pipeline=PipelineData(available=False))) if r["section"] != "total"]
    assert [(r["section"], r["metric"], r["value"]) for r in rows] == [("pipeline", "available", "0")]


def test_export_uploads_one_file_into_the_day_folder_and_needs_a_folder():
    calls = []
    assert export(board(), DAY, folder_id="F1", upload=lambda *a: calls.append(a)) == "scoreboard.csv"
    assert [(c[0], c[1], c[2]) for c in calls] == [("F1", "2026-11-16", "scoreboard.csv")]
    with pytest.raises(ValueError):
        export(board(), DAY, folder_id="", upload=lambda *a: None)



# ── review fixes ────────────────────────────────────────────────────────────

def test_a_borrower_typed_name_cannot_ping_the_channel_or_add_a_link():
    p = PipelineData(by_stage=[StageCount("application", "Application", 1)],
                     blockers=[Blocker("<!channel>", "0101", "<https://x.example|Click>", 5, "borrower", 0)])
    text_ = pipeline_text(p)
    assert "<!channel>" not in text_ and "<https" not in text_
    assert "&lt;!channel&gt;" in text_


def test_leads_are_not_counted_as_active_deals():
    p = PipelineData(by_stage=[StageCount("new lead", "New lead", 300), StageCount("booked", "Booked", 40),
                               StageCount("application", "Application", 4), StageCount("closed won", "Closed won", 2)])
    assert (p.active, p.open_deals) == (4, 346)  # leads are open, not active


def test_many_long_stages_and_escaped_names_still_fit_one_slack_section():
    hostile = "<" * 200
    p = PipelineData(by_stage=[StageCount(f"s{i}", hostile, 99) for i in range(60)],
                     blockers=[Blocker(hostile, "9999", hostile, 999, "title", 99) for _ in range(40)])
    text_ = pipeline_text(p)
    assert len(text_) <= 2900 and "_48 more stage(s)_" in text_


def test_free_text_names_in_the_csv_are_redacted():
    caller = Row("josh@example.com", 3, 1, 0, 0, 0)
    raw = build_csv(board(total=caller, by_caller=[caller, Row("call +1 (813) 555-0101", 1, 0, 0, 0, 0)]), DAY)
    assert b"@example.com" not in raw and b"555-0101" not in raw and b"[redacted]" in raw
