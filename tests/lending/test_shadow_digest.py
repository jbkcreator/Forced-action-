"""R1 0.9 Seam B: the shadow digest section builder, from plain rows to text. Expected values are worked examples."""
from __future__ import annotations

from datetime import date, datetime, timezone

from src.tasks.shadow_digest import MATCH_N_THRESHOLD, _kappa, _top_mismatches, build_shadow_section

REPORT_DATE = date(2026, 11, 20)
YESTERDAY_NOON = datetime(2026, 11, 19, 17, 0, tzinfo=timezone.utc)  # 12:00 ET on the 19th
EARLIER = datetime(2026, 11, 10, 17, 0, tzinfo=timezone.utc)
LATE_YESTERDAY_ET = datetime(2026, 11, 20, 3, 30, tzinfo=timezone.utc)  # 22:30 ET on the 19th


def _row(proposed="approve", actual=None, *, first_name="Alex", last4="1234", channel="ghl_sms",
         created_at=EARLIER, decided_at=None, deal_outcome=None, outcome_at=None) -> dict:
    match = None if actual in (None, "no_decision") else proposed == actual
    return {"proposed": {"decision": proposed}, "actual": actual, "decision_match": match,
            "first_name": first_name, "last4": last4, "channel": channel, "created_at": created_at,
            "decided_at": decided_at or (created_at if actual in ("approve", "edit", "reject") else None),
            "deal_outcome": deal_outcome, "outcome_at": outcome_at}


def _section(class_rows=(), baseline_rows=(), day_number=5, shadow_sent_count=0) -> str:
    return build_shadow_section(class_name="auto-send-judge-v1", class_rows=list(class_rows),
                                baseline_rows=list(baseline_rows), day_number=day_number,
                                shadow_sent_count=shadow_sent_count, report_date=REPORT_DATE)


def _twenty_decided() -> list[dict]:
    """15 approve/approve, 3 approve/edit (risky), 2 edit/edit."""
    return ([_row("approve", "approve") for _ in range(15)] + [_row("approve", "edit") for _ in range(3)]
            + [_row("edit", "edit") for _ in range(2)])


class TestHeader:
    def test_day_n_of_14(self):
        assert "day 5/14" in _section(day_number=5)

    def test_day_14_still_counts_as_in_window(self):
        assert "day 14/14" in _section(day_number=14)

    def test_after_day_14_says_still_in_shadow(self):
        assert "14 days done, still in shadow" in _section(day_number=20)


class TestCounts:
    def test_cumulative_counts_split_decided_no_decision_and_unchecked(self):
        rows = [_row("approve", "approve"), _row("approve", "no_decision"), _row("approve", None)]
        assert "Since start: scored 3  |  decided 1  |  no decision 1  |  not yet checked 1" in _section(rows)

    def test_yesterday_counts_use_the_eastern_day(self):
        rows = [_row("approve", "approve", created_at=YESTERDAY_NOON),
                _row("approve", "edit", created_at=EARLIER, decided_at=LATE_YESTERDAY_ET),
                _row("approve", "approve", created_at=EARLIER)]
        assert "Yesterday: scored 1  |  decided 2" in _section(rows)


class TestRiskyMisses:
    def test_rate_over_decided_rows_only(self):
        rows = [_row("approve", "approve") for _ in range(9)] + [_row("approve", "reject"), _row("approve", "no_decision")]
        assert "10.0% (1/10)" in _section(rows)

    def test_no_decided_rows_shows_n_zero(self):
        assert "— (n=0)" in _section([_row("approve", None)])


class TestMatchTable:
    def test_under_threshold_says_too_few(self):
        section = _section([_row("approve", "approve") for _ in range(MATCH_N_THRESHOLD - 1)])
        assert "too few to judge (need 20 decided, have 19)" in section
        assert "~1.5 cards/day" in section

    def test_at_threshold_shows_three_rows_and_no_volume_note(self):
        section = _section(_twenty_decided(), baseline_rows=_twenty_decided())
        assert "always-approve (ref)" in section and "baseline" in section
        assert "too few to judge" not in section
        assert "~1.5 cards/day" not in section

    def test_always_approve_and_class_match_rates(self):
        section = _section(_twenty_decided(), baseline_rows=_twenty_decided())
        assert "75.0%" in section  # always-approve: 15 of 20 actual approvals
        assert "85.0%" in section  # v1: 17 of 20 matched

    def test_baseline_row_shows_its_own_n(self):
        baseline = _twenty_decided() + [_row("approve", "approve") for _ in range(5)]
        baseline_line = next(line for line in _section(_twenty_decided(), baseline).splitlines()
                             if line.startswith("baseline"))
        assert baseline_line.rstrip().endswith("25")


class TestKappa:
    def test_judge_that_always_approves_has_kappa_zero(self):
        rows = [_row("approve", "approve") for _ in range(15)] + [_row("approve", "edit") for _ in range(5)]
        assert _kappa(rows) == 0.0

    def test_perfect_agreement_over_two_classes_is_one(self):
        rows = [_row("approve", "approve") for _ in range(10)] + [_row("edit", "edit") for _ in range(10)]
        assert _kappa(rows) == 1.0

    def test_none_under_threshold(self):
        assert _kappa([_row("approve", "approve")] * (MATCH_N_THRESHOLD - 1)) is None


class TestTopMismatches:
    def test_first_name_last4_plain_type_and_both_decisions(self):
        lines = _top_mismatches([_row("approve", "reject", first_name="Dan", last4="9876", channel="ghl_email")])
        assert lines == ["Dan …9876 · email · judge: approve / Josh: reject"]

    def test_risky_misses_listed_before_other_mismatches(self):
        rows = [_row("edit", "approve", first_name="Other"), _row("approve", "edit", first_name="Risky")]
        assert _top_mismatches(rows)[0].startswith("Risky")

    def test_at_most_three(self):
        assert len(_top_mismatches([_row("approve", "reject") for _ in range(10)])) == 3

    def test_no_mismatch_block_when_all_match(self):
        assert "Top mismatches" not in _section([_row("approve", "approve") for _ in range(5)])

    def test_unknown_name_without_phone(self):
        assert _top_mismatches([_row("approve", "edit", first_name=None, last4=None)])[0].startswith("Unknown · text")


class TestDealOutcomes:
    def test_only_outcomes_settled_yesterday(self):
        rows = [_row("approve", "approve", deal_outcome="funded", outcome_at=YESTERDAY_NOON),
                _row("approve", "approve", deal_outcome="declined", outcome_at=YESTERDAY_NOON),
                _row("approve", "approve", deal_outcome="funded", outcome_at=EARLIER)]
        assert "Deals settled yesterday on judged cards: funded 1  |  declined 1" in _section(rows)

    def test_no_line_without_settled_outcomes(self):
        assert "Deals settled" not in _section([_row("approve", "approve", deal_outcome="funded", outcome_at=EARLIER)])


class TestZeroSends:
    def test_zero(self):
        assert "✅ Sent by shadow: 0" in _section()

    def test_nonzero_alarms(self):
        assert "🚨 Sent by shadow: 3" in _section(shadow_sent_count=3)
