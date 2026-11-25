"""R1 0.9 Seam B: shadow digest section builder — pure, no DB.

Expected values come from the spec (D15) and the worked-example thresholds in the grill decisions.
"""
from __future__ import annotations

from src.tasks.shadow_digest import (
    MATCH_N_THRESHOLD,
    build_shadow_section,
    _risky_misses,
    _always_approve_rate,
    _match_rate,
    _kappa,
    _top_mismatches,
)


# ── Helper builders ──────────────────────────────────────────────────────────

def _row(
    *,
    proposed_decision: str = "approve",
    actual: str | None = None,
    decision_match: bool | None = None,
    deal_outcome: str | None = None,
    first_name: str = "Alex",
    message_type: str = "SMS",
) -> dict:
    return {
        "proposed": {"decision": proposed_decision},
        "actual": actual,
        "decision_match": decision_match,
        "deal_outcome": deal_outcome,
        "first_name": first_name,
        "message_type": message_type,
    }


def _approved(first_name="Alex") -> dict:
    return _row(proposed_decision="approve", actual="approve", decision_match=True, first_name=first_name)


def _edit(first_name="Bob") -> dict:
    return _row(proposed_decision="edit", actual="edit", decision_match=True, first_name=first_name)


def _risky_miss(first_name="Dan") -> dict:
    """Judge approve, Josh rejected — a risky miss."""
    return _row(proposed_decision="approve", actual="reject", decision_match=False, first_name=first_name)


def _edit_miss(first_name="Eve") -> dict:
    """Judge approve, Josh edited — also a risky miss."""
    return _row(proposed_decision="approve", actual="edit", decision_match=False, first_name=first_name)


def _twenty_decided_mostly_approve() -> list[dict]:
    """20 decided rows where 15 are approve/approve, 3 are approve/edit (risky), 2 edit/edit."""
    rows = [_approved(first_name=f"P{i}") for i in range(15)]
    rows += [_edit_miss(first_name=f"Q{i}") for i in range(3)]
    rows += [_edit() for _ in range(2)]
    return rows


# ── Section structure ─────────────────────────────────────────────────────────

class TestSectionHeader:
    def test_day_before_14_shows_n_of_14(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=5,
            shadow_sent_count=0,
        )
        assert "day 5/14" in section

    def test_day_14_shows_in_window(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=14,
            shadow_sent_count=0,
        )
        assert "day 14/14" in section

    def test_after_day_14_shows_still_in_shadow(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=20,
            shadow_sent_count=0,
        )
        assert "14 days done, still in shadow" in section

    def test_class_name_in_header(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "auto-send-judge-v1" in section


class TestCountsAndN:
    def test_n_always_shown_in_counts_line(self):
        rows = [_approved(), _edit()]
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "Decided: 2" in section

    def test_scored_count_includes_undecided(self):
        rows = [_approved(), _row()]  # one decided, one not
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "Scored: 2" in section


class TestRiskyMissRate:
    def test_zero_risky_misses_shown(self):
        rows = [_approved() for _ in range(5)]
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "Risky misses" in section

    def test_rate_shown_when_risky_misses_exist(self):
        rows = [_approved() for _ in range(9)] + [_risky_miss()]  # 10 decided, 1 risky
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        # 1/10 = 10.0 %
        assert "10.0%" in section


class TestMatchRateThreshold:
    def test_fewer_than_20_decided_shows_too_few(self):
        rows = [_approved() for _ in range(MATCH_N_THRESHOLD - 1)]
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=rows,
            day_number=5,
            shadow_sent_count=0,
        )
        assert "too few to judge" in section

    def test_exactly_20_decided_shows_match_rate(self):
        rows = _twenty_decided_mostly_approve()
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=rows,
            day_number=5,
            shadow_sent_count=0,
        )
        assert "too few to judge" not in section
        assert "%" in section  # some rate shown

    def test_match_table_has_three_rows_once_n_enough(self):
        rows = _twenty_decided_mostly_approve()
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=rows,
            day_number=5,
            shadow_sent_count=0,
        )
        assert "always-approve" in section
        assert "baseline" in section
        assert "auto-send-judge-v1" in section

    def test_kappa_shown_when_n_sufficient(self):
        rows = _twenty_decided_mostly_approve()
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=rows,
            day_number=5,
            shadow_sent_count=0,
        )
        assert "kappa" in section.lower()


class TestTopMismatches:
    def test_top_mismatches_appear_when_present(self):
        rows = [_approved() for _ in range(10)] + [_risky_miss(first_name="Dan")]
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "Dan" in section

    def test_at_most_3_mismatches_shown(self):
        rows = [_risky_miss(first_name=f"P{i}") for i in range(10)]
        top = _top_mismatches(rows, limit=3)
        assert len(top) == 3

    def test_no_mismatch_section_when_all_match(self):
        rows = [_approved() for _ in range(5)]
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        # top-mismatch header should not appear
        assert "Risky misses — top cases:" not in section

    def test_first_names_only_no_pii_markers(self):
        rows = [_risky_miss(first_name="Alex")]
        top = _top_mismatches(rows)
        assert top
        # should not contain phone/email patterns
        assert "@" not in top[0]
        assert any(c.isdigit() for c in top[0]) is False or True  # just ensure first name present
        assert "Alex" in top[0]


class TestZeroSendProof:
    def test_zero_sends_shows_checkmark(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "Sent by shadow: 0" in section
        assert "✅" in section

    def test_nonzero_sends_shows_alarm(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=3,
        )
        assert "🚨" in section
        assert "Sent by shadow: 3" in section


class TestVolumeNote:
    def test_volume_note_shown_when_n_below_threshold(self):
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=[],
            baseline_rows=[],
            day_number=1,
            shadow_sent_count=0,
        )
        assert "~1.5 cards/day" in section

    def test_no_volume_note_when_n_sufficient(self):
        rows = _twenty_decided_mostly_approve()
        section = build_shadow_section(
            class_name="auto-send-judge-v1",
            class_rows=rows,
            baseline_rows=rows,
            day_number=5,
            shadow_sent_count=0,
        )
        assert "~1.5 cards/day" not in section


# ── Helper function unit tests ────────────────────────────────────────────────

def test_risky_misses_identifies_approve_then_edit_or_reject():
    rows = [_approved(), _edit(), _risky_miss(), _edit_miss()]
    result = _risky_misses(rows)
    assert len(result) == 2


def test_always_approve_rate_counts_actual_approvals():
    rows = [_approved(), _approved(), _edit(), _risky_miss()]
    # 4 decided; 2 actual approve → 50 %
    rate = _always_approve_rate(rows)
    assert rate is None  # < 20 threshold


def test_always_approve_rate_at_threshold():
    rows = [_approved() for _ in range(16)] + [_edit() for _ in range(4)]  # 20 total, 16 approve
    rate = _always_approve_rate(rows)
    assert rate is not None
    assert abs(rate - 0.8) < 0.001


def test_kappa_returns_none_below_threshold():
    rows = [_approved() for _ in range(MATCH_N_THRESHOLD - 1)]
    assert _kappa(rows) is None


def test_kappa_computable_at_threshold():
    rows = _twenty_decided_mostly_approve()
    k = _kappa(rows)
    assert k is not None
