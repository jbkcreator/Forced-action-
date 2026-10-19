"""Split T-27 / Runbook §8 Test 2: real lender matrix -> 3 qualifying profiles with ranked lenders,
a 4th non-qualifying profile with structured failure codes. Placeholders never shown as confirmed."""
from __future__ import annotations

from dataclasses import replace

import pytest

from config.lender_matrix import LENDER_MATRIX
from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK
from src.lending.prequal_acceptance import (
    COST_BASIS,
    SUBJECT_TO_CONFIRMATION,
    TEST2_PROFILES,
    evaluate_profile,
)


def _outcomes():
    return {p.name: evaluate_profile(p, matrix=LENDER_MATRIX) for p in TEST2_PROFILES}


def test_four_profiles_three_qualify_one_does_not():
    outcomes = _outcomes()
    assert len(outcomes) == 4
    assert [o.qualifies for o in outcomes.values()] == [True, True, True, False]


def test_qualifying_profiles_rank_fitting_lenders_and_mark_every_placeholder():
    for outcome in list(_outcomes().values())[:3]:
        assert outcome.limits and outcome.ranking
        assert [r["rank"] for r in outcome.ranking] == list(range(1, len(outcome.ranking) + 1))
        for row in outcome.ranking:
            assert row["status"] == SUBJECT_TO_CONFIRMATION  # every lender is unverified today
            assert row["cost_basis"] == COST_BASIS


def test_backflip_ranks_first_when_it_fits():
    """Josh A2: Backflip rows first whenever they fit, then cheapest (unknown cost last)."""
    for outcome in list(_outcomes().values())[:3]:
        assert outcome.ranking[0]["lender_key"].startswith("generic_backflip")


def test_non_qualifying_profile_outputs_structured_failure_codes_and_no_letter():
    outcome = list(_outcomes().values())[3]
    assert outcome.qualifies is False and outcome.limits == [] and outcome.ranking == []
    assert outcome.failure_codes and all(isinstance(c, str) and c == c.lower() for c in outcome.failure_codes)
    assert "credit_below_min" in outcome.failure_codes
    assert set(outcome.failure_codes_by_lender) == {r.key for r in LENDER_MATRIX}


def test_a_confirmed_lender_is_not_marked_subject_to_confirmation():
    confirmed = tuple(replace(r, verified=True, field_status={}) for r in LENDER_MATRIX)
    outcome = evaluate_profile(TEST2_PROFILES[0], matrix=confirmed)
    assert outcome.ranking and all(row["status"] == "confirmed" for row in outcome.ranking)


def test_watermark_text_is_the_runbook_string_exactly():
    assert NON_BINDING_PREQUAL_WATERMARK == "NON-BINDING PRE-QUALIFICATION ESTIMATE — FOR INFORMATIONAL PURPOSES ONLY"


def test_demo_writes_three_watermarked_pdfs_and_a_json_report(tmp_path):
    import importlib.util
    import json
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "scripts" / "lending_prequal_demo.py"
    spec = importlib.util.spec_from_file_location("lending_prequal_demo", path)
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)

    demo.main(["--out", str(tmp_path)])
    pdfs = sorted(p.name for p in tmp_path.glob("*.pdf"))
    assert len(pdfs) == 3 and not any(p.startswith("4_") for p in pdfs)
    report = json.loads((tmp_path / "test2_report.json").read_text(encoding="utf-8"))
    assert [r["qualifies"] for r in report] == [True, True, True, False]
    assert report[3]["failure_codes"] and report[0]["ranking"]
